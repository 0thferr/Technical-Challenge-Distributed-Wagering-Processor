import type { Message } from "@aws-sdk/client-sqs";
import { Injectable } from "@nestjs/common";
import { sha256 } from "../../application/use-cases/canonical-payload";
import { MetricsService } from "../observability/metrics.service";
import { StructuredLogger } from "../observability/structured-logger";
import { DeadLetterMessageRecorder } from "./dead-letter-message-recorder";
import {
  DEAD_LETTER_FAILURE_TYPES,
  type DeadLetterFailureType,
} from "./dead-letter-types";
import { SqsQueueService } from "./sqs-queue.service";

interface ParsedDeadLetter {
  payload: Record<string, unknown> | null;
  messageId?: string;
  eventId?: string;
}

const WAGER_PAYLOAD_FIELDS = [
  "providerId",
  "externalTransactionId",
  "playerId",
  "walletId",
  "roundId",
  "gameId",
  "kind",
  "money",
  "referenceExternalTransactionId",
  "idempotencyKey",
] as const;
const MONEY_PAYLOAD_FIELDS = ["amount", "currency"] as const;

@Injectable()
export class DeadLetterQueueConsumer {
  constructor(
    private readonly queue: SqsQueueService,
    private readonly recorder: DeadLetterMessageRecorder,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  async runUntilStopped(isStopping: () => boolean): Promise<void> {
    while (!isStopping()) {
      try {
        const messages = await this.queue.receiveDeadLetters();
        for (let index = 0; index < messages.length; index += 1) {
          if (isStopping()) {
            await this.returnUnprocessedMessages(messages.slice(index));
            break;
          }
          const message = messages[index];
          if (message) await this.handleMessage(message);
        }
      } catch (error) {
        if (isStopping()) break;
        this.metrics.recordRetry("dead-letter-consumer");
        this.logger.error("dead_letter_receive_failed", {
          errorCode: this.errorCode(error),
        });
        await this.pause(1_000);
      }
    }
  }

  async handleMessage(message: Message): Promise<void> {
    const receiptHandle = message.ReceiptHandle;
    if (!receiptHandle) return;
    const body = message.Body ?? "";
    const parsed = this.parseBody(body);
    const stableMessageId =
      parsed.eventId ??
      parsed.messageId ??
      message.MessageId ??
      this.hashBody(body);
    const sourceQueue = this.sourceQueue(message);
    const failure = this.failureMetadata(message);
    const receiveCount = this.receiveCount(message);

    try {
      const record = await this.recorder.record({
        sourceQueue,
        messageId: stableMessageId,
        failureType: failure.failureType,
        failureCode: failure.failureCode,
        body,
        payload: parsed.payload,
        receiveCount,
      });
      await this.queue.deleteDeadLetter(receiptHandle);
      this.logger.info("dead_letter_recorded_and_acknowledged", {
        messageId: stableMessageId,
        sourceQueue,
        failureType: failure.failureType,
        failureCode: failure.failureCode,
        deliveryCount: record.deliveryCount,
      });
    } catch (error) {
      this.metrics.recordRetry("dead-letter-consumer");
      this.logger.error("dead_letter_registration_failed", {
        messageId: stableMessageId,
        sourceQueue,
        failureType: failure.failureType,
        failureCode: failure.failureCode,
        errorCode: this.errorCode(error),
      });
      const delaySeconds = Math.min(2 ** Math.max(1, receiveCount), 300);
      try {
        await this.queue.changeDeadLetterVisibility(
          receiptHandle,
          delaySeconds,
        );
      } catch (visibilityError) {
        this.logger.warn("dead_letter_visibility_backoff_failed", {
          messageId: stableMessageId,
          errorCode: this.errorCode(visibilityError),
        });
      }
      // Keep the message in SQS until the operational record is committed and the ACK succeeds.
    }
  }

  private parseBody(body: string): ParsedDeadLetter {
    let value: Record<string, unknown> | null = null;
    try {
      const parsed: unknown = JSON.parse(body);
      if (this.isRecord(parsed)) value = parsed;
    } catch {
      return { payload: null };
    }

    const messageId = this.nonEmptyString(value?.messageId);
    const eventId = this.nonEmptyString(value?.eventId);
    return {
      payload: this.safeWagerPayload(value),
      ...(messageId ? { messageId } : {}),
      ...(eventId ? { eventId } : {}),
    };
  }

  private safeWagerPayload(
    value: Record<string, unknown> | null,
  ): Record<string, unknown> | null {
    if (!value) return null;
    if (
      value.type !== "WagerTransactionRequested" ||
      !this.isRecord(value.data)
    )
      return null;
    const data: Record<string, unknown> = {};
    for (const field of WAGER_PAYLOAD_FIELDS) {
      const fieldValue = value.data[field];
      if (field === "money") {
        if (!this.isRecord(fieldValue)) continue;
        const money: Record<string, string> = {};
        for (const moneyField of MONEY_PAYLOAD_FIELDS) {
          const value = fieldValue[moneyField];
          if (typeof value === "string") money[moneyField] = value;
        }
        data.money = money;
        continue;
      }
      if (typeof fieldValue === "string") data[field] = fieldValue;
    }
    return {
      messageId: this.nonEmptyString(value.messageId) ?? null,
      type: value.type,
      occurredAt: this.nonEmptyString(value.occurredAt) ?? null,
      data,
    };
  }

  private failureMetadata(message: Message): {
    failureType: DeadLetterFailureType;
    failureCode: string;
  } {
    const typeValue = message.MessageAttributes?.failureType?.StringValue;
    const failureType = DEAD_LETTER_FAILURE_TYPES.find(
      (candidate) => candidate === typeValue,
    );
    if (failureType) {
      return {
        failureType,
        failureCode:
          this.nonEmptyString(
            message.MessageAttributes?.failureCode?.StringValue,
          ) ?? "UNCLASSIFIED_PERMANENT_FAILURE",
      };
    }

    if (message.Attributes?.DeadLetterQueueSourceArn) {
      return {
        failureType: "RETRY_EXHAUSTED",
        failureCode: "MAX_RECEIVE_COUNT_EXCEEDED",
      };
    }
    return {
      failureType: "UNKNOWN",
      failureCode: "DLQ_REASON_UNAVAILABLE",
    };
  }

  private sourceQueue(message: Message): string {
    const explicit = this.nonEmptyString(
      message.MessageAttributes?.sourceQueue?.StringValue,
    );
    if (explicit) return explicit;
    const sourceArn = this.nonEmptyString(
      message.Attributes?.DeadLetterQueueSourceArn,
    );
    if (sourceArn) return sourceArn.split(":").at(-1) ?? sourceArn;
    return "wager-transactions.fifo";
  }

  private receiveCount(message: Message): number {
    const value = Number.parseInt(
      message.Attributes?.ApproximateReceiveCount ?? "1",
      10,
    );
    return Number.isInteger(value) && value > 0 ? value : 1;
  }

  private async returnUnprocessedMessages(messages: Message[]): Promise<void> {
    await Promise.allSettled(
      messages.map(async (message) => {
        if (!message.ReceiptHandle) return;
        try {
          await this.queue.changeDeadLetterVisibility(message.ReceiptHandle, 0);
        } catch (error) {
          this.logger.warn("dead_letter_shutdown_visibility_failed", {
            messageId: message.MessageId,
            errorCode: this.errorCode(error),
          });
        }
      }),
    );
  }

  private isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  private nonEmptyString(value: unknown): string | undefined {
    return typeof value === "string" && value.length > 0 ? value : undefined;
  }

  private hashBody(body: string): string {
    return sha256(body);
  }

  private errorCode(error: unknown): string {
    return error instanceof Error ? error.name : "UNKNOWN";
  }

  private pause(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }
}
