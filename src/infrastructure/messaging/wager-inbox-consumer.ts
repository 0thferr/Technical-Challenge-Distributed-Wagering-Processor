import type { Message } from "@aws-sdk/client-sqs";
import { Injectable } from "@nestjs/common";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import {
  ApplicationError,
  ConflictApplicationError,
} from "../../application/application-error";
import { sha256 } from "../../application/use-cases/canonical-payload";
import type { WagerSubmission } from "../../application/use-cases/process-wager-transaction";
import { ProcessWagerTransaction } from "../../application/use-cases/process-wager-transaction";
import { WagerTransactionKind } from "../../domain/wagering/wager-transaction";
import { QueuedWagerTransactionDto } from "../../presentation/http/dto/wager-transaction.dto";
import { MetricsService } from "../observability/metrics.service";
import { StructuredLogger } from "../observability/structured-logger";
import { DeadLetterOperationsService } from "./dead-letter-operations.service";
import { SqsQueueService } from "./sqs-queue.service";

interface WagerRequestEnvelope {
  messageId: string;
  type: string;
  occurredAt: string;
  data: QueuedWagerTransactionDto;
}

@Injectable()
export class WagerInboxConsumer {
  constructor(
    private readonly queue: SqsQueueService,
    private readonly processor: ProcessWagerTransaction,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
    private readonly deadLetterOperations: DeadLetterOperationsService,
  ) {}

  async runUntilStopped(isStopping: () => boolean): Promise<void> {
    while (!isStopping()) {
      try {
        const messages = await this.queue.receiveTransactions();
        for (let index = 0; index < messages.length; index += 1) {
          if (isStopping()) {
            await this.returnUnprocessedMessages(messages.slice(index));
            break;
          }
          const message = messages[index];
          if (!message) continue;
          await this.handleMessage(message);
        }
      } catch (error) {
        if (isStopping()) break;
        this.metrics.recordRetry("inbox-consumer");
        this.logger.error("sqs_receive_failed", {
          errorCode: this.errorCode(error),
        });
        await this.pause(1_000);
      }
    }
  }

  async handleMessage(message: Message): Promise<void> {
    const body = message.Body ?? "";
    const receiptHandle = message.ReceiptHandle;
    if (!receiptHandle) return;
    let envelope: WagerRequestEnvelope | undefined;
    try {
      const parsed = JSON.parse(body) as WagerRequestEnvelope;
      envelope = parsed;
      const validation = await this.validateEnvelope(parsed);
      if (validation) throw validation;
      const data = parsed.data;
      const submission: WagerSubmission = {
        providerId: data.providerId,
        externalTransactionId: data.externalTransactionId,
        playerId: data.playerId,
        walletId: data.walletId,
        roundId: data.roundId,
        gameId: data.gameId,
        kind: data.kind,
        money: data.money,
        idempotencyKey: data.idempotencyKey,
        correlationId: parsed.messageId,
        ...(data.referenceExternalTransactionId
          ? {
              referenceExternalTransactionId:
                data.referenceExternalTransactionId,
            }
          : {}),
      };
      const result = await this.processor.execute(submission, {
        consumerName: "wager-transaction-consumer",
        messageId: parsed.messageId,
      });
      const replaySourceQueue =
        message.MessageAttributes?.deadLetterSourceQueue?.StringValue;
      const replayMessageId =
        message.MessageAttributes?.deadLetterMessageId?.StringValue;
      if (replaySourceQueue && replayMessageId) {
        await this.deadLetterOperations.recordReplayOutcome(
          replaySourceQueue,
          replayMessageId,
          result.status !== "REJECTED",
          result.failureCode,
        );
      }
      await this.queue.deleteTransaction(receiptHandle);
      this.logger.info("sqs_transaction_acknowledged", {
        correlationId: envelope.messageId,
        messageId: parsed.messageId,
        providerId: data.providerId,
        walletId: data.walletId,
      });
    } catch (error) {
      if (this.isPermanent(error)) {
        try {
          await this.queue.enqueueDeadLetter(
            body,
            envelope?.messageId ?? message.MessageId ?? sha256(body),
            {
              failureType: this.deadLetterFailureType(error),
              failureCode: this.errorCode(error),
              sourceQueue: "wager-transactions.fifo",
            },
          );
          await this.queue.deleteTransaction(receiptHandle);
        } catch (deadLetterError) {
          this.metrics.recordRetry("inbox-consumer");
          this.logger.error("sqs_dead_letter_failed", {
            messageId: envelope?.messageId ?? message.MessageId,
            errorCode: this.errorCode(deadLetterError),
          });
          return;
        }
        this.logger.warn("sqs_message_routed_to_dlq", {
          messageId: envelope?.messageId ?? message.MessageId,
          failureCode:
            error instanceof ApplicationError ? error.code : "INVALID_MESSAGE",
        });
        return;
      }
      this.metrics.recordRetry("inbox-consumer");
      this.logger.error("sqs_transaction_processing_failed", {
        messageId: envelope?.messageId ?? message.MessageId,
        transactionId: undefined,
        walletId: envelope?.data?.walletId,
        providerId: envelope?.data?.providerId,
        errorCode: this.errorCode(error),
      });
      const receiveCount = Number.parseInt(
        message.Attributes?.ApproximateReceiveCount ?? "1",
        10,
      );
      const delaySeconds = Math.min(2 ** Math.max(1, receiveCount), 300);
      try {
        await this.queue.changeTransactionVisibility(
          receiptHandle,
          delaySeconds,
        );
      } catch (visibilityError) {
        this.logger.warn("sqs_visibility_backoff_failed", {
          messageId: envelope?.messageId ?? message.MessageId,
          errorCode: this.errorCode(visibilityError),
        });
      }
      // Do not delete. SQS redelivery and its redrive policy take the message to the DLQ.
    }
  }

  private async validateEnvelope(
    envelope: WagerRequestEnvelope,
  ): Promise<ApplicationError | undefined> {
    if (
      typeof envelope?.messageId !== "string" ||
      envelope.messageId.length === 0 ||
      envelope.type !== "WagerTransactionRequested" ||
      typeof envelope.occurredAt !== "string" ||
      Number.isNaN(Date.parse(envelope.occurredAt)) ||
      !envelope.data ||
      typeof envelope.data.idempotencyKey !== "string" ||
      envelope.data.idempotencyKey.length === 0
    ) {
      return new ApplicationError(
        "Invalid SQS message envelope",
        "INVALID_MESSAGE",
        400,
      );
    }
    const dto = plainToInstance(QueuedWagerTransactionDto, envelope.data);
    const errors = await validate(dto, {
      whitelist: true,
      forbidNonWhitelisted: true,
    });
    if (errors.length > 0 || dto.kind === WagerTransactionKind.Opening) {
      return new ApplicationError(
        "Invalid SQS wager transaction",
        "INVALID_MESSAGE",
        400,
      );
    }
    return undefined;
  }

  private async returnUnprocessedMessages(messages: Message[]): Promise<void> {
    await Promise.allSettled(
      messages.map(async (message) => {
        if (!message.ReceiptHandle) return;
        try {
          await this.queue.changeTransactionVisibility(
            message.ReceiptHandle,
            0,
          );
        } catch (error) {
          this.logger.warn("sqs_shutdown_visibility_return_failed", {
            messageId: message.MessageId,
            errorCode: this.errorCode(error),
          });
        }
      }),
    );
  }

  private isPermanent(error: unknown): boolean {
    return (
      (error instanceof ApplicationError &&
        !error.retryable &&
        error.statusCode < 500) ||
      error instanceof SyntaxError ||
      error instanceof ConflictApplicationError
    );
  }

  private deadLetterFailureType(
    error: unknown,
  ): "INVALID_MESSAGE" | "PERMANENT_FAILURE" {
    if (
      error instanceof SyntaxError ||
      (error instanceof ApplicationError && error.code === "INVALID_MESSAGE")
    ) {
      return "INVALID_MESSAGE";
    }
    return "PERMANENT_FAILURE";
  }

  private errorCode(error: unknown): string {
    if (error instanceof ApplicationError) return error.code;
    if (error instanceof Error) return error.name;
    return "UNKNOWN";
  }

  private pause(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }
}
