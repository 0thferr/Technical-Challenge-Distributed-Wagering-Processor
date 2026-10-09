import { randomUUID } from "node:crypto";
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { Inject, Injectable } from "@nestjs/common";
import {
  ApplicationError,
  RetryableInfrastructureError,
} from "../../application/application-error";
import { sha256 } from "../../application/use-cases/canonical-payload";
import type { WagerSubmission } from "../../application/use-cases/process-wager-transaction";
import type { AppEnvironment } from "../config/environment";
import { APP_ENVIRONMENT } from "../config/environment";
import { MetricsService } from "../observability/metrics.service";
import type { DeadLetterFailureMetadata } from "./dead-letter-types";

@Injectable()
export class SqsQueueService {
  private readonly client: SQSClient;

  constructor(
    @Inject(APP_ENVIRONMENT) private readonly environment: AppEnvironment,
    private readonly metrics: MetricsService,
  ) {
    this.client = new SQSClient({
      region: environment.awsRegion,
      endpoint: environment.sqsEndpoint,
      credentials: {
        accessKeyId: environment.awsAccessKeyId,
        secretAccessKey: environment.awsSecretAccessKey,
      },
    });
  }

  async enqueueTransaction(
    input: WagerSubmission,
  ): Promise<{ messageId: string }> {
    const messageId = randomUUID();
    const { correlationId: _correlationId, ...data } = input;
    const body = {
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data,
    };
    const deduplicationId = sha256(
      `${input.providerId}:${input.idempotencyKey}:${JSON.stringify(input)}`,
    );
    try {
      await this.client.send(
        new SendMessageCommand({
          QueueUrl: this.environment.transactionQueueUrl,
          MessageBody: JSON.stringify(body),
          MessageGroupId: input.walletId,
          MessageDeduplicationId: deduplicationId,
        }),
      );
    } catch (error) {
      if (this.isRetryable(error)) {
        throw new RetryableInfrastructureError(
          "SQS is temporarily unavailable",
        );
      }
      throw new ApplicationError(
        "SQS rejected the queued transaction",
        "SQS_REQUEST_REJECTED",
        503,
      );
    }
    return { messageId };
  }

  async enqueueTransactionReplay(
    body: string,
    messageId: string,
    sourceQueue: string,
    walletId: string,
  ): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.environment.transactionQueueUrl,
        MessageBody: body,
        MessageGroupId: walletId,
        MessageDeduplicationId: sha256(messageId),
        MessageAttributes: {
          deadLetterReplay: { DataType: "String", StringValue: "true" },
          deadLetterSourceQueue: {
            DataType: "String",
            StringValue: sourceQueue,
          },
          deadLetterMessageId: {
            DataType: "String",
            StringValue: messageId,
          },
        },
      }),
    );
  }

  async enqueueEvent(
    event: Record<string, unknown>,
    eventId: string,
    aggregateId: string,
  ): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.environment.eventQueueUrl,
        MessageBody: JSON.stringify(event),
        MessageGroupId: aggregateId,
        MessageDeduplicationId: eventId,
      }),
    );
  }

  async enqueueDeadLetter(
    body: string,
    messageId: string,
    metadata: DeadLetterFailureMetadata,
  ): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.environment.deadLetterQueueUrl,
        MessageBody: body,
        MessageGroupId: metadata.sourceQueue,
        MessageDeduplicationId: sha256(messageId),
        MessageAttributes: {
          failureType: {
            DataType: "String",
            StringValue: metadata.failureType,
          },
          failureCode: {
            DataType: "String",
            StringValue: metadata.failureCode,
          },
          sourceQueue: {
            DataType: "String",
            StringValue: metadata.sourceQueue,
          },
        },
      }),
    );
    this.metrics.recordDeadLetter("wager-transactions-dlq.fifo");
  }

  async receiveDeadLetters(): Promise<Message[]> {
    const result = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.environment.deadLetterQueueUrl,
        MaxNumberOfMessages: 5,
        WaitTimeSeconds: 20,
        VisibilityTimeout: 60,
        MessageSystemAttributeNames: [
          "ApproximateReceiveCount",
          "DeadLetterQueueSourceArn",
          "MessageGroupId",
        ],
        MessageAttributeNames: ["All"],
      }),
    );
    return result.Messages ?? [];
  }

  async deleteDeadLetter(receiptHandle: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: this.environment.deadLetterQueueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  async changeDeadLetterVisibility(
    receiptHandle: string,
    seconds: number,
  ): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.environment.deadLetterQueueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: seconds,
      }),
    );
  }

  async receiveTransactions(): Promise<Message[]> {
    const result = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.environment.transactionQueueUrl,
        MaxNumberOfMessages: 5,
        WaitTimeSeconds: 20,
        VisibilityTimeout: 60,
        MessageSystemAttributeNames: ["ApproximateReceiveCount"],
        MessageAttributeNames: ["All"],
      }),
    );
    return result.Messages ?? [];
  }

  async deleteTransaction(receiptHandle: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: this.environment.transactionQueueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  async changeTransactionVisibility(
    receiptHandle: string,
    seconds: number,
  ): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.environment.transactionQueueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: seconds,
      }),
    );
  }

  async assertReachable(): Promise<void> {
    await Promise.all(
      [
        this.environment.transactionQueueUrl,
        this.environment.deadLetterQueueUrl,
        this.environment.eventQueueUrl,
      ].map((QueueUrl) =>
        this.client.send(
          new GetQueueAttributesCommand({
            QueueUrl,
            AttributeNames: ["QueueArn"],
          }),
        ),
      ),
    );
  }

  async deadLetterVisibleCount(): Promise<number> {
    const result = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.environment.deadLetterQueueUrl,
        AttributeNames: ["ApproximateNumberOfMessages"],
      }),
    );
    const count = result.Attributes?.ApproximateNumberOfMessages ?? "0";
    const parsed = Number.parseInt(count, 10);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  async eventVisibleCount(): Promise<number> {
    const result = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.environment.eventQueueUrl,
        AttributeNames: ["ApproximateNumberOfMessages"],
      }),
    );
    const count = result.Attributes?.ApproximateNumberOfMessages ?? "0";
    const parsed = Number.parseInt(count, 10);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  async close(): Promise<void> {
    this.client.destroy();
  }

  private isRetryable(error: unknown): boolean {
    if (typeof error !== "object" || error === null) return false;
    const details = error as {
      $metadata?: { httpStatusCode?: unknown };
      $retryable?: unknown;
      code?: unknown;
      name?: unknown;
    };
    if (details.$retryable) return true;
    const statusCode = details.$metadata?.httpStatusCode;
    if (typeof statusCode === "number")
      return statusCode === 429 || statusCode >= 500;
    const code = typeof details.code === "string" ? details.code : details.name;
    return (
      typeof code === "string" &&
      [
        "ECONNRESET",
        "ECONNREFUSED",
        "ETIMEDOUT",
        "EAI_AGAIN",
        "ENOTFOUND",
        "TimeoutError",
      ].includes(code)
    );
  }
}
