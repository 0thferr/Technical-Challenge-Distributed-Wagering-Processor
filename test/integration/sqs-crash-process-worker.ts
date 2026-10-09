import "reflect-metadata";
import { bootstrapWorker } from "../../src/bootstrap";
import { SqsQueueService } from "../../src/infrastructure/messaging/sqs-queue.service";

// This subprocess exercises inbox recovery only. Leaving an unrelated DLQ
// long poll open while the injected hard exit runs can lease a later test's
// message in LocalStack without a live process able to acknowledge it.
Object.defineProperty(SqsQueueService.prototype, "receiveDeadLetters", {
  configurable: true,
  writable: true,
  value: function (this: SqsQueueService): Promise<never[]> {
    return new Promise<never[]>(() => {});
  },
});

if (process.env.TEST_CRASH_BEFORE_SQS_ACK === "true") {
  Object.defineProperty(SqsQueueService.prototype, "deleteTransaction", {
    configurable: true,
    writable: true,
    value: async function (
      this: SqsQueueService,
      receiptHandle: string,
    ): Promise<void> {
      // The database commit has completed before the consumer calls delete.
      // Release the visibility lease so the restarted real worker can redeliver
      // immediately, then terminate without acknowledging the message.
      await this.changeTransactionVisibility(receiptHandle, 0);
      process.exit(87);
    },
  });
}

if (process.env.TEST_EXIT_AFTER_SQS_ACK === "true") {
  const deleteTransaction = SqsQueueService.prototype.deleteTransaction;
  Object.defineProperty(SqsQueueService.prototype, "deleteTransaction", {
    configurable: true,
    writable: true,
    value: async function (
      this: SqsQueueService,
      receiptHandle: string,
    ): Promise<void> {
      await deleteTransaction.call(this, receiptHandle);
      process.exit(88);
    },
  });
}

void bootstrapWorker().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : "Worker failed");
  process.exitCode = 1;
});
