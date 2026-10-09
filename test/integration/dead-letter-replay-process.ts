import { readEnvironment } from "../../src/infrastructure/config/environment";
import { DeadLetterReplayPublisher } from "../../src/infrastructure/messaging/dead-letter-replay-publisher";
import { SqsQueueService } from "../../src/infrastructure/messaging/sqs-queue.service";
import { MetricsService } from "../../src/infrastructure/observability/metrics.service";
import { StructuredLogger } from "../../src/infrastructure/observability/structured-logger";
import { appDataSource } from "../../src/infrastructure/persistence/data-source";

async function main(): Promise<void> {
  const environment = readEnvironment();
  const metrics = new MetricsService();
  const logger = new StructuredLogger();
  const queue = new SqsQueueService(environment, metrics);
  try {
    await appDataSource.initialize();
    await new DeadLetterReplayPublisher(
      appDataSource,
      queue,
      metrics,
      logger,
    ).publishBatch();
  } finally {
    await queue.close();
    if (appDataSource.isInitialized) await appDataSource.destroy();
  }
}

void main().catch((error: unknown) => {
  console.error(
    error instanceof Error ? error.message : "Unknown replay error",
  );
  process.exitCode = 1;
});
