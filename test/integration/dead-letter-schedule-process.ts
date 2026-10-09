import { DeadLetterOperationsService } from "../../src/infrastructure/messaging/dead-letter-operations.service";
import { MetricsService } from "../../src/infrastructure/observability/metrics.service";
import { StructuredLogger } from "../../src/infrastructure/observability/structured-logger";
import { appDataSource } from "../../src/infrastructure/persistence/data-source";

async function main(): Promise<void> {
  const [sourceQueue, messageId] = process.argv.slice(2);
  if (!sourceQueue || !messageId)
    throw new Error("sourceQueue and messageId required");
  try {
    await appDataSource.initialize();
    await new DeadLetterOperationsService(
      appDataSource,
      new MetricsService(),
      new StructuredLogger(),
    ).requestReplay(sourceQueue, messageId);
  } finally {
    if (appDataSource.isInitialized) await appDataSource.destroy();
  }
}

void main().catch((error: unknown) => {
  console.error(
    error instanceof Error ? error.message : "Unknown scheduling error",
  );
  process.exitCode = 1;
});
