import { readEnvironment } from "../src/infrastructure/config/environment";
import { DeadLetterOperationsService } from "../src/infrastructure/messaging/dead-letter-operations.service";
import { DeadLetterRetentionService } from "../src/infrastructure/messaging/dead-letter-retention.service";
import { MetricsService } from "../src/infrastructure/observability/metrics.service";
import { StructuredLogger } from "../src/infrastructure/observability/structured-logger";
import { appDataSource } from "../src/infrastructure/persistence/data-source";

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2);
  const [sourceQueue, messageId] = args;
  const environment = readEnvironment();
  const metrics = new MetricsService();
  const logger = new StructuredLogger();

  try {
    await appDataSource.initialize();
    const operations = new DeadLetterOperationsService(
      appDataSource,
      metrics,
      logger,
    );
    if (command === "list") {
      const limit = args[0] ? Number.parseInt(args[0], 10) : 100;
      const records = await operations.list(limit);
      console.info(JSON.stringify(records, null, 2));
      return;
    }
    if (command === "prepare") {
      if (!sourceQueue || !messageId) throw new Error(usage());
      const status = await operations.prepareReplay(sourceQueue, messageId);
      console.info(JSON.stringify({ messageId, sourceQueue, status }));
      return;
    }
    if (command === "replay") {
      if (!sourceQueue || !messageId) throw new Error(usage());
      await operations.requestReplay(sourceQueue, messageId);
      console.info(
        JSON.stringify({ messageId, sourceQueue, status: "REPLAY_QUEUED" }),
      );
      return;
    }
    if (command === "resolve") {
      if (!sourceQueue || !messageId) throw new Error(usage());
      await operations.resolve(sourceQueue, messageId);
      console.info(
        JSON.stringify({ messageId, sourceQueue, status: "RESOLVED" }),
      );
      return;
    }
    if (command === "retention") {
      const deleted = await new DeadLetterRetentionService(
        appDataSource,
        environment,
        metrics,
        logger,
      ).purgeExpired();
      console.info(JSON.stringify({ deleted }));
      return;
    }
    throw new Error(usage());
  } catch (error) {
    console.error(
      JSON.stringify({
        level: "error",
        message: "dead_letter_operation_failed",
        errorCode: error instanceof Error ? error.message : "UNKNOWN",
      }),
    );
    process.exitCode = 1;
  } finally {
    if (appDataSource.isInitialized) await appDataSource.destroy();
  }
}

function usage(): string {
  return [
    "Usage:",
    "  bun scripts/dead-letter-operations.ts list [limit]",
    "  bun scripts/dead-letter-operations.ts prepare <source-queue> <message-id>",
    "  bun scripts/dead-letter-operations.ts replay <source-queue> <message-id>",
    "  bun scripts/dead-letter-operations.ts resolve <source-queue> <message-id>",
    "  bun scripts/dead-letter-operations.ts retention",
  ].join("\n");
}

void main();
