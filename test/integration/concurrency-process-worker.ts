import { DataSource } from "typeorm";
import type { WagerSubmission } from "../../src/application/use-cases/process-wager-transaction";
import { ProcessWagerTransaction } from "../../src/application/use-cases/process-wager-transaction";
import { WagerTransactionStatus } from "../../src/domain/wagering/wager-transaction";
import { MetricsService } from "../../src/infrastructure/observability/metrics.service";
import { StructuredLogger } from "../../src/infrastructure/observability/structured-logger";
import { dataSourceOptions } from "../../src/infrastructure/persistence/data-source";

async function main(): Promise<void> {
  const submission = JSON.parse(
    process.env.TEST_WAGER_SUBMISSION ?? "null",
  ) as WagerSubmission | null;
  const count = Number.parseInt(process.env.TEST_SUBMISSION_COUNT ?? "0", 10);
  if (!submission || !Number.isInteger(count) || count < 1) {
    throw new Error("Test submission and positive count are required");
  }

  const source = new DataSource(dataSourceOptions);
  await source.initialize();
  try {
    const processor = new ProcessWagerTransaction(
      source,
      new MetricsService(),
      new StructuredLogger(),
    );
    const results = await Promise.all(
      Array.from({ length: count }, () => processor.execute(submission)),
    );
    if (
      results.length !== count ||
      results.some(
        (result) => result.status !== WagerTransactionStatus.Processed,
      )
    ) {
      throw new Error("Concurrent submissions did not all return PROCESSED");
    }
  } finally {
    await source.destroy();
  }
}

void main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : "Unknown error");
  process.exitCode = 1;
});
