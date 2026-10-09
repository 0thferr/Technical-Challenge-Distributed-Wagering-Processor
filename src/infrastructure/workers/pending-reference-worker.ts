import { randomUUID } from "node:crypto";
import { Inject, Injectable } from "@nestjs/common";
import type { DataSource } from "typeorm";
import { ProcessWagerTransaction } from "../../application/use-cases/process-wager-transaction";
import { MetricsService } from "../observability/metrics.service";
import { StructuredLogger } from "../observability/structured-logger";
import { DATA_SOURCE } from "../persistence/data-source";

@Injectable()
export class PendingReferenceWorker {
  private readonly leaseOwner = randomUUID();

  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    private readonly processor: ProcessWagerTransaction,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  async retryBatch(): Promise<void> {
    const runner = this.dataSource.createQueryRunner();
    await runner.connect();
    await runner.startTransaction();
    let rows: Array<{ id: string }>;
    try {
      const claimResult = await runner.query(
        `WITH candidates AS (
           SELECT id FROM wager_transactions
           WHERE status = 'PENDING_REFERENCE'
             AND reference_next_attempt_at <= now()
             AND (reference_locked_until IS NULL OR reference_locked_until < now())
           ORDER BY reference_next_attempt_at, created_at
           LIMIT 20
           FOR UPDATE SKIP LOCKED
         )
         UPDATE wager_transactions AS wager
         SET reference_locked_by = $1, reference_locked_until = now() + interval '60 seconds'
         FROM candidates
         WHERE wager.id = candidates.id
         RETURNING wager.id`,
        [this.leaseOwner],
        true,
      );
      rows = claimResult.records as Array<{ id: string }>;
      await runner.commitTransaction();
    } catch (error) {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      throw error;
    } finally {
      await runner.release();
    }

    for (const row of rows) {
      try {
        await this.processor.retryPendingReference(row.id, this.leaseOwner);
      } catch (error) {
        this.metrics.recordRetry("reference-worker");
        this.logger.error("reference_worker_attempt_failed", {
          transactionId: row.id,
          errorCode: error instanceof Error ? error.name : "UNKNOWN",
        });
        await this.dataSource.query(
          `UPDATE wager_transactions
           SET reference_locked_by = NULL, reference_locked_until = NULL,
               reference_next_attempt_at = now() + interval '5 seconds'
           WHERE id = $1 AND reference_locked_by = $2`,
          [row.id, this.leaseOwner],
        );
      }
    }
  }
}
