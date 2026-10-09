import { Inject, Injectable } from "@nestjs/common";
import type { DataSource } from "typeorm";
import type { AppEnvironment } from "../config/environment";
import { APP_ENVIRONMENT } from "../config/environment";
import { MetricsService } from "../observability/metrics.service";
import { StructuredLogger } from "../observability/structured-logger";
import { DATA_SOURCE } from "../persistence/data-source";

@Injectable()
export class DeadLetterRetentionService {
  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    @Inject(APP_ENVIRONMENT) private readonly environment: AppEnvironment,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  async purgeExpired(
    retentionDays = this.environment.deadLetterRetentionDays,
    batchSize = this.environment.deadLetterRetentionBatchSize,
  ): Promise<number> {
    if (!Number.isSafeInteger(retentionDays) || retentionDays < 1) {
      throw new Error("Dead-letter retention days must be a positive integer");
    }
    if (!Number.isSafeInteger(batchSize) || batchSize < 1) {
      throw new Error(
        "Dead-letter retention batch size must be a positive integer",
      );
    }

    const runner = this.dataSource.createQueryRunner();
    await runner.connect();
    let deleted = 0;
    try {
      const lockRows = (await runner.query(
        "SELECT pg_try_advisory_lock(8617, 4001) AS acquired",
      )) as Array<{ acquired: boolean }>;
      if (!lockRows[0]?.acquired) {
        this.logger.info("dead_letter_retention_skipped_locked");
        return 0;
      }

      try {
        for (;;) {
          await runner.startTransaction();
          try {
            const deleteResult = await runner.query(
              `WITH candidates AS (
                 SELECT message.source_queue, message.message_id
                 FROM dead_letter_messages AS message
                 LEFT JOIN wager_transactions AS wager
                   ON wager.provider_id = message.payload->'data'->>'providerId'
                  AND wager.external_transaction_id =
                      message.payload->'data'->>'externalTransactionId'
                 WHERE message.status IN ('REPROCESSED', 'RESOLVED')
                   AND GREATEST(
                         COALESCE(message.resolved_at, '-infinity'::timestamptz),
                         message.last_received_at,
                         COALESCE(wager.created_at, '-infinity'::timestamptz),
                         COALESCE(wager.processed_at, '-infinity'::timestamptz)
                       ) < now() - ($2::int * interval '1 day')
                   AND NOT EXISTS (
                     SELECT 1 FROM dead_letter_replay_outbox AS replay
                     WHERE replay.source_queue = message.source_queue
                       AND replay.message_id = message.message_id
                       AND replay.sent_at IS NULL
                   )
                   AND (wager.status IS NULL OR wager.status NOT IN ('PENDING', 'PENDING_REFERENCE'))
                 ORDER BY GREATEST(
                   COALESCE(message.resolved_at, '-infinity'::timestamptz),
                   message.last_received_at,
                   COALESCE(wager.created_at, '-infinity'::timestamptz),
                   COALESCE(wager.processed_at, '-infinity'::timestamptz)
                 )
                 LIMIT $1
                 FOR UPDATE OF message SKIP LOCKED
               )
               DELETE FROM dead_letter_messages AS message
               USING candidates
               WHERE message.source_queue = candidates.source_queue
                 AND message.message_id = candidates.message_id
               RETURNING message.message_id`,
              [batchSize, retentionDays],
              true,
            );
            const rows = deleteResult.records as Array<{ message_id: string }>;
            await runner.commitTransaction();
            deleted += rows.length;
            this.metrics.recordDeadLetterRetentionDelete(rows.length);
            if (rows.length < batchSize) break;
          } catch (error) {
            if (runner.isTransactionActive) await runner.rollbackTransaction();
            throw error;
          }
        }
      } finally {
        await runner.query("SELECT pg_advisory_unlock(8617, 4001)");
      }
      this.logger.info("dead_letter_retention_completed", {
        deleted,
        retentionDays,
        batchSize,
      });
      return deleted;
    } catch (error) {
      this.metrics.recordRetry("dead-letter-retention");
      this.logger.error("dead_letter_retention_failed", {
        errorCode: error instanceof Error ? error.name : "UNKNOWN",
      });
      throw error;
    } finally {
      await runner.release();
    }
  }
}
