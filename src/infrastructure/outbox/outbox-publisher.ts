import { randomUUID } from "node:crypto";
import { Inject, Injectable } from "@nestjs/common";
import type { DataSource } from "typeorm";
import { SqsQueueService } from "../messaging/sqs-queue.service";
import { MetricsService } from "../observability/metrics.service";
import { StructuredLogger } from "../observability/structured-logger";
import { DATA_SOURCE } from "../persistence/data-source";

interface OutboxRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown> | string;
  occurred_at: Date;
  attempts: number;
}

@Injectable()
export class OutboxPublisher {
  private readonly workerId = randomUUID();

  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    private readonly queue: SqsQueueService,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  async publishBatch(): Promise<void> {
    const runner = this.dataSource.createQueryRunner();
    await runner.connect();
    await runner.startTransaction();
    let rows: OutboxRow[];
    try {
      const claimResult = await runner.query(
        `WITH candidates AS (
           SELECT id FROM outbox_messages
           WHERE published_at IS NULL
             AND COALESCE(next_attempt_at, '-infinity'::timestamptz) <= now()
             AND (locked_until IS NULL OR locked_until < now())
             AND NOT EXISTS (
               SELECT 1 FROM outbox_messages AS earlier
               WHERE earlier.aggregate_id = outbox_messages.aggregate_id
                 AND earlier.published_at IS NULL
                 AND earlier.sequence_id < outbox_messages.sequence_id
             )
           ORDER BY sequence_id
           LIMIT 20
           FOR UPDATE SKIP LOCKED
         )
         UPDATE outbox_messages AS outbox
         SET locked_by = $1, locked_until = now() + interval '60 seconds'
         FROM candidates
         WHERE outbox.id = candidates.id
         RETURNING outbox.id, outbox.aggregate_id, outbox.event_type, outbox.payload,
                   outbox.occurred_at, outbox.attempts`,
        [this.workerId],
        true,
      );
      rows = claimResult.records as OutboxRow[];
      await runner.commitTransaction();
    } catch (error) {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      throw error;
    } finally {
      await runner.release();
    }

    for (const row of rows) {
      try {
        const payload =
          typeof row.payload === "string"
            ? (JSON.parse(row.payload) as Record<string, unknown>)
            : row.payload;
        await this.queue.enqueueEvent(payload, row.id, row.aggregate_id);
        await this.dataSource.query(
          `UPDATE outbox_messages SET published_at = now(), locked_by = NULL, locked_until = NULL
           WHERE id = $1 AND locked_by = $2`,
          [row.id, this.workerId],
        );
      } catch (error) {
        const attempts = row.attempts + 1;
        const retrySeconds = Math.min(2 ** Math.min(attempts, 8), 300);
        await this.dataSource.query(
          `UPDATE outbox_messages SET attempts = attempts + 1,
             next_attempt_at = now() + ($3 * interval '1 second'), locked_by = NULL, locked_until = NULL
           WHERE id = $1 AND locked_by = $2`,
          [row.id, this.workerId, retrySeconds],
        );
        this.metrics.recordRetry("outbox-publisher");
        this.logger.error("outbox_publish_failed", {
          eventId: row.id,
          errorCode: this.errorCode(error),
        });
      }
    }
    const lagRows = (await this.dataSource.query(
      `SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))), 0)::float8 AS lag
       FROM outbox_messages WHERE published_at IS NULL`,
    )) as Array<{ lag: number }>;
    this.metrics.setOutboxLag(lagRows[0]?.lag ?? 0);
  }

  private errorCode(error: unknown): string {
    return error instanceof Error ? error.name : "UNKNOWN";
  }
}
