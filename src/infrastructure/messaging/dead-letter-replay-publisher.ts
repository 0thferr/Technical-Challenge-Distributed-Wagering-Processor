import { randomUUID } from "node:crypto";
import { Inject, Injectable } from "@nestjs/common";
import type { DataSource } from "typeorm";
import { MetricsService } from "../observability/metrics.service";
import { StructuredLogger } from "../observability/structured-logger";
import { DATA_SOURCE } from "../persistence/data-source";
import { SqsQueueService } from "./sqs-queue.service";

interface ReplayRow {
  id: string;
  source_queue: string;
  message_id: string;
  body: string;
  attempts: number;
}

interface ReplayEnvelope {
  data?: { walletId?: unknown };
}

@Injectable()
export class DeadLetterReplayPublisher {
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
    let rows: ReplayRow[];
    try {
      const claimResult = await runner.query(
        `WITH candidates AS (
           SELECT id FROM dead_letter_replay_outbox
           WHERE sent_at IS NULL
             AND COALESCE(next_attempt_at, '-infinity'::timestamptz) <= now()
             AND (locked_until IS NULL OR locked_until < now())
           ORDER BY created_at
           LIMIT 20
           FOR UPDATE SKIP LOCKED
         )
         UPDATE dead_letter_replay_outbox AS replay
         SET locked_by = $1, locked_until = now() + interval '60 seconds'
         FROM candidates
         WHERE replay.id = candidates.id
         RETURNING replay.id, replay.source_queue, replay.message_id,
                   replay.body, replay.attempts`,
        [this.workerId],
        true,
      );
      rows = claimResult.records as ReplayRow[];
      await runner.commitTransaction();
    } catch (error) {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      throw error;
    } finally {
      await runner.release();
    }

    for (const row of rows) await this.publishOne(row);
  }

  private async publishOne(row: ReplayRow): Promise<void> {
    try {
      const envelope = JSON.parse(row.body) as ReplayEnvelope;
      const walletId = envelope.data?.walletId;
      if (typeof walletId !== "string" || walletId.length === 0) {
        throw new Error("Replay envelope has no wallet group id");
      }
      await this.queue.enqueueTransactionReplay(
        row.body,
        row.message_id,
        row.source_queue,
        walletId,
      );

      const runner = this.dataSource.createQueryRunner();
      await runner.connect();
      await runner.startTransaction();
      try {
        await runner.query(
          `UPDATE dead_letter_replay_outbox
           SET sent_at = now(), locked_by = NULL, locked_until = NULL
           WHERE id = $1 AND locked_by = $2`,
          [row.id, this.workerId],
        );
        await runner.query(
          `UPDATE dead_letter_messages
           SET status = CASE WHEN status = 'REPLAY_QUEUED'
                             THEN 'REPROCESSING' ELSE status END
           WHERE source_queue = $1 AND message_id = $2`,
          [row.source_queue, row.message_id],
        );
        await runner.commitTransaction();
      } catch (error) {
        if (runner.isTransactionActive) await runner.rollbackTransaction();
        throw error;
      } finally {
        await runner.release();
      }
      this.logger.info("dead_letter_replay_published", {
        messageId: row.message_id,
        sourceQueue: row.source_queue,
      });
    } catch (error) {
      const attempts = row.attempts + 1;
      const retrySeconds = Math.min(2 ** Math.min(attempts, 8), 300);
      await this.dataSource.query(
        `UPDATE dead_letter_replay_outbox
         SET attempts = attempts + 1,
             next_attempt_at = now() + ($3 * interval '1 second'),
             locked_by = NULL, locked_until = NULL
         WHERE id = $1 AND locked_by = $2`,
        [row.id, this.workerId, retrySeconds],
      );
      this.metrics.recordRetry("dead-letter-replay-publisher");
      this.logger.error("dead_letter_replay_publish_failed", {
        messageId: row.message_id,
        sourceQueue: row.source_queue,
        errorCode: error instanceof Error ? error.name : "UNKNOWN",
      });
    }
  }
}
