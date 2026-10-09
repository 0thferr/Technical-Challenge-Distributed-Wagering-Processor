import { randomUUID } from "node:crypto";
import { Inject, Injectable } from "@nestjs/common";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import type { DataSource } from "typeorm";
import { sha256 } from "../../application/use-cases/canonical-payload";
import { WagerTransactionKind } from "../../domain/wagering/wager-transaction";
import { QueuedWagerTransactionDto } from "../../presentation/http/dto/wager-transaction.dto";
import { MetricsService } from "../observability/metrics.service";
import { StructuredLogger } from "../observability/structured-logger";
import { DATA_SOURCE } from "../persistence/data-source";

export interface DeadLetterSummary {
  source_queue: string;
  message_id: string;
  failure_type: string;
  failure_code: string;
  status: string;
  delivery_count: number;
  sqs_receive_count: number;
  first_received_at: Date;
  last_received_at: Date;
  resolved_at: Date | null;
}

interface DeadLetterRow extends DeadLetterSummary {
  payload: Record<string, unknown> | string | null;
}

@Injectable()
export class DeadLetterOperationsService {
  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  async list(limit = 100): Promise<DeadLetterSummary[]> {
    const boundedLimit = Math.max(1, Math.min(limit, 500));
    return (await this.dataSource.query(
      `SELECT source_queue, message_id, failure_type, failure_code, status,
              delivery_count, sqs_receive_count, first_received_at,
              last_received_at, resolved_at
       FROM dead_letter_messages
       ORDER BY last_received_at DESC
       LIMIT $1`,
      [boundedLimit],
    )) as DeadLetterSummary[];
  }

  async prepareReplay(sourceQueue: string, messageId: string): Promise<string> {
    this.assertSupportedQueue(sourceQueue);
    const runner = this.dataSource.createQueryRunner();
    await runner.connect();
    await runner.startTransaction();
    try {
      const rows = (await runner.query(
        `SELECT source_queue, message_id, failure_type, status, payload
         FROM dead_letter_messages
         WHERE source_queue = $1 AND message_id = $2
         FOR UPDATE`,
        [sourceQueue, messageId],
      )) as DeadLetterRow[];
      const record = rows[0];
      if (!record) throw new Error("Dead-letter occurrence was not found");
      if (record.failure_type !== "RETRY_EXHAUSTED") {
        throw new Error("Only retry-exhausted messages can be replayed");
      }
      if (
        record.status !== "NEEDS_INVESTIGATION" &&
        record.status !== "REPROCESS_FAILED"
      ) {
        throw new Error(`Occurrence cannot be prepared from ${record.status}`);
      }

      const payload = this.readPayload(record.payload);
      await this.validateReplayPayload(payload, messageId);

      const business = payload.data as Record<string, unknown>;
      const committed = (await runner.query(
        `SELECT status FROM wager_transactions
         WHERE provider_id = $1
           AND (external_transaction_id = $2 OR idempotency_key = $3)
         ORDER BY created_at DESC
         LIMIT 1`,
        [
          business.providerId,
          business.externalTransactionId,
          business.idempotencyKey,
        ],
      )) as Array<{ status: string }>;
      if (committed.length > 0) {
        const outcome =
          committed[0]?.status === "PROCESSED"
            ? "REPROCESSED"
            : "REPROCESS_FAILED";
        await runner.query(
          `UPDATE dead_letter_messages
           SET status = $3, resolved_at = now()
           WHERE source_queue = $1 AND message_id = $2`,
          [sourceQueue, messageId, outcome],
        );
        await runner.commitTransaction();
        this.metrics.recordDeadLetterReplay(
          outcome === "REPROCESSED" ? "reprocessed" : "failed",
        );
        return outcome;
      }

      await runner.query(
        `UPDATE dead_letter_messages
         SET status = 'READY_FOR_REPROCESS', resolved_at = NULL
         WHERE source_queue = $1 AND message_id = $2`,
        [sourceQueue, messageId],
      );
      await runner.commitTransaction();
      return "READY_FOR_REPROCESS";
    } catch (error) {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      throw error;
    } finally {
      await runner.release();
    }
  }

  async requestReplay(sourceQueue: string, messageId: string): Promise<void> {
    this.assertSupportedQueue(sourceQueue);
    const runner = this.dataSource.createQueryRunner();
    await runner.connect();
    await runner.startTransaction();
    try {
      const rows = (await runner.query(
        `SELECT source_queue, message_id, failure_type, status, payload
         FROM dead_letter_messages
         WHERE source_queue = $1 AND message_id = $2
         FOR UPDATE`,
        [sourceQueue, messageId],
      )) as DeadLetterRow[];
      const record = rows[0];
      if (!record) throw new Error("Dead-letter occurrence was not found");
      if (
        record.status !== "READY_FOR_REPROCESS" ||
        record.failure_type !== "RETRY_EXHAUSTED"
      ) {
        throw new Error("Occurrence is not eligible for controlled replay");
      }
      const payload = this.readPayload(record.payload);
      await this.validateReplayPayload(payload, messageId);

      const business = payload.data as Record<string, unknown>;
      const committed = (await runner.query(
        `SELECT 1 FROM wager_transactions
         WHERE provider_id = $1
           AND (external_transaction_id = $2 OR idempotency_key = $3)
         LIMIT 1`,
        [
          business.providerId,
          business.externalTransactionId,
          business.idempotencyKey,
        ],
      )) as Array<Record<string, unknown>>;
      if (committed.length > 0) {
        throw new Error("A matching financial transaction already exists");
      }

      const body = JSON.stringify(payload);
      const bodyHash = sha256(body);
      await runner.query(
        `INSERT INTO dead_letter_replay_outbox (
           id, source_queue, message_id, body, body_hash
         ) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (source_queue, message_id) DO UPDATE
         SET body = EXCLUDED.body, body_hash = EXCLUDED.body_hash,
             attempts = 0, next_attempt_at = NULL, sent_at = NULL,
             locked_by = NULL, locked_until = NULL, created_at = now()`,
        [randomUUID(), sourceQueue, messageId, body, bodyHash],
      );
      await runner.query(
        `UPDATE dead_letter_messages
         SET status = 'REPLAY_QUEUED', replay_body_hash = $3,
             resolved_at = NULL
         WHERE source_queue = $1 AND message_id = $2`,
        [sourceQueue, messageId, bodyHash],
      );
      await runner.commitTransaction();
      this.metrics.recordDeadLetterReplay("queued");
      this.logger.info("dead_letter_replay_scheduled", {
        messageId,
        sourceQueue,
      });
    } catch (error) {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      throw error;
    } finally {
      await runner.release();
    }
  }

  async recordReplayOutcome(
    sourceQueue: string,
    messageId: string,
    success: boolean,
    failureCode?: string,
  ): Promise<void> {
    const status = success ? "REPROCESSED" : "REPROCESS_FAILED";
    const rows = (await this.dataSource.query(
      `UPDATE dead_letter_messages
       SET status = $3, failure_code = COALESCE($4, failure_code),
           resolved_at = now()
       WHERE source_queue = $1 AND message_id = $2
         AND status IN ('REPLAY_QUEUED', 'REPROCESSING', 'REPROCESSED', 'REPROCESS_FAILED')
       RETURNING status`,
      [sourceQueue, messageId, status, failureCode ?? null],
    )) as Array<{ status: string }>;
    if (rows.length === 0) {
      throw new Error(
        "Replay status could not be reconciled with the DLQ record",
      );
    }
    this.metrics.recordDeadLetterReplay(success ? "reprocessed" : "failed");
    this.logger.info("dead_letter_replay_completed", {
      messageId,
      sourceQueue,
      status,
      ...(failureCode ? { failureCode } : {}),
    });
  }

  async resolve(sourceQueue: string, messageId: string): Promise<void> {
    const rows = (await this.dataSource.query(
      `UPDATE dead_letter_messages
       SET status = 'RESOLVED', resolved_at = now()
       WHERE source_queue = $1 AND message_id = $2
         AND status IN (
           'NEEDS_INVESTIGATION', 'READY_FOR_REPROCESS',
           'REPROCESS_FAILED', 'REPROCESSED'
         )
       RETURNING message_id`,
      [sourceQueue, messageId],
    )) as Array<{ message_id: string }>;
    if (rows.length === 0) {
      throw new Error("Occurrence is missing or still being replayed");
    }
    this.logger.info("dead_letter_resolved", { messageId, sourceQueue });
  }

  private assertSupportedQueue(sourceQueue: string): void {
    if (sourceQueue !== "wager-transactions.fifo") {
      throw new Error(
        "Controlled replay is supported only for wager transactions",
      );
    }
  }

  private readPayload(
    value: Record<string, unknown> | string | null,
  ): Record<string, unknown> {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("Occurrence does not contain a replayable wager payload");
    }
    return parsed as Record<string, unknown>;
  }

  private async validateReplayPayload(
    payload: Record<string, unknown>,
    messageId: string,
  ): Promise<void> {
    if (
      payload.messageId !== messageId ||
      payload.type !== "WagerTransactionRequested" ||
      typeof payload.occurredAt !== "string" ||
      Number.isNaN(Date.parse(payload.occurredAt)) ||
      !payload.data ||
      typeof payload.data !== "object" ||
      Array.isArray(payload.data)
    ) {
      throw new Error("Occurrence does not contain a valid wager envelope");
    }
    const dto = plainToInstance(
      QueuedWagerTransactionDto,
      payload.data as Record<string, unknown>,
    );
    const errors = await validate(dto, {
      whitelist: true,
      forbidNonWhitelisted: true,
    });
    if (errors.length > 0 || dto.kind === WagerTransactionKind.Opening) {
      throw new Error("Occurrence payload is not eligible for wager replay");
    }
  }
}
