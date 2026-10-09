import { Injectable } from "@nestjs/common";
import {
  Counter,
  collectDefaultMetrics,
  Gauge,
  Histogram,
  Registry,
} from "prom-client";

@Injectable()
export class MetricsService {
  readonly registry = new Registry();
  private readonly transactions: Counter<"status">;
  private readonly duplicates: Counter<"source">;
  private readonly retries: Counter<"worker">;
  private readonly deadLetterMessages: Counter<"queue">;
  private readonly deadLetterVisible: Gauge;
  private readonly lockConflicts: Counter<"sqlState">;
  private readonly outboxLag: Gauge;
  private readonly processingLatency: Histogram<"operation">;
  private readonly reconciliationMismatches: Counter;
  private readonly deadLetterReplays: Counter<"status">;
  private readonly deadLetterRetentionDeletes: Counter;

  constructor() {
    collectDefaultMetrics({ register: this.registry });
    this.transactions = new Counter({
      name: "wager_transactions_total",
      help: "Wager transactions completed by status",
      labelNames: ["status"],
      registers: [this.registry],
    });
    this.duplicates = new Counter({
      name: "wager_duplicates_total",
      help: "Persistent idempotency or inbox duplicates detected",
      labelNames: ["source"],
      registers: [this.registry],
    });
    this.retries = new Counter({
      name: "wager_worker_retries_total",
      help: "Background worker retries",
      labelNames: ["worker"],
      registers: [this.registry],
    });
    this.deadLetterMessages = new Counter({
      name: "wager_dead_letter_messages_total",
      help: "Messages explicitly routed to the dead letter queue",
      labelNames: ["queue"],
      registers: [this.registry],
    });
    this.deadLetterVisible = new Gauge({
      name: "wager_dead_letter_messages_visible",
      help: "Current number of visible messages in the wagering DLQ",
      registers: [this.registry],
    });
    this.lockConflicts = new Counter({
      name: "wager_database_lock_conflicts_total",
      help: "Retryable PostgreSQL concurrency conflicts",
      labelNames: ["sqlState"],
      registers: [this.registry],
    });
    this.outboxLag = new Gauge({
      name: "wager_outbox_oldest_pending_seconds",
      help: "Age in seconds of the oldest unpublished outbox event",
      registers: [this.registry],
    });
    this.processingLatency = new Histogram({
      name: "wager_processing_duration_seconds",
      help: "Financial transaction processing duration",
      labelNames: ["operation"],
      buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers: [this.registry],
    });
    this.reconciliationMismatches = new Counter({
      name: "wager_reconciliation_mismatches_total",
      help: "Wallet reconciliations that found a ledger mismatch",
      registers: [this.registry],
    });
    this.deadLetterReplays = new Counter({
      name: "wager_dead_letter_replays_total",
      help: "Administrative dead-letter replay outcomes",
      labelNames: ["status"],
      registers: [this.registry],
    });
    this.deadLetterRetentionDeletes = new Counter({
      name: "wager_dead_letter_retention_deleted_total",
      help: "Operational dead-letter records removed by retention",
      registers: [this.registry],
    });
  }

  recordTransaction(status: string): void {
    this.transactions.inc({ status });
  }

  recordDuplicate(source: "idempotency" | "inbox"): void {
    this.duplicates.inc({ source });
  }

  recordRetry(
    worker:
      | "inbox-consumer"
      | "dead-letter-consumer"
      | "dead-letter-replay-publisher"
      | "dead-letter-retention"
      | "outbox-publisher"
      | "reference-worker",
  ): void {
    this.retries.inc({ worker });
  }

  recordDeadLetter(queue: string): void {
    this.deadLetterMessages.inc({ queue });
  }

  setDeadLetterVisible(count: number): void {
    this.deadLetterVisible.set(count);
  }

  recordLockConflict(sqlState: string): void {
    this.lockConflicts.inc({ sqlState });
  }

  observeProcessing(
    operation: "http" | "sqs" | "reference-worker",
    seconds: number,
  ): void {
    this.processingLatency.observe({ operation }, seconds);
  }

  setOutboxLag(seconds: number): void {
    this.outboxLag.set(seconds);
  }

  recordReconciliationMismatch(): void {
    this.reconciliationMismatches.inc();
  }

  recordDeadLetterReplay(status: "queued" | "reprocessed" | "failed"): void {
    this.deadLetterReplays.inc({ status });
  }

  recordDeadLetterRetentionDelete(count: number): void {
    if (count > 0) this.deadLetterRetentionDeletes.inc(count);
  }
}
