import {
  Inject,
  Injectable,
  type OnModuleDestroy,
  type OnModuleInit,
} from "@nestjs/common";
import type { AppEnvironment } from "../config/environment";
import { APP_ENVIRONMENT } from "../config/environment";
import { DeadLetterQueueConsumer } from "../messaging/dead-letter-queue-consumer";
import { DeadLetterReplayPublisher } from "../messaging/dead-letter-replay-publisher";
import { DeadLetterRetentionService } from "../messaging/dead-letter-retention.service";
import { SqsQueueService } from "../messaging/sqs-queue.service";
import { WagerInboxConsumer } from "../messaging/wager-inbox-consumer";
import { MetricsService } from "../observability/metrics.service";
import { StructuredLogger } from "../observability/structured-logger";
import { OutboxPublisher } from "../outbox/outbox-publisher";
import { PendingReferenceWorker } from "./pending-reference-worker";

@Injectable()
export class WorkerCoordinator implements OnModuleInit, OnModuleDestroy {
  private stopping = false;
  private tasks: Promise<void>[] = [];

  constructor(
    @Inject(APP_ENVIRONMENT) private readonly environment: AppEnvironment,
    private readonly inboxConsumer: WagerInboxConsumer,
    private readonly deadLetterConsumer: DeadLetterQueueConsumer,
    private readonly replayPublisher: DeadLetterReplayPublisher,
    private readonly deadLetterRetention: DeadLetterRetentionService,
    private readonly outboxPublisher: OutboxPublisher,
    private readonly referenceWorker: PendingReferenceWorker,
    private readonly queue: SqsQueueService,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  onModuleInit(): void {
    if (!this.environment.workersEnabled) return;
    this.stopping = false;
    this.tasks = [
      this.inboxConsumer.runUntilStopped(() => this.stopping),
      this.deadLetterConsumer.runUntilStopped(() => this.stopping),
      this.runPeriodic("dead_letter_replay", 250, () =>
        this.replayPublisher.publishBatch(),
      ),
      this.runPeriodic(
        "dead_letter_retention",
        this.environment.deadLetterRetentionIntervalMs,
        async () => {
          await this.deadLetterRetention.purgeExpired();
        },
      ),
      this.runPeriodic("outbox", 250, () =>
        this.outboxPublisher.publishBatch(),
      ),
      this.runPeriodic("pending_reference", 1_000, () =>
        this.referenceWorker.retryBatch(),
      ),
      this.runPeriodic("queue_metrics", 10_000, async () => {
        this.metrics.setDeadLetterVisible(
          await this.queue.deadLetterVisibleCount(),
        );
      }),
    ];
    this.logger.info("workers_started");
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    await Promise.allSettled(this.tasks);
    this.logger.info("workers_stopped");
  }

  private async runPeriodic(
    name: string,
    intervalMs: number,
    work: () => Promise<void>,
  ): Promise<void> {
    while (!this.stopping) {
      try {
        await work();
      } catch (error) {
        this.logger.error("worker_iteration_failed", {
          worker: name,
          errorCode: error instanceof Error ? error.name : "UNKNOWN",
        });
      }
      if (!this.stopping)
        await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }
}
