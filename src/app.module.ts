import { Module } from "@nestjs/common";
import { ProcessWagerTransaction } from "./application/use-cases/process-wager-transaction";
import { WagerQueryService } from "./application/use-cases/wager-query-service";
import { WalletService } from "./application/use-cases/wallet-service";
import {
  APP_ENVIRONMENT,
  readEnvironment,
} from "./infrastructure/config/environment";
import { DeadLetterMessageRecorder } from "./infrastructure/messaging/dead-letter-message-recorder";
import { DeadLetterOperationsService } from "./infrastructure/messaging/dead-letter-operations.service";
import { DeadLetterQueueConsumer } from "./infrastructure/messaging/dead-letter-queue-consumer";
import { DeadLetterReplayPublisher } from "./infrastructure/messaging/dead-letter-replay-publisher";
import { DeadLetterRetentionService } from "./infrastructure/messaging/dead-letter-retention.service";
import { SqsQueueService } from "./infrastructure/messaging/sqs-queue.service";
import { WagerInboxConsumer } from "./infrastructure/messaging/wager-inbox-consumer";
import { MetricsService } from "./infrastructure/observability/metrics.service";
import { StructuredLogger } from "./infrastructure/observability/structured-logger";
import { WorkerMetricsServer } from "./infrastructure/observability/worker-metrics-server";
import { OutboxPublisher } from "./infrastructure/outbox/outbox-publisher";
import {
  appDataSource,
  DATA_SOURCE,
} from "./infrastructure/persistence/data-source";
import { InfrastructureLifecycle } from "./infrastructure/persistence/infrastructure-lifecycle";
import { PendingReferenceWorker } from "./infrastructure/workers/pending-reference-worker";
import { WorkerCoordinator } from "./infrastructure/workers/worker-coordinator";
import { HealthController } from "./presentation/http/health.controller";
import { MetricsController } from "./presentation/http/metrics.controller";
import { WageringController } from "./presentation/http/wagering.controller";
import { WalletController } from "./presentation/http/wallet.controller";

@Module({
  controllers: [
    WalletController,
    WageringController,
    HealthController,
    MetricsController,
  ],
  providers: [
    { provide: APP_ENVIRONMENT, useFactory: readEnvironment },
    {
      provide: DATA_SOURCE,
      useFactory: async () => {
        if (!appDataSource.isInitialized) await appDataSource.initialize();
        return appDataSource;
      },
    },
    MetricsService,
    StructuredLogger,
    WorkerMetricsServer,
    SqsQueueService,
    DeadLetterMessageRecorder,
    DeadLetterOperationsService,
    DeadLetterQueueConsumer,
    DeadLetterReplayPublisher,
    DeadLetterRetentionService,
    ProcessWagerTransaction,
    WalletService,
    WagerQueryService,
    WagerInboxConsumer,
    OutboxPublisher,
    PendingReferenceWorker,
    WorkerCoordinator,
    InfrastructureLifecycle,
  ],
})
export class AppModule {}
