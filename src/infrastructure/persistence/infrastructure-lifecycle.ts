import { Injectable, type OnApplicationShutdown } from "@nestjs/common";
import { SqsQueueService } from "../messaging/sqs-queue.service";
import { appDataSource } from "./data-source";

@Injectable()
export class InfrastructureLifecycle implements OnApplicationShutdown {
  constructor(private readonly queue: SqsQueueService) {}

  async onApplicationShutdown(): Promise<void> {
    await this.queue.close();
    if (appDataSource.isInitialized) await appDataSource.destroy();
  }
}
