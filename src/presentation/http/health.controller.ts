import { Controller, Get, HttpException, Inject } from "@nestjs/common";
import type { DataSource } from "typeorm";
import { SqsQueueService } from "../../infrastructure/messaging/sqs-queue.service";
import { DATA_SOURCE } from "../../infrastructure/persistence/data-source";

@Controller("health")
export class HealthController {
  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    private readonly queue: SqsQueueService,
  ) {}

  @Get("live")
  live(): { status: "ok" } {
    return { status: "ok" };
  }

  @Get("ready")
  async ready(): Promise<{
    status: "ok";
    checks: { postgres: "ok"; sqs: "ok" };
  }> {
    try {
      await Promise.all([
        this.dataSource.query("SELECT 1"),
        this.queue.assertReachable(),
      ]);
      return { status: "ok", checks: { postgres: "ok", sqs: "ok" } };
    } catch {
      throw new HttpException(
        {
          statusCode: 503,
          code: "NOT_READY",
          message: "PostgreSQL or SQS is unavailable",
          checks: {
            postgres: this.dataSource.isInitialized ? "unknown" : "unavailable",
            sqs: "unknown",
          },
        },
        503,
      );
    }
  }
}
