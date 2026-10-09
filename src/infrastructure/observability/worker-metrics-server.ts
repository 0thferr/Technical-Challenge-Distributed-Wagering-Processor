import { createServer, type Server } from "node:http";
import {
  Inject,
  Injectable,
  type OnApplicationShutdown,
  type OnModuleInit,
} from "@nestjs/common";
import type { AppEnvironment } from "../config/environment";
import { APP_ENVIRONMENT } from "../config/environment";
import { MetricsService } from "./metrics.service";
import { StructuredLogger } from "./structured-logger";

@Injectable()
export class WorkerMetricsServer
  implements OnModuleInit, OnApplicationShutdown
{
  private server: Server | undefined;

  constructor(
    @Inject(APP_ENVIRONMENT) private readonly environment: AppEnvironment,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  onModuleInit(): void {
    if (!this.environment.workersEnabled) return;
    this.server = createServer((request, response) => {
      if (request.method !== "GET" || request.url !== "/metrics") {
        response.writeHead(404, { "content-type": "application/json" });
        response.end(JSON.stringify({ code: "NOT_FOUND" }));
        return;
      }
      void this.metrics.registry.metrics().then((body) => {
        response.writeHead(200, {
          "content-type": this.metrics.registry.contentType,
        });
        response.end(body);
      });
    });
    this.server.listen(this.environment.metricsPort, "0.0.0.0", () => {
      this.logger.info("worker_metrics_server_started", {
        port: this.environment.metricsPort,
      });
    });
  }

  async onApplicationShutdown(): Promise<void> {
    if (!this.server?.listening) return;
    await new Promise<void>((resolve, reject) => {
      this.server?.close((error) => (error ? reject(error) : resolve()));
    });
  }
}
