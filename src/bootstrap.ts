import "reflect-metadata";
import { ValidationPipe } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module";
import { readEnvironment } from "./infrastructure/config/environment";
import { StructuredLogger } from "./infrastructure/observability/structured-logger";
import { ApiExceptionFilter } from "./presentation/http/api-exception.filter";

export async function bootstrapHttp(): Promise<void> {
  const app = await NestFactory.create(AppModule, { logger: false });
  const logger = app.get(StructuredLogger);
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
      transformOptions: { enableImplicitConversion: false },
    }),
  );
  app.useGlobalFilters(new ApiExceptionFilter(logger));
  app.enableShutdownHooks();
  const environment = readEnvironment();
  await app.listen(environment.port, "0.0.0.0");
  logger.info("http_server_started", { port: environment.port });
}

export async function bootstrapWorker(): Promise<void> {
  process.env.WORKERS_ENABLED = "true";
  const { NestFactory } = await import("@nestjs/core");
  const { AppModule } = await import("./app.module");
  const context = await NestFactory.createApplicationContext(AppModule, {
    logger: false,
  });
  context.enableShutdownHooks();
}
