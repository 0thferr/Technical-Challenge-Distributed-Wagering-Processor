import {
  type ArgumentsHost,
  Catch,
  type ExceptionFilter,
  HttpException,
} from "@nestjs/common";
import { ApplicationError } from "../../application/application-error";
import { DomainError } from "../../domain/errors/domain-error";
import { StructuredLogger } from "../../infrastructure/observability/structured-logger";

interface HttpResponse {
  status(status: number): HttpResponse;
  json(body: unknown): void;
}

@Catch()
export class ApiExceptionFilter implements ExceptionFilter {
  constructor(private readonly logger: StructuredLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<HttpResponse>();
    if (exception instanceof ApplicationError) {
      response.status(exception.statusCode).json({
        statusCode: exception.statusCode,
        code: exception.code,
        message: exception.message,
        retryable: exception.retryable,
      });
      return;
    }
    if (exception instanceof DomainError) {
      response.status(400).json({
        statusCode: 400,
        code: exception.code,
        message: exception.message,
      });
      return;
    }
    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      const raw = exception.getResponse();
      if (
        typeof raw === "object" &&
        raw !== null &&
        "transactionId" in raw &&
        "status" in raw
      ) {
        response.status(status).json(raw);
        return;
      }
      const detail =
        typeof raw === "object" && raw !== null
          ? (raw as { message?: unknown }).message
          : raw;
      const message = Array.isArray(detail)
        ? detail.map((entry) => String(entry)).join("; ")
        : typeof detail === "string"
          ? detail
          : "Request could not be processed";
      response.status(status).json({
        statusCode: status,
        code:
          status === 400
            ? "VALIDATION_ERROR"
            : status === 503
              ? "NOT_READY"
              : "HTTP_ERROR",
        message,
      });
      return;
    }
    this.logger.error("unhandled_http_exception", {
      errorType: exception instanceof Error ? exception.name : "UnknownError",
    });
    response.status(500).json({
      statusCode: 500,
      code: "INTERNAL_ERROR",
      message: "An unexpected error occurred",
    });
  }
}
