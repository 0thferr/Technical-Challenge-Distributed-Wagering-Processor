export class ApplicationError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly statusCode: number,
    public readonly retryable = false,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class NotFoundApplicationError extends ApplicationError {
  constructor(resource = "Resource") {
    super(`${resource} not found`, "NOT_FOUND", 404);
  }
}

export class ConflictApplicationError extends ApplicationError {
  constructor(code: string, message: string) {
    super(message, code, 409);
  }
}

export class RetryableInfrastructureError extends ApplicationError {
  constructor(message = "A temporary infrastructure error occurred") {
    super(message, "TEMPORARY_INFRASTRUCTURE_ERROR", 503, true);
  }
}
