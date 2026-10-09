export interface AppEnvironment {
  port: number;
  metricsPort: number;
  databaseUrl: string;
  sqsEndpoint: string;
  awsRegion: string;
  awsAccessKeyId: string;
  awsSecretAccessKey: string;
  transactionQueueUrl: string;
  deadLetterQueueUrl: string;
  eventQueueUrl: string;
  workersEnabled: boolean;
  deadLetterRetentionDays: number;
  deadLetterRetentionBatchSize: number;
  deadLetterRetentionIntervalMs: number;
}

export const APP_ENVIRONMENT = Symbol("APP_ENVIRONMENT");

function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

function localDefault(value: string): string | undefined {
  return process.env.NODE_ENV === "production" ? undefined : value;
}

function readPort(name: string, fallback: string): number {
  const value = required(name, fallback);
  if (!/^\d+$/.test(value)) {
    throw new Error(`${name} must be an integer between 1 and 65535`);
  }
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`${name} must be an integer between 1 and 65535`);
  }
  return port;
}

function readPositiveInteger(name: string, fallback: string): number {
  const value = required(name, fallback);
  if (!/^\d+$/.test(value)) {
    throw new Error(`${name} must be a positive integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

export function readEnvironment(): AppEnvironment {
  return {
    port: readPort("PORT", "3000"),
    metricsPort: readPort("METRICS_PORT", "9464"),
    databaseUrl: required(
      "DATABASE_URL",
      localDefault("postgres://wagering:wagering@localhost:5432/wagering"),
    ),
    sqsEndpoint: required(
      "SQS_ENDPOINT",
      localDefault("http://localhost:4566"),
    ),
    awsRegion: required("AWS_REGION", localDefault("us-east-1")),
    awsAccessKeyId: required("AWS_ACCESS_KEY_ID", localDefault("test")),
    awsSecretAccessKey: required("AWS_SECRET_ACCESS_KEY", localDefault("test")),
    transactionQueueUrl: required(
      "SQS_TRANSACTION_QUEUE_URL",
      localDefault(
        "http://localhost:4566/000000000000/wager-transactions.fifo",
      ),
    ),
    deadLetterQueueUrl: required(
      "SQS_DEAD_LETTER_QUEUE_URL",
      localDefault(
        "http://localhost:4566/000000000000/wager-transactions-dlq.fifo",
      ),
    ),
    eventQueueUrl: required(
      "SQS_EVENT_QUEUE_URL",
      localDefault("http://localhost:4566/000000000000/wager-events.fifo"),
    ),
    workersEnabled: process.env.WORKERS_ENABLED === "true",
    deadLetterRetentionDays: readPositiveInteger(
      "DEAD_LETTER_RETENTION_DAYS",
      "90",
    ),
    deadLetterRetentionBatchSize: readPositiveInteger(
      "DEAD_LETTER_RETENTION_BATCH_SIZE",
      "500",
    ),
    deadLetterRetentionIntervalMs: readPositiveInteger(
      "DEAD_LETTER_RETENTION_INTERVAL_MS",
      "86400000",
    ),
  };
}
