const compose = Bun.spawn(
  ["docker", "compose", "up", "-d", "--wait", "postgres", "localstack"],
  {
    env: {
      ...Bun.env,
      POSTGRES_HOST_PORT: "55432",
      LOCALSTACK_HOST_PORT: "54566",
    },
    stdout: "inherit",
    stderr: "inherit",
  },
);
const composeStatus = await compose.exited;
if (composeStatus !== 0) process.exit(composeStatus);

const tests = Bun.spawn(
  ["bun", "test", "--max-concurrency=1", "test/integration"],
  {
    env: {
      ...Bun.env,
      DATABASE_URL: "postgres://wagering:wagering@localhost:55432/wagering",
      SQS_ENDPOINT: "http://localhost:54566",
      SQS_TRANSACTION_QUEUE_URL:
        "http://localhost:54566/000000000000/wager-transactions.fifo",
      SQS_DEAD_LETTER_QUEUE_URL:
        "http://localhost:54566/000000000000/wager-transactions-dlq.fifo",
      SQS_EVENT_QUEUE_URL:
        "http://localhost:54566/000000000000/wager-events.fifo",
      RUN_INTEGRATION_TESTS: "true",
    },
    stdout: "inherit",
    stderr: "inherit",
  },
);
process.exit(await tests.exited);
