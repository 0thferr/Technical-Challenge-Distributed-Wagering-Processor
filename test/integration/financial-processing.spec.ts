import {
  afterAll,
  beforeAll,
  describe,
  expect,
  setDefaultTimeout,
  test,
} from "bun:test";
import { randomUUID } from "node:crypto";
import { createServer } from "node:net";
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  type Message,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { DataSource } from "typeorm";
import {
  ProcessWagerTransaction,
  type WagerSubmission,
} from "../../src/application/use-cases/process-wager-transaction";
import { WalletService } from "../../src/application/use-cases/wallet-service";
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../src/domain/wagering/wager-transaction";
import { readEnvironment } from "../../src/infrastructure/config/environment";
import { DeadLetterMessageRecorder } from "../../src/infrastructure/messaging/dead-letter-message-recorder";
import { DeadLetterOperationsService } from "../../src/infrastructure/messaging/dead-letter-operations.service";
import { DeadLetterQueueConsumer } from "../../src/infrastructure/messaging/dead-letter-queue-consumer";
import { DeadLetterRetentionService } from "../../src/infrastructure/messaging/dead-letter-retention.service";
import { SqsQueueService } from "../../src/infrastructure/messaging/sqs-queue.service";
import { WagerInboxConsumer } from "../../src/infrastructure/messaging/wager-inbox-consumer";
import { MetricsService } from "../../src/infrastructure/observability/metrics.service";
import { StructuredLogger } from "../../src/infrastructure/observability/structured-logger";
import { OutboxPublisher } from "../../src/infrastructure/outbox/outbox-publisher";
import { dataSourceOptions } from "../../src/infrastructure/persistence/data-source";
import { PendingReferenceWorker } from "../../src/infrastructure/workers/pending-reference-worker";

const integrationEnabled = process.env.RUN_INTEGRATION_TESTS === "true";
const productionRedriveEnabled =
  process.env.RUN_PRODUCTION_REDRIVE_TESTS === "true";
const runIfEnabled = integrationEnabled ? describe : describe.skip;
const environment = readEnvironment();
setDefaultTimeout(30_000);

runIfEnabled("PostgreSQL financial processing", () => {
  const source = new DataSource(dataSourceOptions);
  const metrics = new MetricsService();
  const logger = new StructuredLogger();
  let processor: ProcessWagerTransaction;
  let wallets: WalletService;
  let queue: SqsQueueService;
  let deadLetterOperations: DeadLetterOperationsService;
  let sqsInspector: SQSClient | undefined;

  beforeAll(async () => {
    await source.initialize();
    await source.runMigrations({ transaction: "all" });
    processor = new ProcessWagerTransaction(source, metrics, logger);
    wallets = new WalletService(source, metrics, logger);
    queue = new SqsQueueService(environment, metrics);
    deadLetterOperations = new DeadLetterOperationsService(
      source,
      metrics,
      logger,
    );
    sqsInspector = new SQSClient({
      region: environment.awsRegion,
      endpoint: environment.sqsEndpoint,
      credentials: {
        accessKeyId: environment.awsAccessKeyId,
        secretAccessKey: environment.awsSecretAccessKey,
      },
    });
    await queue.assertReachable();
    await drainQueue(environment.transactionQueueUrl);
    await drainQueue(environment.deadLetterQueueUrl);
    await drainQueue(environment.eventQueueUrl);
  }, 90_000);

  afterAll(async () => {
    if (source.isInitialized) await source.destroy();
    sqsInspector?.destroy();
    await queue.close();
  });

  async function createWallet(balance = "100.00") {
    return wallets.create({
      playerId: randomUUID(),
      initialBalance: { amount: balance, currency: "BRL" },
    });
  }

  function submission(
    wallet: { id: string; playerId: string },
    overrides: Partial<WagerSubmission> = {},
  ): WagerSubmission {
    const id = randomUUID();
    return {
      providerId: "integration-provider",
      externalTransactionId: `external-${id}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `round-${id}`,
      gameId: "integration-game",
      kind: WagerTransactionKind.Bet,
      money: { amount: "10.00", currency: "BRL" },
      idempotencyKey: `key-${id}`,
      ...overrides,
    };
  }

  async function captureRejection(
    operation: Promise<unknown>,
  ): Promise<unknown> {
    try {
      await operation;
    } catch (error) {
      return error;
    }
    throw new Error("Expected operation to reject");
  }

  test("configures the transaction queue redrive policy and visibility", async () => {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    const attributes = await sqsInspector.send(
      new GetQueueAttributesCommand({
        QueueUrl: readEnvironment().transactionQueueUrl,
        AttributeNames: ["RedrivePolicy", "VisibilityTimeout"],
      }),
    );
    const redrivePolicy = JSON.parse(
      attributes.Attributes?.RedrivePolicy ?? "{}",
    ) as { deadLetterTargetArn?: string; maxReceiveCount?: string };
    expect(attributes.Attributes?.VisibilityTimeout).toBe("60");
    expect(redrivePolicy.maxReceiveCount).toBe("5");
    expect(redrivePolicy.deadLetterTargetArn).toContain(
      "wager-transactions-dlq.fifo",
    );
  });

  test("the financial schema rejects the ambiguous FAILED status", async () => {
    const wallet = await createWallet("20.00");
    const result = await processor.execute(submission(wallet));
    const error = await captureRejection(
      source.query(
        "UPDATE wager_transactions SET status = 'FAILED' WHERE id = $1",
        [result.transactionId],
      ),
    );
    expect(error).toBeInstanceOf(Error);
    const rows = (await source.query(
      "SELECT status FROM wager_transactions WHERE id = $1",
      [result.transactionId],
    )) as Array<{ status: string }>;
    expect(rows[0]?.status).toBe("PROCESSED");
  });

  if (productionRedriveEnabled) {
    test("redrives through the production queue policy without changing visibility settings", async () => {
      if (!sqsInspector) throw new Error("SQS inspector was not initialized");
      await drainQueue(environment.transactionQueueUrl);
      await drainQueue(environment.deadLetterQueueUrl);
      const attributes = await sqsInspector.send(
        new GetQueueAttributesCommand({
          QueueUrl: environment.transactionQueueUrl,
          AttributeNames: ["RedrivePolicy", "VisibilityTimeout"],
        }),
      );
      const policy = JSON.parse(
        attributes.Attributes?.RedrivePolicy ?? "{}",
      ) as { deadLetterTargetArn?: string; maxReceiveCount?: string };
      expect(attributes.Attributes?.VisibilityTimeout).toBe("60");
      expect(policy.maxReceiveCount).toBe("5");
      expect(policy.deadLetterTargetArn).toContain(
        "wager-transactions-dlq.fifo",
      );

      const wallet = await createWallet("100.00");
      const request = submission(wallet, {
        money: { amount: "9.00", currency: "BRL" },
      });
      const sent = await queue.enqueueTransaction(request);
      await source.query(
        "DROP TRIGGER IF EXISTS fail_test_production_redrive ON wallet_ledger_entries",
      );
      await source.query(
        "DROP FUNCTION IF EXISTS fail_test_production_redrive()",
      );
      await source.query(`
        CREATE FUNCTION fail_test_production_redrive() RETURNS trigger AS $$
        BEGIN
          IF NEW.wallet_id = '${wallet.id}'::uuid THEN
            RAISE EXCEPTION 'injected transient ledger failure';
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
      `);
      await source.query(`
        CREATE TRIGGER fail_test_production_redrive
        BEFORE INSERT ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION fail_test_production_redrive()
      `);

      const inboxConsumer = new WagerInboxConsumer(
        queue,
        processor,
        metrics,
        logger,
        deadLetterOperations,
      );
      const deadLetterConsumer = new DeadLetterQueueConsumer(
        queue,
        new DeadLetterMessageRecorder(source),
        metrics,
        logger,
      );
      const receiveCounts: number[] = [];
      let registered = false;
      const deadline = Date.now() + 110_000;
      try {
        while (Date.now() < deadline && !registered) {
          const transactionMessages = await sqsInspector.send(
            new ReceiveMessageCommand({
              QueueUrl: environment.transactionQueueUrl,
              MaxNumberOfMessages: 1,
              WaitTimeSeconds: 1,
              MessageSystemAttributeNames: ["ApproximateReceiveCount"],
            }),
          );
          const transactionMessage = transactionMessages.Messages?.[0];
          if (transactionMessage) {
            receiveCounts.push(
              Number.parseInt(
                transactionMessage.Attributes?.ApproximateReceiveCount ?? "0",
                10,
              ),
            );
            await inboxConsumer.handleMessage(transactionMessage);
          }

          const deadLetters = await sqsInspector.send(
            new ReceiveMessageCommand({
              QueueUrl: environment.deadLetterQueueUrl,
              MaxNumberOfMessages: 1,
              WaitTimeSeconds: 0,
              MessageSystemAttributeNames: [
                "ApproximateReceiveCount",
                "DeadLetterQueueSourceArn",
              ],
              MessageAttributeNames: ["All"],
            }),
          );
          const deadLetter = deadLetters.Messages?.[0];
          if (deadLetter) {
            await deadLetterConsumer.handleMessage(deadLetter);
            registered = true;
          }
        }
      } finally {
        await source.query(
          "DROP TRIGGER IF EXISTS fail_test_production_redrive ON wallet_ledger_entries",
        );
        await source.query(
          "DROP FUNCTION IF EXISTS fail_test_production_redrive()",
        );
      }

      expect(registered).toBe(true);
      expect(receiveCounts).toEqual([1, 2, 3, 4, 5]);
      const operationalRecord = (await source.query(
        `SELECT failure_type, failure_code, status FROM dead_letter_messages
         WHERE source_queue = 'wager-transactions.fifo' AND message_id = $1`,
        [sent.messageId],
      )) as Array<{
        failure_type: string;
        failure_code: string;
        status: string;
      }>;
      expect(operationalRecord).toEqual([
        {
          failure_type: "RETRY_EXHAUSTED",
          failure_code: "MAX_RECEIVE_COUNT_EXCEEDED",
          status: "NEEDS_INVESTIGATION",
        },
      ]);
      const financialState = (await source.query(
        `SELECT wallet.balance::text AS balance,
                (SELECT count(*) FROM wager_transactions
                 WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
                (SELECT count(*) FROM wallet_ledger_entries
                 WHERE wallet_id = $1 AND direction = 'DEBIT')::text AS debits
         FROM wallets AS wallet WHERE wallet.id = $1`,
        [wallet.id, request.providerId, request.idempotencyKey],
      )) as Array<{ balance: string; transactions: string; debits: string }>;
      expect(financialState[0]).toEqual({
        balance: "100.00",
        transactions: "0",
        debits: "0",
      });
    }, 120_000);
  }

  test("locks a wallet so only one of two concurrent 80 BRL bets can debit 100 BRL", async () => {
    const wallet = await createWallet();
    const common = {
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: "race-round",
      gameId: "game",
    };
    const [first, second] = await Promise.all([
      processor.execute(
        submission(wallet, {
          ...common,
          externalTransactionId: randomUUID(),
          idempotencyKey: randomUUID(),
          money: { amount: "80.00", currency: "BRL" },
        }),
      ),
      processor.execute(
        submission(wallet, {
          ...common,
          externalTransactionId: randomUUID(),
          idempotencyKey: randomUUID(),
          money: { amount: "80.00", currency: "BRL" },
        }),
      ),
    ]);
    expect([first.status, second.status].sort()).toEqual(
      [
        WagerTransactionStatus.Processed,
        WagerTransactionStatus.Rejected,
      ].sort(),
    );
    expect((await wallets.get(wallet.id)).balance.amount).toBe("20.00");
    const ledger = (await source.query(
      "SELECT count(*)::text AS count FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'",
      [wallet.id],
    )) as Array<{ count: string }>;
    expect(ledger[0]?.count).toBe("1");
  });

  test("returns the original response snapshot for retries and conflicts on changed payload", async () => {
    const wallet = await createWallet("200.00");
    const request = submission(wallet, {
      money: { amount: "40.00", currency: "BRL" },
    });
    const first = await processor.execute(request);
    await processor.execute(
      submission(wallet, { money: { amount: "20.00", currency: "BRL" } }),
    );
    const replay = await processor.execute(request);
    expect(first.balance.amount).toBe("160.00");
    expect(replay.balance.amount).toBe("160.00");
    expect(replay.transactionId).toBe(first.transactionId);
    expect(replay.idempotentReplay).toBe(true);
    let payloadMismatch: unknown;
    try {
      await processor.execute({
        ...request,
        money: { amount: "41.00", currency: "BRL" },
      });
    } catch (error) {
      payloadMismatch = error;
    }
    expect(payloadMismatch).toMatchObject({
      code: "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH",
    });
    let externalConflict: unknown;
    try {
      await processor.execute({ ...request, idempotencyKey: randomUUID() });
    } catch (error) {
      externalConflict = error;
    }
    expect(externalConflict).toMatchObject({
      code: "EXTERNAL_TRANSACTION_CONFLICT",
    });
  });

  test("enforces wallet uniqueness and ledger immutability in PostgreSQL", async () => {
    const playerId = randomUUID();
    const wallet = await wallets.create({
      playerId,
      initialBalance: { amount: "10.00", currency: "BRL" },
    });
    expect(
      await captureRejection(
        wallets.create({
          playerId,
          initialBalance: { amount: "11.00", currency: "BRL" },
        }),
      ),
    ).toMatchObject({ code: "WALLET_ALREADY_EXISTS" });

    const entries = (await source.query(
      "SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1",
      [wallet.id],
    )) as Array<{ id: string }>;
    expect(entries).toHaveLength(1);
    expect(
      await captureRejection(
        source.query(
          "UPDATE wallet_ledger_entries SET amount = 9.00 WHERE id = $1",
          [entries[0]?.id],
        ),
      ),
    ).toBeInstanceOf(Error);
    expect(
      await captureRejection(
        source.query("DELETE FROM wallet_ledger_entries WHERE id = $1", [
          entries[0]?.id,
        ]),
      ),
    ).toBeInstanceOf(Error);
  });

  test("rolls back wallet, transaction, inbox, ledger and outbox atomically", async () => {
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "10.00", currency: "BRL" },
    });
    const inbox = {
      consumerName: "atomicity-consumer",
      messageId: randomUUID(),
    };
    const outboxBefore = (await source.query(
      "SELECT count(*)::text AS count FROM outbox_messages WHERE aggregate_id = $1",
      [wallet.id],
    )) as Array<{ count: string }>;

    await source.query(`
      CREATE FUNCTION fail_test_ledger_insert() RETURNS trigger AS $$
      BEGIN
        IF NEW.wallet_id = '${wallet.id}'::uuid THEN
          RAISE EXCEPTION 'injected ledger failure';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);
    await source.query(`
      CREATE TRIGGER fail_test_ledger_insert
      BEFORE INSERT ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION fail_test_ledger_insert()
    `);
    try {
      expect(
        await captureRejection(processor.execute(request, inbox)),
      ).toMatchObject({ retryable: true });
    } finally {
      await source.query(
        "DROP TRIGGER IF EXISTS fail_test_ledger_insert ON wallet_ledger_entries",
      );
      await source.query("DROP FUNCTION IF EXISTS fail_test_ledger_insert()");
    }

    const state = (await source.query(
      `SELECT wallet.balance::text AS balance,
              (SELECT count(*) FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
              (SELECT count(*) FROM inbox_messages WHERE consumer_name = $4 AND message_id = $5)::text AS inbox,
              (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1)::text AS ledger,
              (SELECT count(*) FROM outbox_messages WHERE aggregate_id = $1)::text AS outbox
       FROM wallets AS wallet WHERE wallet.id = $1`,
      [
        wallet.id,
        request.providerId,
        request.idempotencyKey,
        inbox.consumerName,
        inbox.messageId,
      ],
    )) as Array<{
      balance: string;
      transactions: string;
      inbox: string;
      ledger: string;
      outbox: string;
    }>;
    expect(state[0]).toMatchObject({
      balance: "100.00",
      transactions: "0",
      inbox: "0",
      ledger: "1",
    });
    expect(state[0]?.outbox).toBe(outboxBefore[0]?.count);
  });

  test("reconciliation reports a mismatch without silently changing balance", async () => {
    const wallet = await createWallet("100.00");
    await source.query("UPDATE wallets SET balance = 99.00 WHERE id = $1", [
      wallet.id,
    ]);

    const result = await wallets.reconcile(wallet.id);

    expect(result).toMatchObject({
      storedBalance: { amount: "99.00", currency: "BRL" },
      calculatedBalance: { amount: "100.00", currency: "BRL" },
      difference: { amount: "-1.00", currency: "BRL" },
      consistent: false,
    });
    expect((await wallets.get(wallet.id)).balance.amount).toBe("99.00");
    await source.query("UPDATE wallets SET balance = 100.00 WHERE id = $1", [
      wallet.id,
    ]);
  });

  test("records LOSS without changing wallet version or creating a balance event", async () => {
    const wallet = await createWallet("30.00");
    const loss = await processor.execute(
      submission(wallet, {
        kind: WagerTransactionKind.Loss,
        money: { amount: "12.00", currency: "BRL" },
      }),
    );
    const state = (await source.query(
      `SELECT balance::text AS balance, version FROM wallets WHERE id = $1`,
      [wallet.id],
    )) as Array<{ balance: string; version: number }>;
    const eventCounts = (await source.query(
      `SELECT count(*) FILTER (WHERE event_type = 'WagerTransactionProcessed')::text AS processed,
              count(*) FILTER (WHERE event_type = 'WalletBalanceChanged')::text AS changed
       FROM outbox_messages WHERE aggregate_id = $1`,
      [wallet.id],
    )) as Array<{ processed: string; changed: string }>;

    expect(loss.status).toBe(WagerTransactionStatus.Processed);
    expect(state[0]).toEqual({ balance: "30.00", version: 1 });
    expect(eventCounts[0]).toEqual({ processed: "2", changed: "1" });
  });

  test("rejects duplicate refunds and insufficient-balance rollbacks distinctly", async () => {
    const refundWallet = await createWallet("100.00");
    const bet = submission(refundWallet, {
      externalTransactionId: `bet-${randomUUID()}`,
      roundId: "reversal-round",
      money: { amount: "20.00", currency: "BRL" },
    });
    await processor.execute(bet);
    const refund = submission(refundWallet, {
      kind: WagerTransactionKind.Refund,
      externalTransactionId: `refund-${randomUUID()}`,
      referenceExternalTransactionId: bet.externalTransactionId,
      roundId: bet.roundId,
      money: { amount: "20.00", currency: "BRL" },
    });
    const firstRefund = await processor.execute(refund);
    const duplicateRefund = await processor.execute({
      ...refund,
      externalTransactionId: `refund-${randomUUID()}`,
      idempotencyKey: `key-${randomUUID()}`,
    });

    expect(firstRefund.status).toBe(WagerTransactionStatus.Processed);
    expect(duplicateRefund).toMatchObject({
      status: WagerTransactionStatus.Rejected,
      failureCode: "REFERENCE_ALREADY_REVERSED",
    });
    expect((await wallets.get(refundWallet.id)).balance.amount).toBe("100.00");

    const rollbackWallet = await createWallet("0.00");
    const winExternalTransactionId = `win-${randomUUID()}`;
    await processor.execute(
      submission(rollbackWallet, {
        kind: WagerTransactionKind.Win,
        externalTransactionId: winExternalTransactionId,
        roundId: "rollback-round",
        money: { amount: "50.00", currency: "BRL" },
      }),
    );
    await processor.execute(
      submission(rollbackWallet, {
        roundId: "rollback-round",
        money: { amount: "40.00", currency: "BRL" },
      }),
    );
    const rollback = await processor.execute(
      submission(rollbackWallet, {
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: winExternalTransactionId,
        roundId: "rollback-round",
        money: { amount: "50.00", currency: "BRL" },
      }),
    );
    expect(rollback).toMatchObject({
      status: WagerTransactionStatus.Rejected,
      failureCode: "ROLLBACK_INSUFFICIENT_BALANCE",
    });
    expect((await wallets.get(rollbackWallet.id)).balance.amount).toBe("10.00");
  });

  test("deduplicates 50 concurrent deliveries with a persistent idempotency key", async () => {
    const wallet = await createWallet("500.00");
    const request = submission(wallet, {
      money: { amount: "7.00", currency: "BRL" },
    });
    const results = await Promise.all(
      Array.from({ length: 50 }, () => processor.execute(request)),
    );
    expect(new Set(results.map((result) => result.transactionId)).size).toBe(1);
    expect((await wallets.get(wallet.id)).balance.amount).toBe("493.00");
    const ledger = (await source.query(
      "SELECT count(*)::text AS count FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'",
      [wallet.id],
    )) as Array<{ count: string }>;
    expect(ledger[0]?.count).toBe("1");
  });

  test("deduplicates 50 submissions across three independent Bun processes", async () => {
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "7.00", currency: "BRL" },
    });
    const counts = [17, 17, 16];
    const childResults = await Promise.all(
      counts.map((count) => {
        const child = Bun.spawn(
          ["bun", "run", "test/integration/concurrency-process-worker.ts"],
          {
            env: {
              ...Bun.env,
              TEST_WAGER_SUBMISSION: JSON.stringify(request),
              TEST_SUBMISSION_COUNT: String(count),
            },
            stdout: "ignore",
            stderr: "pipe",
          },
        );
        const stderr = new Response(child.stderr).text();
        return child.exited.then(async (exitCode) => ({
          exitCode,
          stderr: await stderr,
        }));
      }),
    );
    const failures = childResults.filter((result) => result.exitCode !== 0);
    if (failures.length > 0) {
      throw new Error(
        `Concurrent submission worker failed: ${failures.map((failure) => failure.stderr).join("; ")}`,
      );
    }
    expect(childResults.map((result) => result.exitCode)).toEqual([0, 0, 0]);
    expect((await wallets.get(wallet.id)).balance.amount).toBe("93.00");
    const transactions = (await source.query(
      `SELECT count(*)::text AS count FROM wager_transactions
       WHERE provider_id = $1 AND idempotency_key = $2`,
      [request.providerId, request.idempotencyKey],
    )) as Array<{ count: string }>;
    expect(transactions[0]?.count).toBe("1");
    const ledger = (await source.query(
      "SELECT count(*)::text AS count FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'",
      [wallet.id],
    )) as Array<{ count: string }>;
    expect(ledger[0]?.count).toBe("1");
  }, 60_000);

  test("keeps inbox, ledger and outbox idempotent after a committed message is redelivered", async () => {
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "9.00", currency: "BRL" },
    });
    const inbox = {
      consumerName: "integration-consumer",
      messageId: randomUUID(),
    };
    const first = await processor.execute(request, inbox);
    const replay = await processor.execute(request, inbox);
    expect(replay).toMatchObject({
      transactionId: first.transactionId,
      status: first.status,
      balance: first.balance,
      idempotentReplay: true,
      duplicateMessage: true,
    });
    expect((await wallets.get(wallet.id)).balance.amount).toBe("91.00");
    const counts = (await source.query(
      `SELECT (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT')::text AS ledger,
              (SELECT count(*) FROM inbox_messages WHERE consumer_name = $2 AND message_id = $3 AND processed_at IS NOT NULL)::text AS inbox`,
      [wallet.id, inbox.consumerName, inbox.messageId],
    )) as Array<{ ledger: string; inbox: string }>;
    expect(counts[0]).toEqual({ ledger: "1", inbox: "1" });
  });

  test("reprocesses an out-of-order refund once its bet is committed", async () => {
    const wallet = await createWallet("100.00");
    const reference = `bet-${randomUUID()}`;
    const roundId = randomUUID();
    const refund = submission(wallet, {
      kind: WagerTransactionKind.Refund,
      externalTransactionId: `refund-${randomUUID()}`,
      referenceExternalTransactionId: reference,
      roundId,
      money: { amount: "20.00", currency: "BRL" },
    });
    const pending = await processor.execute(refund);
    expect(pending.status).toBe(WagerTransactionStatus.PendingReference);
    const bet = submission(wallet, {
      externalTransactionId: reference,
      roundId,
      money: { amount: "20.00", currency: "BRL" },
    });
    await processor.execute(bet);
    await source.query(
      "UPDATE wager_transactions SET reference_next_attempt_at = now() WHERE id = $1",
      [pending.transactionId],
    );
    const referenceWorker = new PendingReferenceWorker(
      source,
      processor,
      metrics,
      logger,
    );
    await referenceWorker.retryBatch();
    expect((await wallets.get(wallet.id)).balance.amount).toBe("100.00");
    const result = await source.query(
      "SELECT status FROM wager_transactions WHERE id = $1",
      [pending.transactionId],
    );
    expect((result as Array<{ status: string }>)[0]?.status).toBe(
      WagerTransactionStatus.Processed,
    );
  });

  test("redelivers a real FIFO message after commit without duplicating its debit", async () => {
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "11.00", currency: "BRL" },
    });
    const sent = await queue.enqueueTransaction(request);
    const first = await receiveMessageById(sent.messageId);
    const envelope = JSON.parse(first.Body ?? "{}") as {
      messageId: string;
      data: WagerSubmission;
    };
    await processor.execute(envelope.data, {
      consumerName: "wager-transaction-consumer",
      messageId: envelope.messageId,
    });

    // Simulate a worker dying after SQL commit but before DeleteMessage.
    await queue.changeTransactionVisibility(first.ReceiptHandle as string, 0);
    const redelivery = await receiveMessageById(sent.messageId);
    const redeliveredEnvelope = JSON.parse(redelivery.Body ?? "{}") as {
      messageId: string;
      data: WagerSubmission;
    };
    const duplicate = await processor.execute(redeliveredEnvelope.data, {
      consumerName: "wager-transaction-consumer",
      messageId: redeliveredEnvelope.messageId,
    });
    expect(duplicate.duplicateMessage).toBe(true);
    await queue.deleteTransaction(redelivery.ReceiptHandle as string);
    expect((await wallets.get(wallet.id)).balance.amount).toBe("89.00");
    const ledger = (await source.query(
      "SELECT count(*)::text AS count FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'",
      [wallet.id],
    )) as Array<{ count: string }>;
    expect(ledger[0]?.count).toBe("1");
  });

  test("restarts the HTTP process after commit and replays the original result", async () => {
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "10.00", currency: "BRL" },
    });
    const { idempotencyKey, ...body } = request;
    const port = await findAvailablePort();
    const url = `http://127.0.0.1:${port}/wagering/transactions`;
    const childEnvironment = {
      ...Bun.env,
      PORT: String(port),
      WORKERS_ENABLED: "false",
    };
    let firstProcess: ReturnType<typeof Bun.spawn> | undefined;
    let restartedProcess: ReturnType<typeof Bun.spawn> | undefined;
    let committedTransactionId = "";

    try {
      firstProcess = Bun.spawn(
        ["bun", "run", "test/integration/http-crash-process-worker.ts"],
        {
          env: { ...childEnvironment, TEST_CRASH_AFTER_COMMIT: "true" },
          stdout: "ignore",
          stderr: "ignore",
        },
      );
      await waitForHttpReady(firstProcess, port);
      const lostResponse = await Promise.allSettled([
        fetch(url, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "idempotency-key": idempotencyKey,
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(10_000),
        }),
      ]);
      expect(lostResponse[0]?.status).toBe("rejected");
      expect(await firstProcess.exited).toBe(86);

      const committed = (await source.query(
        `SELECT transaction.id, transaction.status, transaction.response_snapshot,
                wallet.balance::text AS balance, wallet.version,
                (SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id = transaction.id)::text AS ledger_count,
                (SELECT count(*) FROM outbox_messages WHERE payload->'data'->>'transactionId' = transaction.id::text)::text AS outbox_count
         FROM wager_transactions AS transaction
         JOIN wallets AS wallet ON wallet.id = transaction.wallet_id
         WHERE transaction.provider_id = $1 AND transaction.idempotency_key = $2`,
        [request.providerId, idempotencyKey],
      )) as Array<{
        id: string;
        status: string;
        response_snapshot: Record<string, unknown>;
        balance: string;
        version: number;
        ledger_count: string;
        outbox_count: string;
      }>;
      expect(committed).toHaveLength(1);
      committedTransactionId = committed[0]?.id ?? "";
      expect(committed[0]).toMatchObject({
        status: WagerTransactionStatus.Processed,
        balance: "90.00",
        version: 2,
        ledger_count: "1",
        outbox_count: "2",
      });
      expect(committed[0]?.response_snapshot).toMatchObject({
        status: WagerTransactionStatus.Processed,
        balance: { amount: "90.00", currency: "BRL" },
      });

      restartedProcess = Bun.spawn(
        ["bun", "run", "test/integration/http-crash-process-worker.ts"],
        {
          env: childEnvironment,
          stdout: "ignore",
          stderr: "ignore",
        },
      );
      await waitForHttpReady(restartedProcess, port);
      const replayResponse = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": idempotencyKey,
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(10_000),
      });
      expect(replayResponse.status).toBe(200);
      expect(await replayResponse.json()).toMatchObject({
        transactionId: committed[0]?.id,
        status: WagerTransactionStatus.Processed,
        balance: { amount: "90.00", currency: "BRL" },
        idempotentReplay: true,
      });
    } finally {
      for (const child of [restartedProcess, firstProcess]) {
        if (child) child.kill();
      }
      if (restartedProcess) await restartedProcess.exited;
    }

    const finalState = (await source.query(
      `SELECT wallet.balance::text AS balance, wallet.version,
              (SELECT count(*) FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
              (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT')::text AS debits,
              (SELECT count(*) FROM outbox_messages WHERE payload->'data'->>'transactionId' = $4)::text AS outbox
       FROM wallets AS wallet WHERE wallet.id = $1`,
      [wallet.id, request.providerId, idempotencyKey, committedTransactionId],
    )) as Array<{
      balance: string;
      version: number;
      transactions: string;
      debits: string;
      outbox: string;
    }>;
    expect(finalState[0]).toEqual({
      balance: "90.00",
      version: 2,
      transactions: "1",
      debits: "1",
      outbox: "2",
    });
  }, 60_000);

  test("runs the real inbox consumer through a failed ack and SQS redelivery", async () => {
    await drainQueue(environment.transactionQueueUrl);
    await drainQueue(environment.deadLetterQueueUrl);
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "11.00", currency: "BRL" },
    });
    const sent = await queue.enqueueTransaction(request);
    let stopping = false;

    class FailFirstAckQueue extends SqsQueueService {
      readonly receiveCounts: string[] = [];
      private failedAck = false;

      override async receiveTransactions(): Promise<Message[]> {
        const messages = await super.receiveTransactions();
        this.receiveCounts.push(
          ...messages.map(
            (message) => message.Attributes?.ApproximateReceiveCount ?? "",
          ),
        );
        return messages;
      }

      override async deleteTransaction(receiptHandle: string): Promise<void> {
        if (!this.failedAck) {
          this.failedAck = true;
          throw new Error("Injected failure before SQS acknowledgement");
        }
        await super.deleteTransaction(receiptHandle);
        stopping = true;
      }

      override async changeTransactionVisibility(
        receiptHandle: string,
        _seconds: number,
      ): Promise<void> {
        await super.changeTransactionVisibility(receiptHandle, 0);
      }
    }

    const consumerQueue = new FailFirstAckQueue(environment, metrics);
    try {
      const consumer = new WagerInboxConsumer(
        consumerQueue,
        processor,
        metrics,
        logger,
        deadLetterOperations,
      );
      await consumer.runUntilStopped(() => stopping);
      expect(consumerQueue.receiveCounts).toEqual(["1", "2"]);
      expect(await consumerQueue.deadLetterVisibleCount()).toBe(0);
    } finally {
      await consumerQueue.close();
    }

    const state = (await source.query(
      `SELECT wallet.balance::text AS balance, wallet.version,
              (SELECT count(*) FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
              (SELECT count(*) FROM inbox_messages WHERE consumer_name = 'wager-transaction-consumer' AND message_id = $4 AND processed_at IS NOT NULL)::text AS inbox,
              (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT')::text AS debits,
              (SELECT count(*) FROM outbox_messages WHERE payload->'data'->>'transactionId' =
                (SELECT id::text FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3))::text AS outbox
       FROM wallets AS wallet WHERE wallet.id = $1`,
      [wallet.id, request.providerId, request.idempotencyKey, sent.messageId],
    )) as Array<{
      balance: string;
      version: number;
      transactions: string;
      inbox: string;
      debits: string;
      outbox: string;
    }>;
    expect(state[0]).toEqual({
      balance: "89.00",
      version: 2,
      transactions: "1",
      inbox: "1",
      debits: "1",
      outbox: "2",
    });
  }, 60_000);

  test("restarts the real worker after commit and before SQS acknowledgement", async () => {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    const inspector = sqsInspector;
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "8.00", currency: "BRL" },
    });
    const isolatedQueueName = `worker-restart-${randomUUID()}.fifo`;
    const createQueueResult = await inspector.send(
      new CreateQueueCommand({
        QueueName: isolatedQueueName,
        Attributes: {
          FifoQueue: "true",
          ContentBasedDeduplication: "false",
          VisibilityTimeout: "60",
        },
      }),
    );
    const isolatedQueueUrl = createQueueResult.QueueUrl;
    if (!isolatedQueueUrl)
      throw new Error("Could not create isolated FIFO queue");
    const isolatedQueue = new SqsQueueService(
      { ...environment, transactionQueueUrl: isolatedQueueUrl },
      metrics,
    );
    const sent = await isolatedQueue.enqueueTransaction(request);
    const metricsPort = await findAvailablePort();
    const childEnvironment = {
      ...Bun.env,
      SQS_TRANSACTION_QUEUE_URL: isolatedQueueUrl,
      WORKERS_ENABLED: "true",
      METRICS_PORT: String(metricsPort),
    };
    let firstWorker: ReturnType<typeof Bun.spawn> | undefined;
    let restartedWorker: ReturnType<typeof Bun.spawn> | undefined;

    try {
      firstWorker = Bun.spawn(
        ["bun", "run", "test/integration/sqs-crash-process-worker.ts"],
        {
          env: { ...childEnvironment, TEST_CRASH_BEFORE_SQS_ACK: "true" },
          stdout: "ignore",
          stderr: "ignore",
        },
      );
      expect(await waitForChildExit(firstWorker, 20_000)).toBe(87);

      const committed = (await source.query(
        `SELECT transaction.id, transaction.status, wallet.balance::text AS balance,
                wallet.version,
                (SELECT count(*) FROM inbox_messages
                 WHERE consumer_name = 'wager-transaction-consumer' AND message_id = $2 AND processed_at IS NOT NULL)::text AS inbox,
                (SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id = transaction.id)::text AS ledger,
                (SELECT count(*) FROM outbox_messages WHERE payload->'data'->>'transactionId' = transaction.id::text)::text AS outbox
         FROM wager_transactions AS transaction
         JOIN wallets AS wallet ON wallet.id = transaction.wallet_id
         WHERE transaction.provider_id = $1 AND transaction.idempotency_key = $3`,
        [request.providerId, sent.messageId, request.idempotencyKey],
      )) as Array<{
        id: string;
        status: string;
        balance: string;
        version: number;
        inbox: string;
        ledger: string;
        outbox: string;
      }>;
      expect(committed).toHaveLength(1);
      expect(committed[0]).toMatchObject({
        status: WagerTransactionStatus.Processed,
        balance: "92.00",
        version: 2,
        inbox: "1",
        ledger: "1",
        outbox: "2",
      });

      restartedWorker = Bun.spawn(
        ["bun", "run", "test/integration/sqs-crash-process-worker.ts"],
        {
          env: { ...childEnvironment, TEST_EXIT_AFTER_SQS_ACK: "true" },
          stdout: "ignore",
          stderr: "ignore",
        },
      );
      expect(await waitForChildExit(restartedWorker, 20_000)).toBe(88);
      await waitForQueueEmpty(inspector, isolatedQueueUrl, 20_000);
      const replayState = (await source.query(
        `SELECT wallet.balance::text AS balance, wallet.version,
                (SELECT count(*) FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
                (SELECT count(*) FROM inbox_messages
                 WHERE consumer_name = 'wager-transaction-consumer' AND message_id = $4 AND processed_at IS NOT NULL)::text AS inbox,
                (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT')::text AS debits,
                (SELECT count(*) FROM outbox_messages WHERE payload->'data'->>'transactionId' = $5)::text AS outbox
         FROM wallets AS wallet WHERE wallet.id = $1`,
        [
          wallet.id,
          request.providerId,
          request.idempotencyKey,
          sent.messageId,
          committed[0]?.id,
        ],
      )) as Array<{
        balance: string;
        version: number;
        transactions: string;
        inbox: string;
        debits: string;
        outbox: string;
      }>;
      expect(replayState[0]).toEqual({
        balance: "92.00",
        version: 2,
        transactions: "1",
        inbox: "1",
        debits: "1",
        outbox: "2",
      });
    } finally {
      for (const worker of [restartedWorker, firstWorker]) {
        if (worker) worker.kill();
      }
      if (restartedWorker) await restartedWorker.exited;
      if (firstWorker) await firstWorker.exited;
      await isolatedQueue.close();
      await inspector.send(
        new DeleteQueueCommand({ QueueUrl: isolatedQueueUrl }),
      );
    }
  }, 90_000);

  test("records invalid DLQ messages idempotently and keeps them after persistence failures", async () => {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    await drainQueue(environment.transactionQueueUrl);
    await drainQueue(environment.deadLetterQueueUrl);
    const wallet = await createWallet("100.00");
    const invalidRequest = submission(wallet, {
      kind: WagerTransactionKind.Opening,
    });
    const messageId = randomUUID();
    const invalidBody = JSON.stringify({
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      diagnosticToken: "test-secret",
      data: {
        ...invalidRequest,
        diagnosticToken: "test-secret",
        money: { ...invalidRequest.money, diagnosticToken: "test-secret" },
      },
    });
    await queue.enqueueDeadLetter(invalidBody, messageId, {
      failureType: "INVALID_MESSAGE",
      failureCode: "INVALID_MESSAGE",
      sourceQueue: "wager-transactions.fifo",
    });

    await source.query(
      "DROP TRIGGER IF EXISTS fail_test_dead_letter_record ON dead_letter_messages",
    );
    await source.query(
      "DROP FUNCTION IF EXISTS fail_test_dead_letter_record()",
    );
    await source.query(`
      CREATE FUNCTION fail_test_dead_letter_record() RETURNS trigger AS $$
      BEGIN
        RAISE EXCEPTION 'injected dead-letter registry failure';
      END;
      $$ LANGUAGE plpgsql
    `);
    await source.query(`
      CREATE TRIGGER fail_test_dead_letter_record
      AFTER INSERT ON dead_letter_messages
      FOR EACH ROW EXECUTE FUNCTION fail_test_dead_letter_record()
    `);

    class FailFirstDeadLetterAckQueue extends SqsQueueService {
      private failFirstAck = true;

      override async deleteDeadLetter(receiptHandle: string): Promise<void> {
        if (this.failFirstAck) {
          this.failFirstAck = false;
          throw new Error("Injected failure before dead-letter ACK");
        }
        await super.deleteDeadLetter(receiptHandle);
      }

      override async changeDeadLetterVisibility(
        receiptHandle: string,
        _seconds: number,
      ): Promise<void> {
        await super.changeDeadLetterVisibility(receiptHandle, 0);
      }
    }

    const deadLetterQueue = new FailFirstDeadLetterAckQueue(
      environment,
      metrics,
    );
    const deadLetterConsumer = new DeadLetterQueueConsumer(
      deadLetterQueue,
      new DeadLetterMessageRecorder(source),
      metrics,
      logger,
    );
    try {
      const firstDelivery = await receiveDeadLetter(messageId, 10_000);
      expect(firstDelivery).toBeDefined();
      expect(firstDelivery?.MessageAttributes?.failureType?.StringValue).toBe(
        "INVALID_MESSAGE",
      );
      await deadLetterConsumer.handleMessage(firstDelivery as Message);

      const afterRegistryFailure = (await source.query(
        `SELECT count(*)::text AS count FROM dead_letter_messages
         WHERE source_queue = 'wager-transactions.fifo' AND message_id = $1`,
        [messageId],
      )) as Array<{ count: string }>;
      expect(afterRegistryFailure[0]?.count).toBe("0");

      await source.query(
        "DROP TRIGGER IF EXISTS fail_test_dead_letter_record ON dead_letter_messages",
      );
      await source.query(
        "DROP FUNCTION IF EXISTS fail_test_dead_letter_record()",
      );

      const secondDelivery = await receiveDeadLetter(messageId, 10_000);
      expect(secondDelivery).toBeDefined();
      await deadLetterConsumer.handleMessage(secondDelivery as Message);
      const afterAckFailure = (await source.query(
        `SELECT count(*)::text AS count, max(delivery_count)::integer AS deliveries
         FROM dead_letter_messages
         WHERE source_queue = 'wager-transactions.fifo' AND message_id = $1`,
        [messageId],
      )) as Array<{ count: string; deliveries: number | null }>;
      expect(afterAckFailure[0]).toEqual({ count: "1", deliveries: 1 });

      const redelivery = await receiveDeadLetter(messageId, 10_000);
      expect(redelivery).toBeDefined();
      await deadLetterConsumer.handleMessage(redelivery as Message);

      const deadLetterRecord = (await source.query(
        `SELECT source_queue, message_id, failure_type, failure_code, body_hash,
                payload->'data'->>'idempotencyKey' AS idempotency_key,
                payload->'data'->>'kind' AS kind, status, sqs_receive_count,
                delivery_count, payload
         FROM dead_letter_messages
         WHERE source_queue = 'wager-transactions.fifo' AND message_id = $1`,
        [messageId],
      )) as Array<{
        source_queue: string;
        message_id: string;
        failure_type: string;
        failure_code: string;
        body_hash: string;
        idempotency_key: string;
        kind: string;
        status: string;
        sqs_receive_count: number;
        delivery_count: number;
        payload: Record<string, unknown>;
      }>;
      expect(deadLetterRecord).toHaveLength(1);
      expect(deadLetterRecord[0]).toMatchObject({
        source_queue: "wager-transactions.fifo",
        message_id: messageId,
        failure_type: "INVALID_MESSAGE",
        failure_code: "INVALID_MESSAGE",
        idempotency_key: invalidRequest.idempotencyKey,
        kind: WagerTransactionKind.Opening,
        status: "NEEDS_INVESTIGATION",
        delivery_count: 2,
      });
      expect(deadLetterRecord[0]?.body_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(deadLetterRecord[0]?.sqs_receive_count).toBeGreaterThanOrEqual(3);
      expect(JSON.stringify(deadLetterRecord[0]?.payload)).not.toContain(
        "test-secret",
      );

      const [mainQueue, deadLetterQueueState] = await Promise.all([
        sqsInspector.send(
          new GetQueueAttributesCommand({
            QueueUrl: environment.transactionQueueUrl,
            AttributeNames: ["ApproximateNumberOfMessages"],
          }),
        ),
        sqsInspector.send(
          new GetQueueAttributesCommand({
            QueueUrl: environment.deadLetterQueueUrl,
            AttributeNames: [
              "ApproximateNumberOfMessages",
              "ApproximateNumberOfMessagesNotVisible",
            ],
          }),
        ),
      ]);
      expect(mainQueue.Attributes?.ApproximateNumberOfMessages).toBe("0");
      expect(deadLetterQueueState.Attributes?.ApproximateNumberOfMessages).toBe(
        "0",
      );
      expect(
        deadLetterQueueState.Attributes?.ApproximateNumberOfMessagesNotVisible,
      ).toBe("0");

      const persisted = (await source.query(
        `SELECT wallet.balance::text AS balance, wallet.version,
                (SELECT count(*) FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
                (SELECT count(*) FROM inbox_messages WHERE consumer_name = 'wager-transaction-consumer' AND message_id = $4)::text AS inbox
         FROM wallets AS wallet WHERE wallet.id = $1`,
        [
          wallet.id,
          invalidRequest.providerId,
          invalidRequest.idempotencyKey,
          messageId,
        ],
      )) as Array<{
        balance: string;
        version: number;
        transactions: string;
        inbox: string;
      }>;
      expect(persisted[0]).toEqual({
        balance: "100.00",
        version: 1,
        transactions: "0",
        inbox: "0",
      });
    } finally {
      await source.query(
        "DROP TRIGGER IF EXISTS fail_test_dead_letter_record ON dead_letter_messages",
      );
      await source.query(
        "DROP FUNCTION IF EXISTS fail_test_dead_letter_record()",
      );
      await deadLetterQueue.close();
      await drainQueue(environment.transactionQueueUrl);
      await drainQueue(environment.deadLetterQueueUrl);
    }
  }, 60_000);

  test("records a permanent idempotency conflict without changing the wallet", async () => {
    await drainQueue(environment.transactionQueueUrl);
    await drainQueue(environment.deadLetterQueueUrl);
    const wallet = await createWallet("100.00");
    const original = submission(wallet, {
      money: { amount: "10.00", currency: "BRL" },
    });
    await processor.execute(original);
    const conflicting = submission(wallet, {
      idempotencyKey: original.idempotencyKey,
      money: { amount: "12.00", currency: "BRL" },
    });
    const sent = await queue.enqueueTransaction(conflicting);
    const message = await receiveMessageById(sent.messageId);
    const inboxConsumer = new WagerInboxConsumer(
      queue,
      processor,
      metrics,
      logger,
      deadLetterOperations,
    );
    await inboxConsumer.handleMessage(message);

    const deadLetter = await receiveDeadLetter(sent.messageId, 10_000);
    expect(deadLetter).toBeDefined();
    expect(deadLetter?.MessageAttributes?.failureType?.StringValue).toBe(
      "PERMANENT_FAILURE",
    );
    expect(deadLetter?.MessageAttributes?.failureCode?.StringValue).toBe(
      "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH",
    );
    const deadLetterQueue = new SqsQueueService(environment, metrics);
    try {
      const deadLetterConsumer = new DeadLetterQueueConsumer(
        deadLetterQueue,
        new DeadLetterMessageRecorder(source),
        metrics,
        logger,
      );
      await deadLetterConsumer.handleMessage(deadLetter as Message);
    } finally {
      await deadLetterQueue.close();
    }

    const operationalRecord = (await source.query(
      `SELECT failure_type, failure_code, status, payload->'data'->'money' AS money
       FROM dead_letter_messages
       WHERE source_queue = 'wager-transactions.fifo' AND message_id = $1`,
      [sent.messageId],
    )) as Array<{
      failure_type: string;
      failure_code: string;
      status: string;
      money: { amount: string; currency: string };
    }>;
    expect(operationalRecord).toEqual([
      {
        failure_type: "PERMANENT_FAILURE",
        failure_code: "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH",
        status: "NEEDS_INVESTIGATION",
        money: { amount: "12.00", currency: "BRL" },
      },
    ]);
    const financialState = (await source.query(
      `SELECT wallet.balance::text AS balance, wallet.version,
              (SELECT count(*) FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
              (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT')::text AS debits
       FROM wallets AS wallet WHERE wallet.id = $1`,
      [wallet.id, original.providerId, original.idempotencyKey],
    )) as Array<{
      balance: string;
      version: number;
      transactions: string;
      debits: string;
    }>;
    expect(financialState[0]).toEqual({
      balance: "90.00",
      version: 2,
      transactions: "1",
      debits: "1",
    });
    const replayError = await captureRejection(
      deadLetterOperations.prepareReplay(
        "wager-transactions.fifo",
        sent.messageId,
      ),
    );
    expect(replayError).toBeInstanceOf(Error);
    expect((replayError as Error).message).toContain(
      "Only retry-exhausted messages can be replayed",
    );
  });

  test("replays a retry-exhausted DLQ message through a durable outbox after process restart", async () => {
    await drainQueue(environment.transactionQueueUrl);
    await drainQueue(environment.deadLetterQueueUrl);
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "7.00", currency: "BRL" },
    });
    const messageId = randomUUID();
    const body = JSON.stringify({
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: request,
    });
    await queue.enqueueDeadLetter(body, messageId, {
      failureType: "RETRY_EXHAUSTED",
      failureCode: "MAX_RECEIVE_COUNT_EXCEEDED",
      sourceQueue: "wager-transactions.fifo",
    });

    const dlqQueue = new SqsQueueService(environment, metrics);
    try {
      const deadLetter = await receiveDeadLetter(messageId, 10_000);
      expect(deadLetter).toBeDefined();
      await new DeadLetterQueueConsumer(
        dlqQueue,
        new DeadLetterMessageRecorder(source),
        metrics,
        logger,
      ).handleMessage(deadLetter as Message);
    } finally {
      await dlqQueue.close();
    }

    const prepared = await Promise.allSettled([
      deadLetterOperations.prepareReplay("wager-transactions.fifo", messageId),
      deadLetterOperations.prepareReplay("wager-transactions.fifo", messageId),
    ]);
    expect(
      prepared.filter((result) => result.status === "fulfilled"),
    ).toHaveLength(1);
    expect(
      prepared.filter((result) => result.status === "rejected"),
    ).toHaveLength(1);

    const scheduleProcess = Bun.spawn(
      [
        "bun",
        "run",
        "test/integration/dead-letter-schedule-process.ts",
        "wager-transactions.fifo",
        messageId,
      ],
      {
        env: {
          ...Bun.env,
          DATABASE_URL: environment.databaseUrl,
          RUN_INTEGRATION_TESTS: "true",
        },
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const schedulingStatus = await scheduleProcess.exited;
    const schedulingOutput = await new Response(scheduleProcess.stdout).text();
    const schedulingErrors = await new Response(scheduleProcess.stderr).text();
    if (schedulingStatus !== 0) {
      throw new Error(
        `Replay scheduling process failed: ${schedulingOutput}${schedulingErrors}`,
      );
    }

    const competingSchedules = await Promise.allSettled([
      deadLetterOperations.requestReplay("wager-transactions.fifo", messageId),
      deadLetterOperations.requestReplay("wager-transactions.fifo", messageId),
    ]);
    expect(
      competingSchedules.every((result) => result.status === "rejected"),
    ).toBe(true);

    const replayPublisherProcess = Bun.spawn(
      ["bun", "run", "test/integration/dead-letter-replay-process.ts"],
      {
        env: {
          ...Bun.env,
          DATABASE_URL: environment.databaseUrl,
          SQS_ENDPOINT: environment.sqsEndpoint,
          SQS_TRANSACTION_QUEUE_URL: environment.transactionQueueUrl,
          SQS_DEAD_LETTER_QUEUE_URL: environment.deadLetterQueueUrl,
          SQS_EVENT_QUEUE_URL: environment.eventQueueUrl,
          RUN_INTEGRATION_TESTS: "true",
          WORKERS_ENABLED: "false",
        },
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const replayProcessStatus = await replayPublisherProcess.exited;
    const replayProcessOutput = await new Response(
      replayPublisherProcess.stdout,
    ).text();
    const replayProcessErrors = await new Response(
      replayPublisherProcess.stderr,
    ).text();
    if (replayProcessStatus !== 0) {
      throw new Error(
        `Replay recovery process failed: ${replayProcessOutput}${replayProcessErrors}`,
      );
    }
    expect(replayProcessStatus).toBe(0);
    expect(replayProcessOutput + replayProcessErrors).not.toContain(
      "dead_letter_replay_publish_failed",
    );

    const replayOutbox = (await source.query(
      `SELECT sent_at, body_hash FROM dead_letter_replay_outbox
       WHERE source_queue = 'wager-transactions.fifo' AND message_id = $1`,
      [messageId],
    )) as Array<{ sent_at: Date | null; body_hash: string }>;
    expect(replayOutbox).toHaveLength(1);
    expect(replayOutbox[0]?.sent_at).toBeInstanceOf(Date);
    expect(replayOutbox[0]?.body_hash).toMatch(/^[a-f0-9]{64}$/);

    class FailFirstReplayAckQueue extends SqsQueueService {
      private failNextAck = true;

      override async deleteTransaction(receiptHandle: string): Promise<void> {
        if (this.failNextAck) {
          this.failNextAck = false;
          await super.changeTransactionVisibility(receiptHandle, 0);
          throw new Error("Injected replay ACK failure after commit");
        }
        await super.deleteTransaction(receiptHandle);
      }

      override async changeTransactionVisibility(
        receiptHandle: string,
        _seconds: number,
      ): Promise<void> {
        await super.changeTransactionVisibility(receiptHandle, 0);
      }
    }
    const replayQueue = new FailFirstReplayAckQueue(environment, metrics);
    try {
      const inboxConsumer = new WagerInboxConsumer(
        replayQueue,
        processor,
        metrics,
        logger,
        deadLetterOperations,
      );
      const firstDelivery = await receiveMessageById(messageId);
      expect(
        firstDelivery.MessageAttributes?.deadLetterReplay?.StringValue,
      ).toBe("true");
      expect(
        firstDelivery.MessageAttributes?.deadLetterMessageId?.StringValue,
      ).toBe(messageId);
      await inboxConsumer.handleMessage(firstDelivery);
      const redelivery = await receiveMessageById(messageId);
      expect(redelivery.Attributes?.ApproximateReceiveCount).toBe("2");
      await inboxConsumer.handleMessage(redelivery);
    } finally {
      await replayQueue.close();
    }

    const result = (await source.query(
      `SELECT wallet.balance::text AS balance, wallet.version,
              (SELECT count(*) FROM wager_transactions
               WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
              (SELECT count(*) FROM wallet_ledger_entries
               WHERE wallet_id = $1 AND direction = 'DEBIT')::text AS debits,
              (SELECT count(*) FROM inbox_messages
               WHERE consumer_name = 'wager-transaction-consumer'
                 AND message_id = $4 AND processed_at IS NOT NULL)::text AS inbox,
              (SELECT count(*) FROM dead_letter_replay_outbox
               WHERE source_queue = 'wager-transactions.fifo' AND message_id = $4
                 AND sent_at IS NOT NULL)::text AS replay_sends
       FROM wallets AS wallet WHERE wallet.id = $1`,
      [wallet.id, request.providerId, request.idempotencyKey, messageId],
    )) as Array<{
      balance: string;
      version: number;
      transactions: string;
      debits: string;
      inbox: string;
      replay_sends: string;
    }>;
    expect(result[0]).toEqual({
      balance: "93.00",
      version: 2,
      transactions: "1",
      debits: "1",
      inbox: "1",
      replay_sends: "1",
    });
    const statusRows = (await source.query(
      `SELECT status FROM dead_letter_messages
       WHERE source_queue = 'wager-transactions.fifo' AND message_id = $1`,
      [messageId],
    )) as Array<{ status: string }>;
    expect(statusRows[0]?.status).toBe("REPROCESSED");
  }, 60_000);

  test("retains only old resolved DLQ records and protects pending investigation or replay", async () => {
    const ids = {
      expired: randomUUID(),
      recent: randomUUID(),
      investigating: randomUUID(),
      reprocessing: randomUUID(),
      failed: randomUUID(),
      ready: randomUUID(),
      unresolved: randomUUID(),
    };
    const oldDate = new Date(Date.now() - 120 * 24 * 60 * 60 * 1_000);
    for (const [messageId, status, resolvedAt] of [
      [ids.expired, "RESOLVED", oldDate],
      [ids.recent, "RESOLVED", new Date()],
      [ids.investigating, "NEEDS_INVESTIGATION", oldDate],
      [ids.reprocessing, "REPROCESSING", oldDate],
      [ids.failed, "REPROCESS_FAILED", oldDate],
      [ids.ready, "READY_FOR_REPROCESS", oldDate],
    ] as const) {
      await source.query(
        `INSERT INTO dead_letter_messages (
           source_queue, message_id, failure_type, failure_code, body_hash,
           sqs_receive_count, status, first_received_at, last_received_at, resolved_at
         ) VALUES ('retention-test', $1, 'UNKNOWN', 'TEST', $2, 1, $3, $4, $4, $5)`,
        [messageId, "a".repeat(64), status, oldDate, resolvedAt],
      );
    }
    const unresolvedWallet = await createWallet("25.00");
    const unresolvedRequest = submission(unresolvedWallet, {
      externalTransactionId: `retention-pending-${ids.unresolved}`,
      idempotencyKey: `retention-pending-${ids.unresolved}`,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: `missing-${ids.unresolved}`,
    });
    const unresolvedResult = await processor.execute(unresolvedRequest);
    expect(unresolvedResult.status).toBe(
      WagerTransactionStatus.PendingReference,
    );
    await source.query(
      `INSERT INTO dead_letter_messages (
         source_queue, message_id, failure_type, failure_code, body_hash,
         payload, sqs_receive_count, status, first_received_at, last_received_at,
         resolved_at
       ) VALUES (
         'retention-test', $1, 'RETRY_EXHAUSTED', 'TEST', $2, $3::jsonb,
         1, 'REPROCESSED', $4, $4, $4
       )`,
      [
        ids.unresolved,
        "b".repeat(64),
        JSON.stringify({
          messageId: ids.unresolved,
          type: "WagerTransactionRequested",
          occurredAt: oldDate.toISOString(),
          data: unresolvedRequest,
        }),
        oldDate,
      ],
    );
    const retention = new DeadLetterRetentionService(
      source,
      {
        ...environment,
        deadLetterRetentionDays: 90,
        deadLetterRetentionBatchSize: 1,
      },
      metrics,
      logger,
    );
    const deleted = await retention.purgeExpired();
    expect(deleted).toBeGreaterThanOrEqual(1);
    const remaining = (await source.query(
      `SELECT message_id, status FROM dead_letter_messages
       WHERE message_id = ANY($1::text[]) ORDER BY message_id`,
      [Object.values(ids)],
    )) as Array<{ message_id: string; status: string }>;
    expect(remaining).toHaveLength(6);
    expect(remaining.map((record) => record.message_id)).not.toContain(
      ids.expired,
    );
    expect(remaining.map((record) => record.message_id).sort()).toEqual(
      [
        ids.recent,
        ids.investigating,
        ids.reprocessing,
        ids.failed,
        ids.ready,
        ids.unresolved,
      ].sort(),
    );
  });

  test("redrives a real transiently failing SQS message to the DLQ after five receives", async () => {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    await drainQueue(environment.transactionQueueUrl);
    await drainQueue(environment.deadLetterQueueUrl);
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "12.00", currency: "BRL" },
    });
    const sent = await queue.enqueueTransaction(request);
    await source.query(
      "DROP TRIGGER IF EXISTS fail_test_dlq_ledger_insert ON wallet_ledger_entries",
    );
    await source.query("DROP FUNCTION IF EXISTS fail_test_dlq_ledger_insert()");
    await source.query(`
      CREATE FUNCTION fail_test_dlq_ledger_insert() RETURNS trigger AS $$
      BEGIN
        IF NEW.wallet_id = '${wallet.id}'::uuid THEN
          RAISE EXCEPTION 'injected transient ledger failure';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);
    await source.query(`
      CREATE TRIGGER fail_test_dlq_ledger_insert
      BEFORE INSERT ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION fail_test_dlq_ledger_insert()
    `);
    let stopping = false;

    class ImmediateRetryQueue extends SqsQueueService {
      readonly receiveCounts: string[] = [];

      override async receiveTransactions(): Promise<Message[]> {
        const messages = await super.receiveTransactions();
        this.receiveCounts.push(
          ...messages.map(
            (message) => message.Attributes?.ApproximateReceiveCount ?? "",
          ),
        );
        return messages;
      }

      override async changeTransactionVisibility(
        receiptHandle: string,
        _seconds: number,
      ): Promise<void> {
        await super.changeTransactionVisibility(receiptHandle, 0);
        if (this.receiveCounts.length >= 5) stopping = true;
      }
    }

    const failingQueue = new ImmediateRetryQueue(environment, metrics);
    let deadLetter: Message | undefined;
    try {
      const consumer = new WagerInboxConsumer(
        failingQueue,
        processor,
        metrics,
        logger,
        deadLetterOperations,
      );
      await consumer.runUntilStopped(() => stopping);
      expect(failingQueue.receiveCounts).toEqual(["1", "2", "3", "4", "5"]);
      let redriveReady = false;
      for (let attempt = 0; attempt < 3 && !redriveReady; attempt += 1) {
        const redrivePoll = await sqsInspector.send(
          new ReceiveMessageCommand({
            QueueUrl: environment.transactionQueueUrl,
            MaxNumberOfMessages: 1,
            WaitTimeSeconds: 0,
            VisibilityTimeout: 0,
            MessageSystemAttributeNames: ["ApproximateReceiveCount"],
          }),
        );
        const sourceMessages = redrivePoll.Messages ?? [];
        if (sourceMessages.length === 0) {
          redriveReady = true;
          break;
        }
        for (const sourceMessage of sourceMessages) {
          const bodyMessageId = (
            JSON.parse(sourceMessage.Body ?? "{}") as { messageId?: string }
          ).messageId;
          expect(bodyMessageId).toBe(sent.messageId);
          if (sourceMessage.ReceiptHandle) {
            await sqsInspector.send(
              new ChangeMessageVisibilityCommand({
                QueueUrl: environment.transactionQueueUrl,
                ReceiptHandle: sourceMessage.ReceiptHandle,
                VisibilityTimeout: 0,
              }),
            );
          }
        }
      }
      expect(redriveReady).toBe(true);
      deadLetter = await receiveDeadLetter(sent.messageId, 15_000);
      expect(deadLetter).toBeDefined();
      expect(deadLetter?.Attributes?.DeadLetterQueueSourceArn).toContain(
        "wager-transactions.fifo",
      );
      const deadLetterQueue = new SqsQueueService(environment, metrics);
      try {
        const deadLetterConsumer = new DeadLetterQueueConsumer(
          deadLetterQueue,
          new DeadLetterMessageRecorder(source),
          metrics,
          logger,
        );
        await deadLetterConsumer.handleMessage(deadLetter as Message);
      } finally {
        await deadLetterQueue.close();
      }
      const mainQueue = await sqsInspector.send(
        new GetQueueAttributesCommand({
          QueueUrl: environment.transactionQueueUrl,
          AttributeNames: [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
          ],
        }),
      );
      expect(mainQueue.Attributes?.ApproximateNumberOfMessages).toBe("0");
      expect(mainQueue.Attributes?.ApproximateNumberOfMessagesNotVisible).toBe(
        "0",
      );
    } finally {
      await source.query(
        "DROP TRIGGER IF EXISTS fail_test_dlq_ledger_insert ON wallet_ledger_entries",
      );
      await source.query(
        "DROP FUNCTION IF EXISTS fail_test_dlq_ledger_insert()",
      );
      await failingQueue.close();
      if (deadLetter?.ReceiptHandle && sqsInspector) {
        await sqsInspector
          .send(
            new DeleteMessageCommand({
              QueueUrl: environment.deadLetterQueueUrl,
              ReceiptHandle: deadLetter.ReceiptHandle,
            }),
          )
          .catch(() => undefined);
      }
    }

    const financialState = (await source.query(
      `SELECT wallet.balance::text AS balance, wallet.version,
              (SELECT count(*) FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3)::text AS transactions,
              (SELECT count(*) FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3 AND status = 'FAILED')::text AS failed,
              (SELECT count(*) FROM inbox_messages WHERE consumer_name = 'wager-transaction-consumer' AND message_id = $4)::text AS inbox,
              (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT')::text AS debits,
              (SELECT count(*) FROM outbox_messages WHERE payload->'data'->>'transactionId' =
                (SELECT id::text FROM wager_transactions WHERE provider_id = $2 AND idempotency_key = $3))::text AS outbox
       FROM wallets AS wallet WHERE wallet.id = $1`,
      [wallet.id, request.providerId, request.idempotencyKey, sent.messageId],
    )) as Array<{
      balance: string;
      version: number;
      transactions: string;
      failed: string;
      inbox: string;
      debits: string;
      outbox: string;
    }>;
    expect(financialState[0]).toEqual({
      balance: "100.00",
      version: 1,
      transactions: "0",
      failed: "0",
      inbox: "0",
      debits: "0",
      outbox: "0",
    });
    const operationalState = (await source.query(
      `SELECT failure_type, failure_code, status, delivery_count,
              payload->'data'->>'idempotencyKey' AS idempotency_key
       FROM dead_letter_messages
       WHERE source_queue = 'wager-transactions.fifo' AND message_id = $1`,
      [sent.messageId],
    )) as Array<{
      failure_type: string;
      failure_code: string;
      status: string;
      delivery_count: number;
      idempotency_key: string;
    }>;
    expect(operationalState).toEqual([
      {
        failure_type: "RETRY_EXHAUSTED",
        failure_code: "MAX_RECEIVE_COUNT_EXCEEDED",
        status: "NEEDS_INVESTIGATION",
        delivery_count: 1,
        idempotency_key: request.idempotencyKey,
      },
    ]);
  }, 100_000);

  test("claims and publishes committed outbox events using two real publishers", async () => {
    const wallet = await createWallet("13.00");
    const first = new OutboxPublisher(source, queue, metrics, logger);
    const second = new OutboxPublisher(source, queue, metrics, logger);
    await Promise.all([first.publishBatch(), second.publishBatch()]);
    let publishedCount = "0";
    for (
      let attempt = 0;
      attempt < 100 && publishedCount !== "2";
      attempt += 1
    ) {
      const eventRows = (await source.query(
        `SELECT count(*)::text AS count FROM outbox_messages
         WHERE aggregate_id = $1 AND event_type IN ('WagerTransactionProcessed', 'WalletBalanceChanged')
           AND published_at IS NOT NULL`,
        [wallet.id],
      )) as Array<{ count: string }>;
      publishedCount = eventRows[0]?.count ?? "0";
      if (publishedCount !== "2")
        await Promise.all([first.publishBatch(), second.publishBatch()]);
    }
    expect(publishedCount).toBe("2");
    expect(await queue.eventVisibleCount()).toBeGreaterThanOrEqual(2);
  });

  test("recovers an outbox event after SQS accepted it but the database mark failed", async () => {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    await drainQueue(environment.eventQueueUrl);
    const wallet = await createWallet("100.00");
    const request = submission(wallet, {
      money: { amount: "14.00", currency: "BRL" },
    });
    const processed = await processor.execute(request);
    const targetRows = (await source.query(
      `SELECT id FROM outbox_messages
       WHERE payload->'data'->>'transactionId' = $1
         AND event_type = 'WagerTransactionProcessed'`,
      [processed.transactionId],
    )) as Array<{ id: string }>;
    expect(targetRows).toHaveLength(1);
    const targetEventId = targetRows[0]?.id;
    if (!targetEventId) throw new Error("Processed outbox event was not found");

    class FailOnceAfterAcceptedQueue extends SqsQueueService {
      targetSends = 0;
      readonly targetEventIds: string[] = [];
      private failFirstTargetSend = true;

      override async enqueueEvent(
        event: Record<string, unknown>,
        eventId: string,
        aggregateId: string,
      ): Promise<void> {
        await super.enqueueEvent(event, eventId, aggregateId);
        if (eventId !== targetEventId) return;
        this.targetSends += 1;
        this.targetEventIds.push(eventId);
        if (this.failFirstTargetSend) {
          this.failFirstTargetSend = false;
          throw new Error("Injected failure after SQS accepted the event");
        }
      }
    }

    const publisherQueue = new FailOnceAfterAcceptedQueue(environment, metrics);
    const publisher = new OutboxPublisher(
      source,
      publisherQueue,
      metrics,
      logger,
    );
    try {
      let firstAttemptState: Array<{
        attempts: number;
        published_at: Date | null;
      }> = [];
      for (let attempt = 0; attempt < 20; attempt += 1) {
        await publisher.publishBatch();
        firstAttemptState = (await source.query(
          "SELECT attempts, published_at FROM outbox_messages WHERE id = $1",
          [targetEventId],
        )) as Array<{ attempts: number; published_at: Date | null }>;
        if (publisherQueue.targetSends > 0) break;
      }
      expect(publisherQueue.targetSends).toBe(1);
      expect(firstAttemptState[0]?.attempts).toBe(1);
      expect(firstAttemptState[0]?.published_at).toBeNull();

      await source.query(
        "UPDATE outbox_messages SET next_attempt_at = now() WHERE id = $1",
        [targetEventId],
      );
      for (let attempt = 0; attempt < 20; attempt += 1) {
        await publisher.publishBatch();
        const state = (await source.query(
          "SELECT published_at FROM outbox_messages WHERE id = $1",
          [targetEventId],
        )) as Array<{ published_at: Date | null }>;
        if (state[0]?.published_at) break;
      }
      expect(publisherQueue.targetSends).toBe(2);
      expect(publisherQueue.targetEventIds).toEqual([
        targetEventId,
        targetEventId,
      ]);
      const published = (await source.query(
        "SELECT attempts, published_at FROM outbox_messages WHERE id = $1",
        [targetEventId],
      )) as Array<{ attempts: number; published_at: Date | null }>;
      expect(published[0]?.attempts).toBe(1);
      expect(published[0]?.published_at).toBeInstanceOf(Date);
    } finally {
      await publisherQueue.close();
    }

    const receivedEvents = await receiveAndDeleteAvailable(
      environment.eventQueueUrl,
    );
    const matching = receivedEvents.filter((message) => {
      try {
        return (
          (JSON.parse(message.Body ?? "{}") as { eventId?: string }).eventId ===
          targetEventId
        );
      } catch {
        return false;
      }
    });
    expect(matching).toHaveLength(1);
    const persisted = (await source.query(
      `SELECT wallet.balance::text AS balance,
              (SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id = $2)::text AS ledger_count,
              (SELECT count(*) FROM wager_transactions WHERE id = $2)::text AS transaction_count
       FROM wallets AS wallet WHERE wallet.id = $1`,
      [wallet.id, processed.transactionId],
    )) as Array<{
      balance: string;
      ledger_count: string;
      transaction_count: string;
    }>;
    expect(persisted[0]).toEqual({
      balance: "86.00",
      ledger_count: "1",
      transaction_count: "1",
    });
  }, 60_000);

  test("documents the downstream event contract with persistent eventId deduplication", async () => {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    await drainQueue(environment.eventQueueUrl);
    const wallet = await createWallet("11.00");
    const expectedRows = (await source.query(
      `SELECT id, event_type FROM outbox_messages
       WHERE aggregate_id = $1 AND event_type = 'WalletBalanceChanged'`,
      [wallet.id],
    )) as Array<{ id: string; event_type: string }>;
    expect(expectedRows).toHaveLength(1);
    const eventId = expectedRows[0]?.id;
    if (!eventId) throw new Error("Wallet event was not persisted");
    const publisher = new OutboxPublisher(source, queue, metrics, logger);
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const published = (await source.query(
        "SELECT published_at FROM outbox_messages WHERE id = $1",
        [eventId],
      )) as Array<{ published_at: Date | null }>;
      if (published[0]?.published_at) break;
      await publisher.publishBatch();
    }
    let message: Message | undefined;
    const messageDeadline = Date.now() + 10_000;
    while (Date.now() < messageDeadline && !message) {
      const messages = await receiveAndDeleteAvailable(
        environment.eventQueueUrl,
      );
      message = messages.find((entry) => {
        try {
          return (
            (JSON.parse(entry.Body ?? "{}") as { eventId?: string }).eventId ===
            eventId
          );
        } catch {
          return false;
        }
      });
      if (!message) await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(message).toBeDefined();
    const event = JSON.parse(message?.Body ?? "{}") as {
      eventId?: string;
      eventType?: string;
      aggregateId?: string;
      version?: number;
      data?: Record<string, unknown>;
    };
    expect(event).toMatchObject({
      eventId,
      eventType: "WalletBalanceChanged",
      aggregateId: wallet.id,
      version: 1,
    });

    const runner = source.createQueryRunner();
    await runner.connect();
    try {
      await runner.query(
        "CREATE TEMP TABLE downstream_receipts (event_id text PRIMARY KEY)",
      );
      await runner.query(
        "CREATE TEMP TABLE downstream_effects (event_id text PRIMARY KEY)",
      );
      let injectFailureAfterReceipt = true;
      const applyDownstreamEvent = async (): Promise<boolean> => {
        await runner.startTransaction();
        try {
          const inserted = (await runner.query(
            `INSERT INTO downstream_receipts (event_id) VALUES ($1)
             ON CONFLICT (event_id) DO NOTHING
             RETURNING event_id`,
            [event.eventId],
          )) as Array<{ event_id: string }>;
          if (inserted.length > 0) {
            if (injectFailureAfterReceipt) {
              injectFailureAfterReceipt = false;
              throw new Error("Injected failure after event receipt insert");
            }
            await runner.query(
              "INSERT INTO downstream_effects (event_id) VALUES ($1)",
              [event.eventId],
            );
          }
          await runner.commitTransaction();
          return inserted.length > 0;
        } catch (error) {
          if (runner.isTransactionActive) await runner.rollbackTransaction();
          throw error;
        }
      };

      const failedDelivery = await captureRejection(applyDownstreamEvent());
      expect(failedDelivery).toBeInstanceOf(Error);
      const rolledBack = (await runner.query(
        `SELECT (SELECT count(*)::text FROM downstream_receipts) AS receipts,
                (SELECT count(*)::text FROM downstream_effects) AS effects`,
      )) as Array<{ receipts: string; effects: string }>;
      expect(rolledBack[0]).toEqual({ receipts: "0", effects: "0" });
      expect(await applyDownstreamEvent()).toBe(true);
      expect(await applyDownstreamEvent()).toBe(false);
      const counts = (await runner.query(
        `SELECT (SELECT count(*)::text FROM downstream_receipts) AS receipts,
                (SELECT count(*)::text FROM downstream_effects) AS effects`,
      )) as Array<{ receipts: string; effects: string }>;
      expect(counts[0]).toEqual({ receipts: "1", effects: "1" });
    } finally {
      await runner.release();
    }
  }, 60_000);

  async function receiveMessageById(messageId: string): Promise<Message> {
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const messages = await queue.receiveTransactions();
      const matching = messages.find((message) => {
        try {
          return (
            (JSON.parse(message.Body ?? "{}") as { messageId?: string })
              .messageId === messageId
          );
        } catch {
          return false;
        }
      });
      if (matching?.ReceiptHandle) return matching;
      for (const message of messages) {
        if (message.ReceiptHandle)
          await queue.changeTransactionVisibility(message.ReceiptHandle, 0);
      }
    }
    throw new Error(`SQS message ${messageId} was not received`);
  }

  async function drainQueue(queueUrl: string): Promise<void> {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    const deadline = Date.now() + 75_000;
    while (Date.now() < deadline) {
      const messages = await sqsInspector.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
          VisibilityTimeout: 30,
        }),
      );
      const received = messages.Messages ?? [];
      if (received.length > 0) {
        await Promise.all(
          received.map((message) =>
            message.ReceiptHandle
              ? sqsInspector?.send(
                  new DeleteMessageCommand({
                    QueueUrl: queueUrl,
                    ReceiptHandle: message.ReceiptHandle,
                  }),
                )
              : Promise.resolve(),
          ),
        );
        continue;
      }
      const attributes = await sqsInspector.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
          ],
        }),
      );
      if (
        attributes.Attributes?.ApproximateNumberOfMessages === "0" &&
        attributes.Attributes?.ApproximateNumberOfMessagesNotVisible === "0"
      ) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error(`Could not drain test queue ${queueUrl}`);
  }

  async function receiveAndDeleteAvailable(
    queueUrl: string,
  ): Promise<Message[]> {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    const received: Message[] = [];
    for (let batch = 0; batch < 100; batch += 1) {
      const result = await sqsInspector.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
          VisibilityTimeout: 30,
        }),
      );
      const messages = result.Messages ?? [];
      if (messages.length === 0) return received;
      received.push(...messages);
      await Promise.all(
        messages.map((message) =>
          message.ReceiptHandle
            ? sqsInspector?.send(
                new DeleteMessageCommand({
                  QueueUrl: queueUrl,
                  ReceiptHandle: message.ReceiptHandle,
                }),
              )
            : Promise.resolve(),
        ),
      );
    }
    throw new Error(`Queue ${queueUrl} did not become empty`);
  }

  async function receiveDeadLetter(
    messageId: string,
    timeoutMs: number,
  ): Promise<Message | undefined> {
    if (!sqsInspector) throw new Error("SQS inspector was not initialized");
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const result = await sqsInspector.send(
        new ReceiveMessageCommand({
          QueueUrl: environment.deadLetterQueueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 1,
          MessageSystemAttributeNames: [
            "ApproximateReceiveCount",
            "DeadLetterQueueSourceArn",
          ],
          MessageAttributeNames: ["All"],
        }),
      );
      for (const message of result.Messages ?? []) {
        let bodyMessageId: string | undefined;
        try {
          bodyMessageId =
            (
              JSON.parse(message.Body ?? "{}") as {
                eventId?: string;
                messageId?: string;
              }
            ).eventId ??
            (JSON.parse(message.Body ?? "{}") as { messageId?: string })
              .messageId;
        } catch {
          bodyMessageId = undefined;
        }
        if (bodyMessageId === messageId) return message;
        if (message.ReceiptHandle) {
          await sqsInspector.send(
            new ChangeMessageVisibilityCommand({
              QueueUrl: environment.deadLetterQueueUrl,
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: 0,
            }),
          );
        }
        throw new Error(
          `Received unexpected DLQ message ${bodyMessageId ?? message.MessageId ?? "without an identifier"}; expected ${messageId}`,
        );
      }
    }
    return undefined;
  }
});

async function findAvailablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("Could not allocate an HTTP worker port");
  }
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
  return address.port;
}

async function waitForHttpReady(
  child: ReturnType<typeof Bun.spawn>,
  port: number,
): Promise<void> {
  const deadline = Date.now() + 15_000;
  let exited = false;
  void child.exited.then(() => {
    exited = true;
  });
  while (Date.now() < deadline) {
    if (exited) {
      throw new Error("HTTP worker exited before becoming ready");
    }
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health/ready`, {
        signal: AbortSignal.timeout(1_000),
      });
      if (response.ok) return;
    } catch {
      // The health endpoint provides the readiness synchronization for this child.
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error("HTTP worker did not become ready within 15 seconds");
}

async function waitForChildExit(
  child: ReturnType<typeof Bun.spawn>,
  timeoutMs: number,
): Promise<number> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      child.exited,
      new Promise<number>((_, reject) => {
        timer = setTimeout(
          () =>
            reject(new Error(`Child process did not exit in ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function waitForQueueEmpty(
  inspector: SQSClient,
  queueUrl: string,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await inspector.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      }),
    );
    if (
      result.Attributes?.ApproximateNumberOfMessages === "0" &&
      result.Attributes?.ApproximateNumberOfMessagesNotVisible === "0"
    ) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`SQS queue did not drain within ${timeoutMs}ms`);
}
