import { describe, expect, mock, test } from "bun:test";
import type { DataSource } from "typeorm";
import { ApplicationError } from "../../src/application/application-error";
import { WalletService } from "../../src/application/use-cases/wallet-service";
import { MetricsService } from "../../src/infrastructure/observability/metrics.service";
import { StructuredLogger } from "../../src/infrastructure/observability/structured-logger";

describe("WalletService opening records", () => {
  const input = {
    playerId: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    initialBalance: { amount: "25.00", currency: "BRL" },
  };

  function setup(failOnOutbox = false) {
    const operations: string[] = [];
    const query = mock(async (sql: string, _parameters: unknown[]) => {
      const table = /^INSERT INTO (\w+)/.exec(sql)?.[1];
      if (!table) throw new Error("Expected an insert");
      operations.push(table);
      if (failOnOutbox && table === "outbox_messages") {
        throw new Error("Outbox write failed");
      }
    });
    const runner = {
      isTransactionActive: true,
      connect: async () => {},
      startTransaction: async () => {
        operations.push("begin");
      },
      query,
      commitTransaction: async () => {
        operations.push("commit");
      },
      rollbackTransaction: async () => {
        operations.push("rollback");
      },
      release: async () => {
        operations.push("release");
      },
    };
    const service = new WalletService(
      { createQueryRunner: () => runner } as unknown as DataSource,
      new MetricsService(),
      new StructuredLogger(),
    );
    return { service, operations, query };
  }

  test("writes the opening ledger and both events before committing", async () => {
    const { service, operations, query } = setup();
    const wallet = await service.create(input);

    expect(operations).toEqual([
      "begin",
      "wallets",
      "wager_transactions",
      "wallet_ledger_entries",
      "outbox_messages",
      "outbox_messages",
      "commit",
      "release",
    ]);
    const opening = query.mock.calls[1]?.[1];
    const ledger = query.mock.calls[2]?.[1];
    expect(ledger?.slice(1, 8)).toEqual([
      wallet.id,
      opening?.[0],
      "CREDIT",
      "25.00",
      "BRL",
      "0.00",
      "25.00",
    ]);
    const events = query.mock.calls.slice(3).map(([, values]) => values);
    expect(events.map((values) => values[2])).toEqual([
      "WagerTransactionProcessed",
      "WalletBalanceChanged",
    ]);
    for (const [eventId, walletId, eventType, payload] of events) {
      expect(walletId).toBe(wallet.id);
      expect(JSON.parse(String(payload))).toMatchObject({
        eventId,
        aggregateId: wallet.id,
        eventType,
        data: { transactionId: opening?.[0], walletId: wallet.id },
      });
    }
  });

  test("rolls back the opening transaction when an outbox insert fails", async () => {
    const { service, operations } = setup(true);

    await expect(service.create(input)).rejects.toMatchObject({
      code: "TEMPORARY_INFRASTRUCTURE_ERROR",
      retryable: true,
    });
    expect(operations.slice(-3)).toEqual([
      "outbox_messages",
      "rollback",
      "release",
    ]);
    expect(operations).not.toContain("commit");
  });
});

describe("WalletService reconciliation", () => {
  test("compares stored and ledger balances from one database statement", async () => {
    const query = mock(async (_sql: string, _parameters?: unknown[]) => [
      {
        id: "wallet-1",
        player_id: "player-1",
        currency: "BRL",
        stored_balance: "75.00",
        calculated_balance: "75.00",
        entries: "2",
      },
    ]);
    const service = new WalletService(
      { query } as unknown as DataSource,
      new MetricsService(),
      new StructuredLogger(),
    );

    const result = await service.reconcile("wallet-1");

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("LEFT JOIN LATERAL");
    expect(result).toMatchObject({
      storedBalance: { amount: "75.00", currency: "BRL" },
      calculatedBalance: { amount: "75.00", currency: "BRL" },
      consistent: true,
      checkedEntries: 2,
    });
  });

  test("classifies an invalid ledger cursor as a client error", async () => {
    const service = new WalletService(
      {
        query: mock(async (_sql: string, _parameters?: unknown[]) => [
          { id: "wallet-1" },
        ]),
      } as unknown as DataSource,
      new MetricsService(),
      new StructuredLogger(),
    );

    await expect(
      service.ledger("wallet-1", "invalid-cursor"),
    ).rejects.toMatchObject(
      expect.objectContaining({
        code: "INVALID_CURSOR",
        message: "Ledger cursor is invalid",
        statusCode: 400,
      } satisfies Partial<ApplicationError>),
    );
  });
});
