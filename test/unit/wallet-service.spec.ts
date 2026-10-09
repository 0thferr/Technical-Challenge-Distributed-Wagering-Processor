import { describe, expect, mock, test } from "bun:test";
import type { DataSource } from "typeorm";
import { ApplicationError } from "../../src/application/application-error";
import { WalletService } from "../../src/application/use-cases/wallet-service";
import { MetricsService } from "../../src/infrastructure/observability/metrics.service";
import { StructuredLogger } from "../../src/infrastructure/observability/structured-logger";

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
        statusCode: 400,
      } satisfies Partial<ApplicationError>),
    );
  });
});
