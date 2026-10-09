import { describe, expect, test } from "bun:test";
import { DomainError } from "../../src/domain/errors/domain-error";
import { Money } from "../../src/domain/money/money";
import { Wallet } from "../../src/domain/wallet/wallet";

describe("Wallet", () => {
  test("debits and credits with an auditable immutable ledger entry", () => {
    const now = new Date("2026-01-01T00:00:00.000Z");
    const wallet = Wallet.open({
      id: "wallet-1",
      playerId: "player-1",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      at: now,
    });
    const bet = wallet.debit({
      id: "entry-1",
      transactionId: "txn-1",
      money: Money.from({ amount: "80.00", currency: "BRL" }),
      at: now,
    });
    expect(bet.balanceBefore.toString()).toBe("100.00");
    expect(bet.balanceAfter.toString()).toBe("20.00");
    expect(wallet.balance.toString()).toBe("20.00");
    expect(wallet.version).toBe(2);
    expect(Object.isFrozen(bet)).toBe(true);
  });

  test("rejects insufficient balance and currency mismatch without changing state", () => {
    const wallet = Wallet.open({
      id: "wallet-1",
      playerId: "player-1",
      initialBalance: Money.from({ amount: "10.00", currency: "BRL" }),
    });
    expect(() =>
      wallet.debit({
        id: "entry-1",
        transactionId: "txn-1",
        money: Money.from({ amount: "10.01", currency: "BRL" }),
        at: new Date(),
      }),
    ).toThrow(DomainError);
    expect(() =>
      wallet.credit({
        id: "entry-2",
        transactionId: "txn-2",
        money: Money.from({ amount: "1.00", currency: "USD" }),
        at: new Date(),
      }),
    ).toThrow(DomainError);
    expect(wallet.balance.toString()).toBe("10.00");
    expect(wallet.version).toBe(1);
  });
});
