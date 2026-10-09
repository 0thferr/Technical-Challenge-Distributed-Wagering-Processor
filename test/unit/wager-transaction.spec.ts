import { describe, expect, test } from "bun:test";
import {
  DomainError,
  InvalidTransactionStateError,
} from "../../src/domain/errors/domain-error";
import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../../src/domain/ledger/wallet-ledger-entry";
import { Money } from "../../src/domain/money/money";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../src/domain/wagering/wager-transaction";

const makeTransaction = (
  kind: WagerTransactionKind,
  referenceExternalTransactionId?: string,
) =>
  WagerTransaction.create({
    id: "txn-1",
    providerId: "provider-1",
    externalTransactionId: "external-1",
    idempotencyKey: "key-1",
    payloadHash: "a".repeat(64),
    walletId: "wallet-1",
    playerId: "player-1",
    roundId: "round-1",
    gameId: "game-1",
    kind,
    money: Money.from({ amount: "10.00", currency: "BRL" }),
    ...(referenceExternalTransactionId
      ? { referenceExternalTransactionId }
      : {}),
    createdAt: new Date("2026-01-01T00:00:00.000Z"),
  });

describe("WagerTransaction", () => {
  test("requires references for refund and rollback and forbids external opening", () => {
    expect(() => makeTransaction(WagerTransactionKind.Refund)).toThrow(
      DomainError,
    );
    expect(() => makeTransaction(WagerTransactionKind.Rollback)).toThrow(
      DomainError,
    );
    expect(() => makeTransaction(WagerTransactionKind.Opening)).toThrow(
      DomainError,
    );
  });

  test("allows pending reference to transition to a terminal state exactly once", () => {
    const transaction = makeTransaction(WagerTransactionKind.Refund, "bet-1");
    transaction.markPendingReference();
    expect(transaction.status).toBe(WagerTransactionStatus.PendingReference);
    transaction.markProcessed("bet-internal", new Date());
    expect(transaction.isTerminal()).toBe(true);
    expect(() => transaction.reject("SOME_FAILURE")).toThrow(
      InvalidTransactionStateError,
    );
  });

  test("keeps only persisted business outcomes as terminal states", () => {
    expect(Object.values(WagerTransactionStatus)).toEqual([
      WagerTransactionStatus.Pending,
      WagerTransactionStatus.PendingReference,
      WagerTransactionStatus.Processed,
      WagerTransactionStatus.Rejected,
    ]);
    const transaction = makeTransaction(WagerTransactionKind.Bet);
    transaction.reject("INSUFFICIENT_BALANCE");
    expect(transaction.isTerminal()).toBe(true);
    expect(() => transaction.markProcessed(undefined, new Date())).toThrow(
      InvalidTransactionStateError,
    );
  });

  test("maps wager operations to the expected ledger directions", () => {
    const bet = makeTransaction(WagerTransactionKind.Bet);
    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(makeTransaction(WagerTransactionKind.Win).ledgerDirectionFor()).toBe(
      LedgerDirection.Credit,
    );
    expect(
      makeTransaction(
        WagerTransactionKind.Refund,
        "bet-1",
      ).ledgerDirectionFor(),
    ).toBe(LedgerDirection.Credit);
    expect(
      makeTransaction(
        WagerTransactionKind.Rollback,
        "bet-1",
      ).ledgerDirectionFor(bet),
    ).toBe(LedgerDirection.Credit);
    expect(
      makeTransaction(
        WagerTransactionKind.Rollback,
        "win-1",
      ).ledgerDirectionFor(makeTransaction(WagerTransactionKind.Win)),
    ).toBe(LedgerDirection.Debit);
    expect(makeTransaction(WagerTransactionKind.Loss).affectsBalance()).toBe(
      false,
    );
  });
});

describe("WalletLedgerEntry", () => {
  test("validates its exact balance equation and is immutable", () => {
    const money = Money.from({ amount: "25.00", currency: "BRL" });
    const createdAt = new Date("2026-01-01T00:00:00.000Z");
    const entry = WalletLedgerEntry.create({
      id: "entry-1",
      walletId: "wallet-1",
      transactionId: "txn-1",
      direction: LedgerDirection.Debit,
      money,
      balanceBefore: Money.from({ amount: "100.00", currency: "BRL" }),
      balanceAfter: Money.from({ amount: "75.00", currency: "BRL" }),
      createdAt,
    });
    expect(entry.isBalanced()).toBe(true);
    expect(Object.isFrozen(entry)).toBe(true);
    createdAt.setUTCFullYear(2030);
    const exposedDate = entry.createdAt;
    exposedDate.setUTCFullYear(2040);
    expect(entry.createdAt.toISOString()).toBe("2026-01-01T00:00:00.000Z");
    expect(() =>
      WalletLedgerEntry.create({
        id: "entry-2",
        walletId: "wallet-1",
        transactionId: "txn-2",
        direction: LedgerDirection.Debit,
        money,
        balanceBefore: Money.from({ amount: "100.00", currency: "BRL" }),
        balanceAfter: Money.from({ amount: "90.00", currency: "BRL" }),
        createdAt: new Date(),
      }),
    ).toThrow(DomainError);

    expect(() =>
      WalletLedgerEntry.create({
        id: "entry-3",
        walletId: "wallet-1",
        transactionId: "txn-3",
        direction: LedgerDirection.Credit,
        money: Money.from({ amount: "20.00", currency: "BRL" }),
        balanceBefore: Money.from({ amount: "-10.00", currency: "BRL" }),
        balanceAfter: Money.from({ amount: "10.00", currency: "BRL" }),
        createdAt: new Date(),
      }),
    ).toThrow(DomainError);
  });
});
