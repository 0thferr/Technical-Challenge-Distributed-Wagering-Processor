import { DomainError } from "../errors/domain-error";
import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../ledger/wallet-ledger-entry";
import type { Money } from "../money/money";

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: {
    id: string;
    playerId: string;
    initialBalance: Money;
    at?: Date;
  }): Wallet {
    props.initialBalance.assertNonNegative();
    const now = props.at ?? new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      now,
    );
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  debit(entry: {
    id: string;
    transactionId: string;
    money: Money;
    at: Date;
  }): WalletLedgerEntry {
    this.assertSameCurrency(entry.money);
    entry.money.assertPositive();
    if (this._balance.isLessThan(entry.money)) {
      throw new DomainError(
        "Wallet has insufficient balance",
        "INSUFFICIENT_BALANCE",
      );
    }
    return this.apply(
      entry.id,
      entry.transactionId,
      entry.money,
      LedgerDirection.Debit,
      entry.at,
    );
  }

  credit(entry: {
    id: string;
    transactionId: string;
    money: Money;
    at: Date;
  }): WalletLedgerEntry {
    this.assertSameCurrency(entry.money);
    entry.money.assertPositive();
    return this.apply(
      entry.id,
      entry.transactionId,
      entry.money,
      LedgerDirection.Credit,
      entry.at,
    );
  }

  private apply(
    id: string,
    transactionId: string,
    money: Money,
    direction: LedgerDirection,
    at: Date,
  ): WalletLedgerEntry {
    const before = this._balance;
    const after =
      direction === LedgerDirection.Credit
        ? before.add(money)
        : before.subtract(money);
    const entry = WalletLedgerEntry.create({
      id,
      walletId: this.id,
      transactionId,
      direction,
      money,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: at,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = at;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new DomainError(
        "Wallet currency does not match transaction currency",
        "CURRENCY_MISMATCH",
      );
    }
  }
}
