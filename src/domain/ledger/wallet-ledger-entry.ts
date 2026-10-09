import { InvalidLedgerEntryError } from "../errors/domain-error";
import type { Money } from "../money/money";

export enum LedgerDirection {
  Debit = "DEBIT",
  Credit = "CREDIT",
}

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export class WalletLedgerEntry {
  private readonly createdAtEpochMs: number;

  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    createdAt: Date,
  ) {
    this.createdAtEpochMs = createdAt.getTime();
    Object.freeze(this);
  }

  get createdAt(): Date {
    return new Date(this.createdAtEpochMs);
  }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    props.money.assertPositive();
    if (props.balanceBefore.isNegative()) {
      throw new InvalidLedgerEntryError(
        "Ledger balance before the entry cannot be negative",
      );
    }
    const expected =
      props.direction === LedgerDirection.Credit
        ? props.balanceBefore.add(props.money)
        : props.balanceBefore.subtract(props.money);
    if (
      !expected.equals(props.balanceAfter) ||
      props.balanceAfter.isNegative()
    ) {
      throw new InvalidLedgerEntryError(
        "Ledger balance equation does not hold",
      );
    }
    return new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt,
    );
  }

  static rehydrate(state: CreateLedgerEntryProps): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  isBalanced(): boolean {
    const expected =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }
}
