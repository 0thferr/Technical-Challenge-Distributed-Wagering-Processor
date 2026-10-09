import {
  DomainError,
  InvalidTransactionStateError,
} from "../errors/domain-error";
import { LedgerDirection } from "../ledger/wallet-ledger-entry";
import type { Money } from "../money/money";

export enum WagerTransactionKind {
  Opening = "OPENING",
  Bet = "BET",
  Win = "WIN",
  Loss = "LOSS",
  Refund = "REFUND",
  Rollback = "ROLLBACK",
}

export enum WagerTransactionStatus {
  Pending = "PENDING",
  PendingReference = "PENDING_REFERENCE",
  Processed = "PROCESSED",
  Rejected = "REJECTED",
}

export interface WagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: string,
    private _processedAt?: Date,
  ) {}

  static create(props: WagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.Opening) {
      throw new DomainError(
        "OPENING transactions are internal",
        "OPENING_TRANSACTION_FORBIDDEN",
      );
    }
    if (props.providerId === "__internal__") {
      throw new DomainError(
        "The internal provider id is reserved",
        "PROVIDER_ID_RESERVED",
      );
    }
    if (
      (props.kind === WagerTransactionKind.Refund ||
        props.kind === WagerTransactionKind.Rollback) &&
      !props.referenceExternalTransactionId
    ) {
      throw new DomainError(
        "Transaction kind requires a reference",
        "REFERENCE_REQUIRED",
      );
    }
    props.money.assertPositive();
    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
  }

  static opening(props: {
    id: string;
    walletId: string;
    playerId: string;
    money: Money;
    createdAt: Date;
  }): WagerTransaction {
    return new WagerTransaction(
      props.id,
      "__internal__",
      `opening:${props.walletId}`,
      `opening:${props.walletId}`,
      "internal",
      props.walletId,
      props.playerId,
      "__opening__",
      "__opening__",
      WagerTransactionKind.Opening,
      props.money,
      undefined,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    const transaction = new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
    );
    return transaction;
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): string | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  isTerminal(): boolean {
    return [
      WagerTransactionStatus.Processed,
      WagerTransactionStatus.Rejected,
    ].includes(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return (
      this.kind === WagerTransactionKind.Refund ||
      this.kind === WagerTransactionKind.Rollback
    );
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback:
        if (!reference)
          throw new DomainError(
            "Rollback direction needs its reference",
            "REFERENCE_REQUIRED",
          );
        return reference.ledgerDirectionFor() === LedgerDirection.Debit
          ? LedgerDirection.Credit
          : LedgerDirection.Debit;
      case WagerTransactionKind.Loss:
      case WagerTransactionKind.Opening:
        throw new DomainError(
          `${this.kind} does not have an external wager direction`,
          "NO_LEDGER_DIRECTION",
        );
    }
  }

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertCanTransition();
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
  }

  markPendingReference(): void {
    this.assertCanTransition();
    this._status = WagerTransactionStatus.PendingReference;
  }

  reject(code: string): void {
    this.assertCanTransition();
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
  }

  private assertCanTransition(): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError(this._status);
  }
}

export interface WagerTransactionState extends WagerTransactionProps {
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: string;
  processedAt?: Date;
}
