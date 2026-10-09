export interface IntegrationEventProps<T> {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
  data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId: string | undefined;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt;
    this.data = Object.freeze({ ...(props.data as object) }) as Readonly<T>;
  }

  toJSON(): {
    eventId: string;
    eventType: string;
    aggregateId: string;
    correlationId: string;
    causationId?: string;
    occurredAt: string;
    version: number;
    data: Readonly<T>;
  } {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}

export interface TransactionEventData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  kind: string;
  status: string;
  money: { amount: string; currency: string };
  referenceExternalTransactionId?: string;
  failureCode?: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;
  constructor(props: IntegrationEventProps<TransactionEventData>) {
    super(props);
  }
}

export class WagerTransactionRejected extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;
  constructor(props: IntegrationEventProps<TransactionEventData>) {
    super(props);
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;
  constructor(props: IntegrationEventProps<TransactionEventData>) {
    super(props);
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: { amount: string; currency: string };
  balanceBefore: { amount: string; currency: string };
  balanceAfter: { amount: string; currency: string };
  walletVersion: number;
}

import type { LedgerDirection } from "../ledger/wallet-ledger-entry";

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;
  constructor(props: IntegrationEventProps<WalletBalanceChangedData>) {
    super(props);
  }
}
