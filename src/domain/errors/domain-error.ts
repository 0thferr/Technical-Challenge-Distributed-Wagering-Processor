export class DomainError extends Error {
  constructor(
    message: string,
    public readonly code: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class InvalidMoneyError extends DomainError {
  constructor(message: string) {
    super(message, "INVALID_MONEY");
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, received: string) {
    super(
      `Currency mismatch: expected ${expected}, received ${received}`,
      "CURRENCY_MISMATCH",
    );
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor(status: string) {
    super(
      `Transaction in terminal state ${status} cannot transition`,
      "INVALID_TRANSACTION_STATE",
    );
  }
}

export class InvalidLedgerEntryError extends DomainError {
  constructor(message: string) {
    super(message, "INVALID_LEDGER_ENTRY");
  }
}
