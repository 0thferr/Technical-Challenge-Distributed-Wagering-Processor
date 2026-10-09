import {
  CurrencyMismatchError,
  InvalidMoneyError,
} from "../errors/domain-error";

export interface MoneyProps {
  amount: string;
  currency: string;
}

// PostgreSQL numeric(20, 2) stores at most 18 integer digits and 2 decimals.
const MAX_MINOR_UNITS = 99_999_999_999_999_999_999n;

/** Exact money represented as integer minor units. The public wire format stays decimal text. */
export class Money {
  private constructor(
    private readonly minorUnits: bigint,
    public readonly currency: string,
  ) {
    if (minorUnits > MAX_MINOR_UNITS || minorUnits < -MAX_MINOR_UNITS) {
      throw new InvalidMoneyError(
        "Amount exceeds the supported precision of numeric(20, 2)",
      );
    }
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    if (!/^[A-Z]{3}$/.test(props.currency)) {
      throw new InvalidMoneyError(
        "Currency must be a three-letter uppercase code",
      );
    }
    if (!/^-?\d+(?:\.\d{1,2})?$/.test(props.amount)) {
      throw new InvalidMoneyError(
        "Amount must be a plain decimal with at most two fractional digits",
      );
    }

    const negative = props.amount.startsWith("-");
    const unsigned = negative ? props.amount.slice(1) : props.amount;
    const [whole = "0", fractional = ""] = unsigned.split(".");
    const minorUnits = BigInt(whole) * 100n + BigInt(fractional.padEnd(2, "0"));
    return new Money(negative ? -minorUnits : minorUnits, props.currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: "0.00", currency });
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits + other.minorUnits, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits - other.minorUnits, this.currency);
  }

  negate(): Money {
    return new Money(-this.minorUnits, this.currency);
  }

  isZero(): boolean {
    return this.minorUnits === 0n;
  }

  isPositive(): boolean {
    return this.minorUnits > 0n;
  }

  isNegative(): boolean {
    return this.minorUnits < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits < other.minorUnits;
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits === other.minorUnits;
  }

  /** Input contracts for wager operations call this before persistence. */
  assertNonNegative(): void {
    if (this.isNegative())
      throw new InvalidMoneyError("Amount cannot be negative");
  }

  assertPositive(): void {
    if (!this.isPositive())
      throw new InvalidMoneyError("Amount must be greater than zero");
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  toString(): string {
    const negative = this.minorUnits < 0n;
    const absolute = negative ? -this.minorUnits : this.minorUnits;
    const whole = absolute / 100n;
    const fractional = (absolute % 100n).toString().padStart(2, "0");
    return `${negative ? "-" : ""}${whole}.${fractional}`;
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}
