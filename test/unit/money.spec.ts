import { describe, expect, test } from "bun:test";
import {
  CurrencyMismatchError,
  InvalidMoneyError,
} from "../../src/domain/errors/domain-error";
import { Money } from "../../src/domain/money/money";

describe("Money", () => {
  test("normalizes decimal strings and calculates exact minor units", () => {
    const amount = Money.from({
      amount: "9007199254740993.01",
      currency: "BRL",
    });
    expect(
      amount.add(Money.from({ amount: "0.99", currency: "BRL" })).toString(),
    ).toBe("9007199254740994.00");
    expect(Money.from({ amount: "12", currency: "BRL" }).toJSON()).toEqual({
      amount: "12.00",
      currency: "BRL",
    });
    expect(Object.isFrozen(amount)).toBe(true);
    expect(Money.from(amount.toJSON()).equals(amount)).toBe(true);
  });

  test("rejects exponent notation, whitespace, invalid decimals and negative contract values", () => {
    for (const amount of ["", " 1.00", "1e3", "1.001", "NaN", "Infinity"]) {
      expect(() => Money.from({ amount, currency: "BRL" })).toThrow(
        InvalidMoneyError,
      );
    }
    expect(() =>
      Money.from({ amount: "-0.01", currency: "BRL" }).assertNonNegative(),
    ).toThrow(InvalidMoneyError);
  });

  test("requires matching currencies for arithmetic and comparison", () => {
    const brl = Money.from({ amount: "1.00", currency: "BRL" });
    const usd = Money.from({ amount: "1.00", currency: "USD" });
    expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.equals(usd)).toThrow(CurrencyMismatchError);
  });

  test("supports negative values only inside arithmetic operations", () => {
    const amount = Money.from({ amount: "3.25", currency: "BRL" }).negate();
    expect(amount.toString()).toBe("-3.25");
    expect(
      amount.add(Money.from({ amount: "3.25", currency: "BRL" })).isZero(),
    ).toBe(true);
  });

  test("rejects values outside PostgreSQL numeric(20, 2) precision", () => {
    expect(
      Money.from({
        amount: "999999999999999999.99",
        currency: "BRL",
      }).toString(),
    ).toBe("999999999999999999.99");
    expect(() =>
      Money.from({ amount: "1000000000000000000.00", currency: "BRL" }),
    ).toThrow(InvalidMoneyError);
  });
});
