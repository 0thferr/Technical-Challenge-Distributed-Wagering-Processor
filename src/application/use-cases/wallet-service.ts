import { randomUUID } from "node:crypto";
import { Inject, Injectable } from "@nestjs/common";
import type { DataSource, QueryRunner } from "typeorm";
import {
  WagerTransactionProcessed,
  WalletBalanceChanged,
} from "../../domain/events/integration-event";
import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../../domain/ledger/wallet-ledger-entry";
import { Money, type MoneyProps } from "../../domain/money/money";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../domain/wagering/wager-transaction";
import { Wallet } from "../../domain/wallet/wallet";
import { MetricsService } from "../../infrastructure/observability/metrics.service";
import { StructuredLogger } from "../../infrastructure/observability/structured-logger";
import { DATA_SOURCE } from "../../infrastructure/persistence/data-source";
import {
  ApplicationError,
  ConflictApplicationError,
  NotFoundApplicationError,
  RetryableInfrastructureError,
} from "../application-error";

interface WalletRow {
  id: string;
  player_id: string;
  currency: string;
  balance: string;
  version: number;
  created_at: Date;
  updated_at: Date;
}

interface LedgerRow {
  id: string;
  transaction_id: string;
  direction: LedgerDirection;
  amount: string;
  currency: string;
  balance_before: string;
  balance_after: string;
  created_at: Date;
}

interface ReconciliationRow {
  stored_balance: string;
  calculated_balance: string;
  entries: string;
}

export interface CreateWalletInput {
  playerId: string;
  initialBalance: MoneyProps;
}

export interface WalletResponse {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
  createdAt: string;
  updatedAt: string;
}

export interface LedgerPage {
  items: Array<{
    id: string;
    transactionId: string;
    direction: LedgerDirection;
    money: MoneyProps;
    balanceBefore: MoneyProps;
    balanceAfter: MoneyProps;
    createdAt: string;
  }>;
  nextCursor: string | null;
  limit: number;
}

@Injectable()
export class WalletService {
  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  async create(input: CreateWalletInput): Promise<WalletResponse> {
    const initialBalance = Money.from(input.initialBalance);
    initialBalance.assertNonNegative();
    const now = new Date();
    const wallet = Wallet.open({
      id: randomUUID(),
      playerId: input.playerId,
      initialBalance,
      at: now,
    });
    const runner = this.dataSource.createQueryRunner();
    try {
      await runner.connect();
      await runner.startTransaction();
      await runner.query(
        `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $6)`,
        [
          wallet.id,
          wallet.playerId,
          wallet.currency,
          wallet.balance.toString(),
          wallet.version,
          now,
        ],
      );
      if (initialBalance.isPositive()) {
        await this.createOpeningRecords(runner, wallet, initialBalance, now);
      }
      await runner.commitTransaction();
      if (initialBalance.isPositive()) {
        this.metrics.recordTransaction(WagerTransactionStatus.Processed);
      }
      this.logger.info("wallet_created", {
        walletId: wallet.id,
        playerId: wallet.playerId,
      });
      return this.toWalletResponse(wallet);
    } catch (error) {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      if (this.sqlState(error) === "23505") {
        throw new ConflictApplicationError(
          "WALLET_ALREADY_EXISTS",
          "A wallet already exists for this player and currency",
        );
      }
      throw new RetryableInfrastructureError();
    } finally {
      await runner.release();
    }
  }

  async get(walletId: string): Promise<WalletResponse> {
    const rows = (await this.dataSource.query(
      `SELECT id, player_id, currency, balance::text AS balance, version, created_at, updated_at
       FROM wallets WHERE id = $1`,
      [walletId],
    )) as WalletRow[];
    const row = rows[0];
    if (!row) throw new NotFoundApplicationError("Wallet");
    return this.toWalletResponse(
      Wallet.rehydrate({
        id: row.id,
        playerId: row.player_id,
        currency: row.currency.trim(),
        balance: Money.from({
          amount: row.balance,
          currency: row.currency.trim(),
        }),
        version: row.version,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      }),
    );
  }

  async ledger(
    walletId: string,
    cursor?: string,
    limit = 50,
  ): Promise<LedgerPage> {
    const boundedLimit = Math.min(Math.max(Math.trunc(limit), 1), 100);
    const walletRows = (await this.dataSource.query(
      "SELECT id FROM wallets WHERE id = $1",
      [walletId],
    )) as Array<{
      id: string;
    }>;
    if (!walletRows[0]) throw new NotFoundApplicationError("Wallet");

    const cursorValue = cursor ? this.decodeCursor(cursor) : undefined;
    const rows = (await this.dataSource.query(
      `SELECT id, transaction_id, direction, amount::text AS amount, currency,
              balance_before::text AS balance_before, balance_after::text AS balance_after, created_at
       FROM wallet_ledger_entries
       WHERE wallet_id = $1
         AND ($2::timestamptz IS NULL OR (created_at, id) < ($2::timestamptz, $3::uuid))
       ORDER BY created_at DESC, id DESC LIMIT $4`,
      [
        walletId,
        cursorValue?.createdAt ?? null,
        cursorValue?.id ?? null,
        boundedLimit + 1,
      ],
    )) as LedgerRow[];

    const hasMore = rows.length > boundedLimit;
    const pageRows = rows.slice(0, boundedLimit);
    const last = pageRows.at(-1);
    return {
      items: pageRows.map((row) => ({
        id: row.id,
        transactionId: row.transaction_id,
        direction: row.direction,
        money: { amount: row.amount, currency: row.currency.trim() },
        balanceBefore: {
          amount: row.balance_before,
          currency: row.currency.trim(),
        },
        balanceAfter: {
          amount: row.balance_after,
          currency: row.currency.trim(),
        },
        createdAt: row.created_at.toISOString(),
      })),
      nextCursor:
        hasMore && last
          ? this.encodeCursor({
              id: last.id,
              createdAt: last.created_at.toISOString(),
            })
          : null,
      limit: boundedLimit,
    };
  }

  async reconcile(walletId: string): Promise<{
    walletId: string;
    storedBalance: MoneyProps;
    calculatedBalance: MoneyProps;
    difference: MoneyProps;
    consistent: boolean;
    checkedEntries: number;
  }> {
    // Read the materialized balance and ledger aggregate in one statement so
    // PostgreSQL evaluates both from the same MVCC snapshot during writes.
    const rows = (await this.dataSource.query(
      `SELECT wallet.id, wallet.player_id, wallet.currency,
              wallet.balance::text AS stored_balance,
              COALESCE(ledger.calculated_balance, '0') AS calculated_balance,
              COALESCE(ledger.entries, '0') AS entries
       FROM wallets AS wallet
       LEFT JOIN LATERAL (
         SELECT SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END)::text AS calculated_balance,
                COUNT(*)::text AS entries
         FROM wallet_ledger_entries WHERE wallet_id = wallet.id
       ) AS ledger ON true
       WHERE wallet.id = $1`,
      [walletId],
    )) as Array<WalletRow & ReconciliationRow>;
    const row = rows[0];
    if (!row) throw new NotFoundApplicationError("Wallet");
    const currency = row.currency.trim();
    const stored = Money.from({ amount: row.stored_balance, currency });
    const calculated = Money.from({ amount: row.calculated_balance, currency });
    const difference = stored.subtract(calculated);
    const consistent = difference.isZero();
    if (!consistent) {
      this.metrics.recordReconciliationMismatch();
      this.logger.error("wallet_reconciliation_mismatch", {
        walletId,
        playerId: row.player_id,
        checkedEntries: Number.parseInt(row.entries, 10),
      });
    }
    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: Number.parseInt(row.entries, 10),
    };
  }

  private async createOpeningRecords(
    runner: QueryRunner,
    wallet: Wallet,
    initialBalance: Money,
    at: Date,
  ): Promise<void> {
    const opening = WagerTransaction.opening({
      id: randomUUID(),
      walletId: wallet.id,
      playerId: wallet.playerId,
      money: initialBalance,
      createdAt: at,
    });
    opening.markProcessed(undefined, at);
    const entry = WalletLedgerEntry.create({
      id: randomUUID(),
      walletId: wallet.id,
      transactionId: opening.id,
      direction: LedgerDirection.Credit,
      money: initialBalance,
      balanceBefore: Money.zero(wallet.currency),
      balanceAfter: initialBalance,
      createdAt: at,
    });
    await runner.query(
      `INSERT INTO wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency, status,
        processed_at, created_at
      ) VALUES ($1, '__internal__', $2, $2, $3, $4, $5, '__opening__', '__opening__',
                'OPENING', $6, $7, 'PROCESSED', $8, $8)`,
      [
        opening.id,
        opening.externalTransactionId,
        "0".repeat(64),
        wallet.id,
        wallet.playerId,
        initialBalance.toString(),
        wallet.currency,
        at,
      ],
    );
    await runner.query(
      `INSERT INTO wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, amount, currency,
        balance_before, balance_after, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        entry.id,
        entry.walletId,
        entry.transactionId,
        entry.direction,
        entry.money.toString(),
        entry.money.currency,
        entry.balanceBefore.toString(),
        entry.balanceAfter.toString(),
        entry.createdAt,
      ],
    );

    const processed = new WagerTransactionProcessed({
      eventId: randomUUID(),
      aggregateId: wallet.id,
      correlationId: opening.id,
      occurredAt: at,
      data: {
        transactionId: opening.id,
        providerId: opening.providerId,
        externalTransactionId: opening.externalTransactionId,
        walletId: wallet.id,
        kind: WagerTransactionKind.Opening,
        status: WagerTransactionStatus.Processed,
        money: initialBalance.toJSON(),
      },
    });
    await this.insertOutbox(
      runner,
      processed.eventId,
      wallet.id,
      processed.eventType,
      processed.toJSON(),
      at,
    );
    const changed = new WalletBalanceChanged({
      eventId: randomUUID(),
      aggregateId: wallet.id,
      correlationId: opening.id,
      occurredAt: at,
      data: {
        walletId: wallet.id,
        transactionId: opening.id,
        direction: LedgerDirection.Credit,
        money: initialBalance.toJSON(),
        balanceBefore: Money.zero(wallet.currency).toJSON(),
        balanceAfter: initialBalance.toJSON(),
        walletVersion: wallet.version,
      },
    });
    await this.insertOutbox(
      runner,
      changed.eventId,
      wallet.id,
      changed.eventType,
      changed.toJSON(),
      at,
    );
  }

  private async insertOutbox(
    runner: QueryRunner,
    id: string,
    aggregateId: string,
    eventType: string,
    payload: unknown,
    occurredAt: Date,
  ): Promise<void> {
    await runner.query(
      `INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at, created_at)
       VALUES ($1, $2, $3, $4::jsonb, $5, clock_timestamp())`,
      [id, aggregateId, eventType, JSON.stringify(payload), occurredAt],
    );
  }

  private toWalletResponse(wallet: Wallet): WalletResponse {
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
      createdAt: wallet.createdAt.toISOString(),
      updatedAt: wallet.updatedAt.toISOString(),
    };
  }

  private encodeCursor(cursor: { id: string; createdAt: string }): string {
    return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
  }

  private decodeCursor(value: string): { id: string; createdAt: string } {
    try {
      const decoded = JSON.parse(
        Buffer.from(value, "base64url").toString("utf8"),
      ) as {
        id?: unknown;
        createdAt?: unknown;
      };
      if (
        typeof decoded.id !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
          decoded.id,
        ) ||
        typeof decoded.createdAt !== "string" ||
        Number.isNaN(Date.parse(decoded.createdAt))
      ) {
        throw new Error("invalid cursor");
      }
      return {
        id: decoded.id,
        createdAt: new Date(decoded.createdAt).toISOString(),
      };
    } catch {
      throw new ApplicationError(
        "INVALID_CURSOR",
        "Ledger cursor is invalid",
        400,
      );
    }
  }

  private sqlState(error: unknown): string | undefined {
    if (typeof error !== "object" || error === null || !("code" in error))
      return undefined;
    const code = (error as { code?: unknown }).code;
    return typeof code === "string" ? code : undefined;
  }
}
