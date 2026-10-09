import { randomUUID } from "node:crypto";
import { Inject, Injectable } from "@nestjs/common";
import type { DataSource, QueryRunner } from "typeorm";
import { DomainError } from "../../domain/errors/domain-error";
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from "../../domain/events/integration-event";
import {
  LedgerDirection,
  type WalletLedgerEntry,
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
import { canonicalJson, sha256 } from "./canonical-payload";

export interface WagerSubmission {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  idempotencyKey: string;
  correlationId?: string;
}

export interface InboxContext {
  consumerName: string;
  messageId: string;
}

export interface TransactionResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: string;
  duplicateMessage?: boolean;
}

interface WalletRow {
  id: string;
  player_id: string;
  currency: string;
  balance: string;
  version: number;
  created_at: Date;
  updated_at: Date;
}

interface TransactionRow {
  id: string;
  payload_hash: string;
  response_snapshot: TransactionResult | string | null;
  status: WagerTransactionStatus;
}

interface ReferenceRow {
  id: string;
  provider_id: string;
  player_id: string;
  wallet_id: string;
  round_id: string;
  game_id: string;
  kind: WagerTransactionKind;
  amount: string;
  currency: string;
  status: WagerTransactionStatus;
}

interface InboxRow {
  payload_hash: string;
  processed_at: Date | null;
}

interface PendingTransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: WagerTransactionKind;
  amount: string;
  currency: string;
  reference_external_transaction_id: string;
  reference_attempts: number;
  created_at: Date;
}

interface TransactionEventProps {
  transaction: WagerTransaction;
  status: WagerTransactionStatus;
  balance: Money;
  correlationId: string;
  causationId?: string;
  failureCode?: string;
}

const REFERENCE_MAX_ATTEMPTS = 10;
const REFERENCE_INITIAL_DELAY_MS = 5_000;
const REFERENCE_MAX_DELAY_MS = 300_000;

@Injectable()
export class ProcessWagerTransaction {
  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    private readonly metrics: MetricsService,
    private readonly logger: StructuredLogger,
  ) {}

  async execute(
    input: WagerSubmission,
    inbox?: InboxContext,
    operation: "http" | "sqs" | "reference-worker" = inbox ? "sqs" : "http",
  ): Promise<TransactionResult> {
    const startedAt = performance.now();
    let money: Money;
    let transaction: WagerTransaction;
    let payloadHash: string;
    try {
      money = Money.from(input.money);
      money.assertNonNegative();
      const normalizedInput: WagerSubmission = {
        ...input,
        money: money.toJSON(),
        ...(input.referenceExternalTransactionId
          ? {
              referenceExternalTransactionId:
                input.referenceExternalTransactionId,
            }
          : {}),
      };
      payloadHash = this.payloadHash(normalizedInput);
      transaction = WagerTransaction.create({
        id: randomUUID(),
        providerId: input.providerId,
        externalTransactionId: input.externalTransactionId,
        idempotencyKey: input.idempotencyKey,
        payloadHash,
        walletId: input.walletId,
        playerId: input.playerId,
        roundId: input.roundId,
        gameId: input.gameId,
        kind: input.kind,
        money,
        ...(input.referenceExternalTransactionId
          ? {
              referenceExternalTransactionId:
                input.referenceExternalTransactionId,
            }
          : {}),
        createdAt: new Date(),
      });
    } catch (error) {
      if (error instanceof DomainError)
        throw new ApplicationError(error.message, error.code, 400);
      throw error;
    }

    const runner = this.dataSource.createQueryRunner();
    try {
      await runner.connect();
      await runner.startTransaction("READ COMMITTED");
      if (inbox) {
        const inboxHash = sha256(
          canonicalJson({ payloadHash, idempotencyKey: input.idempotencyKey }),
        );
        const duplicate = await this.receiveInbox(runner, inbox, inboxHash);
        if (duplicate) {
          const existing = await this.findByIdempotency(
            runner,
            input.providerId,
            input.idempotencyKey,
          );
          if (!existing || existing.payload_hash.trim() !== payloadHash) {
            throw new RetryableInfrastructureError(
              "Processed inbox entry has no matching idempotency record",
            );
          }
          const snapshot = this.readSnapshot(existing.response_snapshot);
          if (!snapshot) {
            throw new RetryableInfrastructureError(
              "Stored idempotency result is incomplete",
            );
          }
          await runner.commitTransaction();
          this.metrics.recordDuplicate("inbox");
          return {
            ...snapshot,
            idempotentReplay: true,
            duplicateMessage: true,
          };
        }
      }

      const byIdempotency = await this.findByIdempotency(
        runner,
        input.providerId,
        input.idempotencyKey,
      );
      if (byIdempotency) {
        if (byIdempotency.payload_hash.trim() !== payloadHash) {
          throw new ConflictApplicationError(
            "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH",
            "The Idempotency-Key was already used with a different request",
          );
        }
        const snapshot = this.readSnapshot(byIdempotency.response_snapshot);
        if (!snapshot)
          throw new RetryableInfrastructureError(
            "Stored idempotency result is incomplete",
          );
        if (inbox) await this.markInboxProcessed(runner, inbox);
        await runner.commitTransaction();
        this.metrics.recordDuplicate("idempotency");
        this.metrics.observeProcessing(
          operation,
          (performance.now() - startedAt) / 1000,
        );
        return { ...snapshot, idempotentReplay: true };
      }

      const walletRows = (await runner.query(
        `SELECT id, player_id, currency, balance::text AS balance, version, created_at, updated_at
         FROM wallets WHERE id = $1 FOR UPDATE`,
        [input.walletId],
      )) as WalletRow[];
      const walletRow = walletRows[0];
      if (!walletRow) throw new NotFoundApplicationError("Wallet");

      const inserted = (await runner.query(
        `INSERT INTO wager_transactions (
          id, provider_id, external_transaction_id, idempotency_key, payload_hash,
          wallet_id, player_id, round_id, game_id, kind, amount, currency,
          reference_external_transaction_id, status, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'PENDING', $14)
        ON CONFLICT DO NOTHING RETURNING id`,
        [
          transaction.id,
          transaction.providerId,
          transaction.externalTransactionId,
          transaction.idempotencyKey,
          transaction.payloadHash,
          transaction.walletId,
          transaction.playerId,
          transaction.roundId,
          transaction.gameId,
          transaction.kind,
          transaction.money.toString(),
          transaction.money.currency,
          transaction.referenceExternalTransactionId ?? null,
          transaction.createdAt,
        ],
      )) as Array<{ id: string }>;

      if (!inserted[0]) {
        const existing = await this.findByIdempotency(
          runner,
          input.providerId,
          input.idempotencyKey,
        );
        if (existing) {
          if (existing.payload_hash.trim() !== payloadHash) {
            throw new ConflictApplicationError(
              "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH",
              "The Idempotency-Key was already used with a different request",
            );
          }
          const snapshot = this.readSnapshot(existing.response_snapshot);
          if (!snapshot)
            throw new RetryableInfrastructureError(
              "Stored idempotency result is incomplete",
            );
          if (inbox) await this.markInboxProcessed(runner, inbox);
          await runner.commitTransaction();
          this.metrics.recordDuplicate("idempotency");
          this.metrics.observeProcessing(
            operation,
            (performance.now() - startedAt) / 1000,
          );
          return { ...snapshot, idempotentReplay: true };
        }
        throw new ConflictApplicationError(
          "EXTERNAL_TRANSACTION_CONFLICT",
          "The provider transaction identifier has already been used",
        );
      }

      const wallet = Wallet.rehydrate({
        id: walletRow.id,
        playerId: walletRow.player_id,
        currency: walletRow.currency.trim(),
        balance: Money.from({
          amount: walletRow.balance,
          currency: walletRow.currency.trim(),
        }),
        version: walletRow.version,
        createdAt: walletRow.created_at,
        updatedAt: walletRow.updated_at,
      });
      if (input.playerId !== wallet.playerId) {
        return await this.rejectAndCommit(
          runner,
          transaction,
          wallet.balance,
          "WALLET_PLAYER_MISMATCH",
          input,
          payloadHash,
          inbox,
          operation,
          startedAt,
        );
      }
      if (money.currency !== wallet.currency) {
        return await this.rejectAndCommit(
          runner,
          transaction,
          wallet.balance,
          "CURRENCY_MISMATCH",
          input,
          payloadHash,
          inbox,
          operation,
          startedAt,
        );
      }

      const reference = await this.resolveReference(runner, transaction);
      if (reference.kind === "pending") {
        transaction.markPendingReference();
        const nextAttemptAt = new Date(Date.now() + REFERENCE_INITIAL_DELAY_MS);
        const response = this.result(
          transaction.id,
          WagerTransactionStatus.PendingReference,
          wallet.balance,
        );
        await runner.query(
          `UPDATE wager_transactions
           SET status = 'PENDING_REFERENCE', reference_attempts = 1,
               reference_next_attempt_at = $2, response_snapshot = $3::jsonb
           WHERE id = $1`,
          [transaction.id, nextAttemptAt, JSON.stringify(response)],
        );
        await this.enqueueTransactionEvent(runner, {
          transaction,
          status: WagerTransactionStatus.PendingReference,
          balance: wallet.balance,
          correlationId: input.correlationId ?? transaction.id,
          ...(inbox ? { causationId: inbox.messageId } : {}),
        });
        if (inbox) await this.markInboxProcessed(runner, inbox);
        await runner.commitTransaction();
        this.metrics.recordTransaction(WagerTransactionStatus.PendingReference);
        this.metrics.observeProcessing(
          operation,
          (performance.now() - startedAt) / 1000,
        );
        return response;
      }

      if (reference.kind === "invalid") {
        return await this.rejectAndCommit(
          runner,
          transaction,
          wallet.balance,
          reference.code,
          input,
          payloadHash,
          inbox,
          operation,
          startedAt,
        );
      }

      const referenceRow =
        reference.kind === "valid" ? reference.row : undefined;
      if (referenceRow && this.isReversal(transaction.kind)) {
        const duplicateRows = (await runner.query(
          `SELECT id FROM wager_transactions
           WHERE reference_transaction_id = $1 AND kind = $2
             AND status IN ('PROCESSED', 'PENDING_REFERENCE') LIMIT 1`,
          [referenceRow.id, transaction.kind],
        )) as Array<{ id: string }>;
        if (duplicateRows.length > 0) {
          return await this.rejectAndCommit(
            runner,
            transaction,
            wallet.balance,
            "REFERENCE_ALREADY_REVERSED",
            input,
            payloadHash,
            inbox,
            operation,
            startedAt,
            referenceRow.id,
          );
        }
      }

      if (transaction.kind === WagerTransactionKind.Loss) {
        transaction.markProcessed(referenceRow?.id, new Date());
        const response = this.result(
          transaction.id,
          transaction.status,
          wallet.balance,
        );
        await this.persistTerminal(
          runner,
          transaction,
          response,
          referenceRow?.id,
        );
        await this.enqueueTransactionEvent(runner, {
          transaction,
          status: WagerTransactionStatus.Processed,
          balance: wallet.balance,
          correlationId: input.correlationId ?? transaction.id,
          ...(inbox ? { causationId: inbox.messageId } : {}),
        });
        if (inbox) await this.markInboxProcessed(runner, inbox);
        await runner.commitTransaction();
        this.metrics.recordTransaction(WagerTransactionStatus.Processed);
        this.metrics.observeProcessing(
          operation,
          (performance.now() - startedAt) / 1000,
        );
        return response;
      }

      const direction = transaction.ledgerDirectionFor(
        referenceRow ? this.rehydrateReference(referenceRow) : undefined,
      );
      let ledgerEntry: WalletLedgerEntry;
      try {
        ledgerEntry =
          direction === LedgerDirection.Debit
            ? wallet.debit({
                id: randomUUID(),
                transactionId: transaction.id,
                money,
                at: new Date(),
              })
            : wallet.credit({
                id: randomUUID(),
                transactionId: transaction.id,
                money,
                at: new Date(),
              });
      } catch (error) {
        if (
          error instanceof DomainError &&
          error.code === "INSUFFICIENT_BALANCE"
        ) {
          const failureCode =
            transaction.kind === WagerTransactionKind.Rollback
              ? "ROLLBACK_INSUFFICIENT_BALANCE"
              : "INSUFFICIENT_BALANCE";
          return await this.rejectAndCommit(
            runner,
            transaction,
            wallet.balance,
            failureCode,
            input,
            payloadHash,
            inbox,
            operation,
            startedAt,
            referenceRow?.id,
          );
        }
        throw error;
      }

      await runner.query(
        `UPDATE wallets SET balance = $2, version = $3, updated_at = $4 WHERE id = $1`,
        [
          wallet.id,
          wallet.balance.toString(),
          wallet.version,
          wallet.updatedAt,
        ],
      );
      await this.insertLedgerEntry(runner, ledgerEntry);
      transaction.markProcessed(referenceRow?.id, new Date());
      const response = this.result(
        transaction.id,
        transaction.status,
        wallet.balance,
      );
      await this.persistTerminal(
        runner,
        transaction,
        response,
        referenceRow?.id,
      );
      await this.enqueueTransactionEvent(runner, {
        transaction,
        status: WagerTransactionStatus.Processed,
        balance: wallet.balance,
        correlationId: input.correlationId ?? transaction.id,
        ...(inbox ? { causationId: inbox.messageId } : {}),
      });
      await this.enqueueBalanceChanged(
        runner,
        transaction,
        ledgerEntry,
        wallet.version,
        input.correlationId ?? transaction.id,
        inbox,
      );
      if (inbox) await this.markInboxProcessed(runner, inbox);
      await runner.commitTransaction();
      this.metrics.recordTransaction(WagerTransactionStatus.Processed);
      this.metrics.observeProcessing(
        operation,
        (performance.now() - startedAt) / 1000,
      );
      this.logger.info("wager_transaction_processed", {
        correlationId: input.correlationId ?? transaction.id,
        messageId: inbox?.messageId,
        transactionId: transaction.id,
        walletId: transaction.walletId,
        providerId: transaction.providerId,
        status: transaction.status,
      });
      return response;
    } catch (error) {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      const code = this.sqlState(error);
      if (code === "40001" || code === "40P01" || code === "55P03") {
        this.metrics.recordLockConflict(code);
        throw new RetryableInfrastructureError();
      }
      if (error instanceof ApplicationError) throw error;
      if (error instanceof DomainError) {
        throw new ApplicationError(error.message, error.code, 400);
      }
      this.logger.error("wager_transaction_failed", {
        transactionId: transaction.id,
        walletId: transaction.walletId,
        providerId: transaction.providerId,
        sqlState: code,
        errorType: error instanceof Error ? error.name : "UNKNOWN",
      });
      throw new RetryableInfrastructureError();
    } finally {
      await runner.release();
    }
  }

  async retryPendingReference(
    transactionId: string,
    leaseOwner: string,
  ): Promise<void> {
    const startedAt = performance.now();
    const runner = this.dataSource.createQueryRunner();
    try {
      await runner.connect();
      await runner.startTransaction("READ COMMITTED");
      // Keep the lock order consistent with normal processing: wallet first,
      // then wager transaction. The reverse order can deadlock when another
      // operation on this wallet references the pending transaction.
      const eligibilityRows = (await runner.query(
        `SELECT wallet_id FROM wager_transactions
         WHERE id = $1 AND status = 'PENDING_REFERENCE' AND reference_locked_by = $2`,
        [transactionId, leaseOwner],
      )) as Array<{ wallet_id: string }>;
      const eligibility = eligibilityRows[0];
      if (!eligibility) {
        await runner.commitTransaction();
        return;
      }
      const walletRows = (await runner.query(
        `SELECT id, player_id, currency, balance::text AS balance, version, created_at, updated_at
         FROM wallets WHERE id = $1 FOR UPDATE`,
        [eligibility.wallet_id],
      )) as WalletRow[];
      const walletRow = walletRows[0];
      if (!walletRow) throw new NotFoundApplicationError("Wallet");

      const transactionRows = (await runner.query(
        `SELECT id, provider_id, external_transaction_id, idempotency_key, payload_hash,
                wallet_id, player_id, round_id, game_id, kind, amount::text AS amount,
                currency, reference_external_transaction_id, reference_attempts, created_at
         FROM wager_transactions
         WHERE id = $1 AND status = 'PENDING_REFERENCE' AND reference_locked_by = $2
         FOR UPDATE`,
        [transactionId, leaseOwner],
      )) as PendingTransactionRow[];
      const row = transactionRows[0];
      if (!row) {
        await runner.commitTransaction();
        return;
      }
      const wallet = Wallet.rehydrate({
        id: walletRow.id,
        playerId: walletRow.player_id,
        currency: walletRow.currency.trim(),
        balance: Money.from({
          amount: walletRow.balance,
          currency: walletRow.currency.trim(),
        }),
        version: walletRow.version,
        createdAt: walletRow.created_at,
        updatedAt: walletRow.updated_at,
      });
      const transaction = WagerTransaction.rehydrate({
        id: row.id,
        providerId: row.provider_id,
        externalTransactionId: row.external_transaction_id,
        idempotencyKey: row.idempotency_key,
        payloadHash: row.payload_hash.trim(),
        walletId: row.wallet_id,
        playerId: row.player_id,
        roundId: row.round_id,
        gameId: row.game_id,
        kind: row.kind,
        money: Money.from({
          amount: row.amount,
          currency: row.currency.trim(),
        }),
        referenceExternalTransactionId: row.reference_external_transaction_id,
        createdAt: row.created_at,
        status: WagerTransactionStatus.PendingReference,
      });
      const reference = await this.resolveReference(runner, transaction);
      if (reference.kind === "pending") {
        const attempts = row.reference_attempts + 1;
        if (attempts >= REFERENCE_MAX_ATTEMPTS) {
          await this.finishPendingRejection(
            runner,
            transaction,
            wallet.balance,
            "REFERENCE_NOT_FOUND",
            attempts,
          );
          await runner.commitTransaction();
          this.metrics.recordTransaction(WagerTransactionStatus.Rejected);
          this.metrics.observeProcessing(
            "reference-worker",
            (performance.now() - startedAt) / 1000,
          );
          return;
        }
        const delayMs = Math.min(
          REFERENCE_INITIAL_DELAY_MS * 2 ** (attempts - 1),
          REFERENCE_MAX_DELAY_MS,
        );
        await runner.query(
          `UPDATE wager_transactions
           SET reference_attempts = $3, reference_next_attempt_at = $4,
               reference_locked_by = NULL, reference_locked_until = NULL
           WHERE id = $1 AND reference_locked_by = $2`,
          [
            transaction.id,
            leaseOwner,
            attempts,
            new Date(Date.now() + delayMs),
          ],
        );
        await runner.commitTransaction();
        this.metrics.recordRetry("reference-worker");
        return;
      }
      if (reference.kind === "invalid") {
        await this.finishPendingRejection(
          runner,
          transaction,
          wallet.balance,
          reference.code,
          row.reference_attempts,
        );
        await runner.commitTransaction();
        this.metrics.recordTransaction(WagerTransactionStatus.Rejected);
        this.metrics.observeProcessing(
          "reference-worker",
          (performance.now() - startedAt) / 1000,
        );
        return;
      }

      const referenceRow =
        reference.kind === "valid" ? reference.row : undefined;
      if (!referenceRow) {
        await this.finishPendingRejection(
          runner,
          transaction,
          wallet.balance,
          "REFERENCE_INVALID",
          row.reference_attempts,
        );
        await runner.commitTransaction();
        this.metrics.recordTransaction(WagerTransactionStatus.Rejected);
        return;
      }
      if (this.isReversal(transaction.kind)) {
        const duplicates = (await runner.query(
          `SELECT id FROM wager_transactions WHERE reference_transaction_id = $1
           AND kind = $2 AND status = 'PROCESSED' LIMIT 1`,
          [referenceRow.id, transaction.kind],
        )) as Array<{ id: string }>;
        if (duplicates.length > 0) {
          await this.finishPendingRejection(
            runner,
            transaction,
            wallet.balance,
            "REFERENCE_ALREADY_REVERSED",
            row.reference_attempts,
            referenceRow.id,
          );
          await runner.commitTransaction();
          this.metrics.recordTransaction(WagerTransactionStatus.Rejected);
          return;
        }
      }

      const direction = transaction.ledgerDirectionFor(
        this.rehydrateReference(referenceRow),
      );
      let entry: WalletLedgerEntry;
      try {
        entry =
          direction === LedgerDirection.Debit
            ? wallet.debit({
                id: randomUUID(),
                transactionId: transaction.id,
                money: transaction.money,
                at: new Date(),
              })
            : wallet.credit({
                id: randomUUID(),
                transactionId: transaction.id,
                money: transaction.money,
                at: new Date(),
              });
      } catch (error) {
        if (
          error instanceof DomainError &&
          error.code === "INSUFFICIENT_BALANCE"
        ) {
          await this.finishPendingRejection(
            runner,
            transaction,
            wallet.balance,
            transaction.kind === WagerTransactionKind.Rollback
              ? "ROLLBACK_INSUFFICIENT_BALANCE"
              : "INSUFFICIENT_BALANCE",
            row.reference_attempts,
            referenceRow.id,
          );
          await runner.commitTransaction();
          this.metrics.recordTransaction(WagerTransactionStatus.Rejected);
          return;
        }
        throw error;
      }

      await runner.query(
        `UPDATE wallets SET balance = $2, version = $3, updated_at = $4 WHERE id = $1`,
        [
          wallet.id,
          wallet.balance.toString(),
          wallet.version,
          wallet.updatedAt,
        ],
      );
      await this.insertLedgerEntry(runner, entry);
      transaction.markProcessed(referenceRow.id, new Date());
      await runner.query(
        `UPDATE wager_transactions SET status = 'PROCESSED', reference_transaction_id = $3,
          processed_at = $4, reference_next_attempt_at = NULL,
          reference_locked_by = NULL, reference_locked_until = NULL
         WHERE id = $1 AND reference_locked_by = $2`,
        [transaction.id, leaseOwner, referenceRow.id, transaction.processedAt],
      );
      await this.enqueueTransactionEvent(runner, {
        transaction,
        status: WagerTransactionStatus.Processed,
        balance: wallet.balance,
        correlationId: transaction.id,
      });
      await this.enqueueBalanceChanged(
        runner,
        transaction,
        entry,
        wallet.version,
        transaction.id,
      );
      await runner.commitTransaction();
      this.metrics.recordTransaction(WagerTransactionStatus.Processed);
      this.metrics.observeProcessing(
        "reference-worker",
        (performance.now() - startedAt) / 1000,
      );
      this.logger.info("pending_reference_processed", {
        correlationId: transaction.id,
        transactionId: transaction.id,
        walletId: transaction.walletId,
        providerId: transaction.providerId,
      });
    } catch (error) {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      const code = this.sqlState(error);
      if (code === "40001" || code === "40P01" || code === "55P03") {
        this.metrics.recordLockConflict(code);
        throw new RetryableInfrastructureError();
      }
      if (error instanceof ApplicationError) throw error;
      this.logger.error("pending_reference_retry_failed", {
        transactionId,
        sqlState: code,
      });
      throw new RetryableInfrastructureError();
    } finally {
      await runner.release();
    }
  }

  payloadHash(input: WagerSubmission): string {
    const payload = {
      providerId: input.providerId,
      externalTransactionId: input.externalTransactionId,
      playerId: input.playerId,
      walletId: input.walletId,
      roundId: input.roundId,
      gameId: input.gameId,
      kind: input.kind,
      money: input.money,
      ...(input.referenceExternalTransactionId
        ? {
            referenceExternalTransactionId:
              input.referenceExternalTransactionId,
          }
        : {}),
    };
    return sha256(canonicalJson(payload));
  }

  private async receiveInbox(
    runner: QueryRunner,
    inbox: InboxContext,
    payloadHash: string,
  ): Promise<boolean> {
    const inserted = (await runner.query(
      `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
       VALUES ($1, $2, $3, now()) ON CONFLICT DO NOTHING RETURNING message_id`,
      [inbox.consumerName, inbox.messageId, payloadHash],
    )) as Array<{ message_id: string }>;
    if (inserted[0]) return false;
    const rows = (await runner.query(
      `SELECT payload_hash, processed_at FROM inbox_messages
       WHERE consumer_name = $1 AND message_id = $2 FOR UPDATE`,
      [inbox.consumerName, inbox.messageId],
    )) as InboxRow[];
    const row = rows[0];
    if (!row)
      throw new RetryableInfrastructureError(
        "Inbox row disappeared during deduplication",
      );
    if (row.payload_hash.trim() !== payloadHash) {
      throw new ConflictApplicationError(
        "INBOX_MESSAGE_ID_REUSED",
        "Message id was reused with different content",
      );
    }
    return row.processed_at !== null;
  }

  private async markInboxProcessed(
    runner: QueryRunner,
    inbox: InboxContext,
  ): Promise<void> {
    await runner.query(
      `UPDATE inbox_messages SET processed_at = now()
       WHERE consumer_name = $1 AND message_id = $2`,
      [inbox.consumerName, inbox.messageId],
    );
  }

  private async findByIdempotency(
    runner: QueryRunner,
    providerId: string,
    idempotencyKey: string,
  ): Promise<TransactionRow | undefined> {
    const rows = (await runner.query(
      `SELECT id, payload_hash, response_snapshot, status
       FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2 FOR UPDATE`,
      [providerId, idempotencyKey],
    )) as TransactionRow[];
    return rows[0];
  }

  private readSnapshot(
    value: TransactionResult | string | null,
  ): TransactionResult | undefined {
    if (!value) return undefined;
    return typeof value === "string"
      ? (JSON.parse(value) as TransactionResult)
      : value;
  }

  private async resolveReference(
    runner: QueryRunner,
    transaction: WagerTransaction,
  ): Promise<
    | { kind: "none" }
    | { kind: "pending" }
    | { kind: "invalid"; code: string }
    | { kind: "valid"; row: ReferenceRow }
  > {
    const externalId = transaction.referenceExternalTransactionId;
    const referenceIsRequired = transaction.requiresReference();
    const winHasReference =
      transaction.kind === WagerTransactionKind.Win && Boolean(externalId);
    if (!externalId) return { kind: "none" };
    const rows = (await runner.query(
      `SELECT id, provider_id, player_id, wallet_id, round_id, game_id, kind,
              amount::text AS amount, currency, status
       FROM wager_transactions
       WHERE provider_id = $1 AND external_transaction_id = $2
       FOR SHARE`,
      [transaction.providerId, externalId],
    )) as ReferenceRow[];
    const reference = rows[0];
    if (!reference)
      return referenceIsRequired || winHasReference
        ? { kind: "pending" }
        : { kind: "none" };
    if (
      reference.player_id !== transaction.playerId ||
      reference.wallet_id !== transaction.walletId ||
      reference.round_id !== transaction.roundId ||
      reference.currency.trim() !== transaction.money.currency
    ) {
      return { kind: "invalid", code: "REFERENCE_INVALID" };
    }
    if (reference.status !== WagerTransactionStatus.Processed) {
      return { kind: "invalid", code: "REFERENCE_NOT_PROCESSED" };
    }
    if (
      transaction.kind === WagerTransactionKind.Win &&
      reference.kind !== WagerTransactionKind.Bet
    ) {
      return { kind: "invalid", code: "REFERENCE_INVALID" };
    }
    if (
      transaction.kind === WagerTransactionKind.Refund &&
      reference.kind !== WagerTransactionKind.Bet
    ) {
      return { kind: "invalid", code: "REFERENCE_INVALID" };
    }
    if (
      transaction.kind === WagerTransactionKind.Rollback &&
      ![
        WagerTransactionKind.Bet,
        WagerTransactionKind.Win,
        WagerTransactionKind.Refund,
      ].includes(reference.kind)
    ) {
      return { kind: "invalid", code: "REFERENCE_INVALID" };
    }
    if (
      (transaction.kind === WagerTransactionKind.Refund ||
        transaction.kind === WagerTransactionKind.Rollback) &&
      Money.from({
        amount: reference.amount,
        currency: reference.currency.trim(),
      }).equals(transaction.money) === false
    ) {
      return { kind: "invalid", code: "REFERENCE_AMOUNT_MISMATCH" };
    }
    return { kind: "valid", row: reference };
  }

  private rehydrateReference(row: ReferenceRow): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: row.id,
      providerId: row.provider_id,
      externalTransactionId: "",
      idempotencyKey: "",
      payloadHash: "",
      walletId: row.wallet_id,
      playerId: row.player_id,
      roundId: row.round_id,
      gameId: row.game_id,
      kind: row.kind,
      money: Money.from({ amount: row.amount, currency: row.currency.trim() }),
      createdAt: new Date(0),
      status: row.status,
    });
  }

  private async rejectAndCommit(
    runner: QueryRunner,
    transaction: WagerTransaction,
    balance: Money,
    failureCode: string,
    input: WagerSubmission,
    _payloadHash: string,
    inbox: InboxContext | undefined,
    operation: "http" | "sqs" | "reference-worker",
    startedAt: number,
    referenceTransactionId?: string,
  ): Promise<TransactionResult> {
    transaction.reject(failureCode);
    const response = this.result(
      transaction.id,
      WagerTransactionStatus.Rejected,
      balance,
      failureCode,
    );
    await this.persistTerminal(
      runner,
      transaction,
      response,
      referenceTransactionId,
    );
    await this.enqueueTransactionEvent(runner, {
      transaction,
      status: WagerTransactionStatus.Rejected,
      balance,
      correlationId: input.correlationId ?? transaction.id,
      ...(inbox ? { causationId: inbox.messageId } : {}),
      failureCode,
    });
    if (inbox) await this.markInboxProcessed(runner, inbox);
    await runner.commitTransaction();
    this.metrics.recordTransaction(WagerTransactionStatus.Rejected);
    this.metrics.observeProcessing(
      operation,
      (performance.now() - startedAt) / 1000,
    );
    this.logger.warn("wager_transaction_rejected", {
      correlationId: input.correlationId ?? transaction.id,
      messageId: inbox?.messageId,
      transactionId: transaction.id,
      walletId: transaction.walletId,
      providerId: transaction.providerId,
      failureCode,
    });
    return response;
  }

  private result(
    transactionId: string,
    status: WagerTransactionStatus,
    balance: Money,
    failureCode?: string,
  ): TransactionResult {
    return {
      transactionId,
      status,
      balance: balance.toJSON(),
      idempotentReplay: false,
      ...(failureCode ? { failureCode } : {}),
    };
  }

  private async persistTerminal(
    runner: QueryRunner,
    transaction: WagerTransaction,
    response: TransactionResult,
    referenceTransactionId?: string,
  ): Promise<void> {
    await runner.query(
      `UPDATE wager_transactions
       SET status = $2, reference_transaction_id = $3, failure_code = $4,
           processed_at = $5, response_snapshot = $6::jsonb,
           reference_next_attempt_at = NULL, reference_locked_by = NULL, reference_locked_until = NULL
       WHERE id = $1`,
      [
        transaction.id,
        transaction.status,
        referenceTransactionId ?? null,
        transaction.failureCode ?? null,
        transaction.status === WagerTransactionStatus.Processed
          ? (transaction.processedAt ?? null)
          : null,
        JSON.stringify(response),
      ],
    );
  }

  private async finishPendingRejection(
    runner: QueryRunner,
    transaction: WagerTransaction,
    balance: Money,
    failureCode: string,
    attempts: number,
    referenceTransactionId?: string,
  ): Promise<void> {
    transaction.reject(failureCode);
    await runner.query(
      `UPDATE wager_transactions SET status = 'REJECTED', reference_transaction_id = $2,
         failure_code = $3, reference_attempts = $4, reference_next_attempt_at = NULL,
         reference_locked_by = NULL, reference_locked_until = NULL
       WHERE id = $1`,
      [transaction.id, referenceTransactionId ?? null, failureCode, attempts],
    );
    await this.enqueueTransactionEvent(runner, {
      transaction,
      status: WagerTransactionStatus.Rejected,
      balance,
      correlationId: transaction.id,
      failureCode,
    });
  }

  private async insertLedgerEntry(
    runner: QueryRunner,
    entry: WalletLedgerEntry,
  ): Promise<void> {
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
  }

  private async enqueueTransactionEvent(
    runner: QueryRunner,
    props: TransactionEventProps,
  ): Promise<void> {
    const eventProps = {
      eventId: randomUUID(),
      aggregateId: props.transaction.walletId,
      correlationId: props.correlationId,
      ...(props.causationId ? { causationId: props.causationId } : {}),
      occurredAt: new Date(),
      data: {
        transactionId: props.transaction.id,
        providerId: props.transaction.providerId,
        externalTransactionId: props.transaction.externalTransactionId,
        walletId: props.transaction.walletId,
        kind: props.transaction.kind,
        status: props.status,
        money: props.transaction.money.toJSON(),
        ...(props.transaction.referenceExternalTransactionId
          ? {
              referenceExternalTransactionId:
                props.transaction.referenceExternalTransactionId,
            }
          : {}),
        ...(props.failureCode ? { failureCode: props.failureCode } : {}),
      },
    };
    const event =
      props.status === WagerTransactionStatus.Processed
        ? new WagerTransactionProcessed(eventProps)
        : props.status === WagerTransactionStatus.PendingReference
          ? new WagerTransactionPendingReference(eventProps)
          : new WagerTransactionRejected(eventProps);
    await this.insertOutbox(
      runner,
      event.eventId,
      event.aggregateId,
      event.eventType,
      event.toJSON(),
      event.occurredAt,
    );
  }

  private async enqueueBalanceChanged(
    runner: QueryRunner,
    transaction: WagerTransaction,
    entry: WalletLedgerEntry,
    version: number,
    correlationId: string,
    inbox?: InboxContext,
  ): Promise<void> {
    const event = new WalletBalanceChanged({
      eventId: randomUUID(),
      aggregateId: transaction.walletId,
      correlationId,
      ...(inbox ? { causationId: inbox.messageId } : {}),
      occurredAt: entry.createdAt,
      data: {
        walletId: entry.walletId,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: version,
      },
    });
    await this.insertOutbox(
      runner,
      event.eventId,
      event.aggregateId,
      event.eventType,
      event.toJSON(),
      event.occurredAt,
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

  private isReversal(kind: WagerTransactionKind): boolean {
    return (
      kind === WagerTransactionKind.Refund ||
      kind === WagerTransactionKind.Rollback
    );
  }

  private sqlState(error: unknown): string | undefined {
    if (typeof error !== "object" || error === null || !("code" in error))
      return undefined;
    const code = (error as { code?: unknown }).code;
    return typeof code === "string" ? code : undefined;
  }
}
