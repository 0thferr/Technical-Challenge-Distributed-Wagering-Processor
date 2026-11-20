import type { QueryRunner } from "typeorm";
import type { IntegrationEvent } from "../../domain/events/integration-event";
import type { WalletLedgerEntry } from "../../domain/ledger/wallet-ledger-entry";

export async function insertLedgerEntry(
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

export async function insertOutboxEvent(
  runner: QueryRunner,
  event: IntegrationEvent<unknown>,
): Promise<void> {
  await runner.query(
    `INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at, created_at)
       VALUES ($1, $2, $3, $4::jsonb, $5, clock_timestamp())`,
    [
      event.eventId,
      event.aggregateId,
      event.eventType,
      JSON.stringify(event.toJSON()),
      event.occurredAt,
    ],
  );
}
