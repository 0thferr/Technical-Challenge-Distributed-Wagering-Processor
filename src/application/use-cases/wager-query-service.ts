import { Inject, Injectable } from "@nestjs/common";
import type { DataSource } from "typeorm";
import type { MoneyProps } from "../../domain/money/money";
import { DATA_SOURCE } from "../../infrastructure/persistence/data-source";
import { NotFoundApplicationError } from "../application-error";

interface TransactionQueryRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  player_id: string;
  wallet_id: string;
  round_id: string;
  game_id: string;
  kind: string;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: string;
  failure_code: string | null;
  processed_at: Date | null;
  created_at: Date;
  response_snapshot: Record<string, unknown> | string | null;
}

@Injectable()
export class WagerQueryService {
  constructor(@Inject(DATA_SOURCE) private readonly dataSource: DataSource) {}

  async byId(transactionId: string): Promise<Record<string, unknown>> {
    const rows = (await this.dataSource.query(
      `SELECT id, provider_id, external_transaction_id, player_id, wallet_id, round_id, game_id,
              kind, amount::text AS amount, currency, reference_external_transaction_id,
              reference_transaction_id, status, failure_code, processed_at, created_at, response_snapshot
       FROM wager_transactions WHERE id = $1`,
      [transactionId],
    )) as TransactionQueryRow[];
    if (!rows[0]) throw new NotFoundApplicationError("Wager transaction");
    return this.format(rows[0]);
  }

  async byExternal(
    providerId: string,
    externalTransactionId: string,
  ): Promise<Record<string, unknown>> {
    const rows = (await this.dataSource.query(
      `SELECT id, provider_id, external_transaction_id, player_id, wallet_id, round_id, game_id,
              kind, amount::text AS amount, currency, reference_external_transaction_id,
              reference_transaction_id, status, failure_code, processed_at, created_at, response_snapshot
       FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
      [providerId, externalTransactionId],
    )) as TransactionQueryRow[];
    if (!rows[0]) throw new NotFoundApplicationError("Wager transaction");
    return this.format(rows[0]);
  }

  private format(row: TransactionQueryRow): Record<string, unknown> {
    const snapshot =
      typeof row.response_snapshot === "string"
        ? (JSON.parse(row.response_snapshot) as { balance?: MoneyProps })
        : row.response_snapshot;
    return {
      transactionId: row.id,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      playerId: row.player_id,
      walletId: row.wallet_id,
      roundId: row.round_id,
      gameId: row.game_id,
      kind: row.kind,
      money: { amount: row.amount, currency: row.currency.trim() },
      ...(row.reference_external_transaction_id
        ? {
            referenceExternalTransactionId:
              row.reference_external_transaction_id,
          }
        : {}),
      ...(row.reference_transaction_id
        ? { referenceTransactionId: row.reference_transaction_id }
        : {}),
      status: row.status,
      ...(row.failure_code ? { failureCode: row.failure_code } : {}),
      ...(snapshot?.balance ? { originalBalance: snapshot.balance } : {}),
      createdAt: row.created_at.toISOString(),
      ...(row.processed_at
        ? { processedAt: row.processed_at.toISOString() }
        : {}),
    };
  }
}
