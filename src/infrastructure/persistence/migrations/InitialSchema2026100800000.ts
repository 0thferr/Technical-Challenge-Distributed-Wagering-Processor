import type { MigrationInterface, QueryRunner } from "typeorm";

export class InitialSchema2026100800000 implements MigrationInterface {
  name = "InitialSchema2026100800000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY,
        player_id text NOT NULL,
        currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        balance numeric(20, 2) NOT NULL CHECK (balance >= 0),
        version integer NOT NULL CHECK (version >= 1),
        created_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL,
        CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency),
        CONSTRAINT wallets_id_player_currency_unique UNIQUE (id, player_id, currency),
        CONSTRAINT wallets_id_currency_unique UNIQUE (id, currency)
      )
    `);

    await queryRunner.query(`
      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY,
        provider_id text NOT NULL,
        external_transaction_id text NOT NULL,
        idempotency_key text NOT NULL,
        payload_hash char(64) NOT NULL,
        wallet_id uuid NOT NULL,
        player_id text NOT NULL,
        round_id text NOT NULL,
        game_id text NOT NULL,
        kind text NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
        amount numeric(20, 2) NOT NULL CHECK (amount > 0),
        currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        reference_external_transaction_id text,
        reference_transaction_id uuid,
        status text NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
        failure_code text,
        processed_at timestamptz,
        response_snapshot jsonb,
        reference_attempts integer NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
        reference_next_attempt_at timestamptz,
        reference_locked_by text,
        reference_locked_until timestamptz,
        created_at timestamptz NOT NULL,
        CONSTRAINT wager_transactions_idempotency_unique UNIQUE (provider_id, idempotency_key),
        CONSTRAINT wager_transactions_external_unique UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT wager_transactions_id_wallet_unique UNIQUE (id, wallet_id),
        CONSTRAINT wager_transactions_reference_identity_unique
          UNIQUE (id, wallet_id, provider_id, player_id, round_id, currency),
        CONSTRAINT wager_transactions_reference_external_unique
          UNIQUE (id, provider_id, external_transaction_id),
        CONSTRAINT wager_transactions_wallet_fk
          FOREIGN KEY (wallet_id) REFERENCES wallets (id) ON DELETE RESTRICT,
        CONSTRAINT wager_transactions_reference_same_scope_fk
          FOREIGN KEY (reference_transaction_id, wallet_id, provider_id, player_id, round_id, currency)
          REFERENCES wager_transactions (id, wallet_id, provider_id, player_id, round_id, currency)
          ON DELETE RESTRICT,
        CONSTRAINT wager_transactions_reference_external_fk
          FOREIGN KEY (reference_transaction_id, provider_id, reference_external_transaction_id)
          REFERENCES wager_transactions (id, provider_id, external_transaction_id)
          ON DELETE RESTRICT,
        CONSTRAINT wager_transactions_reference_required_check
          CHECK (kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL)
      )
    `);
    await queryRunner.query(`
      CREATE UNIQUE INDEX wager_transactions_one_resolved_reversal_per_type
      ON wager_transactions (reference_transaction_id, kind)
      WHERE kind IN ('REFUND', 'ROLLBACK')
        AND reference_transaction_id IS NOT NULL AND status = 'PROCESSED'
    `);
    await queryRunner.query(
      "CREATE INDEX wager_transactions_wallet_created_idx ON wager_transactions (wallet_id, created_at DESC, id DESC)",
    );
    await queryRunner.query(
      "CREATE INDEX wager_transactions_pending_reference_idx ON wager_transactions (reference_next_attempt_at, created_at) WHERE status = 'PENDING_REFERENCE'",
    );
    await queryRunner.query(`
      CREATE TABLE wallet_ledger_entries (
        id uuid PRIMARY KEY,
        wallet_id uuid NOT NULL,
        transaction_id uuid NOT NULL,
        direction text NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
        amount numeric(20, 2) NOT NULL CHECK (amount > 0),
        currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        balance_before numeric(20, 2) NOT NULL CHECK (balance_before >= 0),
        balance_after numeric(20, 2) NOT NULL CHECK (balance_after >= 0),
        created_at timestamptz NOT NULL,
        CONSTRAINT wallet_ledger_entry_equation_check CHECK (
          (direction = 'CREDIT' AND balance_after = balance_before + amount) OR
          (direction = 'DEBIT' AND balance_after = balance_before - amount)
        ),
        CONSTRAINT wallet_ledger_one_entry_per_transaction_wallet UNIQUE (transaction_id, wallet_id),
        CONSTRAINT wallet_ledger_wallet_currency_fk
          FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency) ON DELETE RESTRICT,
        CONSTRAINT wallet_ledger_transaction_wallet_fk
          FOREIGN KEY (transaction_id, wallet_id) REFERENCES wager_transactions (id, wallet_id) ON DELETE RESTRICT
      )
    `);
    await queryRunner.query(
      "CREATE INDEX wallet_ledger_wallet_cursor_idx ON wallet_ledger_entries (wallet_id, created_at DESC, id DESC)",
    );

    await queryRunner.query(`
      CREATE FUNCTION reject_wallet_ledger_mutation() RETURNS trigger AS $$
      BEGIN
        RAISE EXCEPTION 'wallet ledger entries are immutable' USING ERRCODE = '55000';
        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql
    `);
    await queryRunner.query(`
      CREATE TRIGGER wallet_ledger_no_update_or_delete
      BEFORE UPDATE OR DELETE ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION reject_wallet_ledger_mutation()
    `);
    await queryRunner.query(`
      CREATE TRIGGER wallet_ledger_no_truncate
      BEFORE TRUNCATE ON wallet_ledger_entries
      FOR EACH STATEMENT EXECUTE FUNCTION reject_wallet_ledger_mutation()
    `);

    await queryRunner.query(`
      CREATE TABLE inbox_messages (
        consumer_name text NOT NULL,
        message_id text NOT NULL,
        payload_hash char(64) NOT NULL,
        received_at timestamptz NOT NULL,
        processed_at timestamptz,
        PRIMARY KEY (consumer_name, message_id)
      )
    `);

    await queryRunner.query(`
      CREATE TABLE outbox_messages (
        id uuid PRIMARY KEY,
        aggregate_id uuid NOT NULL,
        event_type text NOT NULL,
        payload jsonb NOT NULL,
        occurred_at timestamptz NOT NULL,
        attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        next_attempt_at timestamptz,
        published_at timestamptz,
        locked_by text,
        locked_until timestamptz,
        created_at timestamptz NOT NULL
      )
    `);
    await queryRunner.query(
      "CREATE INDEX outbox_pending_publish_idx ON outbox_messages (next_attempt_at, created_at) WHERE published_at IS NULL",
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query("DROP TABLE outbox_messages");
    await queryRunner.query("DROP TABLE inbox_messages");
    await queryRunner.query(
      "DROP TRIGGER wallet_ledger_no_truncate ON wallet_ledger_entries",
    );
    await queryRunner.query(
      "DROP TRIGGER wallet_ledger_no_update_or_delete ON wallet_ledger_entries",
    );
    await queryRunner.query("DROP FUNCTION reject_wallet_ledger_mutation()");
    await queryRunner.query("DROP TABLE wallet_ledger_entries");
    await queryRunner.query("DROP TABLE wager_transactions");
    await queryRunner.query("DROP TABLE wallets");
  }
}
