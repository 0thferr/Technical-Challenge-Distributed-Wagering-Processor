import type { MigrationInterface, QueryRunner } from "typeorm";

export class DeadLetterOperations2026100901000 implements MigrationInterface {
  name = "DeadLetterOperations2026100901000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      DO $$
      BEGIN
        IF EXISTS (SELECT 1 FROM wager_transactions WHERE status = 'FAILED') THEN
          RAISE EXCEPTION 'Cannot remove FAILED while persisted wager transactions use it';
        END IF;
      END
      $$
    `);
    await queryRunner.query(
      "ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_status_check",
    );
    await queryRunner.query(`
      ALTER TABLE wager_transactions
      ADD CONSTRAINT wager_transactions_status_check
      CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED'))
    `);

    await queryRunner.query(
      "ALTER TABLE dead_letter_messages DROP CONSTRAINT dead_letter_messages_status_check",
    );
    await queryRunner.query(`
      ALTER TABLE dead_letter_messages
      ADD CONSTRAINT dead_letter_messages_status_check CHECK (
        status IN (
          'NEEDS_INVESTIGATION', 'READY_FOR_REPROCESS', 'REPLAY_QUEUED',
          'REPROCESSING', 'REPROCESSED', 'REPROCESS_FAILED', 'RESOLVED'
        )
      )
    `);
    await queryRunner.query(`
      ALTER TABLE dead_letter_messages
      ADD COLUMN resolved_at timestamptz,
      ADD COLUMN replay_body_hash char(64) CHECK (
        replay_body_hash IS NULL OR replay_body_hash ~ '^[a-f0-9]{64}$'
      )
    `);
    await queryRunner.query(`
      CREATE TABLE dead_letter_replay_outbox (
        id uuid PRIMARY KEY,
        source_queue text NOT NULL,
        message_id text NOT NULL,
        body text NOT NULL,
        body_hash char(64) NOT NULL CHECK (body_hash ~ '^[a-f0-9]{64}$'),
        attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        next_attempt_at timestamptz,
        sent_at timestamptz,
        locked_by text,
        locked_until timestamptz,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT dead_letter_replay_message_unique UNIQUE (source_queue, message_id),
        CONSTRAINT dead_letter_replay_registry_fk
          FOREIGN KEY (source_queue, message_id)
          REFERENCES dead_letter_messages (source_queue, message_id)
          ON DELETE CASCADE
      )
    `);
    await queryRunner.query(`
      CREATE INDEX dead_letter_replay_pending_idx
      ON dead_letter_replay_outbox (next_attempt_at, created_at)
      WHERE sent_at IS NULL
    `);
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      DO $$
      BEGIN
        IF EXISTS (SELECT 1 FROM dead_letter_replay_outbox)
           OR EXISTS (
             SELECT 1 FROM dead_letter_messages
             WHERE status IN ('REPLAY_QUEUED', 'REPROCESSING', 'REPROCESSED', 'REPROCESS_FAILED')
           ) THEN
          RAISE EXCEPTION 'Resolve or retain dead-letter replay records before reverting this migration';
        END IF;
      END
      $$
    `);
    await queryRunner.query("DROP INDEX dead_letter_replay_pending_idx");
    await queryRunner.query("DROP TABLE dead_letter_replay_outbox");
    await queryRunner.query(
      "ALTER TABLE dead_letter_messages DROP CONSTRAINT dead_letter_messages_status_check",
    );
    await queryRunner.query(`
      ALTER TABLE dead_letter_messages
      ADD CONSTRAINT dead_letter_messages_status_check CHECK (
        status IN ('NEEDS_INVESTIGATION', 'READY_FOR_REPROCESS', 'RESOLVED')
      )
    `);
    await queryRunner.query(
      "ALTER TABLE dead_letter_messages DROP COLUMN replay_body_hash, DROP COLUMN resolved_at",
    );
    await queryRunner.query(
      "ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_status_check",
    );
    await queryRunner.query(`
      ALTER TABLE wager_transactions
      ADD CONSTRAINT wager_transactions_status_check
      CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED'))
    `);
  }
}
