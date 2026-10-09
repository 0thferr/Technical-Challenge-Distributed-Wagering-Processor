import type { MigrationInterface, QueryRunner } from "typeorm";

export class DeadLetterRegistry2026100900000 implements MigrationInterface {
  name = "DeadLetterRegistry2026100900000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE dead_letter_messages (
        source_queue text NOT NULL,
        message_id text NOT NULL,
        failure_type text NOT NULL CHECK (
          failure_type IN ('INVALID_MESSAGE', 'PERMANENT_FAILURE', 'RETRY_EXHAUSTED', 'UNKNOWN')
        ),
        failure_code text NOT NULL,
        body_hash char(64) NOT NULL CHECK (body_hash ~ '^[a-f0-9]{64}$'),
        payload jsonb,
        sqs_receive_count integer NOT NULL CHECK (sqs_receive_count >= 1),
        delivery_count integer NOT NULL DEFAULT 1 CHECK (delivery_count >= 1),
        status text NOT NULL DEFAULT 'NEEDS_INVESTIGATION' CHECK (
          status IN ('NEEDS_INVESTIGATION', 'READY_FOR_REPROCESS', 'RESOLVED')
        ),
        first_received_at timestamptz NOT NULL DEFAULT now(),
        last_received_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (source_queue, message_id)
      )
    `);
    await queryRunner.query(
      "CREATE INDEX dead_letter_messages_review_idx ON dead_letter_messages (status, last_received_at DESC)",
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query("DROP INDEX dead_letter_messages_review_idx");
    await queryRunner.query("DROP TABLE dead_letter_messages");
  }
}
