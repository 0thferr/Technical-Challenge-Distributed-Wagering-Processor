import type { MigrationInterface, QueryRunner } from "typeorm";

export class OutboxAggregateOrdering2026100801000
  implements MigrationInterface
{
  name = "OutboxAggregateOrdering2026100801000";

  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      "ALTER TABLE outbox_messages ADD COLUMN sequence_id bigserial",
    );
    await queryRunner.query(`
      WITH ordered AS (
        SELECT id, row_number() OVER (ORDER BY created_at, id) AS sequence_id
        FROM outbox_messages
      )
      UPDATE outbox_messages AS outbox
      SET sequence_id = ordered.sequence_id
      FROM ordered
      WHERE outbox.id = ordered.id
    `);
    await queryRunner.query(
      "ALTER TABLE outbox_messages ADD CONSTRAINT outbox_sequence_id_unique UNIQUE (sequence_id)",
    );
    await queryRunner.query(
      "CREATE INDEX outbox_unpublished_aggregate_order_idx ON outbox_messages (aggregate_id, sequence_id) WHERE published_at IS NULL",
    );
  }

  async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      "DROP INDEX outbox_unpublished_aggregate_order_idx",
    );
    await queryRunner.query(
      "ALTER TABLE outbox_messages DROP CONSTRAINT outbox_sequence_id_unique",
    );
    await queryRunner.query(
      "ALTER TABLE outbox_messages DROP COLUMN sequence_id",
    );
  }
}
