import { Inject, Injectable } from "@nestjs/common";
import type { DataSource } from "typeorm";
import { sha256 } from "../../application/use-cases/canonical-payload";
import { DATA_SOURCE } from "../persistence/data-source";
import type { DeadLetterFailureType } from "./dead-letter-types";

export interface DeadLetterMessageRecord {
  sourceQueue: string;
  messageId: string;
  failureType: DeadLetterFailureType;
  failureCode: string;
  body: string;
  payload: Record<string, unknown> | null;
  receiveCount: number;
}

export interface DeadLetterRecordResult {
  deliveryCount: number;
}

@Injectable()
export class DeadLetterMessageRecorder {
  constructor(@Inject(DATA_SOURCE) private readonly dataSource: DataSource) {}

  async record(
    input: DeadLetterMessageRecord,
  ): Promise<DeadLetterRecordResult> {
    const runner = this.dataSource.createQueryRunner();
    let connected = false;
    try {
      await runner.connect();
      connected = true;
      await runner.startTransaction();
      const rows = (await runner.query(
        `INSERT INTO dead_letter_messages (
           source_queue, message_id, failure_type, failure_code, body_hash,
           payload, sqs_receive_count
         ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
         ON CONFLICT (source_queue, message_id) DO UPDATE
         SET failure_type = EXCLUDED.failure_type,
             failure_code = EXCLUDED.failure_code,
             sqs_receive_count = GREATEST(
               dead_letter_messages.sqs_receive_count,
               EXCLUDED.sqs_receive_count
             ),
             delivery_count = dead_letter_messages.delivery_count + 1,
             last_received_at = now(),
             status = CASE WHEN dead_letter_messages.status IN ('REPLAY_QUEUED', 'REPROCESSING')
                           THEN 'REPROCESS_FAILED' ELSE dead_letter_messages.status END,
             resolved_at = CASE WHEN dead_letter_messages.status IN ('REPLAY_QUEUED', 'REPROCESSING')
                                THEN now() ELSE dead_letter_messages.resolved_at END
         WHERE dead_letter_messages.body_hash = EXCLUDED.body_hash
            OR dead_letter_messages.replay_body_hash = EXCLUDED.body_hash
         RETURNING delivery_count`,
        [
          input.sourceQueue,
          input.messageId,
          input.failureType,
          input.failureCode,
          sha256(input.body),
          input.payload ? JSON.stringify(input.payload) : null,
          input.receiveCount,
        ],
      )) as Array<{ delivery_count: number }>;

      const result = rows[0];
      if (!result) {
        throw new Error(
          "Dead-letter message identifier was reused with different content",
        );
      }
      await runner.commitTransaction();
      return { deliveryCount: result.delivery_count };
    } catch (error) {
      if (connected && runner.isTransactionActive)
        await runner.rollbackTransaction();
      throw error;
    } finally {
      if (connected) await runner.release();
    }
  }
}
