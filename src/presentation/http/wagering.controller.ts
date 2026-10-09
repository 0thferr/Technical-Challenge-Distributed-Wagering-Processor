import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpException,
  Param,
  Post,
} from "@nestjs/common";
import { ApplicationError } from "../../application/application-error";
import type { WagerSubmission } from "../../application/use-cases/process-wager-transaction";
import { ProcessWagerTransaction } from "../../application/use-cases/process-wager-transaction";
import { WagerQueryService } from "../../application/use-cases/wager-query-service";
import { Money } from "../../domain/money/money";
import { WagerTransactionStatus } from "../../domain/wagering/wager-transaction";
import { SqsQueueService } from "../../infrastructure/messaging/sqs-queue.service";
import { WagerTransactionDto } from "./dto/wager-transaction.dto";

@Controller()
export class WageringController {
  constructor(
    private readonly processor: ProcessWagerTransaction,
    private readonly queries: WagerQueryService,
    private readonly queue: SqsQueueService,
  ) {}

  @Post("wagering/transactions")
  @HttpCode(200)
  async submit(
    @Body() body: WagerTransactionDto,
    @Headers("idempotency-key") idempotencyKey?: string,
    @Headers("x-correlation-id") correlationId?: string,
  ): Promise<unknown> {
    const submission = this.toSubmission(body, idempotencyKey, correlationId);
    const result = await this.processor.execute(submission);
    if (result.status === WagerTransactionStatus.PendingReference)
      throw new HttpException(result, 202);
    if (result.status === WagerTransactionStatus.Rejected)
      throw new HttpException(result, 422);
    return result;
  }

  @Post("wagering/transactions/async")
  @HttpCode(202)
  async submitAsync(
    @Body() body: WagerTransactionDto,
    @Headers("idempotency-key") idempotencyKey?: string,
  ): Promise<unknown> {
    const submission = this.toSubmission(body, idempotencyKey);
    const queued = await this.queue.enqueueTransaction(submission);
    return { messageId: queued.messageId, status: "ACCEPTED" };
  }

  @Get("wagering/transactions/:transactionId")
  async byId(@Param("transactionId") transactionId: string): Promise<unknown> {
    this.assertUuid(transactionId);
    return this.queries.byId(transactionId);
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  async byExternal(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ): Promise<unknown> {
    return this.queries.byExternal(providerId, externalTransactionId);
  }

  private toSubmission(
    body: WagerTransactionDto,
    idempotencyKey?: string,
    correlationId?: string,
  ): WagerSubmission {
    if (
      !idempotencyKey ||
      idempotencyKey.trim().length === 0 ||
      idempotencyKey.length > 200
    ) {
      throw new ApplicationError(
        "Idempotency-Key header is required",
        "IDEMPOTENCY_KEY_REQUIRED",
        400,
      );
    }
    const money = Money.from(body.money);
    money.assertPositive();
    return {
      providerId: body.providerId,
      externalTransactionId: body.externalTransactionId,
      playerId: body.playerId,
      walletId: body.walletId,
      roundId: body.roundId,
      gameId: body.gameId,
      kind: body.kind,
      money: money.toJSON(),
      idempotencyKey,
      ...(correlationId && correlationId.length <= 200
        ? { correlationId }
        : {}),
      ...(body.referenceExternalTransactionId
        ? {
            referenceExternalTransactionId: body.referenceExternalTransactionId,
          }
        : {}),
    };
  }

  private assertUuid(value: string): void {
    if (
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
        value,
      )
    ) {
      throw new ApplicationError(
        "Identifier must be a UUID",
        "INVALID_IDENTIFIER",
        400,
      );
    }
  }
}
