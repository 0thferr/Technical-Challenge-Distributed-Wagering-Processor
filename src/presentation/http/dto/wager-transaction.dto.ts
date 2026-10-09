import { Type } from "class-transformer";
import {
  IsEnum,
  IsNotIn,
  IsOptional,
  IsString,
  IsUUID,
  Length,
  Matches,
  ValidateNested,
} from "class-validator";
import { WagerTransactionKind } from "../../../domain/wagering/wager-transaction";

export class MoneyDto {
  @IsString()
  @Matches(/^\d+(?:\.\d{1,2})?$/)
  amount!: string;

  @IsString()
  @Matches(/^[A-Z]{3}$/)
  currency!: string;
}

export class WagerTransactionDto {
  @IsString()
  @Length(1, 100)
  providerId!: string;

  @IsString()
  @Length(1, 200)
  externalTransactionId!: string;

  @IsUUID()
  playerId!: string;

  @IsUUID()
  walletId!: string;

  @IsString()
  @Length(1, 200)
  roundId!: string;

  @IsString()
  @Length(1, 200)
  gameId!: string;

  @IsEnum(WagerTransactionKind)
  @IsNotIn([WagerTransactionKind.Opening])
  kind!: WagerTransactionKind;

  @ValidateNested()
  @Type(() => MoneyDto)
  money!: MoneyDto;

  @IsOptional()
  @IsString()
  @Length(1, 200)
  referenceExternalTransactionId?: string;
}

export class QueuedWagerTransactionDto extends WagerTransactionDto {
  @IsString()
  @Length(1, 200)
  idempotencyKey!: string;
}

export class CreateWalletDto {
  @IsUUID()
  playerId!: string;

  @ValidateNested()
  @Type(() => MoneyDto)
  initialBalance!: MoneyDto;
}

export interface WagerQueueEnvelope {
  messageId: string;
  type: "WagerTransactionRequested";
  occurredAt: string;
  data: WagerTransactionDto & { idempotencyKey: string };
}
