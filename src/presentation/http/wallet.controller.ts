import {
  Body,
  Controller,
  Get,
  HttpCode,
  Param,
  ParseIntPipe,
  Post,
  Query,
} from "@nestjs/common";
import { ApplicationError } from "../../application/application-error";
import { WalletService } from "../../application/use-cases/wallet-service";
import { CreateWalletDto } from "./dto/wager-transaction.dto";

@Controller("wallets")
export class WalletController {
  constructor(private readonly wallets: WalletService) {}

  @Post()
  async create(@Body() body: CreateWalletDto): Promise<unknown> {
    return this.wallets.create({
      playerId: body.playerId,
      initialBalance: body.initialBalance,
    });
  }

  @Get(":walletId")
  async get(@Param("walletId") walletId: string): Promise<unknown> {
    this.assertUuid(walletId);
    return this.wallets.get(walletId);
  }

  @Get(":walletId/ledger")
  async ledger(
    @Param("walletId") walletId: string,
    @Query("cursor") cursor?: string,
    @Query("limit", new ParseIntPipe({ optional: true })) limit?: number,
  ): Promise<unknown> {
    this.assertUuid(walletId);
    return this.wallets.ledger(walletId, cursor, limit ?? 50);
  }

  @Post(":walletId/reconciliation")
  @HttpCode(200)
  async reconcile(@Param("walletId") walletId: string): Promise<unknown> {
    this.assertUuid(walletId);
    return this.wallets.reconcile(walletId);
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
