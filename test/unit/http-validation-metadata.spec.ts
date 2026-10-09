import "reflect-metadata";
import { describe, expect, test } from "bun:test";
import { BadRequestException, ValidationPipe } from "@nestjs/common";
import {
  CreateWalletDto,
  WagerTransactionDto,
} from "../../src/presentation/http/dto/wager-transaction.dto";
import { WageringController } from "../../src/presentation/http/wagering.controller";
import { WalletController } from "../../src/presentation/http/wallet.controller";

describe("HTTP DTO runtime metadata", () => {
  test("retains DTO classes for Nest's ValidationPipe", () => {
    expect(
      Reflect.getMetadata(
        "design:paramtypes",
        WalletController.prototype,
        "create",
      ),
    ).toEqual([CreateWalletDto]);
    expect(
      Reflect.getMetadata(
        "design:paramtypes",
        WageringController.prototype,
        "submit",
      ),
    ).toEqual([WagerTransactionDto, String, String]);
    expect(
      Reflect.getMetadata(
        "design:paramtypes",
        WageringController.prototype,
        "submitAsync",
      ),
    ).toEqual([WagerTransactionDto, String]);
  });

  test("rejects invalid request fields through the configured validation pipe", async () => {
    const pipe = new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
      transformOptions: { enableImplicitConversion: false },
    });

    await expect(
      pipe.transform(
        {
          providerId: "provider-a",
          externalTransactionId: "tx-1",
          playerId: "not-a-uuid",
          walletId: "0192f291-27dd-7d3f-8071-5f8685deef37",
          roundId: "round-1",
          gameId: "game-1",
          kind: "BET",
          money: { amount: "10.00", currency: "BRL" },
        },
        { type: "body", metatype: WagerTransactionDto },
      ),
    ).rejects.toThrow(BadRequestException);
  });
});
