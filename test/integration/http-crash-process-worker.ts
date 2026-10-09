import "reflect-metadata";
import { ProcessWagerTransaction } from "../../src/application/use-cases/process-wager-transaction";
import { bootstrapHttp } from "../../src/bootstrap";

if (process.env.TEST_CRASH_AFTER_COMMIT === "true") {
  const execute = ProcessWagerTransaction.prototype.execute;
  Object.defineProperty(ProcessWagerTransaction.prototype, "execute", {
    configurable: true,
    writable: true,
    value: async function (
      this: ProcessWagerTransaction,
      ...args: Parameters<ProcessWagerTransaction["execute"]>
    ): Promise<Awaited<ReturnType<ProcessWagerTransaction["execute"]>>> {
      await execute.apply(this, args);
      // This child is dedicated to the integration fault: commit has returned,
      // but the HTTP controller has not received a result to send to its caller.
      process.exit(86);
    },
  });
}

void bootstrapHttp().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : "HTTP worker failed");
  process.exitCode = 1;
});
