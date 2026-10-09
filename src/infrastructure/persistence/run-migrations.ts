import type { DataSource } from "typeorm";

async function main(): Promise<void> {
  const action = process.argv[2] ?? "run";
  let dataSource: DataSource | undefined;
  try {
    const persistence = await import("./data-source");
    dataSource = persistence.appDataSource;
    await dataSource.initialize();
    if (action === "run") {
      const migrations = await dataSource.runMigrations({ transaction: "all" });
      console.info(
        JSON.stringify({
          level: "info",
          message: "migrations_applied",
          count: migrations.length,
        }),
      );
    } else if (action === "down") {
      await dataSource.undoLastMigration({ transaction: "all" });
      console.info(
        JSON.stringify({ level: "info", message: "last_migration_reverted" }),
      );
    } else if (action === "show") {
      const pending = await dataSource.showMigrations();
      console.info(
        JSON.stringify({
          level: "info",
          message: "pending_migrations",
          pending,
        }),
      );
    } else {
      throw new Error(`Unknown migration command: ${action}`);
    }
  } catch (error) {
    const details =
      error instanceof Error &&
      error.message.startsWith("Missing required environment variable:")
        ? error.message
        : undefined;
    console.error(
      JSON.stringify({
        level: "error",
        message: "migration_command_failed",
        errorType: error instanceof Error ? error.name : "UnknownError",
        ...(details ? { details } : {}),
      }),
    );
    process.exitCode = 1;
  } finally {
    if (dataSource?.isInitialized) await dataSource.destroy();
  }
}

void main();
