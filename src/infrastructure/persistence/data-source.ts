import "reflect-metadata";
import { DataSource, type DataSourceOptions } from "typeorm";
import { readEnvironment } from "../config/environment";
import { DeadLetterOperations2026100901000 } from "./migrations/DeadLetterOperations2026100901000";
import { DeadLetterRegistry2026100900000 } from "./migrations/DeadLetterRegistry2026100900000";
import { InitialSchema2026100800000 } from "./migrations/InitialSchema2026100800000";
import { OutboxAggregateOrdering2026100801000 } from "./migrations/OutboxAggregateOrdering2026100801000";

const environment = readEnvironment();

export const dataSourceOptions: DataSourceOptions = {
  type: "postgres",
  url: environment.databaseUrl,
  migrations: [
    InitialSchema2026100800000,
    OutboxAggregateOrdering2026100801000,
    DeadLetterRegistry2026100900000,
    DeadLetterOperations2026100901000,
  ],
  synchronize: false,
  migrationsRun: false,
  logging: false,
  extra: { max: 20, application_name: "distributed-wagering-processor" },
};

export const appDataSource = new DataSource(dataSourceOptions);
export const DATA_SOURCE = Symbol("DATA_SOURCE");
