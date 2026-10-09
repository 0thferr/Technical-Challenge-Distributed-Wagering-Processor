import { describe, expect, test } from "bun:test";
import { readEnvironment } from "../../src/infrastructure/config/environment";

describe("environment validation", () => {
  test("rejects ports with trailing non-numeric content", () => {
    const previousPort = process.env.PORT;
    process.env.PORT = "3000garbage";
    try {
      expect(() => readEnvironment()).toThrow(
        "PORT must be an integer between 1 and 65535",
      );
    } finally {
      if (previousPort === undefined) delete process.env.PORT;
      else process.env.PORT = previousPort;
    }
  });

  test("requires infrastructure settings in production without logging secrets", () => {
    const previousNodeEnv = process.env.NODE_ENV;
    const previousDatabaseUrl = process.env.DATABASE_URL;
    process.env.NODE_ENV = "production";
    delete process.env.DATABASE_URL;
    try {
      expect(() => readEnvironment()).toThrow(
        "Missing required environment variable: DATABASE_URL",
      );
    } finally {
      if (previousNodeEnv === undefined) delete process.env.NODE_ENV;
      else process.env.NODE_ENV = previousNodeEnv;
      if (previousDatabaseUrl === undefined) delete process.env.DATABASE_URL;
      else process.env.DATABASE_URL = previousDatabaseUrl;
    }
  });
});
