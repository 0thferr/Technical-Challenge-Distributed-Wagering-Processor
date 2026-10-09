import { Injectable } from "@nestjs/common";

export type LogContext = Record<string, string | number | boolean | undefined>;

@Injectable()
export class StructuredLogger {
  info(message: string, context: LogContext = {}): void {
    this.write("info", message, context);
  }

  warn(message: string, context: LogContext = {}): void {
    this.write("warn", message, context);
  }

  error(message: string, context: LogContext = {}): void {
    this.write("error", message, context);
  }

  private write(level: string, message: string, context: LogContext): void {
    const line = JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      message,
      ...context,
    });
    if (level === "error") console.error(line);
    else if (level === "warn") console.warn(line);
    else console.log(line);
  }
}
