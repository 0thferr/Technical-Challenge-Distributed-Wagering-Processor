process.env.WORKERS_ENABLED = "true";
import("./bootstrap")
  .then(({ bootstrapWorker }) => bootstrapWorker())
  .catch((error: unknown) => {
    const details =
      error instanceof Error &&
      error.message.startsWith("Missing required environment variable:")
        ? error.message
        : undefined;
    console.error(
      JSON.stringify({
        timestamp: new Date().toISOString(),
        level: "error",
        message: "worker_startup_failed",
        errorType: error instanceof Error ? error.name : "UnknownError",
        ...(details ? { details } : {}),
      }),
    );
    process.exitCode = 1;
  });
