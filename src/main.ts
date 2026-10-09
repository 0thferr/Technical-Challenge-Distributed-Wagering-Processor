void import("./bootstrap")
  .then(({ bootstrapHttp }) => bootstrapHttp())
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
        message: "http_startup_failed",
        errorType: error instanceof Error ? error.name : "UnknownError",
        ...(details ? { details } : {}),
      }),
    );
    process.exitCode = 1;
  });
