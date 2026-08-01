import type { RuntimeShutdownController } from "@ramideltoro/nutsnews-worker-runtime";

import type { CanonicalizerService } from "./service.js";

export class CanonicalizerStartupTimeoutError extends Error {
  readonly timeoutMs: number;

  constructor(timeoutMs: number) {
    super(`Canonicalizer startup exceeded ${String(timeoutMs)}ms.`);
    this.name = "CanonicalizerStartupTimeoutError";
    this.timeoutMs = timeoutMs;
  }
}

export async function startCanonicalizerRuntime(options: {
  readonly service: Pick<CanonicalizerService, "start">;
  readonly httpServer: {
    listen(): Promise<unknown>;
  };
  readonly shutdown: Pick<RuntimeShutdownController, "start" | "trigger">;
  readonly timeoutMs: number;
}): Promise<void> {
  await options.httpServer.listen();
  options.shutdown.start();

  try {
    await withStartupTimeout(options.service.start(), options.timeoutMs);
  } catch (error: unknown) {
    await options.shutdown.trigger("manual").catch(() => undefined);
    throw error;
  }
}

async function withStartupTimeout(operation: Promise<void>, timeoutMs: number): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    await Promise.race([
      operation,
      new Promise<void>((_, reject) => {
        timeout = setTimeout(() => {
          reject(new CanonicalizerStartupTimeoutError(timeoutMs));
        }, timeoutMs);
      })
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}
