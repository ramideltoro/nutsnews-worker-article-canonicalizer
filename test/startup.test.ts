import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import {
  CanonicalizerStartupTimeoutError,
  startCanonicalizerRuntime
} from "../src/startup.js";

describe("startCanonicalizerRuntime", () => {
  it("binds HTTP and signal handling before starting dependencies", async () => {
    const events: string[] = [];
    const httpServer = {
      listen: vi.fn(() => {
        events.push("http-listening");
        return Promise.resolve({});
      })
    };
    const shutdown = {
      start: vi.fn(() => {
        events.push("shutdown-listening");
      }),
      trigger: vi.fn(() => Promise.resolve())
    };
    const service = {
      start: vi.fn(() => {
        events.push("dependencies-starting");
        return Promise.resolve();
      })
    };

    await expect(startCanonicalizerRuntime({
      service,
      httpServer,
      shutdown,
      timeoutMs: 1_000
    })).resolves.toBeUndefined();
    expect(events).toEqual([
      "http-listening",
      "shutdown-listening",
      "dependencies-starting"
    ]);
    expect(shutdown.trigger).not.toHaveBeenCalled();
  });

  it("bounds dependency startup and triggers graceful cleanup", async () => {
    const service = {
      start: vi.fn(() => new Promise<void>(() => undefined))
    };
    const httpServer = {
      listen: vi.fn(() => Promise.resolve({}))
    };
    const shutdown = {
      start: vi.fn(),
      trigger: vi.fn(() => Promise.resolve())
    };

    await expect(startCanonicalizerRuntime({
      service,
      httpServer,
      shutdown,
      timeoutMs: 1
    })).rejects.toEqual(new CanonicalizerStartupTimeoutError(1));
    expect(httpServer.listen).toHaveBeenCalledOnce();
    expect(shutdown.start).toHaveBeenCalledOnce();
    expect(shutdown.trigger).toHaveBeenCalledWith("manual");
  });

  it("preserves dependency failures when graceful cleanup also rejects", async () => {
    const dependencyFailure = new Error("broker unavailable");
    const shutdown = {
      start: vi.fn(),
      trigger: vi.fn(() => Promise.reject(new Error("cleanup failed")))
    };

    await expect(startCanonicalizerRuntime({
      service: {
        start: () => Promise.reject(dependencyFailure)
      },
      httpServer: {
        listen: () => Promise.resolve({})
      },
      shutdown,
      timeoutMs: 1_000
    })).rejects.toBe(dependencyFailure);
    expect(shutdown.trigger).toHaveBeenCalledWith("manual");
  });
});
