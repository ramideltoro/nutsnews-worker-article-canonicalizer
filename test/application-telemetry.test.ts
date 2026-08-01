import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import {
  createCanonicalizerApplication,
  loadCanonicalizerConfig
} from "../src/index.js";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("canonicalizer application telemetry identity", () => {
  it("includes immutable deployment and adapter identity in every structured runtime log", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const config = loadCanonicalizerConfig({
      HOSTNAME: "canonicalizer-log-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_CANONICALIZER_BUILD_REVISION: "0123456789abcdef",
      NUTSNEWS_CANONICALIZER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_METRICS_ENABLED: "false",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "stdout"
    });
    const application = createCanonicalizerApplication(config);

    try {
      await application.start();
    } finally {
      await application.stop();
    }

    const records = log.mock.calls.map(([line]) => JSON.parse(String(line)) as Readonly<Record<string, unknown>>);

    expect(records.length).toBeGreaterThan(0);

    for (const record of records) {
      expect(record).toMatchObject({
        service: "canonicalizer",
        version: "0.1.0",
        environment: "test",
        host: "canonicalizer-log-test",
        revision: "0123456789abcdef",
        deployment: "shadow",
        adapter: "in_memory"
      });
    }
  });
});
