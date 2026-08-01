import {
  describe,
  expect,
  it
} from "vitest";

import {
  CanonicalizerConfigError,
  loadCanonicalizerConfig
} from "../src/config.js";

describe("loadCanonicalizerConfig", () => {
  it("loads local test defaults without secret values", () => {
    const config = loadCanonicalizerConfig({
      HOSTNAME: "canonicalizer-host"
    });

    expect(config).toMatchObject({
      serviceName: "nutsnews-worker-article-canonicalizer",
      dependencyMode: "test",
      deploymentMode: "shadow",
      expectedActive: false,
      buildRevision: "unknown",
      host: "canonicalizer-host",
      concurrency: 8,
      prefetch: 16,
      startupTimeoutMs: 30_000,
      shadowMode: true,
      dependencies: {
        databaseConfigured: false,
        rabbitmqConfigured: false
      }
    });
  });

  it("fails production config by missing value names without retaining secrets", () => {
    expect(() => loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE: "production"
    })).toThrow(CanonicalizerConfigError);

    try {
      loadCanonicalizerConfig({
        NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE: "production"
      });
    } catch (error: unknown) {
      expect(error).toBeInstanceOf(CanonicalizerConfigError);
      const configError = error as CanonicalizerConfigError;

      expect(configError.issues).toEqual([
        "NUTSNEWS_CANONICALIZER_DATABASE_URL is required when NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE=production.",
        "NUTSNEWS_CANONICALIZER_RABBITMQ_URL is required when NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE=production.",
        "NUTSNEWS_CANONICALIZER_BUILD_REVISION must identify an immutable build when NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE=production."
      ]);
      expect(configError.message).not.toContain("postgres://");
      expect(configError.message).not.toContain("amqp://");
    }
  });

  it("rejects unsafe bounds and shadow cutover in this repo", () => {
    expect(() => loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_CONCURRENCY: "12",
      NUTSNEWS_CANONICALIZER_PREFETCH: "4",
      NUTSNEWS_CANONICALIZER_STARTUP_TIMEOUT_MS: "999",
      NUTSNEWS_CANONICALIZER_SHADOW_MODE: "false"
    })).toThrow(CanonicalizerConfigError);
  });

  it("accepts explicit production dependency presence without retaining values", () => {
    const config = loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE: "production",
      NUTSNEWS_CANONICALIZER_BUILD_REVISION: "0123456789abcdef",
      NUTSNEWS_CANONICALIZER_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_CANONICALIZER_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });

    expect(config.dependencies).toEqual({
      databaseConfigured: true,
      rabbitmqConfigured: true
    });
    expect(JSON.stringify(config)).not.toContain("postgres://example.invalid");
    expect(JSON.stringify(config)).not.toContain("amqp://example.invalid");
    expect(config).toMatchObject({
      buildRevision: "0123456789abcdef",
      deploymentMode: "shadow",
      expectedActive: false
    });
  });

  it("rejects unknown or unsafe production build identity", () => {
    expect(() => loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE: "production",
      NUTSNEWS_CANONICALIZER_BUILD_REVISION: "unknown",
      NUTSNEWS_CANONICALIZER_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_CANONICALIZER_RABBITMQ_URL: "amqp://example.invalid"
    })).toThrow(CanonicalizerConfigError);

    expect(() => loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_BUILD_REVISION: "revision with spaces"
    })).toThrow(CanonicalizerConfigError);
  });
});
