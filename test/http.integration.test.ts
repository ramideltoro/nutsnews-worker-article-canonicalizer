import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadCanonicalizerConfig } from "../src/config.js";
import {
  createCanonicalizerHttpServer,
  type CanonicalizerHttpServer
} from "../src/http.js";
import {
  createCanonicalizerFailClosedReconciler
} from "../src/reconciliation.js";
import { createCanonicalizationPrometheusTelemetrySink } from "../src/metrics.js";
import { createCanonicalizerService } from "../src/service.js";
import {
  ManualCanonicalizerClock,
  createLocalCanonicalizerDependencies,
  createMinimalCanonicalizationDelivery
} from "../src/test-doubles.js";

let activeServer: CanonicalizerHttpServer | undefined;

afterEach(async () => {
  if (activeServer !== undefined) {
    await activeServer.close();
    activeServer = undefined;
  }
});

describe("canonicalizer HTTP endpoints", () => {
  it("exposes startup observability before dependency initialization completes", async () => {
    const config = loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const service = createCanonicalizerService({
      config,
      dependencies: createLocalCanonicalizerDependencies()
    });
    activeServer = createCanonicalizerHttpServer({
      config,
      service
    });

    await activeServer.listen();

    try {
      await expectJsonStatus(activeServer.url("/livez"), 200, "ok");
      await expectJsonStatus(activeServer.url("/startupz"), 503, "unhealthy");
      await expectJsonStatus(activeServer.url("/readyz"), 503, "unhealthy");

      await service.start();

      await expectJsonStatus(activeServer.url("/startupz"), 200, "ok");
      await expectJsonStatus(activeServer.url("/readyz"), 200, "ok");
    } finally {
      await service.stop();
    }
  });

  it("serves liveness, readiness, startup, metrics, and value-free config schema", async () => {
    const config = loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_BUILD_REVISION: "test-revision",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const metrics = createCanonicalizationPrometheusTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      buildRevision: config.buildRevision,
      deployment: config.deploymentMode,
      expectedActive: config.expectedActive,
      adapter: "test"
    });
    const service = createCanonicalizerService({
      config,
      dependencies: createLocalCanonicalizerDependencies(),
      telemetry: metrics,
      metrics
    });
    activeServer = createCanonicalizerHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await activeServer.listen();

    await expectJsonStatus(activeServer.url("/live"), 200, "ok");
    await expectJsonStatus(activeServer.url("/livez"), 200, "ok");
    await expectJsonStatus(activeServer.url("/startup"), 200, "ok");
    await expectJsonStatus(activeServer.url("/ready"), 200, "ok");
    await expect(service.processDelivery(createMinimalCanonicalizationDelivery())).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });

    const metricsResponse = await fetch(activeServer.url("/metrics"));
    expect(metricsResponse.status).toBe(200);
    const metricsBody = await metricsResponse.text();
    expect(metricsBody).toContain("nutsnews_worker_messages_total");
    expect(metricsBody).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="canonicalization",outcome="success"} 1');
    expect(metricsBody).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="canonicalization",outcome="failure"} 0');
    expect(metricsBody).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="canonicalization",le="30"} 1');
    expect(metricsBody).toContain('nutsnews_worker_build_info{environment="local",service="nutsnews-worker-article-canonicalizer",version="0.1.0",revision="test-revision"} 1');
    expect(metricsBody).toContain('nutsnews_worker_deployment_info{environment="local",service="nutsnews-worker-article-canonicalizer",deployment="shadow",adapter="in_memory"} 1');
    expect(metricsBody).toContain('nutsnews_worker_expected_active{environment="local",service="canonicalizer"} 0');
    expect(metricsBody).toContain('nutsnews_worker_health_probe{environment="local",service="canonicalization",probe="liveness",outcome="ok"} 1');
    expect(metricsBody).toContain('nutsnews_worker_health_probe{environment="local",service="canonicalization",probe="startup",outcome="ok"} 1');
    expect(metricsBody).toContain('nutsnews_worker_health_probe{environment="local",service="canonicalization",probe="readiness",outcome="ok"} 1');
    expect(metricsBody).not.toContain("nutsnews_worker_dependency_duration_ms");

    const schemaResponse = await fetch(activeServer.url("/config-schema"));
    expect(schemaResponse.status).toBe(200);
    const schema = await schemaResponse.json() as { readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[] };

    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_CANONICALIZER_RABBITMQ_URL" && variable.sensitive)).toBe(true);
    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_CANONICALIZER_BUILD_REVISION" && !variable.sensitive)).toBe(true);
    expect(JSON.stringify(schema)).not.toContain("amqp://");

    await service.stop();
  });

  it("protects the reconciliation endpoint with bearer auth", async () => {
    const config = loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const service = createCanonicalizerService({
      config,
      dependencies: createLocalCanonicalizerDependencies()
    });
    activeServer = createCanonicalizerHttpServer({
      config,
      service,
      reconciler: createCanonicalizerFailClosedReconciler(new ManualCanonicalizerClock()),
      reconciliationToken: "test-token"
    });

    await service.start();
    await activeServer.listen();

    const unauthorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(unauthorized.status).toBe(401);

    const authorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      headers: {
        authorization: "Bearer test-token"
      },
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(authorized.status).toBe(200);
    await expect(authorized.json()).resolves.toMatchObject({
      status: "dry_run",
      writesPerformed: false,
      productionVisibilityEnabled: false
    });

    await service.stop();
  });
});

async function expectJsonStatus(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}
