import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadCanonicalizerConfig } from "../src/config.js";
import { createCanonicalizerService } from "../src/service.js";
import {
  InMemoryCanonicalStateStore,
  LocalBrokerTransport,
  LocalCanonicalBrokerOutbox,
  LocalCanonicalTransactionRunner,
  LocalCanonicalizerWorkHandler,
  createLocalCanonicalizerDependencies,
  createMinimalCanonicalizationDelivery
} from "../src/test-doubles.js";

describe("createCanonicalizerService", () => {
  it("starts, becomes ready, registers canonicalization and enrichment routes, and drains cleanly", async () => {
    const context = createServiceContext();

    await context.service.start();

    expect(context.service.isStarted).toBe(true);
    expect(context.service.consumer?.stage).toBe("canonicalization");
    expect(context.broker.assertedRoutes.map((route) => route.stage)).toEqual([
      "canonicalization",
      "enrichment"
    ]);
    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.startup()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("ok");
    expect(context.metrics.collect()).toContain("nutsnews_worker_dependency_duration_ms");

    await context.service.stop();

    expect(context.service.isStarted).toBe(false);
    expect(context.service.broker.state).toBe("closed");
    expect(context.telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("delegates a valid canonicalization delivery and acks duplicate replays without business logic", async () => {
    const context = createServiceContext();
    const delivery = createMinimalCanonicalizationDelivery();

    await context.service.start();

    await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.workHandler.handled[0]?.payload).toMatchObject({
      candidateId: "candidate-world-001",
      dedupeStatus: "new"
    });

    await context.service.stop();
  });

  it("waits for an in-flight delivery during shutdown without wall-clock sleeps", async () => {
    const context = createServiceContext();
    const gate = deferred<undefined>();
    const started = deferred<undefined>();

    context.workHandler.handleGate = gate.promise;
    context.workHandler.onHandleStart = () => {
      started.resolve(undefined);
    };

    await context.service.start();
    const delivery = context.broker.deliverCanonicalization();
    await started.promise;
    const stop = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    expect(context.workHandler.handled).toHaveLength(0);

    gate.resolve(undefined);
    await expect(delivery).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await stop;

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.service.isStarted).toBe(false);
  });

  it("reports readiness unhealthy when canonical state is unhealthy", async () => {
    const context = createServiceContext();

    context.stateStore.status = "unhealthy";
    await context.service.start();

    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    await context.service.stop();
  });

  it("reports readiness unhealthy when broker outbox is unhealthy", async () => {
    const context = createServiceContext();

    context.outbox.status = "unhealthy";
    await context.service.start();

    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    await context.service.stop();
  });
});

function createServiceContext() {
  const config = loadCanonicalizerConfig({
    NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
    NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
  });
  const dependencies = createLocalCanonicalizerDependencies();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createPrometheusRuntimeTelemetrySink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    }
  });
  const service = createCanonicalizerService({
    config,
    dependencies,
    telemetry,
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    metrics,
    outbox: dependencies.brokerOutbox as LocalCanonicalBrokerOutbox,
    service,
    stateStore: dependencies.stateStore as InMemoryCanonicalStateStore,
    telemetry,
    transactionRunner: dependencies.transactionRunner as LocalCanonicalTransactionRunner,
    workHandler: dependencies.workHandler as LocalCanonicalizerWorkHandler
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}
