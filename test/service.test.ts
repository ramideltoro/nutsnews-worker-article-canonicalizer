import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
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
  it("reports the configured shadow role ready without coupling health to production ownership", async () => {
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
    const readiness = await context.service.health.readiness();
    expect(context.config.expectedActive).toBe(false);
    expect(readiness.status).toBe("ok");
    expect(readiness.checks.some((check) => check.name === "deployment-ownership")).toBe(false);
    expect(context.metrics.collect()).toContain("nutsnews_worker_inflight");

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

  it("does not resurrect the broker after shutdown interrupts dependency startup", async () => {
    const config = loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalCanonicalizerDependencies();
    const broker = dependencies.brokerTransport as LocalBrokerTransport;
    const connectGate = deferred<undefined>();
    const connectStarted = deferred<undefined>();
    const connect = broker.connect.bind(broker);
    vi.spyOn(broker, "connect").mockImplementation(async () => {
      connectStarted.resolve(undefined);
      await connectGate.promise;
      await connect();
    });
    const service = createCanonicalizerService({
      config,
      dependencies
    });
    const startup = service.start();

    await connectStarted.promise;
    await expect(service.stop()).resolves.toBeUndefined();
    connectGate.resolve(undefined);

    await expect(startup).rejects.toThrow("Canonicalizer startup was cancelled by shutdown.");
    expect(service.isStarted).toBe(false);
    expect(service.consumer).toBeUndefined();
    expect(service.broker.state).toBe("closed");
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
  it("reports readiness unhealthy when the main queue consumer is cancelled", async () => {
    const context = createServiceContext();

    await context.service.start();
    await context.service.consumer?.cancel();

    const readiness = await context.service.health.readiness();
    expect(readiness.status).toBe("unhealthy");
    const consumerCheck = readiness.checks.find((check) => check.name === "rabbitmq-consumer");
    expect(consumerCheck?.status).toBe("unhealthy");
    expect(consumerCheck?.details).toMatchObject({
      queue: "nutsnews.worker.canonicalization.v1",
      activeConsumers: 0
    });

    await context.service.stop();
  });

  it("fails production readiness when only local state adapters are present", async () => {
    const config = loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE: "production",
      NUTSNEWS_CANONICALIZER_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = {
      ...createLocalCanonicalizerDependencies(),
      adapterMode: "mixed"
    } as const;
    const service = createCanonicalizerService({
      config,
      dependencies
    });

    await service.start();

    expect(service.isStarted).toBe(true);
    expect(service.consumer).toBeUndefined();
    await expect((dependencies.brokerTransport as LocalBrokerTransport).deliverCanonicalization()).rejects.toThrow(
      "No local consumer is registered for canonicalization."
    );
    const readiness = await service.health.readiness();
    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "production-adapters")).toMatchObject({
      status: "unhealthy",
      details: {
        mode: "production",
        reason: "production-adapters-unavailable",
        adapterMode: "mixed"
      }
    });
    expect(readiness.checks.some((check) => check.name === "deployment-ownership")).toBe(false);

    await service.stop();
  });

  it("defensively refuses a consumer when production environment and dependency mode disagree", async () => {
    const localConfig = loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const config = {
      ...localConfig,
      environment: " Production "
    };
    const dependencies = createLocalCanonicalizerDependencies();
    const service = createCanonicalizerService({
      config,
      dependencies
    });

    await service.start();

    expect(service.consumer).toBeUndefined();
    const readiness = await service.health.readiness();
    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "production-adapters")).toMatchObject({
      status: "unhealthy",
      details: {
        mode: "test",
        reason: "production-environment-mode-mismatch",
        adapterMode: "test"
      }
    });

    await service.stop();
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
    config,
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
