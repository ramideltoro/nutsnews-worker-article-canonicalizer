import {
  WORKER_DELIVERY_BEHAVIOR,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  RUNTIME_ALLOWED_METRIC_LABELS,
  RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS,
  createBufferedRuntimeTelemetrySink,
  createRuntimeShutdownController,
  type RuntimeMessageDelivery
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadCanonicalizerConfig } from "../src/config.js";
import {
  CANONICALIZATION_STAGE_LATENCY_BUCKETS_SECONDS,
  createCanonicalizationPrometheusTelemetrySink
} from "../src/metrics.js";
import { createCanonicalizerService } from "../src/service.js";
import {
  combineBestEffortRuntimeTelemetrySinks,
  createBestEffortRuntimeTelemetryFlusher
} from "../src/telemetry.js";
import {
  LocalBrokerTransport,
  InMemoryCanonicalStateStore,
  LocalCanonicalizerWorkHandler,
  ManualCanonicalizerClock,
  createLocalCanonicalizerDependencies,
  createMinimalCanonicalizationDelivery,
  createMinimalCanonicalizationEnvelope,
  createMinimalCanonicalizationPayload
} from "../src/test-doubles.js";

const COMPLETING_MESSAGE_EVENTS = new Set([
  "runtime.message.accepted",
  "runtime.message.duplicate",
  "runtime.message.invalid",
  "runtime.message.retry",
  "runtime.message.dlq"
]);

describe("canonicalization lifecycle telemetry", () => {
  it("exposes a fixed zero-initialized canonical stage series set before traffic", async () => {
    const context = createTelemetryContext();
    const beforeTraffic = context.metrics.collect();

    for (const outcome of [
      "success",
      "duplicate",
      "invalid",
      "retry",
      "dlq"
    ]) {
      expect(metricValue(beforeTraffic, "nutsnews_worker_uplift_stage_events_total", outcome)).toBe(0);
    }

    for (const boundary of CANONICALIZATION_STAGE_LATENCY_BUCKETS_SECONDS) {
      expect(sampleValue(beforeTraffic, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
        le: String(boundary)
      })).toBe(0);
    }

    expect(sampleValue(beforeTraffic, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "+Inf"
    })).toBe(0);
    expect(sampleValue(beforeTraffic, "nutsnews_worker_uplift_stage_latency_seconds_sum")).toBe(0);
    expect(sampleValue(beforeTraffic, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(0);

    const initialSeries = canonicalStageSeries(beforeTraffic);
    expect(initialSeries).toHaveLength(21);

    await context.metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "canonicalization",
      queue: "nutsnews.worker.canonicalization.v1",
      outcome: "success",
      durationMs: 250
    });

    const afterTraffic = context.metrics.collect();
    expect(canonicalStageSeries(afterTraffic)).toEqual(initialSeries);
    expect(metricValue(afterTraffic, "nutsnews_worker_uplift_stage_events_total", "success")).toBe(1);
    expect(metricValue(afterTraffic, "nutsnews_worker_uplift_stage_events_total", "duplicate")).toBe(0);
    expect(sampleValue(afterTraffic, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "0.25"
    })).toBe(1);
    expect(sampleValue(afterTraffic, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(1);
  });

  it("isolates composite destinations so one rejection cannot block the others", async () => {
    const rejectingTelemetry = {
      emit: vi.fn(() => Promise.reject(new Error("log destination unavailable")))
    };
    const acceptingTelemetry = {
      emit: vi.fn(() => Promise.resolve())
    };
    const telemetry = combineBestEffortRuntimeTelemetrySinks(rejectingTelemetry, acceptingTelemetry);
    const event = {
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "canonicalization",
      queue: "nutsnews.worker.canonicalization.v1",
      outcome: "success",
      attributes: {
        dependency: "canonical-state"
      }
    } as const;

    await expect(telemetry?.emit(event)).resolves.toBeUndefined();
    expect(rejectingTelemetry.emit).toHaveBeenCalledWith(event);
    expect(acceptingTelemetry.emit).toHaveBeenCalledWith(event);
  });

  it("treats raw telemetry flusher rejection as non-authoritative", async () => {
    const rawFlusher = {
      flush: vi.fn(() => Promise.reject(new Error("stdout flush unavailable")))
    };
    const flusher = createBestEffortRuntimeTelemetryFlusher(rawFlusher);
    const shutdownCallback = vi.fn(() => Promise.resolve());
    const shutdown = createRuntimeShutdownController({
      callbacks: [
        shutdownCallback
      ],
      telemetryFlusher: flusher,
      timeoutMs: 1_000
    });

    await expect(shutdown.trigger("manual")).resolves.toBeUndefined();
    expect(shutdownCallback).toHaveBeenCalledOnce();
    expect(rawFlusher.flush).toHaveBeenCalledOnce();
  });

  it("emits exactly one completing event and one stage sample for every delivery outcome", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();
    await exerciseLifecycleOutcomes(context);

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.accepted",
      "runtime.message.started",
      "runtime.message.duplicate",
      "runtime.message.started",
      "runtime.message.invalid",
      "runtime.message.started",
      "runtime.message.retry",
      "runtime.message.started",
      "runtime.message.dlq",
      "runtime.message.started",
      "runtime.message.dlq"
    ]);

    const started = messageEvents.filter((event) => event.name === "runtime.message.started");
    const completed = messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name));
    expect(started).toHaveLength(6);
    expect(completed).toHaveLength(started.length);
    expect(context.workHandler.handled).toHaveLength(4);

    expect(completed[0]).toMatchObject({
      name: "runtime.message.accepted",
      outcome: "success",
      messageId: messageId(1),
      idempotencyKey: idempotencyKey(1)
    });
    expect(completed[1]).toMatchObject({
      name: "runtime.message.duplicate",
      outcome: "duplicate",
      messageId: messageId(1),
      idempotencyKey: idempotencyKey(1)
    });
    expect(completed[2]).toMatchObject({
      name: "runtime.message.invalid",
      outcome: "failure",
      attributes: {
        issueCode: "stage-mismatch",
        issuePath: "$.route"
      }
    });
    expect(completed[3]).toMatchObject({
      name: "runtime.message.retry",
      outcome: "retry",
      attributes: {
        reason: "transient-canonicalization-error",
        destination: "nutsnews.worker.canonicalization.v1.retry-30s"
      }
    });
    expect(completed[4]).toMatchObject({
      name: "runtime.message.dlq",
      outcome: "dlq",
      attributes: {
        reason: "retry-exhausted",
        destination: "nutsnews.worker.canonicalization.v1.dlq"
      }
    });
    expect(completed[5]).toMatchObject({
      name: "runtime.message.dlq",
      outcome: "dlq",
      attributes: {
        reason: "terminal-canonicalization-error",
        destination: "nutsnews.worker.canonicalization.v1.dlq"
      }
    });

    const output = context.metrics.collect();
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "success")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "duplicate")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "invalid")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(metricValue(output, "nutsnews_worker_uplift_stage_events_total", "dlq")).toBe(2);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "0.01"
    })).toBe(2);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "0.25"
    })).toBe(6);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "30"
    })).toBe(6);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_bucket", {
      le: "+Inf"
    })).toBe(6);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_sum")).toBe(1);
    expect(sampleValue(output, "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(6);
    expect(output).toContain('nutsnews_worker_build_info{environment="test",service="nutsnews-worker-article-canonicalizer",version="0.1.0",revision="telemetry-test-revision"} 1');
    expect(output).toContain('nutsnews_worker_deployment_info{environment="test",service="nutsnews-worker-article-canonicalizer",deployment="shadow",adapter="in_memory"} 1');
    expect(output).toContain('nutsnews_worker_expected_active{environment="test",service="canonicalizer"} 0');
    expect(CANONICALIZATION_STAGE_LATENCY_BUCKETS_SECONDS).toEqual([
      0.01,
      0.05,
      0.1,
      0.25,
      0.5,
      1,
      2.5,
      5,
      10,
      30,
      60,
      120,
      300
    ]);
    expect(context.metrics.allowedLabels).toEqual(RUNTIME_ALLOWED_METRIC_LABELS);

    for (const line of customMetricSampleLines(output)) {
      expect(metricLabelNames(line)).toEqual(expectedCustomMetricLabelNames(line));
    }

    for (const forbidden of RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS) {
      expect(output).not.toContain(`${forbidden}=`);
    }

    for (const identifier of [
      messageId(1),
      idempotencyKey(1),
      "candidate-world-001",
      "feed-world",
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    ]) {
      expect(output).not.toContain(identifier);
    }

    await context.service.stop();
  });

  it("emits non-unknown immutable identity and truthful mixed adapters for backend production configuration", () => {
    const config = loadCanonicalizerConfig({
      HOSTNAME: "canonicalization-production-test",
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_CANONICALIZER_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_CANONICALIZER_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE: "production",
      NUTSNEWS_CANONICALIZER_RABBITMQ_URL: "amqp://example.invalid"
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
      adapter: "mixed"
    });
    const output = metrics.collect();

    expect(output).toContain('nutsnews_worker_build_info{environment="production",service="nutsnews-worker-article-canonicalizer",version="0.1.0",revision="0123456789abcdef0123456789abcdef01234567"} 1');
    expect(output).toContain('nutsnews_worker_deployment_info{environment="production",service="nutsnews-worker-article-canonicalizer",deployment="shadow",adapter="mixed"} 1');
    expect(output).toContain('nutsnews_worker_expected_active{environment="production",service="canonicalizer"} 0');
    expect(output).not.toContain('revision="unknown"');
    expect(output).not.toContain('deployment="unknown"');
    expect(output).not.toContain('adapter="unknown"');
  });

  it("reports distinct probe health and turns readiness unhealthy after consumer cancellation", async () => {
    const context = createTelemetryContext();

    const initialOutput = context.metrics.collect();
    expect(initialOutput).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="liveness",outcome="ok"} 1');
    expect(initialOutput).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="startup",outcome="unhealthy"} 1');
    expect(initialOutput).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="readiness",outcome="unhealthy"} 1');

    await context.service.start();
    expect(context.metrics.collect()).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="startup",outcome="ok"} 1');
    await context.service.health.readiness();
    await context.service.consumer?.cancel();

    expect((await context.service.health.readiness()).status).toBe("unhealthy");

    const output = context.metrics.collect();
    expect(output).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="liveness",outcome="ok"} 1');
    expect(output).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="startup",outcome="ok"} 1');
    expect(output).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="readiness",outcome="unhealthy"} 1');
    expect(output).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="readiness",outcome="ok"} 0');

    await context.service.stop();
  });

  it("does not manufacture zero-duration dependency observations", async () => {
    const context = createTelemetryContext();

    await context.metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "canonicalization",
      queue: "nutsnews.worker.canonicalization.v1",
      outcome: "success",
      attributes: {
        dependency: "canonical-state"
      }
    });

    expect(context.metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");
  });

  it("turns an idempotency claim failure into exactly one retry completion", async () => {
    const context = createTelemetryContext();

    await context.service.start();
    context.telemetry.clear();
    vi.spyOn(context.stateStore, "claim").mockRejectedValueOnce(new Error("state unavailable"));

    await expect(context.broker.deliverCanonicalization(canonicalizationDelivery(7))).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-claim-error"
    });

    const messageEvents = context.telemetry.events.filter((event) => event.name.startsWith("runtime.message."));
    expect(messageEvents.map((event) => event.name)).toEqual([
      "runtime.message.started",
      "runtime.message.retry"
    ]);
    expect(messageEvents.filter((event) => COMPLETING_MESSAGE_EVENTS.has(event.name))).toHaveLength(1);
    expect(metricValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_events_total", "retry")).toBe(1);
    expect(sampleValue(context.metrics.collect(), "nutsnews_worker_uplift_stage_latency_seconds_count")).toBe(1);

    await context.service.stop();
  });

  it("preserves delivery semantics when the telemetry sink rejects", async () => {
    const config = loadCanonicalizerConfig({
      HOSTNAME: "canonicalization-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalCanonicalizerDependencies();
    const rejectingTelemetry = {
      emit: vi.fn(() => Promise.reject(new Error("telemetry unavailable")))
    };
    const service = createCanonicalizerService({
      config,
      dependencies,
      telemetry: rejectingTelemetry
    });

    await expect(service.start()).resolves.toBeUndefined();
    await expect(
      (dependencies.brokerTransport as LocalBrokerTransport).deliverCanonicalization(canonicalizationDelivery(8))
    ).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(rejectingTelemetry.emit).toHaveBeenCalled();
    expect((dependencies.workHandler as LocalCanonicalizerWorkHandler).handled).toHaveLength(1);
    await expect(service.stop()).resolves.toBeUndefined();
  });

  it("keeps metric control failures outside service and delivery semantics", async () => {
    const config = loadCanonicalizerConfig({
      HOSTNAME: "canonicalization-test",
      NUTSNEWS_ENVIRONMENT: "test",
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalCanonicalizerDependencies();
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
      adapter: dependencies.adapterMode
    });
    const metricFailure = () => {
      throw new Error("metrics unavailable");
    };
    const setInFlight = vi.spyOn(metrics, "setInFlight").mockImplementation(metricFailure);
    const setShutdownDraining = vi.spyOn(metrics, "setShutdownDraining").mockImplementation(metricFailure);
    const setStartupComplete = vi.spyOn(metrics, "setStartupComplete").mockImplementation(metricFailure);
    const setReadinessUnhealthy = vi.spyOn(metrics, "setReadinessUnhealthy").mockImplementation(metricFailure);
    const service = createCanonicalizerService({
      config,
      dependencies,
      metrics
    });

    await expect(service.start()).resolves.toBeUndefined();
    await expect(
      (dependencies.brokerTransport as LocalBrokerTransport).deliverCanonicalization(canonicalizationDelivery(9))
    ).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(service.stop()).resolves.toBeUndefined();
    expect(setInFlight).toHaveBeenCalled();
    expect(setShutdownDraining).toHaveBeenCalled();
    expect(setStartupComplete).toHaveBeenCalled();
    expect(setReadinessUnhealthy).toHaveBeenCalled();
  });
});

function createTelemetryContext() {
  const config = loadCanonicalizerConfig({
    HOSTNAME: "canonicalization-test",
    NUTSNEWS_ENVIRONMENT: "test",
    NUTSNEWS_CANONICALIZER_BUILD_REVISION: "telemetry-test-revision",
    NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
    NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
  });
  const clock = new ManualCanonicalizerClock();
  const dependencies = createLocalCanonicalizerDependencies({
    clock
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
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
    adapter: dependencies.adapterMode
  });
  const service = createCanonicalizerService({
    config,
    dependencies,
    telemetry: {
      emit: async (event) => {
        await telemetry.emit(event);
        await metrics.emit(event);
      }
    },
    metrics
  });
  const workHandler = dependencies.workHandler as LocalCanonicalizerWorkHandler;
  workHandler.onHandleStart = () => {
    clock.advance(250);
  };

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    metrics,
    service,
    stateStore: dependencies.stateStore as InMemoryCanonicalStateStore,
    telemetry,
    workHandler
  };
}

async function exerciseLifecycleOutcomes(context: ReturnType<typeof createTelemetryContext>): Promise<void> {
  const accepted = canonicalizationDelivery(1);
  await expect(context.broker.deliverCanonicalization(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "handled"
  });
  await expect(context.broker.deliverCanonicalization(accepted)).resolves.toMatchObject({
    action: "ack",
    reason: "duplicate"
  });

  await expect(context.broker.deliverCanonicalization(canonicalizationDelivery(2, {
    route: "enrichment"
  }))).resolves.toMatchObject({
    action: "dlq",
    reason: "stage-mismatch"
  });

  context.workHandler.result = {
    status: "retry",
    reason: "transient-canonicalization-error",
    retryAfterMs: 2_000
  };
  await expect(context.broker.deliverCanonicalization(canonicalizationDelivery(3))).resolves.toMatchObject({
    action: "retry",
    reason: "transient-canonicalization-error"
  });

  context.workHandler.result = {
    status: "retry",
    reason: "retry-exhausted"
  };
  await expect(context.broker.deliverCanonicalization(canonicalizationDelivery(4, {
    attempt: {
      count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T00:00:00.000Z",
      lastAttemptAt: "2026-07-23T00:05:00.000Z"
    }
  }))).resolves.toMatchObject({
    action: "dlq",
    reason: "retry-exhausted"
  });

  context.workHandler.result = {
    status: "terminal-failure",
    reason: "terminal-canonicalization-error"
  };
  await expect(context.broker.deliverCanonicalization(canonicalizationDelivery(5))).resolves.toMatchObject({
    action: "dlq",
    reason: "terminal-canonicalization-error"
  });
}

function canonicalizationDelivery(
  sequence: number,
  envelopeOverrides: Partial<WorkerMessageEnvelope> = {}
): RuntimeMessageDelivery {
  return {
    ...createMinimalCanonicalizationDelivery(),
    envelope: createMinimalCanonicalizationEnvelope({
      messageId: messageId(sequence),
      idempotencyKey: idempotencyKey(sequence),
      ...envelopeOverrides
    }),
    payload: createMinimalCanonicalizationPayload({
      idempotencyKey: idempotencyKey(sequence)
    })
  };
}

function messageId(sequence: number): string {
  return `018f1598-2dd5-7c4f-9f92-8f7a7f8b49${String(sequence).padStart(2, "0")}`;
}

function idempotencyKey(sequence: number): string {
  return `fetcher:canonicalization:telemetry-${String(sequence)}`;
}

function metricValue(output: string, metric: string, outcome: string): number {
  return sampleValue(output, metric, {
    outcome
  });
}

function sampleValue(
  output: string,
  metric: string,
  requiredLabels: Readonly<Record<string, string>> = {}
): number {
  const matches = output
    .split("\n")
    .filter((line) => line.startsWith(`${metric}{`) && Object.entries(requiredLabels).every(([name, value]) => line.includes(`${name}="${value}"`)));

  expect(matches).toHaveLength(1);
  const value = matches[0]?.split(" ").at(-1);

  return Number(value);
}

function customMetricSampleLines(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => [
      "nutsnews_worker_build_info{",
      "nutsnews_worker_deployment_info{",
      "nutsnews_worker_expected_active{",
      "nutsnews_worker_health_probe{",
      "nutsnews_worker_uplift_stage_events_total{",
      "nutsnews_worker_uplift_stage_latency_seconds_"
    ].some((prefix) => line.startsWith(prefix)) && line.includes("{"));
}

function canonicalStageSeries(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => [
      "nutsnews_worker_uplift_stage_events_total{",
      "nutsnews_worker_uplift_stage_latency_seconds_"
    ].some((prefix) => line.startsWith(prefix)))
    .map((line) => line.slice(0, line.lastIndexOf(" ")));
}

function metricLabelNames(line: string): readonly string[] {
  const start = line.indexOf("{");
  const end = line.indexOf("}", start);

  return line
    .slice(start + 1, end)
    .split(",")
    .map((label) => label.slice(0, label.indexOf("=")));
}

function expectedCustomMetricLabelNames(line: string): readonly string[] {
  if (line.startsWith("nutsnews_worker_build_info{")) {
    return [
      "environment",
      "service",
      "version",
      "revision"
    ];
  }

  if (line.startsWith("nutsnews_worker_deployment_info{")) {
    return [
      "environment",
      "service",
      "deployment",
      "adapter"
    ];
  }

  if (line.startsWith("nutsnews_worker_expected_active{")) {
    return [
      "environment",
      "service"
    ];
  }

  if (line.startsWith("nutsnews_worker_health_probe{")) {
    return [
      "environment",
      "service",
      "probe",
      "outcome"
    ];
  }

  if (line.startsWith("nutsnews_worker_uplift_stage_events_total{")) {
    return [
      "environment",
      "service",
      "outcome"
    ];
  }

  if (line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_bucket{")) {
    return [
      "environment",
      "service",
      "le"
    ];
  }

  return [
    "environment",
    "service"
  ];
}
