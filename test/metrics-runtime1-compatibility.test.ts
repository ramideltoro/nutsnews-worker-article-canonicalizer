import type {
  PrometheusRuntimeTelemetrySinkOptions,
  RuntimeTelemetryEvent
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

const runtime1Delegate = vi.hoisted(() => {
  const emittedEvents: RuntimeTelemetryEvent[] = [];
  const lastSuccessTimestamps: number[] = [];
  let options: PrometheusRuntimeTelemetrySinkOptions | undefined;
  const sink = {
    allowedLabels: [
      "environment",
      "host",
      "service",
      "version",
      "stage",
      "queue",
      "outcome",
      "dependency",
      "probe",
      "check",
      "revision",
      "deployment",
      "adapter"
    ],
    emit(event: RuntimeTelemetryEvent): void {
      emittedEvents.push(event);
    },
    collect(): string {
      const lines = [
        "# HELP runtime1_delegate_info Test-only Runtime1-style delegate output.",
        "# TYPE runtime1_delegate_info gauge",
        "runtime1_delegate_info 1"
      ];

      if (emittedEvents.some((event) => event.name === "runtime.health.evaluated")) {
        lines.push(
          "# HELP nutsnews_worker_health_probe Worker liveness, startup, and readiness state by probe and outcome.",
          "# TYPE nutsnews_worker_health_probe gauge",
          'nutsnews_worker_health_probe{environment="test",host="runtime1-host",service="canonicalizer",version="0.1.0",outcome="ok",probe="readiness"} 1',
          "# HELP nutsnews_worker_health_check Worker dependency health state by bounded probe, check, and outcome.",
          "# TYPE nutsnews_worker_health_check gauge",
          'nutsnews_worker_health_check{environment="test",host="runtime1-host",service="canonicalizer",version="0.1.0",outcome="ok",probe="readiness",check="broker-lifecycle"} 1',
          "# HELP nutsnews_worker_health_check_duration_seconds Worker health-check duration in seconds.",
          "# TYPE nutsnews_worker_health_check_duration_seconds histogram",
          'nutsnews_worker_health_check_duration_seconds_bucket{environment="test",host="runtime1-host",service="canonicalizer",version="0.1.0",probe="readiness",check="broker-lifecycle",le="+Inf"} 1',
          'nutsnews_worker_health_check_duration_seconds_sum{environment="test",host="runtime1-host",service="canonicalizer",version="0.1.0",probe="readiness",check="broker-lifecycle"} 0.001',
          'nutsnews_worker_health_check_duration_seconds_count{environment="test",host="runtime1-host",service="canonicalizer",version="0.1.0",probe="readiness",check="broker-lifecycle"} 1'
        );
      }

      if (options?.expectedActive !== undefined) {
        lines.push(
          "# HELP nutsnews_worker_expected_active Whether this deployment is expected to own active production work.",
          "# TYPE nutsnews_worker_expected_active gauge",
          `nutsnews_worker_expected_active{environment="test",service="${options.identity.service}"} ${options.expectedActive ? "1" : "0"}`
        );
      }

      return `${lines.join("\n")}\n`;
    },
    setInFlight(): void {},
    setShutdownDraining(): void {},
    setExpectedActive(): void {},
    setLastSuccessTimestamp(timestampSeconds: number): void {
      lastSuccessTimestamps.push(timestampSeconds);
    }
  };

  return {
    emittedEvents,
    lastSuccessTimestamps,
    get options(): PrometheusRuntimeTelemetrySinkOptions | undefined {
      return options;
    },
    set options(value: PrometheusRuntimeTelemetrySinkOptions | undefined) {
      options = value;
    },
    sink
  };
});

vi.mock("@ramideltoro/nutsnews-worker-runtime", async (importOriginal) => {
  const original = await importOriginal<typeof import("@ramideltoro/nutsnews-worker-runtime")>();

  return {
    ...original,
    createPrometheusRuntimeTelemetrySink: vi.fn((options: PrometheusRuntimeTelemetrySinkOptions) => {
      runtime1Delegate.options = options;
      return runtime1Delegate.sink;
    })
  };
});

import { createCanonicalizationPrometheusTelemetrySink } from "../src/metrics.js";

describe("Runtime1 metrics compatibility", () => {
  it("keeps exactly one service-owned health-probe family", async () => {
    runtime1Delegate.emittedEvents.length = 0;
    const metrics = createCanonicalizationPrometheusTelemetrySink({
      identity: {
        service: "nutsnews-worker-article-canonicalizer",
        version: "0.1.0",
        environment: "test",
        host: "canonicalization-test"
      },
      buildRevision: "runtime1-compatibility-test",
      deployment: "shadow",
      expectedActive: false,
      adapter: "test"
    });

    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      outcome: "ok",
      attributes: {
        probe: "readiness"
      }
    });

    expect(runtime1Delegate.emittedEvents).toHaveLength(1);
    expect(runtime1Delegate.options).toMatchObject({
      identity: {
        service: "canonicalizer"
      },
      expectedActive: false,
      cardinality: {
        dependencies: [
          "canonical-state",
          "canonicalization-work-handler",
          "local-canonicalizer-work-handler",
          "canonicalizer-shell",
          "database-transactions",
          "broker-outbox",
          "rabbitmq"
        ],
        healthChecks: [
          "process",
          "service-started",
          "broker-lifecycle",
          "rabbitmq-consumer",
          "canonical-state",
          "database-transactions",
          "broker-outbox",
          "production-adapters"
        ]
      }
    });
    expect("recordDependencyLatency" in runtime1Delegate.sink).toBe(false);
    expect("recordDependencyLatency" in metrics).toBe(false);

    const output = metrics.collect();
    const lines = output.split("\n");
    const helpLines = lines.filter((line) => line.startsWith("# HELP nutsnews_worker_health_probe "));
    const typeLines = lines.filter((line) => line === "# TYPE nutsnews_worker_health_probe gauge");
    const samples = lines.filter((line) => line.startsWith("nutsnews_worker_health_probe{"));
    const series = samples.map((line) => line.slice(0, line.lastIndexOf(" ")));

    expect(output).toContain("runtime1_delegate_info 1");
    expect(helpLines).toHaveLength(1);
    expect(typeLines).toHaveLength(1);
    expect(samples).toHaveLength(9);
    expect(new Set(series).size).toBe(samples.length);
    expect(samples).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalizer",probe="liveness",outcome="ok"} 1');
    expect(samples).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalizer",probe="startup",outcome="unhealthy"} 1');
    expect(samples).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalizer",probe="readiness",outcome="ok"} 1');
    expect(samples.some((sample) => sample.includes('host="runtime1-host"'))).toBe(false);
    expect(output).toContain('nutsnews_worker_health_check{environment="test",host="runtime1-host",service="canonicalizer",version="0.1.0",outcome="ok",probe="readiness",check="broker-lifecycle"} 1');
    expect(output).toContain("nutsnews_worker_health_check_duration_seconds_count{");
    expect(output).toContain('nutsnews_worker_expected_active{environment="test",service="canonicalizer"} 0');

    for (const family of [
      "nutsnews_worker_health_probe",
      "nutsnews_worker_health_check",
      "nutsnews_worker_health_check_duration_seconds"
    ]) {
      expect(lines.filter((line) => line.startsWith(`# HELP ${family} `))).toHaveLength(1);
      expect(lines.filter((line) => line.startsWith(`# TYPE ${family} `))).toHaveLength(1);
    }
  });

  it("updates last success monotonically and invalidates readiness on channel loss", async () => {
    runtime1Delegate.emittedEvents.length = 0;
    runtime1Delegate.lastSuccessTimestamps.length = 0;
    const metrics = createCanonicalizationPrometheusTelemetrySink({
      identity: {
        service: "ignored-by-bounded-metric-identity",
        version: "0.1.0",
        environment: "test",
        host: "canonicalization-test"
      },
      buildRevision: "runtime1-compatibility-test",
      deployment: "shadow",
      expectedActive: false,
      adapter: "test"
    });
    const base = {
      level: "info",
      stage: "canonicalization",
      queue: "nutsnews.worker.canonicalization.v1"
    } as const;

    await metrics.emit({
      ...base,
      name: "runtime.message.accepted",
      at: "2026-08-01T00:02:00.000Z",
      outcome: "success"
    });
    await metrics.emit({
      ...base,
      name: "runtime.message.duplicate",
      at: "2026-08-01T00:01:00.000Z",
      outcome: "duplicate"
    });
    await metrics.emit({
      ...base,
      name: "runtime.message.duplicate",
      at: "2026-08-01T00:03:00.000Z",
      outcome: "duplicate"
    });
    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-08-01T00:03:01.000Z",
      outcome: "ok",
      attributes: {
        probe: "readiness"
      }
    });
    await metrics.emit({
      ...base,
      name: "runtime.broker.consumer_state_changed",
      at: "2026-08-01T00:03:02.000Z",
      outcome: "channel-dropped"
    });

    expect(runtime1Delegate.lastSuccessTimestamps).toEqual([
      Date.parse("2026-08-01T00:02:00.000Z") / 1_000,
      Date.parse("2026-08-01T00:03:00.000Z") / 1_000
    ]);
    expect(metrics.collect()).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalizer",probe="readiness",outcome="unhealthy"} 1');
  });

  it("coalesces monitor and explicit consumer-loss readiness refreshes", async () => {
    runtime1Delegate.emittedEvents.length = 0;
    const metrics = createCanonicalizationPrometheusTelemetrySink({
      identity: {
        service: "canonicalizer",
        version: "0.1.0",
        environment: "test",
        host: "canonicalization-test"
      },
      buildRevision: "runtime1-compatibility-test",
      deployment: "shadow",
      expectedActive: false,
      adapter: "test"
    });
    let releaseRefresh!: () => void;
    const refreshGate = new Promise<void>((resolve) => {
      releaseRefresh = resolve;
    });
    const evaluate = vi.fn(() => refreshGate);
    metrics.setReadinessEvaluator(evaluate);
    const monitorRefresh = metrics.emit({
      name: "runtime.broker.consumer_state_changed",
      level: "error",
      at: "2026-08-01T00:04:00.000Z",
      stage: "canonicalization",
      queue: "nutsnews.worker.canonicalization.v1",
      outcome: "channel-dropped"
    });

    await vi.waitFor(() => {
      expect(evaluate).toHaveBeenCalledOnce();
    });
    const explicitRefresh = metrics.refreshReadinessAfterConsumerLoss();
    releaseRefresh();

    await Promise.all([
      monitorRefresh,
      explicitRefresh
    ]);
    expect(evaluate).toHaveBeenCalledOnce();
  });
});
