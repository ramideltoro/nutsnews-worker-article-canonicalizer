import type { RuntimeTelemetryEvent } from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

const runtime1Delegate = vi.hoisted(() => {
  const emittedEvents: RuntimeTelemetryEvent[] = [];
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
          'nutsnews_worker_health_probe{environment="test",host="runtime1-host",service="canonicalization",version="1.0.0",outcome="ok",probe="readiness"} 1'
        );
      }

      return `${lines.join("\n")}\n`;
    },
    setInFlight(): void {},
    setShutdownDraining(): void {},
    setExpectedActive(): void {},
    setLastSuccessTimestamp(): void {}
  };

  return {
    emittedEvents,
    sink
  };
});

vi.mock("@ramideltoro/nutsnews-worker-runtime", async (importOriginal) => {
  const original = await importOriginal<typeof import("@ramideltoro/nutsnews-worker-runtime")>();

  return {
    ...original,
    createPrometheusRuntimeTelemetrySink: vi.fn(() => runtime1Delegate.sink)
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

    expect(runtime1Delegate.emittedEvents).toHaveLength(0);
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
    expect(samples).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="liveness",outcome="ok"} 1');
    expect(samples).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="startup",outcome="unhealthy"} 1');
    expect(samples).toContain('nutsnews_worker_health_probe{environment="test",service="canonicalization",probe="readiness",outcome="ok"} 1');
  });
});
