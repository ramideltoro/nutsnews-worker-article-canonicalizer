import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeTelemetryEvent
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  CanonicalizerConfig
} from "./config.js";
import type {
  CanonicalizerDependencies
} from "./dependencies.js";

export const CANONICALIZATION_STAGE_LATENCY_BUCKETS_SECONDS = [
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
] as const;

export const CANONICALIZATION_STAGE_OUTCOMES = [
  "success",
  "duplicate",
  "invalid",
  "retry",
  "dlq",
  "failure"
] as const;

export type CanonicalizationStageOutcome = (typeof CANONICALIZATION_STAGE_OUTCOMES)[number];

export interface CanonicalizationMetricsSink {
  readonly allowedLabels: readonly string[];
  emit(event: RuntimeTelemetryEvent): void | Promise<void>;
  collect(): string;
  setInFlight(queue: string, value: number): void;
  setShutdownDraining(draining: boolean): void;
}

export interface CanonicalizationPrometheusTelemetrySink extends CanonicalizationMetricsSink {
  setStartupComplete(started: boolean): void;
  setReadinessUnhealthy(): void;
}

export interface CanonicalizationPrometheusTelemetrySinkOptions extends PrometheusRuntimeTelemetrySinkOptions {
  readonly buildRevision: CanonicalizerConfig["buildRevision"];
  readonly deployment: CanonicalizerConfig["deploymentMode"];
  readonly expectedActive: CanonicalizerConfig["expectedActive"];
  readonly adapter: CanonicalizerDependencies["adapterMode"];
}

const CANONICALIZATION_STAGE_SERVICE = "canonicalization";
const CANONICALIZER_IDENTITY_SERVICE = "canonicalizer";
const CANONICALIZATION_MAIN_QUEUE = "nutsnews.worker.canonicalization.v1";
const HEALTH_PROBES = [
  "liveness",
  "startup",
  "readiness"
] as const;
const HEALTH_OUTCOMES = [
  "ok",
  "degraded",
  "unhealthy"
] as const;
const MAX_LABEL_LENGTH = 96;

interface HistogramState {
  readonly buckets: number[];
  count: number;
  sum: number;
}

export function createCanonicalizationPrometheusTelemetrySink(
  options: CanonicalizationPrometheusTelemetrySinkOptions
): CanonicalizationPrometheusTelemetrySink {
  const adapter = canonicalMetricAdapter(options.adapter);
  const runtime = createPrometheusRuntimeTelemetrySink({
    identity: {
      ...options.identity,
      revision: options.buildRevision,
      deployment: options.deployment,
      adapter
    },
    ...(options.defaultQueue === undefined ? {} : {
      defaultQueue: options.defaultQueue
    })
  });
  const environment = metricLabelValue(options.identity.environment);
  const counters = new Map<CanonicalizationStageOutcome, number>(
    CANONICALIZATION_STAGE_OUTCOMES.map((outcome) => [
      outcome,
      0
    ])
  );
  const histogram: HistogramState = {
    buckets: CANONICALIZATION_STAGE_LATENCY_BUCKETS_SECONDS.map(() => 0),
    count: 0,
    sum: 0
  };
  const health = new Map<(typeof HEALTH_PROBES)[number], (typeof HEALTH_OUTCOMES)[number]>([
    [
      "liveness",
      "ok"
    ],
    [
      "startup",
      "unhealthy"
    ],
    [
      "readiness",
      "unhealthy"
    ]
  ]);

  return {
    allowedLabels: runtime.allowedLabels,
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      // This wrapper owns the bounded health-probe family, so forwarding health
      // events to Runtime 1 would expose duplicate metadata and samples. Keep an
      // unmeasured dependency event out of the runtime histogram as well.
      if (
        event.name !== "runtime.health.evaluated"
        && (event.name !== "runtime.dependency.observed" || measuredDuration(event) !== undefined)
      ) {
        await runtime.emit(event);
      }

      const outcome = canonicalizationStageOutcome(event);

      if (outcome !== undefined) {
        counters.set(outcome, (counters.get(outcome) ?? 0) + 1);
        const durationSeconds = durationSecondsFrom(event.durationMs);

        if (durationSeconds !== undefined) {
          observeHistogram(histogram, durationSeconds);
        }
      }

      const healthProbe = observedHealthProbe(event);

      if (healthProbe !== undefined) {
        health.set(healthProbe.probe, healthProbe.outcome);
      }
    },
    collect(): string {
      const runtimeOutput = runtime.collect().trimEnd();

      return `${[
        runtimeOutput,
        collectBuildIdentityMetrics(options, environment, adapter, runtimeOutput),
        collectExpectedActiveMetric(environment, options.expectedActive, runtimeOutput),
        collectHealthMetrics(environment, health),
        collectStageMetrics(environment, counters, histogram)
      ].filter((output) => output.length > 0).join("\n")}\n`;
    },
    setInFlight(queue, value): void {
      runtime.setInFlight(queue, value);
    },
    setShutdownDraining(draining): void {
      runtime.setShutdownDraining(draining);
    },
    setStartupComplete(started): void {
      health.set("startup", started ? "ok" : "unhealthy");
    },
    setReadinessUnhealthy(): void {
      health.set("readiness", "unhealthy");
    }
  };
}

function collectBuildIdentityMetrics(
  options: CanonicalizationPrometheusTelemetrySinkOptions,
  environment: string,
  adapter: "in_memory" | "mixed" | "production",
  runtimeOutput: string
): string {
  const lines: string[] = [];

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_build_info")) {
    lines.push(
      "# HELP nutsnews_worker_build_info Worker service build and version identity.",
      "# TYPE nutsnews_worker_build_info gauge",
      `nutsnews_worker_build_info${labels({
        environment,
        service: metricLabelValue(options.identity.service),
        version: metricLabelValue(options.identity.version),
        revision: metricLabelValue(options.buildRevision)
      })} 1`
    );
  }

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_deployment_info")) {
    lines.push(
      "# HELP nutsnews_worker_deployment_info Worker deployment and dependency-adapter identity.",
      "# TYPE nutsnews_worker_deployment_info gauge",
      `nutsnews_worker_deployment_info${labels({
        environment,
        service: metricLabelValue(options.identity.service),
        deployment: metricLabelValue(options.deployment),
        adapter: metricLabelValue(adapter)
      })} 1`
    );
  }

  return lines.join("\n");
}

function collectExpectedActiveMetric(
  environment: string,
  expectedActive: boolean,
  runtimeOutput: string
): string {
  if (hasMetricFamily(runtimeOutput, "nutsnews_worker_expected_active")) {
    return "";
  }

  return [
    "# HELP nutsnews_worker_expected_active Whether this worker deployment is expected to own active production work.",
    "# TYPE nutsnews_worker_expected_active gauge",
    `nutsnews_worker_expected_active${labels({
      environment,
      service: CANONICALIZER_IDENTITY_SERVICE
    })} ${expectedActive ? "1" : "0"}`
  ].join("\n");
}

function canonicalMetricAdapter(
  adapter: CanonicalizerDependencies["adapterMode"]
): "in_memory" | "mixed" | "production" {
  return adapter === "test" ? "in_memory" : adapter;
}

function hasMetricFamily(output: string, metric: string): boolean {
  return output.split("\n").some((line) => line.startsWith(`# HELP ${metric} `)
    || line.startsWith(`${metric}{`)
    || line.startsWith(`${metric} `));
}

function canonicalizationStageOutcome(event: RuntimeTelemetryEvent): CanonicalizationStageOutcome | undefined {
  if (event.stage !== CANONICALIZATION_STAGE_SERVICE || event.queue !== CANONICALIZATION_MAIN_QUEUE) {
    return undefined;
  }

  switch (event.name) {
    case "runtime.message.accepted":
      return "success";
    case "runtime.message.duplicate":
      return "duplicate";
    case "runtime.message.invalid":
      return "invalid";
    case "runtime.message.retry":
      return "retry";
    case "runtime.message.dlq":
      return "dlq";
    case "runtime.broker.consumer_state_changed":
    case "runtime.broker.state_changed":
    case "runtime.broker.topology_asserted":
    case "runtime.dependency.observed":
    case "runtime.health.evaluated":
    case "runtime.message.started":
    case "runtime.shutdown.completed":
    case "runtime.shutdown.failed":
    case "runtime.shutdown.started":
      return undefined;
  }
}

function collectStageMetrics(
  environment: string,
  counters: ReadonlyMap<CanonicalizationStageOutcome, number>,
  histogram: HistogramState
): string {
  const lines = [
    "# HELP nutsnews_worker_uplift_stage_events_total Completed worker-uplift stage deliveries by bounded service and outcome.",
    "# TYPE nutsnews_worker_uplift_stage_events_total counter"
  ];

  for (const outcome of CANONICALIZATION_STAGE_OUTCOMES) {
    lines.push(`nutsnews_worker_uplift_stage_events_total${labels({
      environment,
      service: CANONICALIZATION_STAGE_SERVICE,
      outcome
    })} ${formatMetricNumber(counters.get(outcome) ?? 0)}`);
  }

  lines.push(
    "# HELP nutsnews_worker_uplift_stage_latency_seconds Worker-uplift stage completion latency in seconds.",
    "# TYPE nutsnews_worker_uplift_stage_latency_seconds histogram"
  );

  for (const [index, boundary] of CANONICALIZATION_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
    lines.push(`nutsnews_worker_uplift_stage_latency_seconds_bucket${labels({
      environment,
      service: CANONICALIZATION_STAGE_SERVICE,
      le: String(boundary)
    })} ${formatMetricNumber(histogram.buckets[index] ?? 0)}`);
  }

  lines.push(
    `nutsnews_worker_uplift_stage_latency_seconds_bucket${labels({
      environment,
      service: CANONICALIZATION_STAGE_SERVICE,
      le: "+Inf"
    })} ${formatMetricNumber(histogram.count)}`,
    `nutsnews_worker_uplift_stage_latency_seconds_sum${labels({
      environment,
      service: CANONICALIZATION_STAGE_SERVICE
    })} ${formatMetricNumber(histogram.sum)}`,
    `nutsnews_worker_uplift_stage_latency_seconds_count${labels({
      environment,
      service: CANONICALIZATION_STAGE_SERVICE
    })} ${formatMetricNumber(histogram.count)}`
  );

  return lines.join("\n");
}

function observeHistogram(histogram: HistogramState, value: number): void {
  histogram.count += 1;
  histogram.sum += value;

  for (const [index, boundary] of CANONICALIZATION_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
    if (value <= boundary) {
      histogram.buckets[index] = (histogram.buckets[index] ?? 0) + 1;
    }
  }
}

function observedHealthProbe(event: RuntimeTelemetryEvent): {
  readonly probe: (typeof HEALTH_PROBES)[number];
  readonly outcome: (typeof HEALTH_OUTCOMES)[number];
} | undefined {
  if (event.name !== "runtime.health.evaluated") {
    return undefined;
  }

  const probe = event.attributes?.probe;
  const outcome = event.outcome ?? event.attributes?.status;

  if (!isHealthProbe(probe) || !isHealthOutcome(outcome)) {
    return undefined;
  }

  return {
    probe,
    outcome
  };
}

function collectHealthMetrics(
  environment: string,
  health: ReadonlyMap<(typeof HEALTH_PROBES)[number], (typeof HEALTH_OUTCOMES)[number]>
): string {
  if (health.size === 0) {
    return "";
  }

  const lines = [
    "# HELP nutsnews_worker_health_probe Worker liveness, startup, and readiness state by bounded probe and outcome.",
    "# TYPE nutsnews_worker_health_probe gauge"
  ];

  for (const probe of HEALTH_PROBES) {
    const observed = health.get(probe);

    if (observed === undefined) {
      continue;
    }

    for (const outcome of HEALTH_OUTCOMES) {
      lines.push(`nutsnews_worker_health_probe${labels({
        environment,
        service: CANONICALIZATION_STAGE_SERVICE,
        probe,
        outcome
      })} ${outcome === observed ? "1" : "0"}`);
    }
  }

  return lines.join("\n");
}

function measuredDuration(event: RuntimeTelemetryEvent): number | undefined {
  if (event.durationMs !== undefined && Number.isFinite(event.durationMs)) {
    return event.durationMs;
  }

  const value = event.attributes?.durationMs;

  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function durationSecondsFrom(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) ? Math.max(0, value) / 1_000 : undefined;
}

function isHealthProbe(value: unknown): value is (typeof HEALTH_PROBES)[number] {
  return typeof value === "string" && (HEALTH_PROBES as readonly string[]).includes(value);
}

function isHealthOutcome(value: unknown): value is (typeof HEALTH_OUTCOMES)[number] {
  return typeof value === "string" && (HEALTH_OUTCOMES as readonly string[]).includes(value);
}

function labels(values: Readonly<Record<string, string>>): string {
  return `{${Object.entries(values).map(([name, value]) => `${name}="${escapeLabelValue(value)}"`).join(",")}}`;
}

function metricLabelValue(value: string): string {
  const cleaned = value
    .trim()
    .replace(/[^A-Za-z0-9_.:-]+/gu, "_")
    .slice(0, MAX_LABEL_LENGTH);

  return cleaned.length > 0 ? cleaned : "unknown";
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/gu, "\\\\")
    .replace(/"/gu, "\\\"");
}

function formatMetricNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(6)));
}
