import {
  getRetryDestination,
  getWorkerRoute,
  validateWorkerEnvelope,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  createRuntimeMessageProcessor,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
  type RuntimeIdempotencyStore,
  type RuntimeMessageDelivery,
  type RuntimeMessageHandler,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { CanonicalizerConfig } from "./config.js";
import type {
  CanonicalizerDependencies,
  CanonicalizerDependencyProbe
} from "./dependencies.js";
import type {
  CanonicalizationMetricsSink,
  CanonicalizationPrometheusTelemetrySink
} from "./metrics.js";
import { createBestEffortRuntimeTelemetrySink } from "./telemetry.js";

export interface CanonicalizerServiceOptions {
  readonly config: CanonicalizerConfig;
  readonly dependencies: CanonicalizerDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: CanonicalizationMetricsSink;
}

export interface CanonicalizerService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  readonly consumer: BrokerConsumerHandle | undefined;
  start(): Promise<void>;
  stop(): Promise<void>;
  processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult>;
}

export function createCanonicalizerService(options: CanonicalizerServiceOptions): CanonicalizerService {
  const canonicalizationRoute = getWorkerRoute("canonicalization");
  const enrichmentRoute = getWorkerRoute("enrichment");
  const telemetry = createBestEffortRuntimeTelemetrySink(options.telemetry);
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      canonicalizationRoute,
      enrichmentRoute
    ],
    clock: options.dependencies.clock,
    telemetry
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const handler: RuntimeMessageHandler = async (context) => {
    try {
      return await drain.track(async () => {
        setInFlight(options.metrics, canonicalizationRoute.mainQueue.name, drain.inFlight);
        const result = await options.dependencies.workHandler.handle(context, {
          publish: (command) => broker.publish(command),
          recordPendingEnrichment: (request, transaction) => options.dependencies.brokerOutbox.recordPendingEnrichment(request, transaction),
          recordOutbox: (command, receipt) => options.dependencies.brokerOutbox.record(command, receipt),
          withTransaction: (operation) => options.dependencies.transactionRunner.withTransaction(operation)
        });

        await emitRuntimeTelemetry(telemetry, {
          name: "runtime.dependency.observed",
          level: result.status === "ok" ? "info" : "warn",
          at: runtimeNow(options.dependencies.clock),
          stage: "canonicalization",
          queue: canonicalizationRoute.mainQueue.name,
          outcome: result.status === "ok" ? "success" : result.status === "retry" ? "retry" : "failure",
          attributes: {
            event: "canonicalizer.message.delegated",
            dependency: options.dependencies.workHandler.name,
            shadowMode: options.config.shadowMode
          }
        });

        return result;
      });
    } finally {
      setInFlight(options.metrics, canonicalizationRoute.mainQueue.name, drain.inFlight);
    }
  };
  const sharedProcessor = createRuntimeMessageProcessor({
    stage: "canonicalization",
    clock: options.dependencies.clock,
    idempotencyStore: classifyStateStoreFailures(options.dependencies.stateStore),
    telemetry,
    handler
  });
  const processor = async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const startedAtMs = options.dependencies.clock.now().getTime();

    try {
      return await sharedProcessor(delivery);
    } catch (error: unknown) {
      return completeProcessorFailure(delivery, error, telemetry, options.dependencies.clock, startedAtMs);
    }
  };
  let started = false;
  let stopRequested = false;
  let consumer: BrokerConsumerHandle | undefined;

  const assertStartupActive = async (): Promise<void> => {
    if (!stopRequested) {
      return;
    }

    await broker.stop("startup-cancelled").catch(() => undefined);
    throw new Error("Canonicalizer startup was cancelled by shutdown.");
  };

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      return createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          brokerReadinessCheck(broker),
          createBrokerConsumerReadinessCheck(broker, "canonicalization"),
          dependencyReadinessCheck("canonical-state", options.dependencies.stateStore),
          dependencyReadinessCheck("database-transactions", options.dependencies.transactionRunner),
          dependencyReadinessCheck("broker-outbox", options.dependencies.brokerOutbox),
          productionAdapterReadinessCheck(options.config, options.dependencies)
        ],
        clock: options.dependencies.clock,
        telemetry
      });
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return drain.isDraining;
    },
    get consumer(): BrokerConsumerHandle | undefined {
      return consumer;
    },
    async start(): Promise<void> {
      if (started) {
        return;
      }

      if (stopRequested) {
        throw new Error("Canonicalizer startup cannot resume after shutdown.");
      }

      try {
        await broker.start();
      } catch (error: unknown) {
        await broker.stop("startup-failed").catch(() => undefined);
        throw error;
      }
      await assertStartupActive();
      if (productionAdaptersAvailable(options.config, options.dependencies)) {
        const brokerConsumer = await broker.consume("canonicalization", processor);
        await assertStartupActive();
        consumer = {
          stage: brokerConsumer.stage,
          cancel: async () => {
            await brokerConsumer.cancel();
            setReadinessUnhealthy(options.metrics);
          }
        };
      } else {
        consumer = undefined;
        setReadinessUnhealthy(options.metrics);
      }
      started = true;
      setStartupComplete(options.metrics, true);
      setInFlight(options.metrics, canonicalizationRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(telemetry, {
        name: "runtime.dependency.observed",
        level: consumer === undefined ? "warn" : "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "canonicalization",
        queue: canonicalizationRoute.mainQueue.name,
        outcome: consumer === undefined ? "failure" : "success",
        attributes: {
          dependency: "canonicalizer-shell",
          mode: options.config.dependencyMode,
          adapterMode: options.dependencies.adapterMode,
          prefetch: options.config.prefetch,
          concurrency: options.config.concurrency,
          shadowMode: options.config.shadowMode,
          consumerStarted: consumer !== undefined
        }
      });
      await assertStartupActive();
    },
    async stop(): Promise<void> {
      stopRequested = true;

      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      setShutdownDraining(options.metrics, true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      setShutdownDraining(options.metrics, false);
      setInFlight(options.metrics, canonicalizationRoute.mainQueue.name, drain.inFlight);
      setStartupComplete(options.metrics, false);
      setReadinessUnhealthy(options.metrics);
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies CanonicalizerService;

  return service;
}

function productionAdaptersAvailable(
  config: CanonicalizerConfig,
  dependencies: CanonicalizerDependencies
): boolean {
  return config.dependencyMode !== "production" || dependencies.adapterMode === "production";
}

function setStartupComplete(metrics: CanonicalizationMetricsSink | undefined, started: boolean): void {
  invokeMetricSafely(() => {
    if (isCanonicalizationMetrics(metrics)) {
      metrics.setStartupComplete(started);
    }
  });
}

function setReadinessUnhealthy(metrics: CanonicalizationMetricsSink | undefined): void {
  invokeMetricSafely(() => {
    if (isCanonicalizationMetrics(metrics)) {
      metrics.setReadinessUnhealthy();
    }
  });
}

function setInFlight(
  metrics: CanonicalizationMetricsSink | undefined,
  queue: string,
  inFlight: number
): void {
  invokeMetricSafely(() => metrics?.setInFlight(queue, inFlight));
}

function setShutdownDraining(metrics: CanonicalizationMetricsSink | undefined, draining: boolean): void {
  invokeMetricSafely(() => metrics?.setShutdownDraining(draining));
}

function invokeMetricSafely(operation: () => void): void {
  try {
    operation();
  } catch {
    // Metrics are observational and cannot control service lifecycle or delivery.
  }
}

function isCanonicalizationMetrics(
  metrics: CanonicalizationMetricsSink | undefined
): metrics is CanonicalizationPrometheusTelemetrySink {
  return metrics !== undefined
    && "setStartupComplete" in metrics
    && typeof metrics.setStartupComplete === "function"
    && "setReadinessUnhealthy" in metrics
    && typeof metrics.setReadinessUnhealthy === "function";
}

class CanonicalizerStateStoreError extends Error {
  readonly telemetryReason: string;

  constructor(telemetryReason: string) {
    super(telemetryReason);
    this.name = "CanonicalizerStateStoreError";
    this.telemetryReason = telemetryReason;
  }
}

function classifyStateStoreFailures(store: RuntimeIdempotencyStore): RuntimeIdempotencyStore {
  return {
    claim: async (idempotencyKey, context) => stateStoreOperation(
      "idempotency-claim-error",
      () => store.claim(idempotencyKey, context)
    ),
    markCompleted: async (idempotencyKey, completion) => stateStoreOperation(
      "idempotency-completion-error",
      () => store.markCompleted(idempotencyKey, completion)
    ),
    markFailed: async (idempotencyKey, failure) => stateStoreOperation(
      "idempotency-failure-record-error",
      () => store.markFailed(idempotencyKey, failure)
    )
  };
}

async function stateStoreOperation<T>(reason: string, operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch {
    throw new CanonicalizerStateStoreError(reason);
  }
}

async function completeProcessorFailure(
  delivery: RuntimeMessageDelivery,
  error: unknown,
  telemetry: RuntimeTelemetrySink,
  clock: CanonicalizerDependencies["clock"],
  startedAtMs: number
): Promise<RuntimeMessageProcessingResult> {
  const queue = getWorkerRoute("canonicalization").mainQueue.name;
  const durationMs = Math.max(0, clock.now().getTime() - startedAtMs);
  const envelopeResult = validateWorkerEnvelope(delivery.envelope);

  if (!envelopeResult.ok) {
    const issues = envelopeResult.issues.map((issue) => ({
      path: issue.path,
      code: issue.code,
      message: issue.message
    }));
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.invalid",
      level: "warn",
      at: runtimeNow(clock),
      stage: "canonicalization",
      queue,
      durationMs,
      outcome: "failure",
      attributes: {
        issueCode: issues[0]?.code ?? "invalid-envelope",
        issuePath: issues[0]?.path ?? "$"
      }
    });

    return {
      action: "dlq",
      reason: "invalid-envelope",
      issues
    };
  }

  const envelope = envelopeResult.value;

  if (envelope.route !== "canonicalization") {
    const issues = [
      {
        path: "$.route",
        code: "stage-mismatch",
        message: `Envelope route ${envelope.route} does not match processor stage canonicalization.`
      }
    ];
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.invalid",
      level: "warn",
      at: runtimeNow(clock),
      stage: "canonicalization",
      ...envelopeTelemetryFields(envelope, queue, durationMs),
      outcome: "failure",
      attributes: {
        issueCode: "stage-mismatch",
        issuePath: "$.route"
      }
    });

    return terminalFailureResult(envelope, "stage-mismatch", issues);
  }

  const reason = error instanceof CanonicalizerStateStoreError
    ? error.telemetryReason
    : "processor-error";
  const result = retryOrDlqResult(envelope, reason);
  const destination = result.destination.name;
  const event: RuntimeTelemetryEvent = result.action === "retry"
    ? {
        name: "runtime.message.retry",
        level: "warn",
        at: runtimeNow(clock),
        stage: "canonicalization",
        ...envelopeTelemetryFields(envelope, queue, durationMs),
        outcome: "retry",
        attributes: {
          reason,
          destination
        }
      }
    : {
        name: "runtime.message.dlq",
        level: "error",
        at: runtimeNow(clock),
        stage: "canonicalization",
        ...envelopeTelemetryFields(envelope, queue, durationMs),
        outcome: "dlq",
        attributes: {
          reason,
          destination
        }
      };
  await emitRuntimeTelemetry(telemetry, event);

  return result;
}

function retryOrDlqResult(envelope: WorkerMessageEnvelope, reason: string) {
  const destination = getRetryDestination(envelope.route, envelope.attempt.count);

  return "ttlMs" in destination
    ? {
        action: "retry",
        reason,
        envelope,
        destination
      } as const
    : {
        action: "dlq",
        reason,
        envelope,
        destination
      } as const;
}

function terminalFailureResult(
  envelope: WorkerMessageEnvelope,
  reason: string,
  issues: readonly { readonly path: string; readonly code: string; readonly message: string }[]
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.max);

  return "routingKey" in destination && !("ttlMs" in destination)
    ? {
        action: "dlq",
        reason,
        envelope,
        destination,
        issues
      }
    : {
        action: "dlq",
        reason,
        envelope,
        issues
      };
}

function envelopeTelemetryFields(
  envelope: WorkerMessageEnvelope,
  queue: string,
  durationMs: number
): Readonly<Record<string, string | number>> {
  const base = {
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    idempotencyKey: envelope.idempotencyKey,
    queue,
    attempt: envelope.attempt.count,
    durationMs
  } as const;

  return envelope.tracestate === undefined
    ? base
    : {
        ...base,
        tracestate: envelope.tracestate
      };
}

function livenessCheck(): RuntimeHealthCheck {
  return {
    name: "process",
    critical: true,
    check: () => "ok"
  };
}

function startupCheck(isStarted: () => boolean): RuntimeHealthCheck {
  return {
    name: "service-started",
    critical: true,
    check: () => isStarted() ? "ok" : "unhealthy"
  };
}

function brokerReadinessCheck(broker: BrokerLifecycle): RuntimeHealthCheck {
  return {
    name: "broker-lifecycle",
    critical: true,
    check: () => broker.state === "ready"
      ? {
          status: "ok",
          details: {
            state: broker.state
          }
        }
      : {
          status: "unhealthy",
          details: {
            state: broker.state
          }
        }
  };
}

function dependencyReadinessCheck(
  name: string,
  dependency: {
    readonly name: string;
    probe(): CanonicalizerDependencyProbe | Promise<CanonicalizerDependencyProbe>;
  }
): RuntimeHealthCheck {
  return {
    name,
    critical: true,
    check: async () => {
      const probe = await dependency.probe();

      return {
        status: probe.status,
        details: {
          dependency: dependency.name,
          summary: probe.summary
        }
      };
    }
  };
}

function productionAdapterReadinessCheck(
  config: CanonicalizerConfig,
  dependencies: CanonicalizerDependencies
): RuntimeHealthCheck {
  return {
    name: "production-adapters",
    critical: true,
    check: () => {
      if (config.dependencyMode !== "production") {
        return {
          status: "ok",
          details: {
            mode: "test"
          }
        };
      }

      return dependencies.adapterMode === "production"
        ? {
            status: "ok",
            details: {
              mode: "production",
              adapterMode: dependencies.adapterMode
            }
          }
        : {
            status: "unhealthy",
            details: {
              mode: "production",
              reason: "production-adapters-unavailable",
              adapterMode: dependencies.adapterMode
            }
          };
    }
  };
}
