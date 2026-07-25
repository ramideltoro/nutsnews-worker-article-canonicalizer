import {
  getWorkerRoute
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  createRuntimeMessageProcessor,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type PrometheusRuntimeTelemetrySink,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { CanonicalizerConfig } from "./config.js";
import type {
  CanonicalizerDependencies,
  CanonicalizerDependencyProbe
} from "./dependencies.js";

export interface CanonicalizerServiceOptions {
  readonly config: CanonicalizerConfig;
  readonly dependencies: CanonicalizerDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
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
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      canonicalizationRoute,
      enrichmentRoute
    ],
    clock: options.dependencies.clock,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const processor = createRuntimeMessageProcessor({
    stage: "canonicalization",
    clock: options.dependencies.clock,
    idempotencyStore: options.dependencies.stateStore,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    }),
    handler: async (context) => {
      try {
        return await drain.track(async () => {
          options.metrics?.setInFlight(canonicalizationRoute.mainQueue.name, drain.inFlight);
          const result = await options.dependencies.workHandler.handle(context, {
            publish: (command) => broker.publish(command),
            recordPendingEnrichment: (request, transaction) => options.dependencies.brokerOutbox.recordPendingEnrichment(request, transaction),
            recordOutbox: (command, receipt) => options.dependencies.brokerOutbox.record(command, receipt),
            withTransaction: (operation) => options.dependencies.transactionRunner.withTransaction(operation)
          });

          await emitRuntimeTelemetry(options.telemetry, {
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
        options.metrics?.setInFlight(canonicalizationRoute.mainQueue.name, drain.inFlight);
      }
    }
  });
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;

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
          dependencyReadinessCheck("canonical-state", options.dependencies.stateStore),
          dependencyReadinessCheck("database-transactions", options.dependencies.transactionRunner),
          dependencyReadinessCheck("broker-outbox", options.dependencies.brokerOutbox),
          shadowModeCheck(options.config)
        ],
        clock: options.dependencies.clock,
        ...(options.telemetry === undefined ? {} : {
          telemetry: options.telemetry
        })
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

      await broker.start();
      consumer = await broker.consume("canonicalization", processor);
      started = true;
      options.metrics?.recordDependencyLatency(canonicalizationRoute.mainQueue.name, 0, "success");
      options.metrics?.setInFlight(canonicalizationRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.dependency.observed",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "canonicalization",
        queue: canonicalizationRoute.mainQueue.name,
        outcome: "success",
        attributes: {
          dependency: "canonicalizer-shell",
          mode: options.config.dependencyMode,
          prefetch: options.config.prefetch,
          concurrency: options.config.concurrency,
          shadowMode: options.config.shadowMode
        }
      });
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      options.metrics?.setShutdownDraining(true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      options.metrics?.setShutdownDraining(false);
      options.metrics?.setInFlight(canonicalizationRoute.mainQueue.name, drain.inFlight);
      consumer = undefined;
      started = false;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies CanonicalizerService;

  return service;
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

function shadowModeCheck(config: CanonicalizerConfig): RuntimeHealthCheck {
  return {
    name: "shadow-mode",
    critical: true,
    check: () => config.shadowMode
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "shadow-mode-disabled"
          }
        }
  };
}
