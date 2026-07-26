import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadCanonicalizerConfig,
  type CanonicalizerConfig
} from "./config.js";
import { createCanonicalizationWorkHandler } from "./canonicalization.js";
import { createCanonicalizerHttpServer } from "./http.js";
import { PayloadRabbitMqTransport } from "./rabbitmq-transport.js";
import { createCanonicalizerService } from "./service.js";
import { createLocalCanonicalizerDependencies } from "./test-doubles.js";

export {
  CANONICALIZER_CONFIG_SCHEMA,
  CANONICALIZER_SERVICE_NAME,
  CANONICALIZER_SERVICE_VERSION,
  loadCanonicalizerConfig,
  type CanonicalizerConfig
} from "./config.js";
export type {
  CanonicalBrokerOutbox,
  CanonicalCandidateInput,
  CanonicalCandidatePayload,
  CanonicalDatabaseTransaction,
  CanonicalDatabaseTransactionRunner,
  CanonicalDecisionKind,
  CanonicalEnrichmentRequest,
  CanonicalInvalidDecision,
  CanonicalResolutionDecision,
  CanonicalizerDependencies,
  CanonicalizerDependencyProbe,
  CanonicalizerWorkHandler,
  CanonicalizerWorkTools,
  CanonicalStateStore
} from "./dependencies.js";
export {
  createCanonicalizationWorkHandler,
  type CanonicalizationWorkHandlerOptions
} from "./canonicalization.js";
export {
  createCanonicalizerHttpServer,
  type CanonicalizerHttpServer
} from "./http.js";
export {
  sha256Hex,
  stableArticleId,
  stableEnrichmentRequestId,
  stableUuid
} from "./ids.js";
export {
  buildCanonicalReplayReport,
  type BuildCanonicalReplayReportOptions,
  type CanonicalAmbiguityReviewItem,
  type CanonicalDecision,
  type CanonicalReplayLegacySummary,
  type CanonicalReplayRateComparison,
  type CanonicalReplayReport
} from "./replay-report.js";
export {
  createCanonicalizerService,
  type CanonicalizerService
} from "./service.js";
export {
  PayloadRabbitMqTransport
} from "./rabbitmq-transport.js";
export {
  InMemoryCanonicalStateStore,
  LocalBrokerTransport,
  LocalCanonicalBrokerOutbox,
  LocalCanonicalTransactionRunner,
  LocalCanonicalizerWorkHandler,
  ManualCanonicalizerClock,
  createLocalCanonicalizerDependencies,
  createMinimalCanonicalizationDelivery,
  createMinimalCanonicalizationEnvelope,
  createMinimalCanonicalizationPayload
} from "./test-doubles.js";
export {
  normalizeArticleUrl,
  type ArticleUrlNormalizationResult,
  type NormalizedArticleUrl
} from "./url-normalization.js";

export interface CanonicalizerApplication {
  readonly config: CanonicalizerConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export function createCanonicalizerApplication(config = loadCanonicalizerConfig()): CanonicalizerApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host
  };
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createPrometheusRuntimeTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const productionBrokerTransport = config.dependencyMode === "production"
    ? new PayloadRabbitMqTransport({
        url: requiredEnv("NUTSNEWS_CANONICALIZER_RABBITMQ_URL"),
        prefetch: config.prefetch,
        clock: SYSTEM_RUNTIME_CLOCK
      })
    : undefined;
  const baseDependencies = createLocalCanonicalizerDependencies({
    clock: SYSTEM_RUNTIME_CLOCK,
    ...(productionBrokerTransport === undefined ? {} : {
      brokerTransport: productionBrokerTransport
    })
  });
  const dependencies = {
    ...baseDependencies,
    workHandler: createCanonicalizationWorkHandler({
      config,
      dependencies: baseDependencies,
      ...(telemetry === undefined ? {} : {
        telemetry
      })
    })
  };
  const service = createCanonicalizerService({
    config,
    dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const httpServer = createCanonicalizerHttpServer({
    config,
    service,
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        await httpServer.close();
      },
      async () => {
        await service.stop();
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: logSink
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      await service.start();
      await httpServer.listen();
      shutdown.start();
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
}

function combineTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await sink.emit(event);
      }
    }
  };
}

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.4.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "0.4.0") {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }
}

function requiredEnv(key: string): string {
  const value = process.env[key]?.trim();

  if (value === undefined || value.length === 0) {
    throw new Error(`${key} is required for production canonicalizer dependencies.`);
  }

  return value;
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createCanonicalizerApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start canonicalizer");
    process.exitCode = 1;
  });
}
