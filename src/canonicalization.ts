import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerPublishCommand,
  type RuntimeMessageContext,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { CanonicalizerConfig } from "./config.js";
import type {
  CanonicalCandidateInput,
  CanonicalCandidatePayload,
  CanonicalEnrichmentRequest,
  CanonicalizerDependencies,
  CanonicalizerWorkHandler,
  CanonicalResolutionDecision,
  CanonicalizerWorkTools
} from "./dependencies.js";
import {
  sha256Hex,
  stableEnrichmentRequestId,
  stableUuid
} from "./ids.js";
import { createBestEffortRuntimeTelemetrySink } from "./telemetry.js";
import { normalizeArticleUrl } from "./url-normalization.js";

export interface CanonicalizationWorkHandlerOptions {
  readonly config: CanonicalizerConfig;
  readonly dependencies: CanonicalizerDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
}

const CANONICALIZATION_QUEUE = "nutsnews.worker.canonicalization.v1";

export function createCanonicalizationWorkHandler(options: CanonicalizationWorkHandlerOptions): CanonicalizerWorkHandler {
  const telemetry = createBestEffortRuntimeTelemetrySink(options.telemetry);

  return {
    name: "canonicalization-work-handler",
    handle: (context, tools) => handleCanonicalization(context, tools, options, telemetry)
  };
}

async function handleCanonicalization(
  context: RuntimeMessageContext,
  tools: CanonicalizerWorkTools,
  options: CanonicalizationWorkHandlerOptions,
  telemetry: RuntimeTelemetrySink
) {
  const candidate = canonicalCandidateFromContext(context);
  const normalized = normalizeArticleUrl(candidate.canonicalUrl);
  const decidedAt = runtimeNow(options.dependencies.clock);

  if (!normalized.ok) {
    await options.dependencies.stateStore.recordInvalidCandidate(candidate, {
      decision: "invalid",
      candidateId: candidate.candidateId,
      feedId: candidate.feedId,
      sourceItemId: candidate.sourceItemId,
      canonicalUrl: candidate.canonicalUrl,
      reasons: [
        normalized.reason
      ],
      decidedAt
    });
    await emitDecisionTelemetry(telemetry, candidate, "invalid", [
      normalized.reason
    ], decidedAt);

    return {
      status: "ok"
    } as const;
  }

  const input = {
    ...candidate,
    normalizedUrl: normalized.value.url,
    removedTrackingParameters: normalized.value.removedParameters,
    materialFingerprint: materialFingerprint(candidate, normalized.value.url),
    identitySeed: normalized.value.url,
    decidedAt
  } satisfies CanonicalCandidateInput;
  let enrichmentRequestToPublish: CanonicalEnrichmentRequest | undefined;
  const decision = await tools.withTransaction(async (transaction) => {
    const resolved = await options.dependencies.stateStore.resolveCandidate(input, transaction);

    if (shouldPublishEnrichment(resolved)) {
      const request = enrichmentRequest(input, resolved, options.config);

      await tools.recordPendingEnrichment(request, transaction);
      enrichmentRequestToPublish = request;
    }

    return resolved;
  });

  if (enrichmentRequestToPublish !== undefined) {
    const command = enrichmentPublishCommand(context, enrichmentRequestToPublish);
    const receipt = await tools.publish(command);

    await tools.recordOutbox(command, receipt);
  }

  await emitDecisionTelemetry(telemetry, candidate, decision.decision, decision.reasons, decidedAt, {
    canonicalArticleId: decision.canonicalArticleId,
    articleVersion: decision.articleVersion
  });

  return {
    status: "ok"
  } as const;
}

function canonicalCandidateFromContext(context: RuntimeMessageContext): CanonicalCandidatePayload {
  const publishedAt = stringOptional(context.payload.publishedAt);
  const duplicateOfArticleId = stringOptional(context.payload.duplicateOfArticleId);

  return {
    candidateId: stringValue(context.payload.candidateId, "candidateId"),
    feedId: stringValue(context.payload.feedId, "feedId"),
    sourceItemId: stringValue(context.payload.sourceItemId, "sourceItemId"),
    originalUrl: stringValue(context.payload.originalUrl, "originalUrl"),
    canonicalUrl: stringValue(context.payload.canonicalUrl, "canonicalUrl"),
    title: stringValue(context.payload.title, "title"),
    sourceName: stringValue(context.payload.sourceName, "sourceName"),
    ...(publishedAt === undefined ? {} : {
      publishedAt
    }),
    dedupeStatus: context.payload.dedupeStatus === "duplicate" ? "duplicate" : "new",
    ...(duplicateOfArticleId === undefined ? {} : {
      duplicateOfArticleId
    })
  };
}

function materialFingerprint(candidate: ReturnType<typeof canonicalCandidateFromContext>, normalizedUrl: string): string {
  return sha256Hex([
    normalizedUrl,
    candidate.title,
    candidate.publishedAt ?? ""
  ].join("\u001f"));
}

function isEnrichmentDecision(
  decision: CanonicalResolutionDecision
): decision is CanonicalResolutionDecision & { readonly decision: "new" | "changed" } {
  return decision.decision === "new" || decision.decision === "changed";
}

function shouldPublishEnrichment(
  decision: CanonicalResolutionDecision
): decision is CanonicalResolutionDecision & { readonly decision: "new" | "changed" } {
  return decision.publishEnrichment && isEnrichmentDecision(decision);
}

function enrichmentRequest(
  input: CanonicalCandidateInput,
  decision: CanonicalResolutionDecision & { readonly decision: "new" | "changed" },
  config: CanonicalizerConfig
): CanonicalEnrichmentRequest {
  const requestId = stableEnrichmentRequestId([
    decision.canonicalArticleId,
    String(decision.articleVersion),
    input.materialFingerprint
  ]);

  return {
    requestId,
    canonicalArticleId: decision.canonicalArticleId,
    articleVersion: decision.articleVersion,
    candidateId: input.candidateId,
    canonicalUrl: input.normalizedUrl,
    reason: decision.decision,
    producedAt: input.decidedAt,
    producer: {
      name: config.serviceName,
      version: config.serviceVersion
    },
    payloadRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/canonicalizer/${encodeURIComponent(decision.canonicalArticleId)}/${encodeURIComponent(requestId)}`,
      mediaType: "application/json"
    }
  };
}

function enrichmentPublishCommand(
  context: RuntimeMessageContext,
  request: CanonicalEnrichmentRequest
): BrokerPublishCommand {
  const route = getWorkerRoute("enrichment");
  const idempotencyKey = `canonicalizer:enrichment:${request.requestId}`;
  const payload = enrichmentPublishPayload(context, request, idempotencyKey);
  const validation = validateStagePayload(payload);

  if (!validation.ok) {
    throw new Error(`Invalid enrichment request payload: ${validation.issues.map((issue) => `${issue.path}:${issue.code}`).join(", ")}`);
  }

  return {
    envelope: assertWorkerEnvelope({
      schemaId: route.schemaId,
      schemaVersion: 1,
      route: "enrichment",
      messageId: stableUuid([
        "enrichment-message",
        request.requestId
      ]),
      causationId: context.envelope.messageId,
      correlationId: context.envelope.correlationId,
      traceparent: context.envelope.traceparent,
      ...(context.envelope.tracestate === undefined ? {} : {
        tracestate: context.envelope.tracestate
      }),
      idempotencyKey,
      aggregate: {
        type: "article",
        id: request.canonicalArticleId,
        version: request.articleVersion
      },
      occurredAt: request.producedAt,
      attempt: {
        count: 1,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: request.producedAt
      },
      producer: request.producer,
      payloadRef: {
        ...request.payloadRef,
        sizeBytes: getStagePayloadSizeBytes(payload)
      }
    }),
    payload
  };
}

function enrichmentPublishPayload(
  context: RuntimeMessageContext,
  request: CanonicalEnrichmentRequest,
  idempotencyKey: string
): Readonly<Record<string, unknown>> {
  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.enrichmentRequest,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: stringValue(context.payload.pipelineRunId, "pipelineRunId"),
    stageExecutionId: stableUuid([
      "enrichment-stage-execution",
      request.requestId
    ]),
    sourceMessageId: context.envelope.messageId,
    idempotencyKey,
    traceparent: context.envelope.traceparent,
    ...(context.envelope.tracestate === undefined ? {} : {
      tracestate: context.envelope.tracestate
    }),
    producedAt: request.producedAt,
    requestId: request.requestId,
    canonicalArticleId: request.canonicalArticleId,
    articleVersion: request.articleVersion,
    candidateId: request.candidateId,
    canonicalUrl: request.canonicalUrl,
    reason: request.reason,
    payloadRef: request.payloadRef
  };
}

async function emitDecisionTelemetry(
  telemetry: RuntimeTelemetrySink,
  candidate: ReturnType<typeof canonicalCandidateFromContext>,
  decision: string,
  reasons: readonly string[],
  at: string,
  extras: Readonly<Record<string, string | number>> = {}
): Promise<void> {
  await emitRuntimeTelemetry(telemetry, {
    name: "runtime.dependency.observed",
    level: decision === "invalid" || decision === "ambiguous" ? "warn" : "info",
    at,
    stage: "canonicalization",
    queue: CANONICALIZATION_QUEUE,
    outcome: decision === "invalid" || decision === "ambiguous" ? "failure" : "success",
    attributes: {
      event: "canonicalizer.candidate.decided",
      dependency: "canonical-state",
      candidateId: candidate.candidateId,
      feedId: candidate.feedId,
      decision,
      reasons: reasons.join(","),
      ...extras
    }
  });
}

function stringValue(value: unknown, key: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Canonicalization payload is missing ${key}.`);
  }

  return value;
}

function stringOptional(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}
