import {
  emitRuntimeTelemetry,
  runtimeNow,
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
  stableEnrichmentRequestId
} from "./ids.js";
import { normalizeArticleUrl } from "./url-normalization.js";

export interface CanonicalizationWorkHandlerOptions {
  readonly config: CanonicalizerConfig;
  readonly dependencies: CanonicalizerDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
}

const CANONICALIZATION_QUEUE = "nutsnews.worker.canonicalization.v1";

export function createCanonicalizationWorkHandler(options: CanonicalizationWorkHandlerOptions): CanonicalizerWorkHandler {
  return {
    name: "canonicalization-work-handler",
    handle: (context, tools) => handleCanonicalization(context, tools, options)
  };
}

async function handleCanonicalization(
  context: RuntimeMessageContext,
  tools: CanonicalizerWorkTools,
  options: CanonicalizationWorkHandlerOptions
) {
  const candidate = canonicalCandidateFromContext(context);
  const normalized = normalizeArticleUrl(candidate.canonicalUrl);
  const decidedAt = runtimeNow(options.dependencies.clock);

  if (!normalized.ok) {
    await options.dependencies.stateStore.recordInvalidCandidate(candidate, {
      decision: "invalid",
      reasons: [
        normalized.reason
      ],
      decidedAt
    });
    await emitDecisionTelemetry(options, candidate, "invalid", [
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
  const decision = await tools.withTransaction(async (transaction) => {
    const resolved = await options.dependencies.stateStore.resolveCandidate(input, transaction);

    if (isEnrichmentDecision(resolved)) {
      const request = enrichmentRequest(input, resolved, options.config);

      await tools.recordPendingEnrichment(request, transaction);
    }

    return resolved;
  });

  await emitDecisionTelemetry(options, candidate, decision.decision, decision.reasons, decidedAt, {
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

async function emitDecisionTelemetry(
  options: CanonicalizationWorkHandlerOptions,
  candidate: ReturnType<typeof canonicalCandidateFromContext>,
  decision: string,
  reasons: readonly string[],
  at: string,
  extras: Readonly<Record<string, string | number>> = {}
): Promise<void> {
  await emitRuntimeTelemetry(options.telemetry, {
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
