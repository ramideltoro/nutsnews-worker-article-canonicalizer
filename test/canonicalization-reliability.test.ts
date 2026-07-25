import {
  createBufferedRuntimeTelemetrySink,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { createCanonicalizationWorkHandler } from "../src/canonicalization.js";
import { loadCanonicalizerConfig } from "../src/config.js";
import { buildCanonicalReplayReport } from "../src/replay-report.js";
import { createCanonicalizerService } from "../src/service.js";
import {
  InMemoryCanonicalStateStore,
  LocalBrokerTransport,
  LocalCanonicalBrokerOutbox,
  LocalCanonicalTransactionRunner,
  ManualCanonicalizerClock,
  createLocalCanonicalizerDependencies,
  createMinimalCanonicalizationDelivery
} from "../src/test-doubles.js";

import type {
  CanonicalCandidateInput,
  CanonicalDatabaseTransaction,
  CanonicalEnrichmentRequest,
  CanonicalInvalidDecision,
  CanonicalResolutionDecision
} from "../src/dependencies.js";

describe("canonicalization reliability proof", () => {
  it("serializes simultaneous first sightings into one canonical identity and one pending enrichment event", async () => {
    const context = createReliabilityContext();

    await context.service.start();

    try {
      const results = await Promise.all([
        context.broker.deliverCanonicalization(articleDelivery(1, {
          candidateId: "candidate-race-a",
          feedId: "feed-race-a",
          sourceItemId: "guid-race-a",
          canonicalUrl: "https://articles.example.test/race/story?id=42&utm_source=a",
          title: "Race Story"
        })),
        context.broker.deliverCanonicalization(articleDelivery(2, {
          candidateId: "candidate-race-b",
          feedId: "feed-race-b",
          sourceItemId: "guid-race-b",
          canonicalUrl: "https://articles.example.test/race/story?utm_medium=email&id=42",
          title: "Race Story"
        }))
      ]);
      const decisions = resolutionDecisions(context.stateStore);

      expect(results.map((result) => result.action)).toEqual([
        "ack",
        "ack"
      ]);
      expect(decisionCount(decisions, "new")).toBe(1);
      expect(decisionCount(decisions, "duplicate")).toBe(1);
      expect(new Set(decisions.map((decision) => decision.canonicalArticleId)).size).toBe(1);
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });

  it("quarantines GUID and URL collisions without blocking unrelated work", async () => {
    const context = createReliabilityContext();

    await context.service.start();

    try {
      await context.broker.deliverCanonicalization(articleDelivery(1, {
        candidateId: "candidate-collision-a",
        feedId: "feed-collision",
        sourceItemId: "guid-a",
        canonicalUrl: "https://articles.example.test/collision/a"
      }));
      await context.broker.deliverCanonicalization(articleDelivery(2, {
        candidateId: "candidate-collision-b",
        feedId: "feed-collision",
        sourceItemId: "guid-b",
        canonicalUrl: "https://articles.example.test/collision/b"
      }));

      await Promise.all([
        context.broker.deliverCanonicalization(articleDelivery(3, {
          candidateId: "candidate-collision-conflict",
          feedId: "feed-collision",
          sourceItemId: "guid-a",
          canonicalUrl: "https://articles.example.test/collision/b"
        })),
        context.broker.deliverCanonicalization(articleDelivery(4, {
          candidateId: "candidate-collision-unrelated",
          feedId: "feed-collision-other",
          sourceItemId: "guid-other",
          canonicalUrl: "https://articles.example.test/collision/other"
        }))
      ]);

      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new",
        "new",
        "ambiguous",
        "new"
      ]);
      expect(resolutionDecision(context.stateStore.decisions[2]).reasons).toEqual([
        "source-guid-url-conflict"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(3);
    } finally {
      await context.service.stop();
    }
  });

  it("retries cleanly after a crash before inbox insert", async () => {
    const clock = new ManualCanonicalizerClock();
    const stateStore = new ClaimCrashStateStore(clock);
    const context = createReliabilityContext({
      clock,
      stateStore
    });
    const delivery = articleDelivery(1, {
      candidateId: "candidate-crash-before-inbox",
      canonicalUrl: "https://articles.example.test/crash/before-inbox"
    });

    await context.service.start();

    try {
      await expect(context.broker.deliverCanonicalization(delivery)).rejects.toThrow("crash-before-inbox-insert");
      expect(context.stateStore.decisions).toHaveLength(0);
      expect(context.outbox.pendingEnrichment).toHaveLength(0);

      await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });

  it("retries cleanly after a crash after inbox insert but before canonical resolution", async () => {
    const clock = new ManualCanonicalizerClock();
    const stateStore = new ResolveCrashStateStore(clock);
    const context = createReliabilityContext({
      clock,
      stateStore
    });
    const delivery = articleDelivery(1, {
      candidateId: "candidate-crash-before-canonical",
      canonicalUrl: "https://articles.example.test/crash/before-canonical"
    });

    await context.service.start();

    try {
      await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
        action: "retry",
        reason: "handler-error"
      });
      expect(context.stateStore.decisions).toHaveLength(0);
      expect(context.outbox.pendingEnrichment).toHaveLength(0);

      await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });

  it("rolls back canonical state when pending outbox insert crashes", async () => {
    const outbox = new PendingOutboxCrash();
    const context = createReliabilityContext({
      outbox
    });
    const delivery = articleDelivery(1, {
      candidateId: "candidate-crash-before-outbox",
      canonicalUrl: "https://articles.example.test/crash/before-outbox"
    });

    await context.service.start();

    try {
      await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
        action: "retry",
        reason: "handler-error"
      });
      expect(context.stateStore.decisions).toHaveLength(0);
      expect(context.outbox.pendingEnrichment).toHaveLength(0);

      await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });

  it("does not duplicate enrichment after a crash after outbox insert and before ack", async () => {
    const clock = new ManualCanonicalizerClock();
    const stateStore = new CompletionCrashStateStore(clock);
    const context = createReliabilityContext({
      clock,
      stateStore
    });
    const delivery = articleDelivery(1, {
      candidateId: "candidate-crash-before-ack",
      canonicalUrl: "https://articles.example.test/crash/before-ack"
    });

    await context.service.start();

    try {
      await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
        action: "retry",
        reason: "handler-error"
      });
      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(1);

      await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new",
        "duplicate"
      ]);
      expect(resolutionDecision(context.stateStore.decisions[1]).reasons).toEqual([
        "candidate-replay"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
      expect(context.broker.published).toHaveLength(0);
    } finally {
      await context.service.stop();
    }
  });

  it("reports sanitized replay corpus duplicate and change rates with ambiguity review output", async () => {
    const context = createReliabilityContext();

    await context.service.start();

    try {
      for (const candidate of SANITIZED_REPLAY_CORPUS) {
        await context.broker.deliverCanonicalization(articleDelivery(candidate.sequence, candidate.payload));
      }

      const report = buildCanonicalReplayReport({
        corpusName: "sanitized-parity-inventory-canonicalizer-fixture",
        decisions: context.stateStore.decisions,
        legacy: {
          totalCandidates: SANITIZED_REPLAY_CORPUS.length,
          duplicateCount: 1,
          changeCount: 1
        }
      });

      expect(report.decisionCounts).toEqual({
        new: 2,
        duplicate: 1,
        alias: 1,
        changed: 1,
        ambiguous: 1,
        invalid: 0
      });
      expect(report.duplicateRate).toBeCloseTo(1 / 6);
      expect(report.changeRate).toBeCloseTo(1 / 6);
      expect(report.legacyComparison?.duplicateRate.delta).toBe(0);
      expect(report.legacyComparison?.changeRate.delta).toBe(0);
      expect(report.ambiguityReview).toEqual([
        {
          candidateId: "candidate-replay-ambiguous",
          feedId: "feed-replay",
          sourceItemId: "guid-replay-a",
          canonicalArticleId: resolutionDecision(context.stateStore.decisions[0]).canonicalArticleId,
          normalizedUrl: "https://articles.example.test/replay/b",
          reasons: [
            "source-guid-url-conflict"
          ]
        }
      ]);
    } finally {
      await context.service.stop();
    }
  });
});

const SANITIZED_REPLAY_CORPUS: readonly {
  readonly sequence: number;
  readonly payload: Readonly<Record<string, unknown>>;
}[] = [
  {
    sequence: 1,
    payload: {
      candidateId: "candidate-replay-a",
      feedId: "feed-replay",
      sourceItemId: "guid-replay-a",
      canonicalUrl: "https://articles.example.test/replay/a?utm_source=legacy",
      title: "Replay A"
    }
  },
  {
    sequence: 2,
    payload: {
      candidateId: "candidate-replay-duplicate",
      feedId: "feed-replay-other",
      sourceItemId: "guid-replay-duplicate",
      canonicalUrl: "https://articles.example.test/replay/a",
      title: "Replay A"
    }
  },
  {
    sequence: 3,
    payload: {
      candidateId: "candidate-replay-changed",
      feedId: "feed-replay",
      sourceItemId: "guid-replay-a",
      canonicalUrl: "https://articles.example.test/replay/a",
      title: "Replay A Updated"
    }
  },
  {
    sequence: 4,
    payload: {
      candidateId: "candidate-replay-alias",
      feedId: "feed-replay",
      sourceItemId: "guid-replay-a",
      canonicalUrl: "https://www.example.test/replay/a",
      title: "Replay A Updated"
    }
  },
  {
    sequence: 5,
    payload: {
      candidateId: "candidate-replay-b",
      feedId: "feed-replay",
      sourceItemId: "guid-replay-b",
      canonicalUrl: "https://articles.example.test/replay/b",
      title: "Replay B"
    }
  },
  {
    sequence: 6,
    payload: {
      candidateId: "candidate-replay-ambiguous",
      feedId: "feed-replay",
      sourceItemId: "guid-replay-a",
      canonicalUrl: "https://articles.example.test/replay/b",
      title: "Replay B"
    }
  }
];

interface ReliabilityContextOptions {
  readonly clock?: ManualCanonicalizerClock;
  readonly stateStore?: InMemoryCanonicalStateStore;
  readonly outbox?: LocalCanonicalBrokerOutbox;
}

function createReliabilityContext(options: ReliabilityContextOptions = {}) {
  const config = loadCanonicalizerConfig({
    NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
    NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
  });
  const clock = options.clock ?? new ManualCanonicalizerClock();
  const baseDependencies = createLocalCanonicalizerDependencies({
    clock,
    stateStore: options.stateStore ?? new InMemoryCanonicalStateStore(clock),
    brokerOutbox: options.outbox ?? new LocalCanonicalBrokerOutbox()
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
  const dependencies = {
    ...baseDependencies,
    workHandler: createCanonicalizationWorkHandler({
      config,
      dependencies: baseDependencies,
      telemetry
    })
  };
  const service = createCanonicalizerService({
    config,
    dependencies,
    telemetry
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    outbox: dependencies.brokerOutbox as LocalCanonicalBrokerOutbox,
    service,
    stateStore: dependencies.stateStore as InMemoryCanonicalStateStore,
    transactionRunner: dependencies.transactionRunner as LocalCanonicalTransactionRunner
  };
}

function articleDelivery(sequence: number, payloadOverrides: Readonly<Record<string, unknown>> = {}) {
  const candidateId = typeof payloadOverrides.candidateId === "string" ? payloadOverrides.candidateId : `candidate-${String(sequence)}`;
  const idempotencyKey = `fetcher:canonicalization:${candidateId}:${String(sequence)}`;

  return createMinimalCanonicalizationDelivery({
    envelope: {
      messageId: uuid(11_000 + sequence),
      causationId: uuid(12_000 + sequence),
      correlationId: uuid(13_000 + sequence),
      idempotencyKey,
      aggregate: {
        type: "candidate",
        id: candidateId,
        version: 1
      },
      payloadRef: {
        kind: "backend-record",
        uri: `backend://worker-uplift/feed-fetcher/reliability/${candidateId}`,
        mediaType: "application/json",
        sizeBytes: 512
      }
    },
    payload: {
      candidateId,
      stageExecutionId: uuid(14_000 + sequence),
      sourceMessageId: uuid(15_000 + sequence),
      idempotencyKey,
      ...payloadOverrides
    }
  });
}

function resolutionDecisions(stateStore: InMemoryCanonicalStateStore): readonly CanonicalResolutionDecision[] {
  return stateStore.decisions.map(resolutionDecision);
}

function resolutionDecision(
  decision: CanonicalResolutionDecision | CanonicalInvalidDecision | undefined
): CanonicalResolutionDecision {
  if (decision === undefined || decision.decision === "invalid") {
    throw new Error("Expected canonical resolution decision.");
  }

  return decision;
}

function decisionCount(decisions: readonly CanonicalResolutionDecision[], decisionKind: CanonicalResolutionDecision["decision"]): number {
  return decisions.filter((decision) => decision.decision === decisionKind).length;
}

function uuid(counter: number): string {
  return `018f1598-2dd5-7c4f-9f92-${counter.toString(16).padStart(12, "0")}`;
}

class ClaimCrashStateStore extends InMemoryCanonicalStateStore {
  private crashNextClaim = true;

  override claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    if (this.crashNextClaim) {
      this.crashNextClaim = false;
      return Promise.reject(new Error("crash-before-inbox-insert"));
    }

    return super.claim(idempotencyKey, context);
  }
}

class ResolveCrashStateStore extends InMemoryCanonicalStateStore {
  private crashNextResolve = true;

  override resolveCandidate(input: CanonicalCandidateInput, transaction: CanonicalDatabaseTransaction): Promise<CanonicalResolutionDecision> {
    if (this.crashNextResolve) {
      this.crashNextResolve = false;
      return Promise.reject(new Error("crash-before-canonical-resolution"));
    }

    return super.resolveCandidate(input, transaction);
  }
}

class PendingOutboxCrash extends LocalCanonicalBrokerOutbox {
  private crashNextPending = true;

  override recordPendingEnrichment(request: CanonicalEnrichmentRequest, transaction: CanonicalDatabaseTransaction): Promise<void> {
    if (this.crashNextPending) {
      this.crashNextPending = false;
      return Promise.reject(new Error("crash-before-outbox-insert"));
    }

    return super.recordPendingEnrichment(request, transaction);
  }
}

class CompletionCrashStateStore extends InMemoryCanonicalStateStore {
  private crashNextCompletion = true;

  override markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    if (this.crashNextCompletion) {
      this.crashNextCompletion = false;
      return Promise.reject(new Error("crash-before-ack"));
    }

    return super.markCompleted(idempotencyKey, completion);
  }
}
