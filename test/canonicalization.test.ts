import {
  createBufferedRuntimeTelemetrySink,
  type BrokerPublishCommand,
  type RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  describe,
  expect,
  it
} from "vitest";

import { createCanonicalizationWorkHandler } from "../src/canonicalization.js";
import { loadCanonicalizerConfig } from "../src/config.js";
import { createCanonicalizerService } from "../src/service.js";
import {
  InMemoryCanonicalStateStore,
  LocalBrokerTransport,
  LocalCanonicalBrokerOutbox,
  LocalCanonicalTransactionRunner,
  createLocalCanonicalizerDependencies,
  createMinimalCanonicalizationDelivery,
  createMinimalCanonicalizationEnvelope,
  createMinimalCanonicalizationPayload
} from "../src/test-doubles.js";

import type {
  CanonicalEnrichmentRequest,
  CanonicalInvalidDecision,
  CanonicalResolutionDecision,
  CanonicalizerWorkTools
} from "../src/dependencies.js";

describe("createCanonicalizationWorkHandler", () => {
  it("records a new article decision and pending enrichment intent in one transaction", async () => {
    const context = createCanonicalizationContext();

    await context.service.start();

    try {
      const delivery = articleDelivery(1);

      await expect(context.broker.deliverCanonicalization(delivery)).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });

      const decision = resolutionDecision(context.stateStore.decisions[0]);

      expect(decision).toMatchObject({
        decision: "new",
        articleVersion: 1,
        normalizedUrl: "https://articles.example.test/world/story-one",
        reasons: [
          "normalized-url-first-seen"
        ],
        publishEnrichment: true
      });
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
      expect(context.outbox.pendingEnrichment[0]?.transaction).toBe(context.transactionRunner.transactions[0]);
      expect(context.outbox.pendingEnrichment[0]?.request).toMatchObject({
        canonicalArticleId: decision.canonicalArticleId,
        articleVersion: 1,
        candidateId: "candidate-1",
        canonicalUrl: "https://articles.example.test/world/story-one",
        reason: "new"
      });
      expect(context.broker.published).toHaveLength(1);
      expect(context.outbox.records).toHaveLength(1);

      const command = publishedCommand(context, 0);
      const request = pendingEnrichmentRequest(context, 0);
      const expectedIdempotencyKey = `canonicalizer:enrichment:${request.requestId}`;

      expect(command.payload).toMatchObject({
        schemaId: STAGE_PAYLOAD_SCHEMA_IDS.enrichmentRequest,
        schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
        pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
        sourceMessageId: uuid(1_001),
        idempotencyKey: expectedIdempotencyKey,
        requestId: request.requestId,
        canonicalArticleId: decision.canonicalArticleId,
        articleVersion: 1,
        candidateId: "candidate-1",
        canonicalUrl: "https://articles.example.test/world/story-one",
        reason: "new"
      });
      expect(command.envelope).toMatchObject({
        route: "enrichment",
        causationId: uuid(1_001),
        correlationId: uuid(3_001),
        idempotencyKey: expectedIdempotencyKey,
        aggregate: {
          type: "article",
          id: decision.canonicalArticleId,
          version: 1
        },
        producer: {
          name: "nutsnews-worker-article-canonicalizer"
        },
        payloadRef: {
          kind: "backend-record",
          mediaType: "application/json"
        }
      });
      expect(command.envelope.payloadRef.sizeBytes).toBeGreaterThan(0);
      expect(context.outbox.records[0]?.command).toBe(command);
    } finally {
      await context.service.stop();
    }
  });

  it("resolves the same article across feeds and retries to one canonical ID", async () => {
    const context = createCanonicalizationContext();

    await context.service.start();

    try {
      await context.broker.deliverCanonicalization(articleDelivery(1, {
        candidateId: "candidate-cross-feed-a",
        feedId: "feed-a",
        sourceItemId: "guid-a",
        canonicalUrl: "https://Articles.Example.test/story?id=42&utm_source=feed",
        title: "Shared Story"
      }));
      await context.broker.deliverCanonicalization(articleDelivery(2, {
        candidateId: "candidate-cross-feed-b",
        feedId: "feed-b",
        sourceItemId: "guid-b",
        canonicalUrl: "https://articles.example.test/story?utm_medium=email&id=42",
        title: "Shared Story"
      }));

      const first = resolutionDecision(context.stateStore.decisions[0]);
      const second = resolutionDecision(context.stateStore.decisions[1]);

      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new",
        "duplicate"
      ]);
      expect(second.canonicalArticleId).toBe(first.canonicalArticleId);
      expect(second.normalizedUrl).toBe("https://articles.example.test/story?id=42");
      expect(second.reasons).toEqual([
        "material-fingerprint-match"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
      expect(context.broker.published).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });

  it("records aliases without scheduling duplicate enrichment work", async () => {
    const context = createCanonicalizationContext();

    await context.service.start();

    try {
      await context.broker.deliverCanonicalization(articleDelivery(1, {
        candidateId: "candidate-alias-a",
        feedId: "feed-alias",
        sourceItemId: "guid-alias",
        canonicalUrl: "https://articles.example.test/news/story"
      }));
      await context.broker.deliverCanonicalization(articleDelivery(2, {
        candidateId: "candidate-alias-b",
        feedId: "feed-alias",
        sourceItemId: "guid-alias",
        canonicalUrl: "https://www.example.test/news/story"
      }));

      const first = resolutionDecision(context.stateStore.decisions[0]);
      const second = resolutionDecision(context.stateStore.decisions[1]);

      expect(second).toMatchObject({
        decision: "alias",
        canonicalArticleId: first.canonicalArticleId,
        articleVersion: 1,
        normalizedUrl: "https://www.example.test/news/story",
        reasons: [
          "new-url-alias"
        ],
        publishEnrichment: false
      });
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
      expect(context.broker.published).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });

  it("versions material changes without losing identity history", async () => {
    const context = createCanonicalizationContext();

    await context.service.start();

    try {
      await context.broker.deliverCanonicalization(articleDelivery(1, {
        candidateId: "candidate-version-a",
        feedId: "feed-version",
        sourceItemId: "guid-version",
        canonicalUrl: "https://articles.example.test/news/versioned",
        title: "First Headline"
      }));
      await context.broker.deliverCanonicalization(articleDelivery(2, {
        candidateId: "candidate-version-b",
        feedId: "feed-version",
        sourceItemId: "guid-version",
        canonicalUrl: "https://articles.example.test/news/versioned",
        title: "Updated Headline"
      }));

      const first = resolutionDecision(context.stateStore.decisions[0]);
      const second = resolutionDecision(context.stateStore.decisions[1]);

      expect(second).toMatchObject({
        decision: "changed",
        canonicalArticleId: first.canonicalArticleId,
        articleVersion: 2,
        reasons: [
          "material-fingerprint-changed"
        ],
        publishEnrichment: true
      });
      expect(context.outbox.pendingEnrichment).toHaveLength(2);
      expect(context.outbox.pendingEnrichment[1]?.request).toMatchObject({
        canonicalArticleId: first.canonicalArticleId,
        articleVersion: 2,
        reason: "changed"
      });
      expect(context.broker.published).toHaveLength(2);
      expect(publishedCommand(context, 1).payload).toMatchObject({
        canonicalArticleId: first.canonicalArticleId,
        articleVersion: 2,
        reason: "changed"
      });
    } finally {
      await context.service.stop();
    }
  });

  it("marks source and URL conflicts as ambiguous instead of publishing enrichment", async () => {
    const context = createCanonicalizationContext();

    await context.service.start();

    try {
      await context.broker.deliverCanonicalization(articleDelivery(1, {
        candidateId: "candidate-conflict-a",
        feedId: "feed-conflict",
        sourceItemId: "guid-a",
        canonicalUrl: "https://articles.example.test/news/a"
      }));
      await context.broker.deliverCanonicalization(articleDelivery(2, {
        candidateId: "candidate-conflict-b",
        feedId: "feed-conflict",
        sourceItemId: "guid-b",
        canonicalUrl: "https://articles.example.test/news/b"
      }));
      await context.broker.deliverCanonicalization(articleDelivery(3, {
        candidateId: "candidate-conflict-c",
        feedId: "feed-conflict",
        sourceItemId: "guid-a",
        canonicalUrl: "https://articles.example.test/news/b"
      }));

      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new",
        "new",
        "ambiguous"
      ]);
      expect(resolutionDecision(context.stateStore.decisions[2]).reasons).toEqual([
        "source-guid-url-conflict"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(2);
      expect(context.broker.published).toHaveLength(2);
    } finally {
      await context.service.stop();
    }
  });

  it("prevents duplicate enrichment on runtime replay and candidate replay", async () => {
    const context = createCanonicalizationContext();
    const firstDelivery = articleDelivery(1, {
      candidateId: "candidate-replay",
      canonicalUrl: "https://articles.example.test/news/replay"
    });

    await context.service.start();

    try {
      await context.broker.deliverCanonicalization(firstDelivery);
      await expect(context.broker.deliverCanonicalization(firstDelivery)).resolves.toMatchObject({
        action: "ack",
        reason: "duplicate"
      });
      await context.broker.deliverCanonicalization(articleDelivery(2, {
        candidateId: "candidate-replay",
        canonicalUrl: "https://articles.example.test/news/replay"
      }));

      expect(context.stateStore.decisions.map((decision) => decision.decision)).toEqual([
        "new",
        "duplicate"
      ]);
      expect(resolutionDecision(context.stateStore.decisions[1]).reasons).toEqual([
        "candidate-replay"
      ]);
      expect(context.outbox.pendingEnrichment).toHaveLength(1);
      expect(context.broker.published).toHaveLength(1);
    } finally {
      await context.service.stop();
    }
  });

  it("records invalid canonical URLs without opening a transaction", async () => {
    const config = loadCanonicalizerConfig({
      NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
      NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalCanonicalizerDependencies();
    const handler = createCanonicalizationWorkHandler({
      config,
      dependencies,
      telemetry: createBufferedRuntimeTelemetrySink()
    });
    const context = {
      envelope: createMinimalCanonicalizationEnvelope(),
      payload: createMinimalCanonicalizationPayload({
        canonicalUrl: "ftp://articles.example.test/news/story"
      }),
      stage: "canonicalization",
      receivedAt: "2026-07-23T00:00:01.000Z"
    } satisfies RuntimeMessageContext;
    const tools = createWorkTools(dependencies);

    await expect(handler.handle(context, tools)).resolves.toEqual({
      status: "ok"
    });
    expect(inMemoryStateStore(dependencies).decisions[0]).toEqual({
      decision: "invalid",
      candidateId: "candidate-world-001",
      feedId: "feed-world",
      sourceItemId: "guid-001",
      canonicalUrl: "ftp://articles.example.test/news/story",
      reasons: [
        "unsupported-scheme"
      ],
      decidedAt: "2026-07-23T00:00:00.000Z"
    });
    expect(localTransactionRunner(dependencies).transactions).toHaveLength(0);
    expect(localBrokerOutbox(dependencies).pendingEnrichment).toHaveLength(0);
  });
});

function createCanonicalizationContext() {
  const config = loadCanonicalizerConfig({
    NUTSNEWS_CANONICALIZER_HTTP_PORT: "0",
    NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS: "silent"
  });
  const baseDependencies = createLocalCanonicalizerDependencies();
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
      messageId: uuid(1_000 + sequence),
      causationId: uuid(2_000 + sequence),
      correlationId: uuid(3_000 + sequence),
      idempotencyKey,
      aggregate: {
        type: "candidate",
        id: candidateId,
        version: 1
      },
      payloadRef: {
        kind: "backend-record",
        uri: `backend://worker-uplift/feed-fetcher/test/${candidateId}`,
        mediaType: "application/json",
        sizeBytes: 512
      }
    },
    payload: {
      candidateId,
      stageExecutionId: uuid(4_000 + sequence),
      sourceMessageId: uuid(5_000 + sequence),
      idempotencyKey,
      ...payloadOverrides
    }
  });
}

function createWorkTools(dependencies: ReturnType<typeof createLocalCanonicalizerDependencies>): CanonicalizerWorkTools {
  return {
    publish: () => Promise.reject(new Error("unexpected broker publish")),
    recordPendingEnrichment: (request, transaction) => dependencies.brokerOutbox.recordPendingEnrichment(request, transaction),
    recordOutbox: (command, receipt) => dependencies.brokerOutbox.record(command, receipt),
    withTransaction: (operation) => dependencies.transactionRunner.withTransaction(operation)
  };
}

function resolutionDecision(
  decision: CanonicalResolutionDecision | CanonicalInvalidDecision | undefined
): CanonicalResolutionDecision {
  if (decision === undefined || decision.decision === "invalid") {
    throw new Error("Expected canonical resolution decision.");
  }

  return decision;
}

function inMemoryStateStore(dependencies: ReturnType<typeof createLocalCanonicalizerDependencies>): InMemoryCanonicalStateStore {
  return dependencies.stateStore as InMemoryCanonicalStateStore;
}

function localTransactionRunner(dependencies: ReturnType<typeof createLocalCanonicalizerDependencies>): LocalCanonicalTransactionRunner {
  return dependencies.transactionRunner as LocalCanonicalTransactionRunner;
}

function localBrokerOutbox(dependencies: ReturnType<typeof createLocalCanonicalizerDependencies>): LocalCanonicalBrokerOutbox {
  return dependencies.brokerOutbox as LocalCanonicalBrokerOutbox;
}

function publishedCommand(context: ReturnType<typeof createCanonicalizationContext>, index: number): BrokerPublishCommand {
  const command = context.broker.published[index];

  if (command === undefined) {
    throw new Error(`Expected published command at index ${String(index)}.`);
  }

  return command;
}

function pendingEnrichmentRequest(context: ReturnType<typeof createCanonicalizationContext>, index: number): CanonicalEnrichmentRequest {
  const record = context.outbox.pendingEnrichment[index];

  if (record === undefined) {
    throw new Error(`Expected pending enrichment request at index ${String(index)}.`);
  }

  return record.request;
}

function uuid(counter: number): string {
  return `018f1598-2dd5-7c4f-9f92-${counter.toString(16).padStart(12, "0")}`;
}
