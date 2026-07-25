import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createInMemoryIdempotencyStore,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeHandlerResult,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  CanonicalBrokerOutbox,
  CanonicalCandidateInput,
  CanonicalCandidatePayload,
  CanonicalDatabaseTransaction,
  CanonicalDatabaseTransactionRunner,
  CanonicalEnrichmentRequest,
  CanonicalInvalidDecision,
  CanonicalResolutionDecision,
  CanonicalizerDependencies,
  CanonicalizerDependencyProbe,
  CanonicalizerWorkHandler,
  CanonicalizerWorkTools,
  CanonicalStateStore
} from "./dependencies.js";
import { stableArticleId } from "./ids.js";

interface CanonicalArticleRecord {
  readonly canonicalArticleId: string;
  readonly normalizedUrls: Set<string>;
  articleVersion: number;
  materialFingerprint: string;
}

export class ManualCanonicalizerClock implements RuntimeClock {
  private current: Date;

  constructor(initial = "2026-07-23T00:00:00.000Z") {
    this.current = new Date(initial);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class InMemoryCanonicalStateStore implements CanonicalStateStore {
  readonly name: string = "local-canonical-state";
  status: CanonicalizerDependencyProbe["status"] = "ok";
  readonly decisions: (CanonicalResolutionDecision | CanonicalInvalidDecision)[] = [];
  private readonly articles = new Map<string, CanonicalArticleRecord>();
  private readonly urlAliases = new Map<string, string>();
  private readonly sourceAliases = new Map<string, string>();
  private readonly candidateDecisions = new Map<string, CanonicalResolutionDecision>();
  private readonly store;

  constructor(clock: RuntimeClock = new ManualCanonicalizerClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): CanonicalizerDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local canonical state ready" : "local canonical state degraded"
    };
  }

  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    return this.store.claim(idempotencyKey, context);
  }

  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    return this.store.markCompleted(idempotencyKey, completion);
  }

  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    return this.store.markFailed(idempotencyKey, failure);
  }

  resolveCandidate(input: CanonicalCandidateInput, transaction: CanonicalDatabaseTransaction): Promise<CanonicalResolutionDecision> {
    const existingCandidateDecision = this.candidateDecisions.get(input.candidateId);

    if (existingCandidateDecision !== undefined) {
      const duplicateDecision = this.decision(input, transaction, {
        decision: "duplicate",
        canonicalArticleId: existingCandidateDecision.canonicalArticleId,
        articleVersion: existingCandidateDecision.articleVersion,
        reasons: [
          "candidate-replay"
        ],
        publishEnrichment: false
      });

      this.decisions.push(duplicateDecision);
      return Promise.resolve(duplicateDecision);
    }

    if (input.dedupeStatus === "duplicate" && input.duplicateOfArticleId !== undefined) {
      const duplicateDecision = this.decision(input, transaction, {
        decision: "duplicate",
        canonicalArticleId: input.duplicateOfArticleId,
        articleVersion: 1,
        reasons: [
          "upstream-duplicate-hint"
        ],
        publishEnrichment: false
      });

      this.rememberAliases(input, input.duplicateOfArticleId);
      this.candidateDecisions.set(input.candidateId, duplicateDecision);
      this.decisions.push(duplicateDecision);
      return Promise.resolve(duplicateDecision);
    }

    const sourceKey = sourceAliasKey(input);
    const articleIdBySource = this.sourceAliases.get(sourceKey);
    const articleIdByUrl = this.urlAliases.get(input.normalizedUrl);

    if (articleIdBySource !== undefined && articleIdByUrl !== undefined && articleIdBySource !== articleIdByUrl) {
      const ambiguousDecision = this.decision(input, transaction, {
        decision: "ambiguous",
        canonicalArticleId: articleIdBySource,
        articleVersion: this.articles.get(articleIdBySource)?.articleVersion ?? 1,
        reasons: [
          "source-guid-url-conflict"
        ],
        publishEnrichment: false
      });

      this.candidateDecisions.set(input.candidateId, ambiguousDecision);
      this.decisions.push(ambiguousDecision);
      return Promise.resolve(ambiguousDecision);
    }

    const existingArticleId = articleIdBySource ?? articleIdByUrl;

    if (existingArticleId === undefined) {
      const canonicalArticleId = stableArticleId([
        input.identitySeed
      ]);
      const record: CanonicalArticleRecord = {
        canonicalArticleId,
        normalizedUrls: new Set([
          input.normalizedUrl
        ]),
        articleVersion: 1,
        materialFingerprint: input.materialFingerprint
      };
      const newDecision = this.decision(input, transaction, {
        decision: "new",
        canonicalArticleId,
        articleVersion: record.articleVersion,
        reasons: [
          "normalized-url-first-seen"
        ],
        publishEnrichment: true
      });

      this.articles.set(canonicalArticleId, record);
      this.rememberAliases(input, canonicalArticleId);
      this.candidateDecisions.set(input.candidateId, newDecision);
      this.decisions.push(newDecision);
      return Promise.resolve(newDecision);
    }

    const record = this.articles.get(existingArticleId);

    if (record === undefined) {
      const duplicateDecision = this.decision(input, transaction, {
        decision: "duplicate",
        canonicalArticleId: existingArticleId,
        articleVersion: 1,
        reasons: [
          "external-duplicate-alias"
        ],
        publishEnrichment: false
      });

      this.rememberAliases(input, existingArticleId);
      this.candidateDecisions.set(input.candidateId, duplicateDecision);
      this.decisions.push(duplicateDecision);
      return Promise.resolve(duplicateDecision);
    }

    const isNewAlias = !record.normalizedUrls.has(input.normalizedUrl);

    if (isNewAlias) {
      record.normalizedUrls.add(input.normalizedUrl);
      this.rememberAliases(input, record.canonicalArticleId);

      const aliasDecision = this.decision(input, transaction, {
        decision: "alias",
        canonicalArticleId: record.canonicalArticleId,
        articleVersion: record.articleVersion,
        reasons: [
          "new-url-alias"
        ],
        publishEnrichment: false
      });

      this.candidateDecisions.set(input.candidateId, aliasDecision);
      this.decisions.push(aliasDecision);
      return Promise.resolve(aliasDecision);
    }

    if (record.materialFingerprint !== input.materialFingerprint) {
      record.articleVersion += 1;
      record.materialFingerprint = input.materialFingerprint;
      this.rememberAliases(input, record.canonicalArticleId);

      const changedDecision = this.decision(input, transaction, {
        decision: "changed",
        canonicalArticleId: record.canonicalArticleId,
        articleVersion: record.articleVersion,
        reasons: [
          "material-fingerprint-changed"
        ],
        publishEnrichment: true
      });

      this.candidateDecisions.set(input.candidateId, changedDecision);
      this.decisions.push(changedDecision);
      return Promise.resolve(changedDecision);
    }

    this.rememberAliases(input, record.canonicalArticleId);

    const duplicateDecision = this.decision(input, transaction, {
      decision: "duplicate",
      canonicalArticleId: record.canonicalArticleId,
      articleVersion: record.articleVersion,
      reasons: [
        "material-fingerprint-match"
      ],
      publishEnrichment: false
    });

    this.candidateDecisions.set(input.candidateId, duplicateDecision);
    this.decisions.push(duplicateDecision);
    return Promise.resolve(duplicateDecision);
  }

  recordInvalidCandidate(input: CanonicalCandidatePayload, decision: CanonicalInvalidDecision): Promise<void> {
    void input;
    this.decisions.push(decision);
    return Promise.resolve();
  }

  private decision(
    input: CanonicalCandidateInput,
    transaction: CanonicalDatabaseTransaction,
    values: {
      readonly decision: CanonicalResolutionDecision["decision"];
      readonly canonicalArticleId: string;
      readonly articleVersion: number;
      readonly reasons: readonly string[];
      readonly publishEnrichment: boolean;
    }
  ): CanonicalResolutionDecision {
    void transaction;
    return {
      decision: values.decision,
      canonicalArticleId: values.canonicalArticleId,
      articleVersion: values.articleVersion,
      normalizedUrl: input.normalizedUrl,
      materialFingerprint: input.materialFingerprint,
      reasons: values.reasons,
      publishEnrichment: values.publishEnrichment,
      transaction,
      decidedAt: input.decidedAt
    };
  }

  private rememberAliases(input: CanonicalCandidateInput, canonicalArticleId: string): void {
    this.sourceAliases.set(sourceAliasKey(input), canonicalArticleId);
    this.urlAliases.set(input.normalizedUrl, canonicalArticleId);
  }
}

export class LocalCanonicalTransactionRunner implements CanonicalDatabaseTransactionRunner {
  readonly name: string = "local-database-transactions";
  status: CanonicalizerDependencyProbe["status"] = "ok";
  readonly transactions: CanonicalDatabaseTransaction[] = [];

  probe(): CanonicalizerDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local transaction runner ready" : "local transaction runner degraded"
    };
  }

  async withTransaction<T>(operation: (transaction: CanonicalDatabaseTransaction) => Promise<T>): Promise<T> {
    const transaction = {
      transactionId: `local-transaction-${String(this.transactions.length + 1)}`
    };

    this.transactions.push(transaction);

    return operation(transaction);
  }
}

export class LocalCanonicalBrokerOutbox implements CanonicalBrokerOutbox {
  readonly name: string = "local-broker-outbox";
  status: CanonicalizerDependencyProbe["status"] = "ok";
  readonly pendingEnrichment: { readonly request: CanonicalEnrichmentRequest; readonly transaction: CanonicalDatabaseTransaction }[] = [];
  readonly records: { readonly command: BrokerPublishCommand; readonly receipt: BrokerPublishReceipt }[] = [];

  probe(): CanonicalizerDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local broker outbox ready" : "local broker outbox degraded"
    };
  }

  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    this.records.push({
      command,
      receipt
    });
    return Promise.resolve();
  }

  recordPendingEnrichment(request: CanonicalEnrichmentRequest, transaction: CanonicalDatabaseTransaction): Promise<void> {
    this.pendingEnrichment.push({
      request,
      transaction
    });
    return Promise.resolve();
  }
}

export class LocalCanonicalizerWorkHandler implements CanonicalizerWorkHandler {
  readonly name: string = "local-canonicalizer-work-handler";
  readonly handled: RuntimeMessageContext[] = [];
  result: RuntimeHandlerResult = {
    status: "ok"
  };
  handleGate: Promise<void> | undefined;
  onHandleStart: (() => void) | undefined;

  async handle(context: RuntimeMessageContext, tools: CanonicalizerWorkTools): Promise<RuntimeHandlerResult> {
    void tools;
    this.onHandleStart?.();
    await this.handleGate;
    this.handled.push(context);

    return this.result;
  }
}

export class LocalBrokerTransport implements RuntimeBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly published: BrokerPublishCommand[] = [];
  readonly assertedRoutes: WorkerRoute[] = [];
  private readonly consumers = new Map<WorkerStage, BrokerDeliveryHandler>();
  private deliveryCount = 0;
  private connected = false;
  private closed = false;

  get inFlightDeliveryCount(): number {
    return this.deliveryCount;
  }

  connect(): Promise<void> {
    this.connected = true;
    this.closed = false;
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.assertedRoutes.splice(0, this.assertedRoutes.length, ...routes);
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    if (!this.connected || this.closed) {
      throw new Error("Local broker transport is not connected.");
    }

    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    if (!this.connected || this.closed) {
      throw new Error("Local broker transport is not connected.");
    }

    this.consumers.set(stage, handler);

    return Promise.resolve({
      stage,
      cancel: () => {
        this.consumers.delete(stage);
        return Promise.resolve();
      }
    });
  }

  async deliver(stage: WorkerStage, delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
    const handler = this.consumers.get(stage);

    if (handler === undefined) {
      throw new Error(`No local consumer is registered for ${stage}.`);
    }

    this.deliveryCount += 1;

    try {
      return await handler(delivery);
    } finally {
      this.deliveryCount = Math.max(0, this.deliveryCount - 1);
    }
  }

  deliverCanonicalization(delivery: RuntimeMessageDelivery = createMinimalCanonicalizationDelivery()): Promise<RuntimeMessageProcessingResult> {
    return this.deliver("canonicalization", delivery);
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closed = true;
    this.connected = false;
    this.consumers.clear();
    return Promise.resolve();
  }
}

export interface LocalCanonicalizerDependencyOptions {
  readonly clock?: RuntimeClock;
  readonly stateStore?: CanonicalStateStore;
  readonly transactionRunner?: CanonicalDatabaseTransactionRunner;
  readonly brokerOutbox?: CanonicalBrokerOutbox;
  readonly brokerTransport?: RuntimeBrokerTransport;
  readonly workHandler?: CanonicalizerWorkHandler;
}

export function createLocalCanonicalizerDependencies(options: LocalCanonicalizerDependencyOptions = {}): CanonicalizerDependencies {
  const clock = options.clock ?? new ManualCanonicalizerClock();

  return {
    clock,
    stateStore: options.stateStore ?? new InMemoryCanonicalStateStore(clock),
    transactionRunner: options.transactionRunner ?? new LocalCanonicalTransactionRunner(),
    brokerOutbox: options.brokerOutbox ?? new LocalCanonicalBrokerOutbox(),
    brokerTransport: options.brokerTransport ?? new LocalBrokerTransport(),
    workHandler: options.workHandler ?? new LocalCanonicalizerWorkHandler()
  };
}

export function createMinimalCanonicalizationPayload(overrides: Readonly<Record<string, unknown>> = {}): Readonly<Record<string, unknown>> {
  const now = "2026-07-23T00:00:00.000Z";

  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.canonicalArticleCandidate,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3702",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3703",
    idempotencyKey: "fetcher:canonicalization:candidate-world-001",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: now,
    candidateId: "candidate-world-001",
    feedId: "feed-world",
    sourceItemId: "guid-001",
    originalUrl: "https://articles.example.test/world/story-one",
    canonicalUrl: "https://articles.example.test/world/story-one",
    title: "Story One",
    sourceName: "World Source",
    dedupeStatus: "new",
    ...overrides
  };
}

export function createMinimalCanonicalizationEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("canonicalization");
  const now = "2026-07-23T00:00:00.000Z";

  return assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "canonicalization",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3720",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3710",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3710",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "fetcher:canonicalization:candidate-world-001",
    aggregate: {
      type: "candidate",
      id: "candidate-world-001",
      version: 1
    },
    occurredAt: now,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: now
    },
    producer: {
      name: "fetcher",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/feed-fetcher/feed-world/candidate-world-001",
      mediaType: "application/json",
      sizeBytes: 512
    },
    ...overrides
  });
}

export function createMinimalCanonicalizationDelivery(
  overrides: {
    readonly envelope?: Partial<WorkerMessageEnvelope>;
    readonly payload?: Readonly<Record<string, unknown>>;
  } = {}
): RuntimeMessageDelivery {
  return {
    envelope: createMinimalCanonicalizationEnvelope(overrides.envelope ?? {}),
    payload: createMinimalCanonicalizationPayload(overrides.payload),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}

function sourceAliasKey(input: Pick<CanonicalCandidateInput, "feedId" | "sourceItemId">): string {
  return `${input.feedId}\u001f${input.sourceItemId}`;
}
