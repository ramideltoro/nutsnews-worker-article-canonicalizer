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
  CanonicalDatabaseTransaction,
  CanonicalDatabaseTransactionRunner,
  CanonicalizerDependencies,
  CanonicalizerDependencyProbe,
  CanonicalizerWorkHandler,
  CanonicalizerWorkTools,
  CanonicalStateStore
} from "./dependencies.js";

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
