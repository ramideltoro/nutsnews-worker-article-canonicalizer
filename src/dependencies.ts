import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeHandlerResult,
  RuntimeIdempotencyStore,
  RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";

export interface CanonicalizerDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export interface CanonicalStateStore extends RuntimeIdempotencyStore {
  readonly name: string;
  probe(): CanonicalizerDependencyProbe | Promise<CanonicalizerDependencyProbe>;
  resolveCandidate(input: CanonicalCandidateInput, transaction: CanonicalDatabaseTransaction): Promise<CanonicalResolutionDecision>;
  recordInvalidCandidate(input: CanonicalCandidatePayload, decision: CanonicalInvalidDecision): Promise<void>;
}

export interface CanonicalDatabaseTransaction {
  readonly transactionId: string;
  addCommitOperation?(operation: () => void): void;
}

export interface CanonicalDatabaseTransactionRunner {
  readonly name: string;
  probe(): CanonicalizerDependencyProbe | Promise<CanonicalizerDependencyProbe>;
  withTransaction<T>(operation: (transaction: CanonicalDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface CanonicalBrokerOutbox {
  readonly name: string;
  probe(): CanonicalizerDependencyProbe | Promise<CanonicalizerDependencyProbe>;
  recordPendingEnrichment(request: CanonicalEnrichmentRequest, transaction: CanonicalDatabaseTransaction): Promise<void>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
}

export interface CanonicalizerWorkTools {
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordPendingEnrichment(request: CanonicalEnrichmentRequest, transaction: CanonicalDatabaseTransaction): Promise<void>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: CanonicalDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface CanonicalizerWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: CanonicalizerWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface CanonicalizerDependencies {
  readonly adapterMode: "test" | "mixed" | "production";
  readonly clock: RuntimeClock;
  readonly stateStore: CanonicalStateStore;
  readonly transactionRunner: CanonicalDatabaseTransactionRunner;
  readonly brokerOutbox: CanonicalBrokerOutbox;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly workHandler: CanonicalizerWorkHandler;
}

export interface CanonicalCandidatePayload {
  readonly candidateId: string;
  readonly feedId: string;
  readonly sourceItemId: string;
  readonly originalUrl: string;
  readonly canonicalUrl: string;
  readonly title: string;
  readonly sourceName: string;
  readonly publishedAt?: string;
  readonly dedupeStatus: "new" | "duplicate";
  readonly duplicateOfArticleId?: string;
}

export interface CanonicalCandidateInput extends CanonicalCandidatePayload {
  readonly normalizedUrl: string;
  readonly removedTrackingParameters: readonly string[];
  readonly materialFingerprint: string;
  readonly identitySeed: string;
  readonly decidedAt: string;
}

export type CanonicalDecisionKind =
  | "new"
  | "duplicate"
  | "alias"
  | "changed"
  | "ambiguous"
  | "invalid";

export interface CanonicalResolutionDecision {
  readonly decision: Exclude<CanonicalDecisionKind, "invalid">;
  readonly candidateId: string;
  readonly feedId: string;
  readonly sourceItemId: string;
  readonly canonicalArticleId: string;
  readonly articleVersion: number;
  readonly normalizedUrl: string;
  readonly materialFingerprint: string;
  readonly reasons: readonly string[];
  readonly publishEnrichment: boolean;
  readonly transaction: CanonicalDatabaseTransaction;
  readonly decidedAt: string;
}

export interface CanonicalInvalidDecision {
  readonly decision: "invalid";
  readonly candidateId: string;
  readonly feedId: string;
  readonly sourceItemId: string;
  readonly canonicalUrl: string;
  readonly reasons: readonly string[];
  readonly decidedAt: string;
}

export interface CanonicalEnrichmentRequest {
  readonly requestId: string;
  readonly canonicalArticleId: string;
  readonly articleVersion: number;
  readonly candidateId: string;
  readonly canonicalUrl: string;
  readonly reason: "new" | "changed";
  readonly producedAt: string;
  readonly producer: {
    readonly name: string;
    readonly version: string;
  };
  readonly payloadRef: {
    readonly kind: "backend-record";
    readonly uri: string;
    readonly mediaType: "application/json";
  };
}
