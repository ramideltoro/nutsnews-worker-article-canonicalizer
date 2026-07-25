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
}

export interface CanonicalDatabaseTransaction {
  readonly transactionId: string;
}

export interface CanonicalDatabaseTransactionRunner {
  readonly name: string;
  probe(): CanonicalizerDependencyProbe | Promise<CanonicalizerDependencyProbe>;
  withTransaction<T>(operation: (transaction: CanonicalDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface CanonicalBrokerOutbox {
  readonly name: string;
  probe(): CanonicalizerDependencyProbe | Promise<CanonicalizerDependencyProbe>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
}

export interface CanonicalizerWorkTools {
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: CanonicalDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface CanonicalizerWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: CanonicalizerWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface CanonicalizerDependencies {
  readonly clock: RuntimeClock;
  readonly stateStore: CanonicalStateStore;
  readonly transactionRunner: CanonicalDatabaseTransactionRunner;
  readonly brokerOutbox: CanonicalBrokerOutbox;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly workHandler: CanonicalizerWorkHandler;
}
