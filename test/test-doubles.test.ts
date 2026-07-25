import {
  getWorkerRoute,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  LocalBrokerTransport,
  LocalCanonicalBrokerOutbox,
  LocalCanonicalTransactionRunner,
  createMinimalCanonicalizationDelivery
} from "../src/test-doubles.js";

describe("canonicalizer test doubles", () => {
  it("requires a registered local broker consumer before delivery", async () => {
    const broker = new LocalBrokerTransport();

    await broker.connect();

    await expect(broker.deliverCanonicalization(createMinimalCanonicalizationDelivery())).rejects.toThrow("No local consumer is registered for canonicalization.");
  });

  it("records local transaction and outbox boundaries without external dependencies", async () => {
    const runner = new LocalCanonicalTransactionRunner();
    const outbox = new LocalCanonicalBrokerOutbox();
    const route = getWorkerRoute("enrichment");
    const command = {
      envelope: {
        schemaId: route.schemaId,
        schemaVersion: 1,
        route: "enrichment",
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3801",
        causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3720",
        correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3710",
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        idempotencyKey: "canonicalizer:enrichment:candidate-world-001",
        aggregate: {
          type: "candidate",
          id: "candidate-world-001",
          version: 1
        },
        occurredAt: "2026-07-23T00:00:00.000Z",
        attempt: {
          count: 1,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z"
        },
        producer: {
          name: "canonicalizer",
          version: "0.1.0"
        },
        payloadRef: {
          kind: "backend-record",
          uri: "backend://worker-uplift/canonicalizer/candidate-world-001",
          mediaType: "application/json",
          sizeBytes: 512
        }
      } satisfies WorkerMessageEnvelope,
      payload: {}
    };

    await expect(runner.withTransaction((transaction) => Promise.resolve(transaction.transactionId))).resolves.toBe("local-transaction-1");
    await outbox.record(command, {
      messageId: command.envelope.messageId,
      stage: "enrichment",
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });

    expect(runner.transactions).toHaveLength(1);
    expect(outbox.records).toHaveLength(1);
  });
});
