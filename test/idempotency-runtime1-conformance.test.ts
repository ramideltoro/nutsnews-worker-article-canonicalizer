import {
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  CANONICALIZER_IDEMPOTENCY_LEASE_MS,
  InMemoryCanonicalStateStore,
  ManualCanonicalizerClock,
  createMinimalCanonicalizationEnvelope
} from "../src/test-doubles.js";

describe("Runtime 1.0 canonicalizer idempotency conformance", () => {
  it("bounds leases to five minutes and atomically reclaims a lost claim response", async () => {
    const clock = new ManualCanonicalizerClock();
    const store = new InMemoryCanonicalStateStore(clock);
    const idempotencyKey = "fetcher:canonicalization:lease-reclaim";
    const first = claimed(await store.claim(idempotencyKey, claimContext(1)));

    clock.advance(CANONICALIZER_IDEMPOTENCY_LEASE_MS - 1);
    await expect(store.claim(idempotencyKey, claimContext(2))).resolves.toMatchObject({
      status: "in-progress",
      firstSeenAt: first.firstSeenAt
    });

    clock.advance(1);
    const reclaimed = claimed(await store.claim(idempotencyKey, claimContext(2)));

    expect(store.idempotencyLeaseMs).toBe(300_000);
    expect(reclaimed.replay).toBe(true);
    expect(reclaimed.firstSeenAt).toBe(first.firstSeenAt);
    expect(reclaimed.claimToken).not.toBe(first.claimToken);
    await expect(store.markFailed(idempotencyKey, failure(first.claimToken, 1))).rejects.toThrow(
      "Cannot fail an idempotency claim owned by another delivery."
    );
    await expect(store.markCompleted(idempotencyKey, completion(first.claimToken, 1))).rejects.toThrow(
      "Cannot complete an idempotency claim owned by another delivery."
    );
    await expect(store.releaseClaim(idempotencyKey, failure(first.claimToken, 1))).resolves.toEqual({
      status: "not-owned"
    });
    await expect(store.markCompleted(idempotencyKey, completion(reclaimed.claimToken, 2))).resolves.toBeUndefined();
  });

  it("uses token-aware release without disturbing a concurrent owner", async () => {
    const store = new InMemoryCanonicalStateStore();
    const idempotencyKey = "fetcher:canonicalization:conditional-release";
    const first = claimed(await store.claim(idempotencyKey, claimContext(1)));

    await expect(store.releaseClaim(idempotencyKey, failure(first.claimToken, 1))).resolves.toEqual({
      status: "released"
    });
    const second = claimed(await store.claim(idempotencyKey, claimContext(2)));

    expect(second.claimToken).not.toBe(first.claimToken);
    await expect(store.releaseClaim(idempotencyKey, failure(first.claimToken, 1))).resolves.toEqual({
      status: "not-owned"
    });
    await expect(store.claim(idempotencyKey, claimContext(3))).resolves.toMatchObject({
      status: "in-progress"
    });
    await expect(store.markCompleted(idempotencyKey, completion(second.claimToken, 2))).resolves.toBeUndefined();
  });

  it("invalidates expired ownership before another claimant reclaims it", async () => {
    const clock = new ManualCanonicalizerClock();
    const store = new InMemoryCanonicalStateStore(clock);
    const idempotencyKey = "fetcher:canonicalization:expiry-before-reclaim";
    const expired = claimed(await store.claim(idempotencyKey, claimContext(1)));

    clock.advance(CANONICALIZER_IDEMPOTENCY_LEASE_MS);

    await expect(store.markCompleted(idempotencyKey, completion(expired.claimToken, 1))).rejects.toThrow(
      "Cannot complete an idempotency claim owned by another delivery."
    );
    await expect(store.markFailed(idempotencyKey, failure(expired.claimToken, 1))).rejects.toThrow(
      "Cannot fail an idempotency claim owned by another delivery."
    );
    await expect(store.releaseClaim(idempotencyKey, failure(expired.claimToken, 1))).resolves.toEqual({
      status: "not-owned"
    });

    const reclaimed = claimed(await store.claim(idempotencyKey, claimContext(2)));
    expect(reclaimed.claimToken).not.toBe(expired.claimToken);
    await expect(store.markCompleted(idempotencyKey, completion(reclaimed.claimToken, 2))).resolves.toBeUndefined();
  });

  it("preserves completed work across failure and release paths", async () => {
    const store = new InMemoryCanonicalStateStore();
    const idempotencyKey = "fetcher:canonicalization:preserve-completed";
    const claim = claimed(await store.claim(idempotencyKey, claimContext(1)));

    await store.markCompleted(idempotencyKey, completion(claim.claimToken, 1));

    await expect(store.markFailed(idempotencyKey, failure(claim.claimToken, 1))).rejects.toThrow(
      "Cannot fail an idempotency claim owned by another delivery."
    );
    await expect(store.releaseClaim(idempotencyKey, failure(claim.claimToken, 1))).resolves.toEqual({
      status: "preserved-completed"
    });
    await expect(store.claim(idempotencyKey, claimContext(2))).resolves.toMatchObject({
      status: "already-completed",
      completedAt: "2026-07-23T00:00:02.000Z"
    });
  });

  it("rejects leases outside the Runtime 1.0 production bound", () => {
    const clock = new ManualCanonicalizerClock();

    expect(() => new InMemoryCanonicalStateStore(clock, 0)).toThrow("between 1 and 300000 milliseconds");
    expect(() => new InMemoryCanonicalStateStore(clock, CANONICALIZER_IDEMPOTENCY_LEASE_MS + 1)).toThrow(
      "between 1 and 300000 milliseconds"
    );
  });
});

function claimed(result: RuntimeIdempotencyClaimResult): Extract<RuntimeIdempotencyClaimResult, {
  readonly status: "claimed";
}> {
  if (result.status !== "claimed") {
    throw new Error(`Expected claimed idempotency state, received ${result.status}.`);
  }

  return result;
}

function claimContext(sequence: number): RuntimeIdempotencyClaimContext {
  return {
    envelope: createMinimalCanonicalizationEnvelope({
      messageId: messageId(sequence)
    }),
    stage: "canonicalization",
    receivedAt: `2026-07-23T00:00:0${String(sequence)}.000Z`
  };
}

function completion(claimToken: string, sequence: number): RuntimeIdempotencyCompletion {
  return {
    completedAt: `2026-07-23T00:00:0${String(sequence + 1)}.000Z`,
    messageId: messageId(sequence),
    claimToken,
    stage: "canonicalization"
  };
}

function failure(claimToken: string, sequence: number): RuntimeIdempotencyFailure {
  return {
    failedAt: `2026-07-23T00:00:0${String(sequence + 1)}.000Z`,
    messageId: messageId(sequence),
    claimToken,
    stage: "canonicalization",
    reason: "conformance-failure",
    retryable: true
  };
}

function messageId(sequence: number): string {
  return `018f1598-2dd5-7c4f-9f92-8f7a7f8c${sequence.toString(16).padStart(4, "0")}`;
}
