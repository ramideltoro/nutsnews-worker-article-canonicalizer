# Architecture Notes

## Scope

The article canonicalizer owns the worker-uplift service boundary that consumes `canonicalArticleCandidate` messages on the contracted `canonicalization` route. It normalizes article identity inputs, resolves canonical article IDs, records duplicate and alias decisions, versions material changes, and stores pending enrichment outbox intent.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.4.0`
- Input route boundary: `getWorkerRoute("canonicalization")`
- Downstream publish route boundary: `getWorkerRoute("enrichment")`
- Health: separate liveness, startup, and readiness probes
- Metrics: bounded Prometheus text from the shared runtime sink
- Shutdown: stop accepting deliveries, wait for in-flight handlers, cancel consumers, close broker lifecycle

## Shell Flow

1. Validate value-free configuration and secret presence by variable name.
2. Assert exact contracts/runtime package versions.
3. Start the shared broker lifecycle and assert canonicalization/enrichment topology.
4. Register a `canonicalization` consumer through the shared runtime message processor.
5. Validate incoming envelopes and canonicalization payloads before delegated work.
6. Claim the durable idempotency interface before delegating to the injected handler.
7. Expose canonical state, database transaction, and broker outbox tools to the handler.
8. Drain in-flight handlers before broker shutdown.

The handler remains value-free: it never stores article bodies, AI output, credentials, or production secret values. It does not fetch article pages, call AI providers, approve content, translate content, persist backend article rows, or publish user-facing articles.

## Canonical Identity Flow

Identity resolution uses safe canonicalization payload fields only.

1. Normalize `canonicalUrl` first. The normalized URL is the primary identity seed and is stable across feed retries and cross-feed duplicates.
2. Remove only approved tracking parameters such as `utm_*`, `gclid`, `fbclid`, `ref`, and similar campaign identifiers.
3. Preserve identity-significant query parameters, sort them deterministically, strip fragments, strip default ports, and normalize scheme and host casing.
4. Use `(feedId, sourceItemId)` as a source GUID alias, not as the primary canonical article ID when a normalized URL exists.
5. Use feed title and publication time in the material fingerprint so meaningful feed metadata changes create a new article version without losing the canonical article history.
6. Record conflicts between a known source GUID alias and a different known normalized URL alias as `ambiguous`.

The state model records candidate decisions and aliases so replayed candidate IDs return `duplicate` without scheduling additional enrichment work. New and changed decisions record one pending enrichment outbox item inside the same transaction callback as the identity decision.

Local transaction doubles serialize transaction callbacks and stage state/outbox commit operations until the callback succeeds. This keeps race and crash tests aligned with the intended production database invariant: canonical identity state and pending enrichment outbox intent commit together or not at all.

## Contracts Gap

The current contracts package exposes `canonicalArticleCandidate` on the `canonicalization` stage and `enrichmentResult` on the `enrichment` stage. It does not expose a canonicalizer-to-enrichment request payload schema.

Until `ramideltoro/nutsnews-worker-contracts#16` adds that request contract, this repository records a typed pending enrichment request but does not publish a broker message that downstream enrichment consumers would reject as a stage or payload mismatch.

## Dependency Interfaces

The repository defines narrow interfaces for:

- broker transport;
- canonical state/idempotency;
- database transaction runner;
- broker outbox;
- canonicalizer work handler.

Local doubles back tests and health probes without production dependencies. Backend-owned deployment configuration supplies real database and RabbitMQ values later.

## Safety Bounds

`NUTSNEWS_CANONICALIZER_CONCURRENCY` caps concurrent canonicalization handlers. `NUTSNEWS_CANONICALIZER_PREFETCH` must be greater than or equal to concurrency.

`NUTSNEWS_CANONICALIZER_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.
