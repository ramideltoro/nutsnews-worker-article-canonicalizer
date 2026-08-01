# Architecture Notes

## Scope

The article canonicalizer owns the worker-uplift service boundary that consumes `canonicalArticleCandidate` messages on the contracted `canonicalization` route. It normalizes article identity inputs, resolves canonical article IDs, records duplicate and alias decisions, versions material changes, stores pending enrichment outbox intent, and publishes contracted `enrichmentRequest` messages.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.4.0`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.5.0`
- Input route boundary: `getWorkerRoute("canonicalization")`
- Downstream publish route boundary: `getWorkerRoute("enrichment")`
- Health: separate liveness, startup, and readiness probes; container health uses liveness, while readiness reflects configured-role usability through the active `canonicalization` main-queue consumer and required dependency adapters
- Metrics: bounded Prometheus text from the shared runtime sink plus immutable build/deployment/adapter identity, canonical stage lifecycle counters, fixed-bucket seconds latency, distinct probe state, and a shadow ownership gauge
- Startup: bind HTTP first, expose unhealthy startup/readiness while dependencies initialize, and fail within the configured startup timeout
- Shutdown: stop accepting deliveries, wait for in-flight handlers, cancel consumers, close broker lifecycle; telemetry emission and raw-log flushing remain non-authoritative

## Shell Flow

1. Validate value-free configuration and secret presence by variable name.
2. Assert exact contracts/runtime package versions.
3. Bind the HTTP observability surface before dependency initialization.
4. Start the shared broker lifecycle within the configured startup deadline and assert canonicalization/enrichment topology.
5. Register a `canonicalization` consumer through the shared runtime message processor only when the full production adapter bundle is available; the current shadow backend composition deliberately stops before this step.
6. Validate incoming envelopes and canonicalization payloads before delegated work.
7. Claim the durable idempotency interface before delegating to the injected handler.
8. Expose canonical state, database transaction, and broker outbox tools to the handler.
9. Drain in-flight handlers before broker shutdown.

Every delivery produces one `runtime.message.started` event and exactly one completing event (`accepted`, `duplicate`, `invalid`, `retry`, or `dlq`). The service-local runtime 0.5 adapter converts idempotency claim and state-record failures into one retry or exhausted-attempt DLQ outcome. Composite telemetry destinations are isolated from one another, and both the processor and real canonicalization handler treat telemetry as best effort so post-side-effect emission failures cannot change delivery disposition. The metrics wrapper derives `nutsnews_worker_uplift_stage_events_total` and `nutsnews_worker_uplift_stage_latency_seconds` from only those completing events. Its fixed bucket boundaries are `0.01`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `10`, `30`, `60`, `120`, and `300` seconds plus `+Inf`. The only labels on those series are bounded environment, service, outcome, and histogram boundary values; delivery identifiers remain structured metadata.

`nutsnews_worker_expected_active` is fixed to `0` while this service remains shadow-only and is used only to gate production paging and cutover ownership. Readiness is independent of that signal and reports whether the configured shadow role, consumer, and required dependencies are usable. `nutsnews_worker_build_info` and `nutsnews_worker_deployment_info` expose the immutable revision, shadow deployment, and bounded adapter mode; scrape freshness comes from Prometheus scrape timestamps. Distinct `nutsnews_worker_health_probe` series begin with explicit liveness/startup/readiness states, retain operational probe results, and immediately mark readiness unhealthy on consumer cancellation. Unmeasured dependency observations are excluded from the runtime 0.5 metric adapter rather than recorded as zero latency.

Dependency bundles declare an explicit `adapterMode`. The current backend production process reports `mixed`: its RabbitMQ transport is real, while canonical state, transactions, and outbox are local test implementations. It may connect and assert broker topology so liveness and metrics remain inspectable, but it never registers the canonicalization consumer and can never report ready. This prevents real RabbitMQ deliveries from being acknowledged against ephemeral state.

The handler remains value-free: it never stores article bodies, AI output, credentials, or production secret values. It does not fetch article pages, call AI providers, approve content, translate content, persist backend article rows, or publish user-facing articles.

## Canonical Identity Flow

Identity resolution uses safe canonicalization payload fields only.

1. Normalize `canonicalUrl` first. The normalized URL is the primary identity seed and is stable across feed retries and cross-feed duplicates.
2. Remove only approved tracking parameters such as `utm_*`, `gclid`, `fbclid`, `ref`, and similar campaign identifiers.
3. Preserve identity-significant query parameters, sort them deterministically, strip fragments, strip default ports, and normalize scheme and host casing.
4. Use `(feedId, sourceItemId)` as a source GUID alias, not as the primary canonical article ID when a normalized URL exists.
5. Use feed title and publication time in the material fingerprint so meaningful feed metadata changes create a new article version without losing the canonical article history.
6. Record conflicts between a known source GUID alias and a different known normalized URL alias as `ambiguous`.

The state model records candidate decisions and aliases so replayed candidate IDs return `duplicate` without scheduling additional enrichment work. New and changed decisions record one pending enrichment outbox item inside the same transaction callback as the identity decision, then publish one broker-confirmed `enrichmentRequest` command after the transaction commits.

Local transaction doubles serialize transaction callbacks and stage state/outbox commit operations until the callback succeeds. This keeps race and crash tests aligned with the intended production database invariant: canonical identity state and pending enrichment outbox intent commit together or not at all.

## Enrichment Request Contract

The contracts package exposes `enrichmentRequest` as the canonicalizer-to-enrichment payload schema on the `enrichment` stage. The payload carries canonical article identity, version, candidate ID, normalized canonical URL, request reason, and a durable backend record reference. It does not carry article page bodies.

## Dependency Interfaces

The repository defines narrow interfaces for:

- broker transport;
- canonical state/idempotency;
- database transaction runner;
- broker outbox;
- canonicalizer work handler.

Local doubles back tests and health probes without production dependencies. Backend-owned deployment configuration supplies real database and RabbitMQ values later.

## Safety Bounds

`NUTSNEWS_CANONICALIZER_CONCURRENCY` caps concurrent canonicalization handlers. `NUTSNEWS_CANONICALIZER_PREFETCH` must be greater than or equal to concurrency. `NUTSNEWS_CANONICALIZER_STARTUP_TIMEOUT_MS` bounds dependency startup while the HTTP probes report startup/readiness as unhealthy.

`NUTSNEWS_CANONICALIZER_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.
