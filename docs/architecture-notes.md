# Architecture Notes

## Scope

The article canonicalizer owns the worker-uplift service boundary that consumes `canonicalArticleCandidate` messages on the contracted `canonicalization` route. Issue #98 creates the deployable shell, dependency seams, health endpoints, and CI/container baseline. Later issues add canonical URL normalization, canonical identity assignment, and dedupe business logic.

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

The bootstrap handler is intentionally local and value-free. It does not implement identity normalization, dedupe decisions, page enrichment, AI, approval, translation, persistence, or publication logic.

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
