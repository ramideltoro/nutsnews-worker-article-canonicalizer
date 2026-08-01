# nutsnews-worker-article-canonicalizer

Deployable worker-uplift article canonicalizer service shell for NutsNews.

## Responsibility

Own the canonicalizer service boundary that consumes contracted canonicalization candidate messages, reserves state for canonical article identity and dedupe decisions, and publishes only canonicalization-owned downstream events in shadow mode.

The service performs deterministic URL normalization, canonical identity resolution, alias tracking, material-change versioning, pending enrichment outbox recording, and contract-backed canonicalizer-to-enrichment request publication.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-canonicalizer:${GITHUB_SHA}`. This repository is deployable only through backend-owned infrastructure.

The image runs as a non-root user, exposes port `8080`, and uses `/live` for container process health. It serves:

- `GET /live`
- `GET /livez`
- `GET /startup`
- `GET /startupz`
- `GET /ready`
- `GET /readyz`
- `GET /metrics`
- `GET /config-schema`

## Runtime Dependencies

The service consumes exact immutable worker-uplift package versions:

- `@ramideltoro/nutsnews-worker-contracts@1.0.0`
- `@ramideltoro/nutsnews-worker-runtime@1.0.0`

Local and CI installs use the owner-scoped GitHub Packages npm registry. No package token value is committed.

The HTTP server binds before dependency startup begins, so liveness, startup, readiness, and metrics remain observable while broker and state adapters initialize. Dependency startup is bounded by `NUTSNEWS_CANONICALIZER_STARTUP_TIMEOUT_MS`; a timeout or dependency failure performs bounded graceful cleanup and returns the original startup failure instead of hanging. `/ready` and `/readyz` report whether the configured shadow role is usable: broker lifecycle, the `canonicalization` main-queue consumer, state, transaction, outbox, and configured adapter checks must pass. Production ownership (`expected_active`) does not gate readiness. `NUTSNEWS_ENVIRONMENT=production` requires production dependency mode, and a second runtime guard refuses to register a consumer if the environment, dependency mode, and adapter mode disagree. The current backend production composition is truthfully identified as `mixed` because RabbitMQ is real while state, transaction, and outbox adapters remain local. It cannot consume or acknowledge a delivery. Consumer cancellation and channel-drop recovery emit bounded structured runtime events and Prometheus consumer-state metrics, immediately invalidate the readiness gauge, and trigger a real bounded readiness reevaluation so Runtime's per-check gauge cannot remain stale.

Each delivery emits exactly one completing lifecycle event: `accepted`, `duplicate`, `invalid`, `retry`, or `dlq`. Runtime 1.0 converts state-store failures into one bounded retry/DLQ completion rather than leaving an unmatched `started` event. Successful claims return an opaque ownership token; completion, failure, and conditional release are compare-and-set transitions, and completed work cannot be downgraded. The service-local conformance store models abandoned in-progress claims with a bounded five-minute lease and atomic reclaim. It uses an injected process clock for deterministic tests and cannot qualify as a production adapter. A future production state adapter must derive lease time from the authoritative database or state server, cap the lease at 300 seconds, and make long-running work finish, renew ownership, or fail closed before another owner can reclaim it; it must pass the same conformance suite before the service may declare `adapterMode: "production"`. `/metrics` converts completion events into the canonical low-cardinality `nutsnews_worker_uplift_stage_events_total{environment,service,outcome}` counter and `nutsnews_worker_uplift_stage_latency_seconds` histogram with fixed buckets from 5 milliseconds through 300 seconds plus `+Inf`. All canonicalizer metrics use the bounded `service="canonicalizer"` identity. All six bounded outcome series—`success`, `duplicate`, `invalid`, `retry`, `dlq`, and `failure`—are seeded at zero from the first scrape; canonicalizer terminal failures continue to use their more specific retry or DLQ disposition. Message, candidate, article, feed, correlation, trace, and idempotency identifiers remain structured log metadata and are never metric labels. Liveness, startup, and readiness gauges are initialized to explicit states before the first scrape; startup follows the service lifecycle and readiness is refreshed by the operational `/ready` probe. Runtime owns the bounded per-check `nutsnews_worker_health_check` gauge and `nutsnews_worker_health_check_duration_seconds` histogram; the wrapper keeps one non-duplicated probe family so synchronous startup, cancellation, channel loss, and shutdown transitions remain visible.

The canonicalizer remains shadow-only, so Runtime exports `nutsnews_worker_expected_active{service="canonicalizer"} 0` as the sole owner of that family. That signal gates paging and cutover ownership, not health. Runtime also updates `nutsnews_worker_last_success_timestamp_seconds` monotonically for accepted and duplicate deliveries. Bounded `nutsnews_worker_build_info` and `nutsnews_worker_deployment_info` series contain the immutable image revision, `shadow` deployment identity, and truthful adapter identity, and structured logs carry the same revision/deployment/adapter context. Scrape freshness remains authoritative through Prometheus `up` sample timestamps rather than a process-generated current-time gauge. Production paging must remain gated on the protected ownership signal until cutover. Runtime 1.0 fixed-bucket seconds histograms are the canonical latency signals; legacy millisecond summaries are no longer emitted. Unmeasured dependency events do not create fabricated zero-duration samples.

## Configuration

The value-free configuration schema lives in `src/config.ts` and is exposed at `/config-schema`. Production deployments must provide dependency values through backend-owned deployment configuration, not this repository.

Important variables:

- `NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE`: `test` or `production`
- `NUTSNEWS_ENVIRONMENT`: setting this to `production` requires `NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE=production`
- `NUTSNEWS_CANONICALIZER_BUILD_REVISION`: bounded local/test revision; production dependencies require an exact lowercase 40-character Git commit SHA
- `NUTSNEWS_CANONICALIZER_DATABASE_URL`
- `NUTSNEWS_CANONICALIZER_RABBITMQ_URL`
- `NUTSNEWS_CANONICALIZER_CONCURRENCY`
- `NUTSNEWS_CANONICALIZER_PREFETCH`
- `NUTSNEWS_CANONICALIZER_STARTUP_TIMEOUT_MS`
- `NUTSNEWS_CANONICALIZER_SHUTDOWN_TIMEOUT_MS`
- `NUTSNEWS_CANONICALIZER_SHADOW_MODE`

`NUTSNEWS_CANONICALIZER_SHADOW_MODE` must remain `true` until backend-owned cutover work explicitly changes the deployment contract. Backend deployment must pass the image commit as `NUTSNEWS_BUILD_REVISION` at image build time; the Dockerfile exposes it to the service as `NUTSNEWS_CANONICALIZER_BUILD_REVISION`.

## Service Boundary

The service registers the contracted `canonicalization` consumer route and downstream `enrichment` publish route through the shared runtime broker lifecycle. The shared message processor validates worker envelopes and canonicalization payloads, applies the durable idempotency interface, delegates work to the canonicalizer handler, emits one completing lifecycle outcome, and drains in-flight deliveries during shutdown.

The canonicalizer handler:

- normalizes HTTP(S) article URLs by lowercasing scheme and host, stripping fragments and default ports, removing approved tracking parameters, and deterministically sorting retained query parameters;
- preserves identity-significant query parameters rather than collapsing all query strings;
- records `new`, `duplicate`, `alias`, `changed`, `ambiguous`, and `invalid` decisions with safe reasons;
- records candidate-to-article aliases and source GUID aliases in the canonical state model;
- versions material changes using safe feed metadata;
- records pending enrichment requests in the same transaction callback as the identity decision;
- publishes one contracted `enrichmentRequest` payload for each `new` or `changed` decision after the transaction commits.

Duplicate, alias, ambiguous, invalid, and runtime replay decisions do not publish enrichment work. The publish path uses `@ramideltoro/nutsnews-worker-contracts@1.0.0` and records the broker receipt in the local outbox interface.

Concurrency, replay, crash-point, and ambiguity proof notes live in `docs/replay-proof.md`.

The repository includes test interfaces and local doubles for:

- broker transport;
- canonical state/idempotency;
- database transaction runner;
- broker outbox;
- canonicalizer work handler.

The repository does not fetch article pages, call AI providers, translate content, persist production article rows, approve content, or publish user-facing articles.

## Development

```sh
export NODE_AUTH_TOKEN="<GitHub classic PAT with read:packages>"
npm ci
npm run ci
docker build --build-arg NUTSNEWS_BUILD_REVISION="$(git rev-parse HEAD)" --secret id=npm_token,env=NODE_AUTH_TOKEN -t nutsnews-worker-article-canonicalizer:local .
```

`npm run ci` runs linting, strict type checking, unit tests, integration tests, build, CycloneDX SBOM generation, and a production dependency audit.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI when package access is granted to this repository. Workflows use least-privilege permissions, request `packages: read` for package install jobs, and request `packages: write` only for image publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
