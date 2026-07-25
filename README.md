# nutsnews-worker-article-canonicalizer

Deployable worker-uplift article canonicalizer service shell for NutsNews.

## Responsibility

Own the canonicalizer service boundary that consumes contracted canonicalization candidate messages, reserves state for canonical article identity and dedupe decisions, and publishes only canonicalization-owned downstream events in shadow mode.

The service now performs deterministic URL normalization, canonical identity resolution, alias tracking, material-change versioning, and pending enrichment outbox recording. Broker publication of the canonicalizer-to-enrichment request is intentionally held until the contracts package adds a dedicated enrichment request payload schema.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-article-canonicalizer:${GITHUB_SHA}`. This repository is deployable only through backend-owned infrastructure.

The image runs as a non-root user, exposes port `8080`, and serves:

- `GET /live`
- `GET /startup`
- `GET /ready`
- `GET /metrics`
- `GET /config-schema`

## Runtime Dependencies

The service consumes exact immutable worker-uplift package versions:

- `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- `@ramideltoro/nutsnews-worker-runtime@0.4.0`

Local and CI installs use the owner-scoped GitHub Packages npm registry. No package token value is committed.

## Configuration

The value-free configuration schema lives in `src/config.ts` and is exposed at `/config-schema`. Production deployments must provide dependency values through backend-owned deployment configuration, not this repository.

Important variables:

- `NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE`: `test` or `production`
- `NUTSNEWS_CANONICALIZER_DATABASE_URL`
- `NUTSNEWS_CANONICALIZER_RABBITMQ_URL`
- `NUTSNEWS_CANONICALIZER_CONCURRENCY`
- `NUTSNEWS_CANONICALIZER_PREFETCH`
- `NUTSNEWS_CANONICALIZER_SHUTDOWN_TIMEOUT_MS`
- `NUTSNEWS_CANONICALIZER_SHADOW_MODE`

`NUTSNEWS_CANONICALIZER_SHADOW_MODE` must remain `true` until backend-owned cutover work explicitly changes the deployment contract.

## Service Boundary

The service registers the contracted `canonicalization` consumer route and downstream `enrichment` publish route through the shared runtime broker lifecycle. The message processor validates worker envelopes and canonicalization payloads, applies the durable idempotency interface, delegates work to the canonicalizer handler, and drains in-flight deliveries during shutdown.

The canonicalizer handler:

- normalizes HTTP(S) article URLs by lowercasing scheme and host, stripping fragments and default ports, removing approved tracking parameters, and deterministically sorting retained query parameters;
- preserves identity-significant query parameters rather than collapsing all query strings;
- records `new`, `duplicate`, `alias`, `changed`, `ambiguous`, and `invalid` decisions with safe reasons;
- records candidate-to-article aliases and source GUID aliases in the canonical state model;
- versions material changes using safe feed metadata;
- records pending enrichment requests in the same transaction callback as the identity decision.

Pending enrichment requests are stored as outbox intent only. The current `@ramideltoro/nutsnews-worker-contracts@0.3.1` package does not expose a broker-valid canonicalizer-to-enrichment request payload; follow-up contracts work is tracked in `ramideltoro/nutsnews-worker-contracts#16`.

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
docker build --secret id=npm_token,env=NODE_AUTH_TOKEN -t nutsnews-worker-article-canonicalizer:local .
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
