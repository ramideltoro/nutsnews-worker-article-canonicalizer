import { randomUUID } from "node:crypto";

import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeIdempotencyClaimContext,
  RuntimeIdempotencyClaimReleaseResult,
  RuntimeIdempotencyClaimResult,
  RuntimeIdempotencyCompletion,
  RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  Pool,
  type PoolClient,
  type QueryResultRow
} from "pg";

import type {
  CanonicalBrokerOutbox,
  CanonicalCandidateInput,
  CanonicalCandidatePayload,
  CanonicalDatabaseTransaction,
  CanonicalDatabaseTransactionRunner,
  CanonicalEnrichmentRequest,
  CanonicalInvalidDecision,
  CanonicalResolutionDecision,
  CanonicalStateStore,
  CanonicalizerDependencies,
  CanonicalizerDependencyProbe
} from "./dependencies.js";
import {
  sha256Hex,
  stableArticleId
} from "./ids.js";

const SCHEMA = "worker_uplift_canonicalizer";
const CANONICALIZER_IDEMPOTENCY_LEASE_MS = 300_000;

interface PgCanonicalTransaction extends CanonicalDatabaseTransaction {
  readonly client: PoolClient;
}

interface InboxRow extends QueryResultRow {
  readonly status: string;
  readonly received_at: Date;
  readonly processed_at: Date | null;
  readonly lease_active: boolean;
}

interface IdentityRow extends QueryResultRow {
  readonly canonical_url_hash: string;
  readonly operation_version: number;
  readonly diagnostic_metadata: unknown;
}

interface AliasRow extends QueryResultRow {
  readonly canonical_url_hash: string;
}

interface StatusRow extends QueryResultRow {
  readonly status: string;
}

export function createProductionCanonicalizerDurableAdapters(options: {
  readonly databaseUrl: string;
  readonly applicationName: string;
  readonly maxConnections: number;
  readonly timeoutMs: number;
}): Pick<CanonicalizerDependencies, "stateStore" | "transactionRunner" | "brokerOutbox"> & { close(): Promise<void> } {
  const pool = new Pool({
    connectionString: options.databaseUrl,
    application_name: options.applicationName,
    max: options.maxConnections,
    connectionTimeoutMillis: options.timeoutMs,
    query_timeout: options.timeoutMs,
    statement_timeout: options.timeoutMs,
    idleTimeoutMillis: 30_000,
    allowExitOnIdle: true
  });
  pool.on("error", () => undefined);
  return {
    stateStore: new PostgresCanonicalStateStore(pool),
    transactionRunner: new PostgresCanonicalTransactionRunner(pool),
    brokerOutbox: new PostgresCanonicalBrokerOutbox(pool),
    close: () => pool.end()
  };
}

export class PostgresCanonicalTransactionRunner implements CanonicalDatabaseTransactionRunner {
  readonly name = "postgres-canonical-transactions";

  constructor(private readonly pool: Pool) {}

  probe(): Promise<CanonicalizerDependencyProbe> {
    return probe(this.pool, "canonical PostgreSQL transactions ready");
  }

  async withTransaction<T>(operation: (transaction: CanonicalDatabaseTransaction) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await operation({
        transactionId: randomUUID(),
        client
      } as PgCanonicalTransaction);
      await client.query("COMMIT");
      return result;
    } catch (error: unknown) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}

export class PostgresCanonicalStateStore implements CanonicalStateStore {
  readonly name = "postgres-canonical-state";
  readonly idempotencyLeaseMs = CANONICALIZER_IDEMPOTENCY_LEASE_MS;

  constructor(private readonly pool: Pool) {}

  async probe(): Promise<CanonicalizerDependencyProbe> {
    try {
      const result = await this.pool.query<{ readonly ready: boolean }>(
        `SELECT has_schema_privilege(current_user, '${SCHEMA}', 'USAGE')
          AND has_table_privilege(current_user, '${SCHEMA}.inbox', 'SELECT,INSERT,UPDATE')
          AND has_table_privilege(current_user, '${SCHEMA}.article_identities', 'SELECT,INSERT,UPDATE')
          AND has_table_privilege(current_user, '${SCHEMA}.article_aliases', 'SELECT,INSERT,UPDATE') AS ready`
      );
      return result.rows[0]?.ready === true
        ? { status: "ok", summary: "canonical PostgreSQL state ready" }
        : { status: "unhealthy", summary: "canonical PostgreSQL state scope incomplete" };
    } catch {
      return { status: "unhealthy", summary: "canonical PostgreSQL state probe failed" };
    }
  }

  async claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    const client = await this.pool.connect();
    const token = randomUUID();
    try {
      await client.query("BEGIN");
      const inserted = await client.query<{ readonly received_at: Date }>(
        `INSERT INTO ${SCHEMA}.inbox (
           message_id, pipeline_run_id, stage_execution_id, source_stage, source_message_id,
           entity_kind, entity_id, schema_version, operation_version, idempotency_key,
           payload_ref, payload_digest, received_at, status, diagnostic_metadata
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::timestamptz,'processing',
           jsonb_build_object(
             'claimToken', $14::text,
             'claimLeaseExpiresAtEpochMs', floor(extract(epoch from clock_timestamp()) * 1000) + $15::bigint
           ))
         ON CONFLICT (idempotency_key) DO NOTHING RETURNING received_at`,
        [
          context.envelope.messageId,
          context.envelope.correlationId,
          context.envelope.messageId,
          context.envelope.producer.name,
          context.envelope.causationId,
          context.envelope.aggregate.type,
          context.envelope.aggregate.id,
          context.envelope.schemaVersion,
          Math.max(1, context.envelope.aggregate.version),
          idempotencyKey,
          context.envelope.payloadRef.uri,
          context.envelope.payloadRef.digest ?? sha256Hex(JSON.stringify(context.envelope.payloadRef)),
          context.receivedAt,
          token,
          this.idempotencyLeaseMs
        ]
      );
      let result: RuntimeIdempotencyClaimResult;
      if ((inserted.rowCount ?? 0) > 0) {
        result = { status: "claimed", firstSeenAt: context.receivedAt, replay: false, claimToken: token };
      } else {
        const existing = await client.query<InboxRow>(
          `SELECT status, received_at, processed_at,
                  status='processing'
                    AND COALESCE((diagnostic_metadata->>'claimLeaseExpiresAtEpochMs')::bigint, 0)
                        > floor(extract(epoch from clock_timestamp()) * 1000) AS lease_active
           FROM ${SCHEMA}.inbox WHERE idempotency_key=$1 FOR UPDATE`,
          [idempotencyKey]
        );
        const row = existing.rows[0];
        if (row === undefined) {
          result = { status: "in-progress", firstSeenAt: context.receivedAt };
        } else if (row.status === "processed") {
          result = {
            status: "already-completed",
            firstSeenAt: row.received_at.toISOString(),
            completedAt: (row.processed_at ?? row.received_at).toISOString()
          };
        } else {
          if (row.lease_active) {
            result = { status: "in-progress", firstSeenAt: row.received_at.toISOString() };
          } else {
            const updated = await client.query(
              `UPDATE ${SCHEMA}.inbox
               SET status='processing', diagnostic_metadata=diagnostic_metadata || jsonb_build_object(
                     'claimToken', $2::text,
                     'claimLeaseExpiresAtEpochMs', floor(extract(epoch from clock_timestamp()) * 1000) + $3::bigint,
                     'replayMessageId', $4::text
                   ),
                   sanitized_error_code=NULL, sanitized_error_message=NULL
               WHERE idempotency_key=$1 AND status <> 'processed'`,
              [idempotencyKey, token, this.idempotencyLeaseMs, context.envelope.messageId]
            );
            result = (updated.rowCount ?? 0) === 1
              ? { status: "claimed", firstSeenAt: row.received_at.toISOString(), replay: true, claimToken: token }
              : { status: "in-progress", firstSeenAt: row.received_at.toISOString() };
          }
        }
      }
      await client.query("COMMIT");
      return result;
    } catch (error: unknown) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    const result = await this.pool.query(
      `UPDATE ${SCHEMA}.inbox SET status='processed', processed_at=$3::timestamptz,
       diagnostic_metadata=diagnostic_metadata || $4::jsonb
       WHERE idempotency_key=$1 AND status='processing'
         AND diagnostic_metadata->>'claimToken'=$2`,
      [idempotencyKey, completion.claimToken, completion.completedAt, JSON.stringify({ completion })]
    );
    requireOwned(result.rowCount, "complete");
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    const result = await this.pool.query(
      `UPDATE ${SCHEMA}.inbox SET status='failed', sanitized_error_code=$3,
       diagnostic_metadata=diagnostic_metadata || $4::jsonb
       WHERE idempotency_key=$1 AND status='processing'
         AND diagnostic_metadata->>'claimToken'=$2`,
      [idempotencyKey, failure.claimToken, safeCode(failure.reason), JSON.stringify({ failure })]
    );
    requireOwned(result.rowCount, "fail");
  }

  async releaseClaim(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<RuntimeIdempotencyClaimReleaseResult> {
    const completed = await this.pool.query<StatusRow>(`SELECT status FROM ${SCHEMA}.inbox WHERE idempotency_key=$1`, [idempotencyKey]);
    if (completed.rows[0]?.status === "processed") {
      return { status: "preserved-completed" };
    }
    const result = await this.pool.query(
      `UPDATE ${SCHEMA}.inbox SET status='failed', sanitized_error_code=$3,
       diagnostic_metadata=diagnostic_metadata || $4::jsonb
       WHERE idempotency_key=$1 AND status='processing'
         AND diagnostic_metadata->>'claimToken'=$2`,
      [idempotencyKey, failure.claimToken, safeCode(failure.reason), JSON.stringify({ failure })]
    );
    return (result.rowCount ?? 0) === 1 ? { status: "released" } : { status: "not-owned" };
  }

  async resolveCandidate(input: CanonicalCandidateInput, transaction: CanonicalDatabaseTransaction): Promise<CanonicalResolutionDecision> {
    const client = transactionClient(transaction);
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [input.identitySeed]);
    const replay = await client.query<IdentityRow>(
      `SELECT canonical_url_hash, operation_version, diagnostic_metadata
       FROM ${SCHEMA}.article_identities
       WHERE diagnostic_metadata->>'candidateId'=$1
       ORDER BY operation_version DESC LIMIT 1`,
      [input.candidateId]
    );
    const replayRow = replay.rows[0];
    if (replayRow !== undefined) {
      const snapshot = decisionSnapshot(replayRow.diagnostic_metadata);
      const pending = await client.query(
        `SELECT 1 FROM ${SCHEMA}.outbox
         WHERE entity_id=$1 AND operation_version=$2 AND status <> 'confirmed' LIMIT 1`,
        [replayRow.canonical_url_hash, replayRow.operation_version]
      );
      return decision(input, transaction, {
        decision: "duplicate",
        canonicalArticleId: replayRow.canonical_url_hash,
        articleVersion: replayRow.operation_version,
        reasons: ["candidate-replay"],
        publishEnrichment: snapshot.publishEnrichment === true && (pending.rowCount ?? 0) > 0
      });
    }
    if (input.dedupeStatus === "duplicate" && input.duplicateOfArticleId !== undefined) {
      return decision(input, transaction, {
        decision: "duplicate",
        canonicalArticleId: input.duplicateOfArticleId,
        articleVersion: 1,
        reasons: ["upstream-duplicate-hint"],
        publishEnrichment: false
      });
    }
    const sourceKey = `${input.feedId}\u001f${input.sourceItemId}`;
    const aliasHash = sha256Hex(input.normalizedUrl);
    const [sourceAlias, urlAlias] = await Promise.all([
      client.query<AliasRow>(
        `SELECT canonical_url_hash FROM ${SCHEMA}.article_aliases
         WHERE diagnostic_metadata->>'sourceKey'=$1 ORDER BY operation_version DESC LIMIT 1`,
        [sourceKey]
      ),
      client.query<AliasRow>(
        `SELECT canonical_url_hash FROM ${SCHEMA}.article_aliases
         WHERE alias_url_hash=$1 ORDER BY operation_version DESC LIMIT 1`,
        [aliasHash]
      )
    ]);
    const sourceId = sourceAlias.rows[0]?.canonical_url_hash;
    const urlId = urlAlias.rows[0]?.canonical_url_hash;
    if (sourceId !== undefined && urlId !== undefined && sourceId !== urlId) {
      return decision(input, transaction, {
        decision: "ambiguous",
        canonicalArticleId: sourceId,
        articleVersion: 1,
        reasons: ["source-guid-url-conflict"],
        publishEnrichment: false
      });
    }
    const existingId = sourceId ?? urlId;
    if (existingId === undefined) {
      const canonicalArticleId = stableArticleId([input.identitySeed]);
      const resolved = decision(input, transaction, {
        decision: "new",
        canonicalArticleId,
        articleVersion: 1,
        reasons: ["normalized-url-first-seen"],
        publishEnrichment: true
      });
      await this.persistDecision(client, input, resolved, aliasHash, sourceKey);
      return resolved;
    }
    const existing = await client.query<IdentityRow>(
      `SELECT canonical_url_hash, operation_version, diagnostic_metadata
       FROM ${SCHEMA}.article_identities
       WHERE canonical_url_hash=$1 ORDER BY operation_version DESC LIMIT 1 FOR UPDATE`,
      [existingId]
    );
    const row = existing.rows[0];
    const existingMetadata = record(row?.diagnostic_metadata);
    const existingVersion = row?.operation_version ?? 1;
    const changed = string(existingMetadata.materialFingerprint) !== input.materialFingerprint;
    const nextVersion = changed ? existingVersion + 1 : existingVersion;
    const resolved = decision(input, transaction, {
      decision: changed ? "changed" : urlId === undefined ? "alias" : "duplicate",
      canonicalArticleId: existingId,
      articleVersion: nextVersion,
      reasons: [changed ? "material-fingerprint-changed" : urlId === undefined ? "new-url-alias" : "material-fingerprint-match"],
      publishEnrichment: changed
    });
    if (changed) {
      await this.persistDecision(client, input, resolved, aliasHash, sourceKey);
    } else {
      await this.persistAlias(client, aliasHash, existingId, existingVersion, input.normalizedUrl, sourceKey);
    }
    return resolved;
  }

  async recordInvalidCandidate(input: CanonicalCandidatePayload, invalid: CanonicalInvalidDecision): Promise<void> {
    await this.pool.query(
      `INSERT INTO ${SCHEMA}.article_aliases (
       alias_url_hash, canonical_url_hash, operation_version, alias_ref, diagnostic_metadata)
       VALUES ($1,$2,1,$3,$4::jsonb) ON CONFLICT DO NOTHING`,
      [sha256Hex(input.originalUrl), sha256Hex(input.canonicalUrl), `invalid://${input.candidateId}`, JSON.stringify({ invalid })]
    );
  }

  private async persistDecision(
    client: PoolClient,
    input: CanonicalCandidateInput,
    resolved: CanonicalResolutionDecision,
    aliasHash: string,
    sourceKey: string
  ): Promise<void> {
    await client.query(
      `INSERT INTO ${SCHEMA}.article_identities (
       original_url_hash, canonical_url_hash, canonical_url, original_url_ref,
       operation_version, identity_status, diagnostic_metadata)
       VALUES ($1,$2,$3,$4,$5,'canonical',$6::jsonb)`,
      [aliasHash, resolved.canonicalArticleId, input.normalizedUrl, input.originalUrl, resolved.articleVersion, JSON.stringify({
        candidateId: input.candidateId,
        materialFingerprint: input.materialFingerprint,
        sourceFeedUrl: input.feedId,
        decisionSnapshot: serializableDecision(resolved)
      })]
    );
    await this.persistAlias(client, aliasHash, resolved.canonicalArticleId, resolved.articleVersion, input.normalizedUrl, sourceKey);
  }

  private async persistAlias(
    client: PoolClient,
    aliasHash: string,
    canonicalArticleId: string,
    version: number,
    normalizedUrl: string,
    sourceKey: string
  ): Promise<void> {
    await client.query(
      `INSERT INTO ${SCHEMA}.article_aliases (
       alias_url_hash, canonical_url_hash, operation_version, alias_ref, diagnostic_metadata)
       VALUES ($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING`,
      [aliasHash, canonicalArticleId, version, normalizedUrl, JSON.stringify({ sourceKey })]
    );
  }
}

export class PostgresCanonicalBrokerOutbox implements CanonicalBrokerOutbox {
  readonly name = "postgres-canonical-broker-outbox";

  constructor(private readonly pool: Pool) {}

  probe(): Promise<CanonicalizerDependencyProbe> {
    return probe(this.pool, "canonical PostgreSQL outbox ready");
  }

  async recordPendingEnrichment(request: CanonicalEnrichmentRequest, transaction: CanonicalDatabaseTransaction): Promise<void> {
    const client = transactionClient(transaction);
    const key = `canonicalizer:enrichment:${request.requestId}`;
    await client.query(
      `INSERT INTO ${SCHEMA}.outbox (
       outbox_message_id,pipeline_run_id,stage_execution_id,destination_stage,routing_key,
       entity_kind,entity_id,schema_version,operation_version,idempotency_key,payload_ref,payload_digest,status,diagnostic_metadata)
       VALUES ($1,$2,$3,'enrichment','nutsnews.worker.enrichment.v1','article',$4,1,$5,$6,$7,$8,'pending',$9::jsonb)
       ON CONFLICT (idempotency_key) DO NOTHING`,
      [request.requestId, request.requestId, request.requestId, request.canonicalArticleId, request.articleVersion, key, request.payloadRef.uri, sha256Hex(JSON.stringify(request)), JSON.stringify({ pendingRequest: request })]
    );
  }

  async record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void> {
    await this.pool.query(
      `UPDATE ${SCHEMA}.outbox SET status='confirmed', outbox_message_id=$2,
       published_at=$3::timestamptz, confirmed_at=$3::timestamptz,
       diagnostic_metadata=diagnostic_metadata || $4::jsonb
       WHERE idempotency_key=$1`,
      [command.envelope.idempotencyKey, receipt.messageId, receipt.confirmedAt, JSON.stringify({ envelope: command.envelope, payload: command.payload })]
    );
  }
}

function decision(
  input: CanonicalCandidateInput,
  transaction: CanonicalDatabaseTransaction,
  values: Pick<CanonicalResolutionDecision, "decision" | "canonicalArticleId" | "articleVersion" | "reasons" | "publishEnrichment">
): CanonicalResolutionDecision {
  return {
    ...values,
    candidateId: input.candidateId,
    feedId: input.feedId,
    sourceItemId: input.sourceItemId,
    normalizedUrl: input.normalizedUrl,
    materialFingerprint: input.materialFingerprint,
    transaction,
    decidedAt: input.decidedAt
  };
}

function serializableDecision(value: CanonicalResolutionDecision): Readonly<Record<string, unknown>> {
  const result: Record<string, unknown> = { ...value };
  delete result.transaction;
  return result;
}

function decisionSnapshot(value: unknown): Readonly<Record<string, unknown>> {
  return record(record(value).decisionSnapshot);
}

function transactionClient(transaction: CanonicalDatabaseTransaction): PoolClient {
  const client = (transaction as Partial<PgCanonicalTransaction>).client;
  if (client === undefined) {
    throw new Error("canonical PostgreSQL operation requires an active transaction");
  }
  return client;
}

async function probe(pool: Pool, summary: string): Promise<CanonicalizerDependencyProbe> {
  try {
    await pool.query("SELECT 1");
    return { status: "ok", summary };
  } catch {
    return { status: "unhealthy", summary: "canonical PostgreSQL probe failed" };
  }
}

function requireOwned(rowCount: number | null, operation: string): void {
  if ((rowCount ?? 0) !== 1) {
    throw new Error(`canonical idempotency ownership lost during ${operation}`);
  }
}

function record(value: unknown): Readonly<Record<string, unknown>> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Readonly<Record<string, unknown>>
    : {};
}

function string(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function safeCode(value: string): string {
  return value.replace(/[^a-zA-Z0-9_.:-]/gu, "_").slice(0, 128);
}
