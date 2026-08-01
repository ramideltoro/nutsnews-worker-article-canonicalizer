import os from "node:os";

export const CANONICALIZER_SERVICE_NAME = "nutsnews-worker-article-canonicalizer" as const;
export const CANONICALIZER_SERVICE_VERSION = "0.1.0" as const;

export type CanonicalizerDependencyMode = "test" | "production";
export type CanonicalizerDeploymentMode = "shadow" | "production";
export type CanonicalizerTelemetryLogMode = "stdout" | "silent";

export interface CanonicalizerConfigVariable {
  readonly name: string;
  readonly description: string;
  readonly requiredInProduction: boolean;
  readonly sensitive: boolean;
  readonly defaultValue?: string;
}

export const CANONICALIZER_CONFIG_SCHEMA = [
  variable("NUTSNEWS_ENVIRONMENT", "Runtime environment label for logs and metrics.", false, false, "local"),
  variable("NUTSNEWS_CANONICALIZER_BUILD_REVISION", "Immutable source revision; production requires an exact lowercase 40-character Git SHA.", true, false, "unknown"),
  variable("NUTSNEWS_CANONICALIZER_HTTP_HOST", "Health and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_CANONICALIZER_HTTP_PORT", "Health and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_CANONICALIZER_DATABASE_URL", "Backend shadow database connection string for canonical identity state.", true, true),
  variable("NUTSNEWS_CANONICALIZER_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_CANONICALIZER_CONCURRENCY", "Maximum concurrent canonicalization message handlers.", false, false, "8"),
  variable("NUTSNEWS_CANONICALIZER_PREFETCH", "Broker prefetch bound for canonicalization deliveries.", false, false, "16"),
  variable("NUTSNEWS_CANONICALIZER_STARTUP_TIMEOUT_MS", "Dependency startup timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_CANONICALIZER_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_CANONICALIZER_SHADOW_MODE", "Keep canonicalizer output isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_CANONICALIZER_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true")
] as const satisfies readonly CanonicalizerConfigVariable[];

export interface CanonicalizerConfig {
  readonly serviceName: typeof CANONICALIZER_SERVICE_NAME;
  readonly serviceVersion: typeof CANONICALIZER_SERVICE_VERSION;
  readonly environment: string;
  readonly host: string;
  readonly buildRevision: string;
  readonly http: {
    readonly host: string;
    readonly port: number;
  };
  readonly dependencyMode: CanonicalizerDependencyMode;
  readonly deploymentMode: CanonicalizerDeploymentMode;
  readonly expectedActive: boolean;
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
  };
  readonly concurrency: number;
  readonly prefetch: number;
  readonly startupTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
  readonly shadowMode: boolean;
  readonly telemetryLogs: CanonicalizerTelemetryLogMode;
  readonly metricsEnabled: boolean;
}

export class CanonicalizerConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid canonicalizer configuration: ${issues.join("; ")}`);
    this.name = "CanonicalizerConfigError";
    this.issues = issues;
  }
}

export function loadCanonicalizerConfig(env: NodeJS.ProcessEnv = process.env): CanonicalizerConfig {
  const issues: string[] = [];
  const environment = nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local");
  const dependencyMode = parseDependencyMode(env.NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE, issues);
  const buildRevision = parseBuildRevision(env.NUTSNEWS_CANONICALIZER_BUILD_REVISION, issues);
  const dependencies = {
    databaseConfigured: hasValue(env.NUTSNEWS_CANONICALIZER_DATABASE_URL),
    rabbitmqConfigured: hasValue(env.NUTSNEWS_CANONICALIZER_RABBITMQ_URL)
  };

  if (isProductionEnvironment(environment) && dependencyMode !== "production") {
    issues.push("NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE must be production when NUTSNEWS_ENVIRONMENT=production.");
  }

  if (dependencyMode === "production") {
    requireConfigured("NUTSNEWS_CANONICALIZER_DATABASE_URL", dependencies.databaseConfigured, issues);
    requireConfigured("NUTSNEWS_CANONICALIZER_RABBITMQ_URL", dependencies.rabbitmqConfigured, issues);

    if (!/^[0-9a-f]{40}$/u.test(buildRevision)) {
      issues.push("NUTSNEWS_CANONICALIZER_BUILD_REVISION must be an exact lowercase 40-character Git SHA when NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE=production.");
    }
  }

  const concurrency = parseInteger(env.NUTSNEWS_CANONICALIZER_CONCURRENCY, "NUTSNEWS_CANONICALIZER_CONCURRENCY", 8, 1, 128, issues);
  const prefetch = parseInteger(env.NUTSNEWS_CANONICALIZER_PREFETCH, "NUTSNEWS_CANONICALIZER_PREFETCH", 16, 1, 512, issues);
  const shadowMode = parseBoolean(env.NUTSNEWS_CANONICALIZER_SHADOW_MODE, "NUTSNEWS_CANONICALIZER_SHADOW_MODE", true, issues);
  const config: CanonicalizerConfig = {
    serviceName: CANONICALIZER_SERVICE_NAME,
    serviceVersion: CANONICALIZER_SERVICE_VERSION,
    environment,
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    buildRevision,
    http: {
      host: nonEmpty(env.NUTSNEWS_CANONICALIZER_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_CANONICALIZER_HTTP_PORT, "NUTSNEWS_CANONICALIZER_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    deploymentMode: shadowMode ? "shadow" : "production",
    expectedActive: !shadowMode,
    dependencies,
    concurrency,
    prefetch,
    startupTimeoutMs: parseInteger(env.NUTSNEWS_CANONICALIZER_STARTUP_TIMEOUT_MS, "NUTSNEWS_CANONICALIZER_STARTUP_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_CANONICALIZER_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_CANONICALIZER_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shadowMode,
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_CANONICALIZER_METRICS_ENABLED, "NUTSNEWS_CANONICALIZER_METRICS_ENABLED", true, issues)
  };

  if (config.prefetch < config.concurrency) {
    issues.push("NUTSNEWS_CANONICALIZER_PREFETCH must be greater than or equal to NUTSNEWS_CANONICALIZER_CONCURRENCY.");
  }

  if (!config.shadowMode) {
    issues.push("NUTSNEWS_CANONICALIZER_SHADOW_MODE must remain true until backend-owned deployment enables cutover.");
  }

  if (issues.length > 0) {
    throw new CanonicalizerConfigError(issues);
  }

  return config;
}

function variable(
  name: string,
  description: string,
  requiredInProduction: boolean,
  sensitive: boolean,
  defaultValue?: string
): CanonicalizerConfigVariable {
  return {
    name,
    description,
    requiredInProduction,
    sensitive,
    ...(defaultValue === undefined ? {} : {
      defaultValue
    })
  };
}

function nonEmpty(value: string | undefined, fallback: string): string {
  if (value === undefined) {
    return fallback;
  }

  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : fallback;
}

function hasValue(value: string | undefined): boolean {
  return value !== undefined && value.trim().length > 0;
}

function isProductionEnvironment(environment: string): boolean {
  return environment.trim().toLowerCase() === "production";
}

function parseBuildRevision(value: string | undefined, issues: string[]): string {
  const revision = nonEmpty(value, "unknown");

  if (/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(revision)) {
    return revision;
  }

  issues.push("NUTSNEWS_CANONICALIZER_BUILD_REVISION must be 1-128 characters using letters, numbers, dot, underscore, or hyphen.");
  return "unknown";
}

function parseDependencyMode(value: string | undefined, issues: string[]): CanonicalizerDependencyMode {
  const normalized = nonEmpty(value, "test");

  if (normalized === "test" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE must be test or production.");
  return "test";
}

function parseTelemetryLogMode(value: string | undefined, issues: string[]): CanonicalizerTelemetryLogMode {
  const normalized = nonEmpty(value, "stdout");

  if (normalized === "stdout" || normalized === "silent") {
    return normalized;
  }

  issues.push("NUTSNEWS_CANONICALIZER_TELEMETRY_LOGS must be stdout or silent.");
  return "stdout";
}

function parseBoolean(
  value: string | undefined,
  key: string,
  fallback: boolean,
  issues: string[]
): boolean {
  if (!hasValue(value)) {
    return fallback;
  }

  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === "true" || normalized === "1") {
    return true;
  }

  if (normalized === "false" || normalized === "0") {
    return false;
  }

  issues.push(`${key} must be true or false.`);
  return fallback;
}

function parseInteger(
  value: string | undefined,
  key: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[]
): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be an integer between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return parsed;
}

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_CANONICALIZER_DEPENDENCY_MODE=production.`);
  }
}
