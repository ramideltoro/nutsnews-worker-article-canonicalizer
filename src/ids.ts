import crypto from "node:crypto";

export function stableArticleId(parts: readonly string[]): string {
  return `article_${sha256Hex(parts.join("\u001f")).slice(0, 32)}`;
}

export function stableEnrichmentRequestId(parts: readonly string[]): string {
  return `enrichment_${sha256Hex(parts.join("\u001f")).slice(0, 32)}`;
}

export function sha256Hex(value: string): string {
  return crypto.createHash("sha256").update(value).digest("hex");
}
