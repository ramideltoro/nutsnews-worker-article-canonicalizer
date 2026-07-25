export interface NormalizedArticleUrl {
  readonly url: string;
  readonly removedParameters: readonly string[];
}

export type ArticleUrlNormalizationResult =
  | {
      readonly ok: true;
      readonly value: NormalizedArticleUrl;
    }
  | {
      readonly ok: false;
      readonly reason: "invalid-url" | "unsupported-scheme" | "credentialed-url" | "missing-host";
    };

const TRACKING_PARAMETER_NAMES = new Set([
  "cmp",
  "cmpid",
  "dclid",
  "fbclid",
  "gclid",
  "igshid",
  "mc_cid",
  "mc_eid",
  "msclkid",
  "oly_anon_id",
  "oly_enc_id",
  "ref",
  "ref_src",
  "spm",
  "twclid",
  "vero_id"
]);

export function normalizeArticleUrl(value: string): ArticleUrlNormalizationResult {
  let url: URL;

  try {
    url = new URL(value);
  } catch {
    return {
      ok: false,
      reason: "invalid-url"
    };
  }

  if (url.protocol !== "https:" && url.protocol !== "http:") {
    return {
      ok: false,
      reason: "unsupported-scheme"
    };
  }

  if (url.username.length > 0 || url.password.length > 0) {
    return {
      ok: false,
      reason: "credentialed-url"
    };
  }

  if (url.hostname.length === 0) {
    return {
      ok: false,
      reason: "missing-host"
    };
  }

  url.hash = "";
  url.protocol = url.protocol.toLowerCase();
  url.hostname = url.hostname.toLowerCase();

  if ((url.protocol === "https:" && url.port === "443") || (url.protocol === "http:" && url.port === "80")) {
    url.port = "";
  }

  url.pathname = normalizePathname(url.pathname);

  const retainedEntries: [string, string][] = [];
  const removedParameters: string[] = [];

  for (const [name, parameterValue] of url.searchParams.entries()) {
    if (isApprovedTrackingParameter(name)) {
      removedParameters.push(name);
      continue;
    }

    retainedEntries.push([
      name,
      parameterValue
    ]);
  }

  retainedEntries.sort(([leftName, leftValue], [rightName, rightValue]) => {
    const nameOrder = leftName.localeCompare(rightName);

    return nameOrder === 0 ? leftValue.localeCompare(rightValue) : nameOrder;
  });

  url.search = "";

  for (const [name, parameterValue] of retainedEntries) {
    url.searchParams.append(name, parameterValue);
  }

  return {
    ok: true,
    value: {
      url: url.toString(),
      removedParameters
    }
  };
}

function normalizePathname(pathname: string): string {
  let normalized = pathname.length === 0 ? "/" : pathname;

  while (normalized.length > 1 && normalized.endsWith("/")) {
    normalized = normalized.slice(0, -1);
  }

  return normalized;
}

function isApprovedTrackingParameter(name: string): boolean {
  const normalized = name.toLowerCase();

  return normalized.startsWith("utm_") || TRACKING_PARAMETER_NAMES.has(normalized);
}
