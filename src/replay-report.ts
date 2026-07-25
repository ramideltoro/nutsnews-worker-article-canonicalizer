import type {
  CanonicalDecisionKind,
  CanonicalInvalidDecision,
  CanonicalResolutionDecision
} from "./dependencies.js";

export type CanonicalDecision = CanonicalResolutionDecision | CanonicalInvalidDecision;

export interface CanonicalReplayLegacySummary {
  readonly totalCandidates: number;
  readonly duplicateCount: number;
  readonly changeCount: number;
}

export interface CanonicalReplayRateComparison {
  readonly currentRate: number;
  readonly legacyRate: number;
  readonly delta: number;
}

export interface CanonicalAmbiguityReviewItem {
  readonly candidateId: string;
  readonly feedId: string;
  readonly sourceItemId: string;
  readonly canonicalArticleId: string;
  readonly normalizedUrl: string;
  readonly reasons: readonly string[];
}

export interface CanonicalReplayReport {
  readonly corpusName: string;
  readonly totalCandidates: number;
  readonly decisionCounts: Readonly<Record<CanonicalDecisionKind, number>>;
  readonly duplicateRate: number;
  readonly changeRate: number;
  readonly ambiguityReview: readonly CanonicalAmbiguityReviewItem[];
  readonly legacyComparison?: {
    readonly duplicateRate: CanonicalReplayRateComparison;
    readonly changeRate: CanonicalReplayRateComparison;
  };
}

export interface BuildCanonicalReplayReportOptions {
  readonly corpusName: string;
  readonly decisions: readonly CanonicalDecision[];
  readonly legacy?: CanonicalReplayLegacySummary;
}

export function buildCanonicalReplayReport(options: BuildCanonicalReplayReportOptions): CanonicalReplayReport {
  const totalCandidates = options.decisions.length;
  const decisionCounts: Record<CanonicalDecisionKind, number> = {
    new: 0,
    duplicate: 0,
    alias: 0,
    changed: 0,
    ambiguous: 0,
    invalid: 0
  };

  for (const decision of options.decisions) {
    decisionCounts[decision.decision] += 1;
  }

  const duplicateRate = rate(decisionCounts.duplicate, totalCandidates);
  const changeRate = rate(decisionCounts.changed, totalCandidates);
  const legacyComparison = options.legacy === undefined
    ? undefined
    : {
        duplicateRate: {
          currentRate: duplicateRate,
          legacyRate: rate(options.legacy.duplicateCount, options.legacy.totalCandidates),
          delta: duplicateRate - rate(options.legacy.duplicateCount, options.legacy.totalCandidates)
        },
        changeRate: {
          currentRate: changeRate,
          legacyRate: rate(options.legacy.changeCount, options.legacy.totalCandidates),
          delta: changeRate - rate(options.legacy.changeCount, options.legacy.totalCandidates)
        }
      };

  return {
    corpusName: options.corpusName,
    totalCandidates,
    decisionCounts,
    duplicateRate,
    changeRate,
    ambiguityReview: options.decisions.flatMap((decision) =>
      decision.decision === "ambiguous"
        ? [
            {
              candidateId: decision.candidateId,
              feedId: decision.feedId,
              sourceItemId: decision.sourceItemId,
              canonicalArticleId: decision.canonicalArticleId,
              normalizedUrl: decision.normalizedUrl,
              reasons: decision.reasons
            }
          ]
        : []
    ),
    ...(legacyComparison === undefined ? {} : {
      legacyComparison
    })
  };
}

function rate(count: number, total: number): number {
  return total === 0 ? 0 : count / total;
}
