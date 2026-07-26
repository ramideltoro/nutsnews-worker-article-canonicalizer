import {
  describe,
  expect,
  it
} from "vitest";

import { createCanonicalizerFailClosedReconciler } from "../src/reconciliation.js";
import { ManualCanonicalizerClock } from "../src/test-doubles.js";

describe("canonicalizer reconciliation", () => {
  it("fails closed instead of synthesizing enrichment requests from partial metadata", async () => {
    const reconciler = createCanonicalizerFailClosedReconciler(new ManualCanonicalizerClock());

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "canonicalizer",
      status: "failed_closed",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.errors[0]).toContain("refusing to synthesize");
  });
});
