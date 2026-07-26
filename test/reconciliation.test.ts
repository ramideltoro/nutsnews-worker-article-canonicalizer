import {
  describe,
  expect,
  it
} from "vitest";

import { createCanonicalizerFailClosedReconciler } from "../src/reconciliation.js";
import { ManualCanonicalizerClock } from "../src/test-doubles.js";

describe("canonicalizer reconciliation", () => {
  it("reports a bounded no-op dry-run when no service-owned replay candidates exist", async () => {
    const reconciler = createCanonicalizerFailClosedReconciler(new ManualCanonicalizerClock());

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "canonicalizer",
      status: "dry_run",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.errors).toEqual([]);
  });
});
