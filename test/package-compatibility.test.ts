import { readFileSync } from "node:fs";

import { getRuntimePackageMetadata } from "@ramideltoro/nutsnews-worker-runtime";
import { describe, expect, it } from "vitest";

import { SUPPORTED_RUNTIME_PACKAGE_VERSION } from "../src/index.js";

describe("package compatibility", () => {
  it("accepts the installed worker runtime release", () => {
    expect(getRuntimePackageMetadata().packageVersion).toBe(SUPPORTED_RUNTIME_PACKAGE_VERSION);
    expect(SUPPORTED_RUNTIME_PACKAGE_VERSION).toBe("0.5.0");
  });

  it("resolves one contracts 0.4.0 copy for the service and runtime", () => {
    const packageJson = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as {
      readonly overrides?: Readonly<Record<string, Readonly<Record<string, string>>>>;
    };
    const packageLock = JSON.parse(readFileSync(new URL("../package-lock.json", import.meta.url), "utf8")) as {
      readonly packages: Readonly<Record<string, {
        readonly version?: string;
      }>>;
    };

    expect(packageJson.overrides?.["@ramideltoro/nutsnews-worker-runtime"]?.["@ramideltoro/nutsnews-worker-contracts"]).toBe("0.4.0");
    expect(packageLock.packages["node_modules/@ramideltoro/nutsnews-worker-contracts"]?.version).toBe("0.4.0");
    expect(packageLock.packages["node_modules/@ramideltoro/nutsnews-worker-runtime/node_modules/@ramideltoro/nutsnews-worker-contracts"]).toBeUndefined();
  });

  it("embeds the immutable image revision and uses liveness for shadow container health", () => {
    const dockerfile = readFileSync(new URL("../Dockerfile", import.meta.url), "utf8");
    const publishWorkflow = readFileSync(new URL("../.github/workflows/publish-container.yml", import.meta.url), "utf8");

    expect(dockerfile).toContain("ARG NUTSNEWS_BUILD_REVISION=unknown");
    expect(dockerfile).toContain("NUTSNEWS_CANONICALIZER_BUILD_REVISION=${NUTSNEWS_BUILD_REVISION}");
    expect(dockerfile).toContain("http://127.0.0.1:8080/live");
    expect(dockerfile).not.toContain("http://127.0.0.1:8080/ready");
    expect(publishWorkflow).toContain("NUTSNEWS_BUILD_REVISION=${{ github.sha }}");
  });
});
