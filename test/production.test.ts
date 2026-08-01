import type { RuntimeIdempotencyClaimContext } from "@ramideltoro/nutsnews-worker-runtime";
import type { Pool } from "pg";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { PostgresCanonicalStateStore } from "../src/production.js";
import { createMinimalCanonicalizationEnvelope } from "../src/test-doubles.js";

function claimContext(): RuntimeIdempotencyClaimContext {
  return {
    envelope: createMinimalCanonicalizationEnvelope(),
    stage: "canonicalization",
    receivedAt: "2026-08-01T20:00:00.000Z"
  };
}

describe("PostgresCanonicalStateStore", () => {
  it("claims a new delivery inside one committed transaction", async () => {
    const statements: string[] = [];
    const client = {
      query: vi.fn((sql: string) => {
        statements.push(sql.trim().split(/\s+/u)[0] ?? "");
        return Promise.resolve(sql.includes("INSERT INTO")
          ? { rowCount: 1, rows: [{ received_at: new Date("2026-08-01T20:00:00.000Z") }] }
          : { rowCount: null, rows: [] });
      }),
      release: vi.fn()
    };
    const pool = {
      connect: vi.fn(() => Promise.resolve(client))
    } as unknown as Pool;

    const result = await new PostgresCanonicalStateStore(pool).claim("canonical:new", claimContext());

    expect(result).toMatchObject({
      status: "claimed",
      firstSeenAt: "2026-08-01T20:00:00.000Z",
      replay: false
    });
    expect(statements).toEqual(["BEGIN", "INSERT", "COMMIT"]);
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("locks and renews an expired delivery claim before committing", async () => {
    const statements: string[] = [];
    const client = {
      query: vi.fn((sql: string) => {
        statements.push(sql.replace(/\s+/gu, " ").trim());
        if (sql.includes("INSERT INTO")) {
          return Promise.resolve({ rowCount: 0, rows: [] });
        }
        if (sql.includes("SELECT status")) {
          return Promise.resolve({
            rowCount: 1,
            rows: [{
              status: "failed",
              received_at: new Date("2026-08-01T19:00:00.000Z"),
              processed_at: null,
              lease_active: false
            }]
          });
        }
        if (sql.includes("UPDATE worker_uplift_canonicalizer.inbox")) {
          return Promise.resolve({ rowCount: 1, rows: [] });
        }
        return Promise.resolve({ rowCount: null, rows: [] });
      }),
      release: vi.fn()
    };
    const pool = {
      connect: vi.fn(() => Promise.resolve(client))
    } as unknown as Pool;

    const result = await new PostgresCanonicalStateStore(pool).claim("canonical:replay", claimContext());

    expect(result).toMatchObject({
      status: "claimed",
      firstSeenAt: "2026-08-01T19:00:00.000Z",
      replay: true
    });
    expect(statements[0]).toBe("BEGIN");
    expect(statements.some((statement) => statement.includes("FOR UPDATE"))).toBe(true);
    expect(statements.at(-1)).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledOnce();
  });

  it("rolls back and releases the connection when a claim query fails", async () => {
    const statements: string[] = [];
    const client = {
      query: vi.fn((sql: string) => {
        statements.push(sql.trim());
        if (sql.includes("INSERT INTO")) {
          return Promise.reject(new Error("database unavailable"));
        }
        return Promise.resolve({ rowCount: null, rows: [] });
      }),
      release: vi.fn()
    };
    const pool = {
      connect: vi.fn(() => Promise.resolve(client))
    } as unknown as Pool;

    await expect(new PostgresCanonicalStateStore(pool).claim("canonical:error", claimContext()))
      .rejects.toThrow("database unavailable");

    expect(statements.at(-1)).toBe("ROLLBACK");
    expect(client.release).toHaveBeenCalledOnce();
  });
});
