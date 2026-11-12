import { describe, expect, it } from "vitest";
import { archiveStaleDashboardEntries } from "./store-maintenance.js";

const now = Date.now();
const old = now - 2 * 60 * 60_000;
const entry = { updatedAt: old } as never;

describe("archive orphaned subagent sessions", () => {
  it("is unchanged when the gate is off", () => {
    const store = { "agent:main:subagent:orphan": entry };
    expect(
      archiveStaleDashboardEntries(store, 3_600_000, {
        archiveSubagentAfterMs: null,
        nowMs: now,
        log: false,
      }),
    ).toBe(0);
    expect(store["agent:main:subagent:orphan"]).toMatchObject({ updatedAt: old });
  });

  it("archives a stale subagent at the configured boundary", () => {
    const store = { "agent:main:subagent:orphan": entry };
    expect(
      archiveStaleDashboardEntries(store, 3_600_000, {
        archiveSubagentAfterMs: 3_600_000,
        nowMs: now,
        log: false,
      }),
    ).toBe(1);
    expect(store["agent:main:subagent:orphan"]).toMatchObject({
      archivedAt: now,
      archiveReason: "age-retention",
    });
  });

  it("keeps live work protected", () => {
    const store = { "agent:main:subagent:live": entry };
    expect(
      archiveStaleDashboardEntries(store, 3_600_000, {
        archiveSubagentAfterMs: 3_600_000,
        nowMs: now,
        log: false,
        preserveKeys: new Set(["agent:main:subagent:live"]),
      }),
    ).toBe(0);
  });
});
