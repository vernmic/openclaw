import { afterEach, expect, it, onTestFinished, vi } from "vitest";
import {
  applySessionEntryLifecycleMutation,
  loadSessionEntry,
  patchSessionEntryCore,
} from "./session-accessor.js";
import { registerSessionMaintenancePreserveKeysProvider } from "./store-maintenance-preserve.js";
import { useTempSessionsFixture } from "./test-helpers.js";
import type { SessionEntry } from "./types.js";

const HOUR = 60 * 60 * 1_000;
const fixture = useTempSessionsFixture("openclaw-subagent-archive-");
afterEach(() => vi.restoreAllMocks());

it("archives restart-orphaned children through native maintenance without retiring protected work", async () => {
  const storePath = fixture.storePath();
  const now = Date.now();
  const scope = (key: string) => ({ sessionKey: key, storePath });
  const child = (suffix: string) => `agent:main:subagent:${suffix}`;
  const stale = (extra: Partial<SessionEntry> = {}): SessionEntry => ({
    sessionId: "overridden-at-seed",
    updatedAt: now - 2 * HOUR,
    ...extra,
  });
  const entries: Record<string, SessionEntry> = {
    [child("session-mode-orphan")]: stale({ spawnedBy: "agent:main:main" }),
    [child("older-than-prune")]: stale({ updatedAt: now - 31 * 24 * HOUR }),
    [child("pinned")]: stale({ pinnedAt: 1, updatedAt: now - 31 * 24 * HOUR }),
    [child("running")]: stale({ status: "running" }),
    [child("locked")]: stale({ modelSelectionLocked: true }),
    [child("runtime-protected")]: stale(),
    [child("recent")]: stale({ lastActivityAt: now }),
    "agent:main:main": stale(),
  };
  onTestFinished(
    registerSessionMaintenancePreserveKeysProvider(() => [child("runtime-protected")]),
  );
  for (const [sessionKey, entry] of Object.entries(entries)) {
    entry.sessionId = sessionKey.split(":").at(-1)!;
    await patchSessionEntryCore(scope(sessionKey), () => entry, {
      fallbackEntry: entry,
      replaceEntry: true,
      skipMaintenance: true,
    });
  }
  const result = await applySessionEntryLifecycleMutation({
    storePath,
    maintenanceOverride: {
      archiveDashboardAfterMs: null,
      archiveSubagentAfterMs: HOUR,
      maxEntries: 100,
      maxDiskBytes: null,
      mode: "enforce",
      pruneAfterMs: 30 * 24 * HOUR,
    },
  });
  expect(result).toMatchObject({ archived: 2, pruned: 0, capped: 0 });
  for (const key of [child("session-mode-orphan"), child("older-than-prune")]) {
    expect(loadSessionEntry(scope(key))).toMatchObject({
      sessionId: entries[key]!.sessionId,
      archivedAt: expect.any(Number),
      archiveReason: "age-retention",
    });
  }
  for (const key of [
    child("pinned"),
    child("running"),
    child("locked"),
    child("runtime-protected"),
    child("recent"),
    "agent:main:main",
  ]) {
    expect(loadSessionEntry(scope(key))).toMatchObject(entries[key]!);
    expect(loadSessionEntry(scope(key))?.archivedAt).toBeUndefined();
  }
});

it.each(["warn", "disabled"] as const)(
  "leaves a young stale child unchanged when %s",
  async (mode) => {
    const target = { storePath: fixture.storePath(), sessionKey: "agent:main:subagent:held" };
    const entry = { sessionId: "held", updatedAt: Date.now() - 2 * HOUR };
    await patchSessionEntryCore(target, () => entry, {
      fallbackEntry: entry,
      replaceEntry: true,
      skipMaintenance: true,
    });
    await applySessionEntryLifecycleMutation({
      storePath: target.storePath,
      maintenanceOverride: {
        archiveSubagentAfterMs: mode === "disabled" ? null : HOUR,
        mode: mode === "warn" ? "warn" : "enforce",
        maxEntries: 100,
        maxDiskBytes: null,
      },
    });
    expect(loadSessionEntry(target)).toMatchObject(entry);
    expect(loadSessionEntry(target)?.archivedAt).toBeUndefined();
  },
);
