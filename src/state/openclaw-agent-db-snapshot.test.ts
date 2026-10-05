import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import {
  AGENT_DB_SNAPSHOT_RETENTION,
  readCertifiedSnapshotAgeMs,
  snapshotAndCertifyAgentDatabase,
} from "./openclaw-agent-db-snapshot.js";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

function makeDatabase(dir: string, name = "agent.sqlite"): string {
  const databasePath = join(dir, name);
  const db = new DatabaseSync(databasePath);
  db.exec("PRAGMA journal_mode=WAL; CREATE TABLE t(a TEXT); INSERT INTO t VALUES ('seed');");
  db.close();
  return databasePath;
}

function snapshotNames(snapshotDir: string): string[] {
  return readdirSync(snapshotDir).filter((name) => name.endsWith(".sqlite"));
}

describe("agent database snapshot and certification", () => {
  it("certifies a fresh database and records the restore point", () => {
    const dir = tempDirs.make("agent-db-snapshot-");
    const databasePath = makeDatabase(dir);
    const snapshotDir = join(dir, "snapshots");

    const outcome = snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir });

    expect(outcome.kind).toBe("certified");
    expect(snapshotNames(snapshotDir)).toHaveLength(1);
    expect(readCertifiedSnapshotAgeMs(snapshotDir)).toBeGreaterThanOrEqual(0);
  });

  it("skips an unchanged database on later passes", () => {
    // The skip path is the whole cost argument: ~21s per changed database against ~0 for the rest.
    const dir = tempDirs.make("agent-db-snapshot-skip-");
    const databasePath = makeDatabase(dir);
    const snapshotDir = join(dir, "snapshots");

    expect(snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir }).kind).toBe(
      "certified",
    );
    expect(snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir }).kind).toBe(
      "unchanged",
    );
    expect(snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir }).kind).toBe(
      "unchanged",
    );
    expect(snapshotNames(snapshotDir)).toHaveLength(1);
  });

  it("detects a commit still sitting in the WAL", () => {
    // Regression: fingerprinting only the main file read un-checkpointed writes as unchanged, so the
    // retained restore point silently aged past the one-hour promise. The main file's mtime does not
    // move until checkpoint.
    const dir = tempDirs.make("agent-db-snapshot-wal-");
    const databasePath = makeDatabase(dir);
    const snapshotDir = join(dir, "snapshots");
    snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir });

    const writer = new DatabaseSync(databasePath);
    try {
      writer.exec("PRAGMA journal_mode=WAL; INSERT INTO t VALUES ('uncheckpointed');");
      // Writer deliberately still open, so the commit stays in the WAL.
      expect(
        snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir }).kind,
      ).toBe("certified");
    } finally {
      writer.close();
    }
  });

  it("treats an absent WAL and an empty WAL as the same state", () => {
    // A read-only open creates a zero-length WAL that outlives the handle. Without normalisation the
    // snapshot's own source open flips the fingerprint and defeats the skip path every cycle.
    const dir = tempDirs.make("agent-db-snapshot-emptywal-");
    const databasePath = makeDatabase(dir);
    const snapshotDir = join(dir, "snapshots");

    snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir });
    const recorded = JSON.parse(
      readFileSync(join(snapshotDir, ".last-certified.json"), "utf8"),
    ) as Record<string, unknown>;

    expect(recorded.walSize).toBe(0);
    expect(recorded.walMtimeNs).toBeNull();
    expect(snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir }).kind).toBe(
      "unchanged",
    );
  });

  it("re-snapshots when the recorded state predates the WAL fields", () => {
    const dir = tempDirs.make("agent-db-snapshot-upgrade-");
    const databasePath = makeDatabase(dir);
    const snapshotDir = join(dir, "snapshots");
    snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir });

    const statePath = join(snapshotDir, ".last-certified.json");
    const legacy = JSON.parse(readFileSync(statePath, "utf8")) as Record<string, unknown>;
    delete legacy.walSize;
    delete legacy.walMtimeNs;
    writeFileSync(statePath, JSON.stringify(legacy));

    // Must not skip on a fingerprint that never covered the WAL.
    expect(snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir }).kind).toBe(
      "certified",
    );
  });

  it("reports an absent database without opening it", () => {
    // Opening would run ensureOpenClawAgentSchema and CREATE the tables of an agent that never ran.
    const dir = tempDirs.make("agent-db-snapshot-absent-");
    const outcome = snapshotAndCertifyAgentDatabase({
      agentId: "ghost",
      databasePath: join(dir, "missing.sqlite"),
      snapshotDir: join(dir, "snapshots"),
    });

    expect(outcome.kind).toBe("absent");
  });

  it("proves corruption when the database is destroyed", () => {
    const dir = tempDirs.make("agent-db-snapshot-destroyed-");
    const databasePath = makeDatabase(dir);
    writeFileSync(databasePath, Buffer.alloc(8192, 0));

    const outcome = snapshotAndCertifyAgentDatabase({
      agentId: "a",
      databasePath,
      snapshotDir: join(dir, "snapshots"),
    });

    // Regression: a failed VACUUM used to return "inconclusive" and retry forever, so the halt could
    // never fire on the worst case it exists for. Both failure paths now classify against the source.
    expect(outcome.kind).toBe("corrupt");
  });

  it("proves corruption when the file is not a database", () => {
    const dir = tempDirs.make("agent-db-snapshot-junk-");
    const databasePath = join(dir, "junk.sqlite");
    writeFileSync(databasePath, Buffer.alloc(4096, 0x41));

    expect(
      snapshotAndCertifyAgentDatabase({
        agentId: "a",
        databasePath,
        snapshotDir: join(dir, "snapshots"),
      }).kind,
    ).toBe("corrupt");
  });

  it("rotates to the retention limit", () => {
    const dir = tempDirs.make("agent-db-snapshot-rotate-");
    const databasePath = makeDatabase(dir);
    const snapshotDir = join(dir, "snapshots");

    for (let index = 0; index < AGENT_DB_SNAPSHOT_RETENTION + 3; index += 1) {
      const writer = new DatabaseSync(databasePath);
      writer.exec(`INSERT INTO t VALUES ('row-${index}');`);
      writer.close();
      snapshotAndCertifyAgentDatabase({
        agentId: "a",
        databasePath,
        snapshotDir,
        now: () => Date.now() + index * 1000,
      });
    }

    expect(snapshotNames(snapshotDir)).toHaveLength(AGENT_DB_SNAPSHOT_RETENTION);
  });

  it("reports no age when nothing has been certified", () => {
    const dir = tempDirs.make("agent-db-snapshot-age-");
    expect(readCertifiedSnapshotAgeMs(join(dir, "snapshots"))).toBeUndefined();
  });
});
