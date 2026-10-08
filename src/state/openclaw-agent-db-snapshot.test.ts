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

  // Re-keyed from openclaw-agent-db.test.ts on 2026-10-07, when Option B scoped the steady-state
  // agent open to foreign_key_check only (ruling: Fizban + Astinus; acceptance: Vern). Five tests
  // there asserted that a physical open detects, refuses, or repairs physical index-btree drift.
  // Detection and refusal move HERE, to the hourly pass. Repair is NOT re-keyed -- it was dropped
  // on purpose, and the call-site comment records that. These tests are what keeps the guarantee
  // relocated rather than lost.
  describe("physical index drift (re-keyed from the per-open gate)", () => {
    /**
     * Build an index btree over one column, then rewrite its schema TEXT to claim different
     * columns. The text looks canonical, so a schema-shape comparison sees nothing; only a
     * whole-file `integrity_check` reports `row N missing from index ...`.
     *
     * Same recipe as `createCacheExpiryIndexPhysicalDrift` in openclaw-agent-db.test.ts, which is
     * how the re-keyed tests used to exercise this through the open path.
     */
    function createPhysicalIndexDrift(databasePath: string): void {
      const db = new DatabaseSync(databasePath);
      try {
        db.exec(`
          ALTER TABLE t ADD COLUMN b TEXT;
          UPDATE t SET b = 'b-value';
          CREATE INDEX ix_t_drift ON t(b);
        `);
        db.enableDefensive?.(false);
        db.exec("PRAGMA writable_schema = ON;");
        db.prepare(
          `UPDATE sqlite_schema
              SET sql = 'CREATE INDEX ix_t_drift ON t(a, b)'
            WHERE name = 'ix_t_drift'`,
        ).run();
        const schemaVersion = Number(
          (db.prepare("PRAGMA schema_version").get() as { schema_version: number }).schema_version,
        );
        db.exec(`PRAGMA writable_schema = OFF; PRAGMA schema_version = ${schemaVersion + 1};`);
      } finally {
        db.close();
      }
    }

    function integrityOf(databasePath: string): string {
      const db = new DatabaseSync(databasePath);
      try {
        return (db.prepare("PRAGMA integrity_check").all() as Array<{ integrity_check: string }>)
          .map((row) => row.integrity_check)
          .join("; ");
      } finally {
        db.close();
      }
    }

    it("carries the drift into the snapshot, so certifying the copy can see it", () => {
      // The property the whole re-key depends on, asserted rather than assumed. VACUUM rebuilds
      // pages, which could plausibly have laundered the drift and left the hourly pass certifying a
      // clean copy of a damaged file -- that would make re-keying WORSE than deleting these tests,
      // because it would encode a guarantee that does not exist. Measured 2026-10-07: it does not
      // launder it, and the copy reports the same index error.
      const dir = tempDirs.make("agent-db-snapshot-drift-survives-");
      const databasePath = makeDatabase(dir);
      createPhysicalIndexDrift(databasePath);
      expect(integrityOf(databasePath)).toMatch(/ix_t_drift/);

      const copyPath = join(dir, "vacuumed.sqlite");
      const source = new DatabaseSync(databasePath);
      try {
        source.exec(`VACUUM INTO '${copyPath.replaceAll("\\", "/")}'`);
      } finally {
        source.close();
      }

      expect(integrityOf(copyPath)).toMatch(/ix_t_drift/);
    });

    it("proves corruption and classifies it against the source", () => {
      // Was: "rejects unrelated current-schema index corruption before exposure" and "rejects stale
      // schema_meta indexes before writable initialization" -- both asserted the OPEN throws. The
      // open no longer page-walks, so the refusal lives here: the pass proves the damage and names
      // the live file, which is what the sidecar latches and halts on.
      const dir = tempDirs.make("agent-db-snapshot-drift-corrupt-");
      const databasePath = makeDatabase(dir);
      createPhysicalIndexDrift(databasePath);

      const outcome = snapshotAndCertifyAgentDatabase({
        agentId: "a",
        databasePath,
        snapshotDir: join(dir, "snapshots"),
      });

      expect(outcome.kind).toBe("corrupt");
      if (outcome.kind !== "corrupt") {
        throw new Error("expected proven corruption");
      }
      // Classified against the SOURCE, not the copy: the verdict must name the live database the
      // gateway would otherwise keep serving.
      expect(outcome.databasePath).toBe(databasePath);
      expect(String(outcome.error)).toMatch(/ix_t_drift|integrity_check/);
    });

    it("does not certify a restore point for a drifted database", () => {
      // Was part of "rechecks integrity after a validated handle is physically reopened": a damaged
      // file must not be able to look healthy, and it must not become the restore point either.
      const dir = tempDirs.make("agent-db-snapshot-drift-nocert-");
      const databasePath = makeDatabase(dir);
      createPhysicalIndexDrift(databasePath);
      const snapshotDir = join(dir, "snapshots");

      snapshotAndCertifyAgentDatabase({ agentId: "a", databasePath, snapshotDir });

      expect(readCertifiedSnapshotAgeMs(snapshotDir)).toBeUndefined();
    });
  });
});
