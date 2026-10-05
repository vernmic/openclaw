import {
  existsSync,
  mkdirSync,
  statfsSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { SqliteFileGeneration } from "../infra/sqlite-file-generation.js";
import {
  assertSqliteIntegrity,
  confirmSqliteFileIntegrity,
  isTerminalSqliteIntegrityError,
} from "../infra/sqlite-integrity.js";

/**
 * Snapshots retained per agent before the oldest is rotated out.
 *
 * Six, not twenty-four (Fizban's ruling 2026-10-05): a restore only ever uses the newest certified
 * snapshot taken before the corruption, and certification reads the whole file, so damage is caught
 * within one interval of the write that caused it. Hours of lookback dominate that detection window,
 * so a day of history buys nothing operational. At 1,841 MB per snapshot this is ~11 GB steady state
 * for the main agent instead of ~44 GB.
 */
export const AGENT_DB_SNAPSHOT_RETENTION = 6;

const CERTIFIED_STATE_BASENAME = ".last-certified.json";

/**
 * Cheap change signal. Deliberately NOT a content hash: sha256 of the live 2 GB agent DB measured
 * 8.52 s on 2026-10-04, against the 8.62 s integrity_check it would be deciding about -- paying the
 * price of the work to decide whether to do the work. A stat is 0.0002 s and answers the only
 * question that matters here: has the file been written since the last certified snapshot.
 */
export type AgentDbMutationFingerprint = {
  size: number;
  mtimeNs: string;
  /** WAL sidecar bytes; 0 when absent OR empty -- the two are the same state. */
  walSize: number;
  walMtimeNs: string | null;
};

export type AgentDbSnapshotOutcome =
  | { kind: "unchanged"; agentId: string }
  | { kind: "absent"; agentId: string }
  | { kind: "certified"; agentId: string; snapshotPath: string; elapsedMs: number }
  /** The snapshot failed but the live database re-confirmed healthy: suspect the snapshot write. */
  | { kind: "snapshot-suspect"; agentId: string; error: Error }
  /** Transient failure (lock/busy). Not proven damage; retry on the next cycle. */
  | { kind: "inconclusive"; agentId: string; error: Error }
  /** PROVEN corruption of the live database. The caller must latch quarantine and halt. */
  | {
      kind: "corrupt";
      agentId: string;
      databasePath: string;
      error: Error;
      generation: SqliteFileGeneration;
    };

function readFingerprint(file: string): AgentDbMutationFingerprint | undefined {
  try {
    const raw = JSON.parse(readFileSync(file, "utf8")) as Partial<AgentDbMutationFingerprint>;
    if (typeof raw.size !== "number" || typeof raw.mtimeNs !== "string") {
      return undefined;
    }
    // A state file written before the WAL fields existed must read as CHANGED, not as unchanged with
    // nulls, or the first pass after upgrade would skip on a fingerprint that never covered the WAL.
    if (!("walSize" in raw) || !("walMtimeNs" in raw)) {
      return undefined;
    }
    return {
      mtimeNs: raw.mtimeNs,
      size: raw.size,
      walMtimeNs: raw.walMtimeNs ?? null,
      walSize: raw.walSize ?? 0,
    };
  } catch {
    return undefined;
  }
}

/**
 * Stat the main file AND its WAL sidecar.
 *
 * In WAL mode a commit lands in `-wal` first; the main file's mtime does not move until checkpoint.
 * Fingerprinting the main file alone therefore reads a database with un-checkpointed writes as
 * UNCHANGED, silently skipping the cycle and leaving a restore point older than the one-hour worst
 * case this design promises. Observed on the live system 2026-10-04: main file mtime 20:51 while
 * `-wal` stood at 21:12. Three stats cost the same 0.0002 s as one.
 */
function currentFingerprint(databasePath: string): AgentDbMutationFingerprint {
  const st = statSync(databasePath, { bigint: true });
  // An ABSENT WAL and a ZERO-LENGTH WAL are the same state: neither holds un-checkpointed data.
  // They must normalize to one value, because a read-only open CREATES a zero-length WAL that
  // outlives the handle -- so the snapshot's own source open would otherwise flip the fingerprint
  // from absent to empty and defeat the skip path on every cycle (observed 2026-10-04: pass 2
  // re-snapshotted an untouched database because walSize went null -> 0).
  // The empty WAL's mtime is noise for the same reason, so it is only carried when the WAL has
  // content.
  let walSize = 0;
  let walMtimeNs: string | null = null;
  try {
    const wal = statSync(`${databasePath}-wal`, { bigint: true });
    walSize = Number(wal.size);
    if (walSize > 0) {
      walMtimeNs = String(wal.mtimeNs);
    }
  } catch {
    // Absent: already normalized to 0 above.
  }
  return { mtimeNs: String(st.mtimeNs), size: Number(st.size), walMtimeNs, walSize };
}

function sameFingerprint(
  left: AgentDbMutationFingerprint | undefined,
  right: AgentDbMutationFingerprint,
): boolean {
  return (
    left !== undefined &&
    left.size === right.size &&
    left.mtimeNs === right.mtimeNs &&
    left.walSize === right.walSize &&
    left.walMtimeNs === right.walMtimeNs
  );
}

/**
 * Snapshot one agent database and certify the SNAPSHOT, not the live file.
 *
 * Ordering matters and is the inverse of the obvious one: validating the live file and then copying
 * leaves a window where writes land between the two, so the retained artifact is not the thing that
 * passed. Snapshotting first makes the certified artifact exactly what a restore would use, and it
 * validates a file nobody is writing -- which removes the transient-lock failure mode entirely.
 *
 * The source is opened as a plain read-only handle, NOT through openOpenClawAgentDatabase: that path
 * runs the per-open integrity gate (10.6 s on a 2 GB file) and the schema ensure, neither of which a
 * snapshot wants.
 */
export function snapshotAndCertifyAgentDatabase(params: {
  agentId: string;
  databasePath: string;
  snapshotDir: string;
  now?: () => number;
  retention?: number;
}): AgentDbSnapshotOutcome {
  const { agentId, databasePath, snapshotDir } = params;
  const now = params.now ?? Date.now;
  if (!existsSync(databasePath)) {
    return { agentId, kind: "absent" };
  }
  const statePath = path.join(snapshotDir, CERTIFIED_STATE_BASENAME);
  const fingerprint = currentFingerprint(databasePath);
  if (sameFingerprint(readFingerprint(statePath), fingerprint)) {
    return { agentId, kind: "unchanged" };
  }

  mkdirSync(snapshotDir, { recursive: true });

  // A snapshot system that fills the volume converts one database's problem into every database's.
  // The source size is a safe upper bound for the snapshot (VACUUM INTO compacts: 2086 MB -> 1842 MB
  // measured 2026-10-04), so require room for one snapshot plus the same again as slack.
  try {
    const fsStat = statfsSync(snapshotDir);
    const freeBytes = Number(fsStat.bavail) * Number(fsStat.bsize);
    const needed = fingerprint.size * 2;
    if (freeBytes < needed) {
      return {
        agentId,
        error: new Error(
          `insufficient free space for snapshot: ${Math.round(freeBytes / 1048576)} MB free, ` +
            `${Math.round(needed / 1048576)} MB required`,
        ),
        kind: "inconclusive",
      };
    }
  } catch {
    // statfs unavailable: proceed rather than block snapshots on a missing capability.
  }

  const startedAt = now();
  const snapshotPath = path.join(snapshotDir, `agent-${new Date(startedAt).getTime()}.sqlite`);
  try {
    const source = new DatabaseSync(databasePath, { readOnly: true });
    try {
      // VACUUM INTO takes a consistent read snapshot and compacts; in WAL mode readers do not block
      // writers, so this does not stall the gateway's own writes.
      source.exec(`VACUUM INTO '${snapshotPath.replace(/'/g, "''")}'`);
    } finally {
      source.close();
    }
  } catch (error) {
    rmSync(snapshotPath, { force: true });
    // A failed VACUUM must NOT be reported as merely transient without asking the source. Corruption
    // severe enough to break the read would otherwise loop as "inconclusive" every hour and never
    // escalate, so the halt would never fire on the worst case it exists for.
    return classifyAgainstSource(agentId, databasePath, error as Error);
  }

  // Validate the snapshot with the CHEAP check: integrity_check + foreign_key_check on an open
  // handle. Deliberately not confirmSqliteFileIntegrity here -- that binds its verdict to a file
  // generation, which means hashing the file twice for the stability read. Measured 2026-10-04 on a
  // 1.8 GB snapshot: the hashing path cost ~66 s end to end against ~25 s for the checks alone, and
  // the generation binding buys nothing for a snapshot that is immutable once written and whose
  // provenance is already recorded as the source's stat fingerprint.
  let snapshotError: Error | undefined;
  try {
    const written = new DatabaseSync(snapshotPath, { readOnly: true });
    try {
      assertSqliteIntegrity(written, `agent ${agentId} snapshot`);
    } finally {
      written.close();
    }
  } catch (error) {
    snapshotError = error as Error;
  }
  if (!snapshotError) {
    writeFileSync(statePath, JSON.stringify({ ...fingerprint, at: startedAt, snapshotPath }));
    rotateSnapshots(snapshotDir, params.retention ?? AGENT_DB_SNAPSHOT_RETENTION);
    return { agentId, elapsedMs: now() - startedAt, kind: "certified", snapshotPath };
  }

  // A failed scan on the snapshot is ADVISORY. It means either the live file is damaged or this
  // snapshot write was. Discard the suspect snapshot and re-confirm against the live file before
  // anyone escalates: terminating the gateway on a disk hiccup would be a false positive, and a
  // known-good snapshot must never be replaced by a suspect one.
  rmSync(snapshotPath, { force: true });
  if (!isTerminalSqliteIntegrityError(snapshotError)) {
    return { agentId, error: snapshotError, kind: "inconclusive" };
  }
  return classifyAgainstSource(agentId, databasePath, snapshotError);
}

/**
 * Decide, against the live file, whether an advisory failure is proven damage.
 *
 * This is the only path that may return `corrupt`, and it is deliberately the only one: every other
 * failure mode -- a bad snapshot write, a lock, a busy database -- must not halt the gateway.
 * `confirmSqliteFileIntegrity` retries three times and latches `terminal` only on proven damage,
 * which is exactly the gate a halt should sit behind. The cost (it hashes the file to bind the
 * verdict to a generation) is affordable here because this path is the exception, not the cycle.
 */
function classifyAgainstSource(
  agentId: string,
  databasePath: string,
  advisory: Error,
): AgentDbSnapshotOutcome {
  const live = confirmSqliteFileIntegrity(databasePath, `agent ${agentId}`);
  if (live.status === "healthy") {
    return { agentId, error: advisory, kind: "snapshot-suspect" };
  }
  if (!live.terminal) {
    return { agentId, error: live.error, kind: "inconclusive" };
  }
  return {
    agentId,
    databasePath,
    error: live.error,
    generation: live.generation,
    kind: "corrupt",
  };
}

/** Keep the newest `retention` snapshots; never touch the certified-state file. */
function rotateSnapshots(snapshotDir: string, retention: number): void {
  let entries: string[];
  try {
    entries = readdirSync(snapshotDir).filter(
      (name) => name.startsWith("agent-") && name.endsWith(".sqlite"),
    );
  } catch {
    return;
  }
  if (entries.length <= retention) {
    return;
  }
  for (const name of entries.sort().slice(0, entries.length - retention)) {
    rmSync(path.join(snapshotDir, name), { force: true });
  }
}

/**
 * Age of the newest certified snapshot for one agent, or undefined when none exists.
 *
 * Exported so the gateway's isBusy gate can override deferral once a restore point is overdue: the
 * idle task defers rather than skips, which is right for a busy hour and wrong for a busy day -- a
 * permanently busy gateway would otherwise never take a snapshot, with no signal. A missing restore
 * point is the exact failure this system exists to remove.
 */
export function readCertifiedSnapshotAgeMs(
  snapshotDir: string,
  now: () => number = Date.now,
): number | undefined {
  try {
    const raw = JSON.parse(
      readFileSync(path.join(snapshotDir, CERTIFIED_STATE_BASENAME), "utf8"),
    ) as {
      at?: number;
    };
    return typeof raw.at === "number" ? Math.max(0, now() - raw.at) : undefined;
  } catch {
    return undefined;
  }
}
