import { existsSync } from "node:fs";
import path from "node:path";
import { listAgentIds } from "../agents/agent-scope-config.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import {
  type AgentDbSnapshotOutcome,
  readCertifiedSnapshotAgeMs,
  snapshotAndCertifyAgentDatabase,
} from "../state/openclaw-agent-db-snapshot.js";
import { recordOpenClawAgentDatabaseOpenFailure } from "../state/openclaw-agent-db.js";
import { resolveOpenClawAgentSqlitePath } from "../state/openclaw-agent-db.paths.js";

/**
 * Hourly snapshot-and-certify pass over every configured agent database.
 *
 * Replaces the per-open integrity gate, which cost ~10.6 s on a 2 GB database and ran on every
 * worker-thread open -- 1,501 times on 2026-10-04, every one returning healthy with zero repairs,
 * about 4.4 hours of CPU. The gate also provided no recovery: it refused to open a damaged file and
 * left nothing to restore from. This pass costs ~21 s per changed database per hour (measured), is
 * free for unchanged ones, and leaves a validated restore point so the worst case is an hour.
 */
export async function snapshotAgentDatabases(params: {
  cfg: OpenClawConfig;
  log: {
    info: (message: string) => void;
    warn: (message: string) => void;
    error?: (message: string) => void;
  };
  env?: NodeJS.ProcessEnv;
  isClosing?: () => boolean;
  /**
   * The recurrence interval, reported as next-due in the summary line. Fizban's addition
   * (2026-10-06): the summary proves a pass EXISTED; next-due proves the recurrence has CONTINUITY,
   * because a recurrence that dies shows up as a summary line with no successor within one interval.
   */
  intervalMs?: number;
  /** Injected for tests; defaults to halting the process on proven corruption. */
  onProvenCorruption?: (outcome: Extract<AgentDbSnapshotOutcome, { kind: "corrupt" }>) => void;
}): Promise<void> {
  const halt = params.onProvenCorruption ?? haltOnProvenCorruption(params.log);
  // Tally every outcome so the pass itself is observable. Without this an all-unchanged pass logs
  // NOTHING, and "ran hourly and found nothing" is indistinguishable from "did not run for five
  // hours" -- which is exactly what the 2026-10-05 production log could not answer (16
  // certifications clustered in two hours, silence in between, no way to tell which). Evidence that
  // the replacement runs on schedule is a precondition for removing the per-open integrity gate.
  const tally: Record<AgentDbSnapshotOutcome["kind"], number> = {
    absent: 0,
    certified: 0,
    corrupt: 0,
    inconclusive: 0,
    "snapshot-suspect": 0,
    unchanged: 0,
  };
  const startedAt = Date.now();
  for (const agentId of listAgentIds(params.cfg)) {
    if (params.isClosing?.()) {
      return;
    }
    const databasePath = resolveOpenClawAgentSqlitePath({ agentId, env: params.env });
    // Derived from the database path, not from resolveAgentDir: that already returns
    // <state>/agents/<id>/agent, so joining "agent" again produced agent/agent/snapshots and every
    // agent mismatched (caught 2026-10-04 before review). dirname(databasePath) is co-located by
    // construction, including for agents with a custom agentDir.
    const snapshotDir = path.join(path.dirname(databasePath), "snapshots");
    let outcome: AgentDbSnapshotOutcome;
    try {
      outcome = snapshotAndCertifyAgentDatabase({ agentId, databasePath, snapshotDir });
      tally[outcome.kind] += 1;
    } catch (error) {
      // One agent's failure must not starve the rest, exactly as in the orphan sweep.
      params.log.warn(`agent-db snapshot failed for agent ${agentId}: ${String(error)}`);
      continue;
    }
    switch (outcome.kind) {
      case "absent":
      case "unchanged":
        break;
      case "certified":
        params.log.info(
          `agent-db snapshot certified for agent ${agentId} in ${outcome.elapsedMs}ms: ${outcome.snapshotPath}`,
        );
        break;
      case "inconclusive":
        // Transient lock/busy, or a failed VACUUM. Not proven damage; the next cycle retries.
        params.log.warn(
          `agent-db snapshot inconclusive for agent ${agentId}, retrying next cycle: ${outcome.error.message}`,
        );
        break;
      case "snapshot-suspect":
        // The snapshot failed its check but the live file re-confirmed healthy. Suspect the snapshot
        // write (disk), not the database. Never halt on this: a disk hiccup must not stop the
        // gateway, and a known-good restore point must not be replaced by a suspect one.
        params.log.warn(
          `agent-db snapshot failed verification for agent ${agentId} but the live database ` +
            `re-confirmed healthy; discarded the snapshot and kept the previous restore point: ${outcome.error.message}`,
        );
        break;
      case "corrupt":
        halt(outcome);
        return;
    }
  }
  // One line per pass, unconditionally. This is the only positive evidence that the cycle executed.
  const nextDue =
    params.intervalMs === undefined
      ? ""
      : `; next due ${new Date(Date.now() + params.intervalMs).toISOString()}`;
  params.log.info(
    `agent-db snapshot pass done in ${Date.now() - startedAt}ms: ` +
      `${tally.certified} certified, ${tally.unchanged} unchanged, ${tally.absent} absent` +
      (tally.inconclusive > 0 ? `, ${tally.inconclusive} inconclusive` : "") +
      (tally["snapshot-suspect"] > 0 ? `, ${tally["snapshot-suspect"]} suspect` : "") +
      nextDue,
  );
}

/**
 * Proven corruption: latch quarantine, then exit without a graceful shutdown.
 *
 * The latch is the fence. `recordOpenClawAgentDatabaseOpenFailure` is documented as latching
 * "background verification damage so later opens fail without rescanning" -- which is exactly what
 * this pass produces. Once recorded, no further open of that file succeeds, in this process or the
 * next, so writes cannot reach the damaged database even before the process dies, and a restart
 * cannot loop into writing it.
 *
 * The exit is deliberately hard. A graceful shutdown flushes session state and transcripts, which
 * is precisely what must not be written into a database just proven damaged.
 *
 * Recovery is operator-driven by design: inspect, attempt repair, and on success clear the
 * quarantine via clearOpenClawAgentDatabaseOpenFailure. If repair fails, restore the newest
 * certified snapshot from the agent's `snapshots/` directory -- at most one hour old.
 */
function haltOnProvenCorruption(log: {
  warn: (message: string) => void;
  error?: (message: string) => void;
}): (outcome: Extract<AgentDbSnapshotOutcome, { kind: "corrupt" }>) => void {
  return (outcome) => {
    const say = log.error ?? log.warn;
    try {
      recordOpenClawAgentDatabaseOpenFailure(
        outcome.databasePath,
        outcome.error,
        outcome.generation,
      );
      say(
        `FATAL: agent database proven corrupt for agent ${outcome.agentId} (${outcome.databasePath}): ` +
          `${outcome.error.message}. Quarantine latched; halting without a graceful shutdown so nothing ` +
          `further is written. Inspect and repair, then clear the quarantine; if repair fails, restore ` +
          `the newest certified snapshot from the agent's snapshots directory.`,
      );
    } catch (error) {
      say(`FATAL: agent database proven corrupt and quarantine latch failed: ${String(error)}`);
    }
    process.exit(1);
  };
}

/**
 * True when any configured agent's newest certified snapshot is older than `maxAgeMs`, or when an
 * existing database has never been certified at all.
 *
 * The gateway's isBusy gate defers rather than skips, which is correct for a busy hour and wrong for
 * a busy day: at sustained load the pass would never run and no restore point would ever be taken,
 * silently. This lets the gate stand down once a restore point is overdue. The cost is one readFile
 * per agent, so it is safe to call from isBusy on every poll.
 */
export function anyAgentSnapshotOverdue(params: {
  cfg: OpenClawConfig;
  maxAgeMs: number;
  env?: NodeJS.ProcessEnv;
  now?: () => number;
}): boolean {
  const now = params.now ?? Date.now;
  for (const agentId of listAgentIds(params.cfg)) {
    const databasePath = resolveOpenClawAgentSqlitePath({ agentId, env: params.env });
    if (!existsSync(databasePath)) {
      continue;
    }
    const age = readCertifiedSnapshotAgeMs(path.join(path.dirname(databasePath), "snapshots"), now);
    if (age === undefined || age > params.maxAgeMs) {
      return true;
    }
  }
  return false;
}
