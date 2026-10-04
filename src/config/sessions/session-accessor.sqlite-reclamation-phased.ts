/**
 * The shared phased-reclamation helper (phase-split piece iv, step 2).
 *
 * The atomic path (runSqliteSessionReclamation) owns the entry and lifecycle-artifacts kinds. The
 * two bulk/historical kinds -- history-eviction and historical-generation -- move to the
 * four-phase split, and this helper is the seam those call sites swap in: it takes the SAME live
 * plan the atomic path took and returns the SAME result shape, so each call site keeps its branch
 * logic byte-for-byte and changes exactly one identifier.
 *
 * It does what the 2026-10-01/02 rulings settled, and nothing more:
 *
 *  - ENQUEUE one ledger row for the session (one entry == one session == one archive identity).
 *    The plan's generation IS the archive identity; reclamation never rotates the generation, so a
 *    re-plan reuses it by construction and the live-uniqueness index dedups an in-flight session.
 *    Only a plan that carries no generation falls back to the ledger's most recent row.
 *  - DRIVE the own entry to terminal through executeReclamationLedgerEntry, handing the per-phase
 *    factory the caller's own in-hand materialized plans: this is the caller's own entry, same
 *    process, so phase 2 READS those bytes (re-validating the snapshot) instead of re-running the
 *    archive Worker -- one materialization per entry. A recovery driver has no in-hand plan and
 *    re-derives from plan_json, which is what the transfer-list rule reserves re-derivation for.
 *  - MAP the terminal ledger state back to the atomic result shape. A decline RESOLVES: it is a
 *    normal outcome, reported the way the atomic path reported it -- expectedEntryMismatch true
 *    for historical-generation (its caller propagates the decline upward), plain deleted false for
 *    history-eviction (its loop just moves to the next candidate).
 *
 * The boot sweep and this drive may collide on the same entry; that is what idempotent takeover is
 * for, and the terminal ledger row is the shared record of who finished.
 */
import {
  openOpenClawAgentDatabase,
  type OpenClawAgentDatabase,
  type OpenClawAgentDatabaseOptions,
} from "../../state/openclaw-agent-db.js";
import {
  enqueueReclamationLedgerEntry,
  findReclamationLedgerGeneration,
  readReclamationLedgerEntry,
  type ReclamationLedgerEntry,
} from "../../state/openclaw-agent-reclamation-queue.js";
import type { SessionLifecycleArchivedTranscript } from "./session-accessor.lifecycle-types.js";
import type {
  MaterializedSessionStateDeletePlan,
  SessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
import type { SqliteSessionReclamationDiagnostics } from "./session-accessor.sqlite-contract.js";
import { readReclamationArchiveName } from "./session-accessor.sqlite-reclamation-archive.js";
import { executeReclamationLedgerEntry } from "./session-accessor.sqlite-reclamation-executor.js";
import {
  createLiveReclamationDeleteExecutor,
  isReclamationSessionFullyDeleted,
} from "./session-accessor.sqlite-reclamation-live.js";
import {
  createReclamationPhaseOperationsFactory,
  type InlineReclamationSnapshotValidator,
  type ReclamationDeleteExecutor,
  type ReclamationPhaseExclusiveWrite,
} from "./session-accessor.sqlite-reclamation-operations.js";
import {
  deserializeSqliteSessionReclamationPlan,
  readReclamationArchivedTranscripts,
  serializeSqliteSessionReclamationPlan,
} from "./session-accessor.sqlite-reclamation-plan.js";
import {
  runSqliteSessionReclamation,
  type SqliteSessionReclamationPlan,
  type SqliteSessionReclamationResult,
} from "./session-accessor.sqlite-reclamation.js";
import { runExclusiveSqliteSessionWrite } from "./session-accessor.sqlite-scope.js";

/** The two kinds the four-phase split admits; entry and lifecycle-artifacts keep the atomic path. */
export type PhasedSqliteSessionReclamationPlan = Extract<
  SqliteSessionReclamationPlan,
  { kind: "history-eviction" | "historical-generation" }
>;

function planGeneration(plan: PhasedSqliteSessionReclamationPlan): string | undefined {
  for (const materialized of plan.materializedPlans) {
    const generation = materialized.snapshot.generation;
    if (typeof generation === "string" && generation) {
      return generation;
    }
  }
  return undefined;
}

/**
 * Enqueues, drives, and maps one of the two phased reclamation kinds. Returns exactly the shape
 * runSqliteSessionReclamation returns for the same plan, so the call sites keep their branch logic.
 */
export async function runPhasedSqliteSessionReclamation(params: {
  /** Live admission guard, re-checked before every phase's write window. */
  assertCommitAllowed?: () => void;
  /** Test seam for the batch-delete phase; defaults to the live primitive binding. */
  deleteReclaimedRows?: ReclamationDeleteExecutor;
  diagnostics?: SqliteSessionReclamationDiagnostics;
  /**
   * Mirrors runSqliteSessionReclamation's flag. The four phases always run in the parent
   * (Ruling 3), so the flag only matters when a plan falls back to the atomic path below.
   */
  forceInProcess?: boolean;
  /** Off / in-process commit hook, mirroring runSqliteSessionReclamation's. */
  onInProcessCommit?: (database: OpenClawAgentDatabase) => void;
  /** Recovery-only test seam for the archive blob step; the inline drive reads in-hand bytes. */
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  now?: () => number;
  /**
   * Per-phase exclusive-write hold observation (the original motivation: reclamation must never
   * hold the agent DB write lock for a seconds-long span). Fired with the REAL wall-clock
   * duration of each phase's exclusive window, after it releases. Observation only.
   */
  observePhaseHold?: (hold: { durationMs: number; operation: string }) => void;
  /** Test seam for the database owner; defaults to the cached agent-database handle. */
  openDatabase?: (options: OpenClawAgentDatabaseOptions) => OpenClawAgentDatabase;
  plan: PhasedSqliteSessionReclamationPlan;
  /** Test seam for phase 2's inline snapshot fence; defaults to the live read-and-compare. */
  validateInlineSnapshot?: InlineReclamationSnapshotValidator;
}): Promise<SqliteSessionReclamationResult> {
  const { plan } = params;
  if (plan.kind !== "history-eviction" && plan.kind !== "historical-generation") {
    throw new Error("Phased session reclamation only admits the bulk and historical kinds");
  }
  if (params.diagnostics) {
    params.diagnostics.kind = plan.kind;
  }
  const now = params.now ?? Date.now;
  const openDatabase = params.openDatabase ?? openOpenClawAgentDatabase;
  const database = openDatabase(plan.databaseOptions);

  const archivedTranscriptsFor = (planJson: string): SessionLifecycleArchivedTranscript[] =>
    readReclamationArchivedTranscripts(
      deserializeSqliteSessionReclamationPlan(planJson),
      (archiveKey) => readReclamationArchiveName(database, archiveKey),
    );

  const declineResult = (): SqliteSessionReclamationResult =>
    (plan.kind === "historical-generation"
      ? {
          kind: plan.kind,
          value: { archivedTranscripts: [], deleted: false, expectedEntryMismatch: true },
        }
      : {
          kind: plan.kind,
          value: { archivedTranscripts: [], deleted: false },
        }) as SqliteSessionReclamationResult;

  const fail = (error: unknown): never => {
    throw error instanceof Error ? error : new Error(String(error));
  };

  /** A terminal ledger row reached by another driver (or a previous attempt). */
  const resolveTerminal = (row: ReclamationLedgerEntry): SqliteSessionReclamationResult => {
    if (row.state === "declined") {
      return declineResult();
    }
    if (row.state === "failed") {
      return fail(row.lastError ?? new Error("Phased session reclamation failed"));
    }
    return {
      kind: plan.kind,
      value: {
        archivedTranscripts: archivedTranscriptsFor(row.planJson),
        deleted: isReclamationSessionFullyDeleted(database, plan.sessionId),
      },
    } as SqliteSessionReclamationResult;
  };

  const generation =
    planGeneration(plan) ?? findReclamationLedgerGeneration(database.db, plan.sessionId);
  if (!generation) {
    // A plan with no archive generation has no archive identity to key a ledger entry on -- the
    // trajectory-only / empty-transcript row the atomic path reclaims without writing an archive.
    // The atomic path never needed a generation for it, so reproduce that path exactly instead of
    // minting a synthetic identity (fourth (iv-3) delta: atomic-matching semantics, no new rule).
    return await runSqliteSessionReclamation({
      assertCommitAllowed: params.assertCommitAllowed,
      diagnostics: params.diagnostics,
      forceInProcess: params.forceInProcess ?? false,
      onInProcessCommit: params.onInProcessCommit,
      plan,
    });
  }
  const { id } = enqueueReclamationLedgerEntry(database.db, {
    generation,
    now: now(),
    planJson: serializeSqliteSessionReclamationPlan(plan),
    sessionId: plan.sessionId,
  });

  let entry = readReclamationLedgerEntry(database.db, id);
  if (!entry) {
    throw new Error("Phased session reclamation lost its ledger entry");
  }
  if (entry.state === "done" || entry.state === "failed" || entry.state === "declined") {
    return resolveTerminal(entry);
  }

  // Every phase's database work runs in the parent under the ordinary exclusive session-write
  // admission; the live guard is re-checked inside each window (Ruling 3: no phases off-thread).
  const exclusive: ReclamationPhaseExclusiveWrite = (operation, run) =>
    runExclusiveSqliteSessionWrite(
      plan.databaseOptions,
      async () => {
        const holdStartedAt = Date.now();
        params.assertCommitAllowed?.();
        const value = await run();
        if (operation === "session.reclamation.phase-delete") {
          params.onInProcessCommit?.(database);
        }
        // Real wall-clock hold (not the injectable ledger clock): the oracle asserts no phase
        // holds the write lock for a seconds-long span.
        params.observePhaseHold?.({ durationMs: Date.now() - holdStartedAt, operation });
        return value;
      },
      operation,
    );

  const createOperations = createReclamationPhaseOperationsFactory({
    database,
    deleteReclaimedRows: params.deleteReclaimedRows ?? createLiveReclamationDeleteExecutor(),
    // The caller already materialized this plan; the inline drive reads those bytes in phase 2 so
    // the entry enqueues behind exactly one materialization (piece iv-3, 2026-10-03 ruling).
    inlineMaterializedPlans: plan.materializedPlans,
    materializePlans: params.materializePlans,
    runExclusiveWrite: exclusive,
    validateInlineSnapshot: params.validateInlineSnapshot,
  });

  const outcome = await executeReclamationLedgerEntry({
    createOperations,
    database: database.db,
    entry,
    now,
    readSessionFullyDeleted: () => isReclamationSessionFullyDeleted(database, plan.sessionId),
  });

  if (outcome.kind === "done") {
    return {
      kind: plan.kind,
      value: {
        archivedTranscripts: outcome.archivedTranscripts,
        deleted: outcome.sessionFullyDeleted,
      },
    } as SqliteSessionReclamationResult;
  }
  if (outcome.kind === "declined") {
    return declineResult();
  }
  if (outcome.kind === "failed") {
    return fail(outcome.error);
  }
  // yielded: a concurrent driver holds the entry; report its terminal row if it already landed.
  entry = readReclamationLedgerEntry(database.db, id) ?? entry;
  if (entry.state === "done" || entry.state === "failed" || entry.state === "declined") {
    return resolveTerminal(entry);
  }
  throw new Error("Phased session reclamation yielded to a concurrent driver");
}
