/**
 * The reclamation phase-operations factory (phase-split piece c, part 4b-2).
 *
 * The ledger executor (session-accessor.sqlite-reclamation-executor.ts) drives one claimed
 * entry through its phases, but it is deliberately transport-free: something must build each
 * phase's real operations. This module is that binding for the two transcript kinds the
 * ruling assigns to the four-phase path (history-eviction / historical-generation); `entry`
 * and `lifecycle-artifacts` keep their atomic, single-transaction path (Ruling 1, 2026-10-01).
 *
 * It is the composition point, so it owns nothing new: it re-uses the pieces the earlier ticks
 * already proved in isolation --
 *
 *  - phase 1 (guard) and the guard-2 re-check at the head of the delete phase call the shared
 *    evaluateSqliteReclamationTargetGuard (part 4b-1), or the recent-activity predicate for the
 *    eviction kind; a decline writes nothing;
 *  - phase 2 (archive-append) re-materializes the archive bytes from transcript_events and
 *    reconciles them with the durable row (part 2), mapping both decline signals -- a moved
 *    delete snapshot and the archive Worker's own snapshot fence -- to a normal
 *    ReclamationPhaseDeclinedError and a sha256 conflict to a terminal failure with NO delete;
 *  - the guard-2 decline cleanup removes the un-consumed step-2 archive row, cursor-gated.
 *
 * The plan is re-materialized from plan_json at EVERY phase: the first Worker post consumed the
 * archive bytes' transfer list, so no in-memory plan survives a phase boundary. plan_json is the
 * durable source of truth; the in-memory plan is an optimization we do not keep.
 *
 * Ruling 3 (2026-10-01, "the Worker windows dissolve"): no phase commits off-thread any more.
 * Every phase's database work runs in the PARENT under the ordinary exclusive session-write
 * admission (runExclusiveSqliteSessionWrite) -- one short window per database transaction, per
 * phase. The one deliberate exception is phase 2's blob re-materialization: the zstd compute
 * stays off-thread in the materialize Worker and runs OUTSIDE any admission window, so the
 * write hold is only the INSERT + readback. The `entry` / `lifecycle-artifacts` kinds keep
 * their single-transaction worker path unchanged (Ruling 1). Wiring the enqueue + push caller
 * site is the next piece.
 */
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import type { ReclamationLedgerEntry } from "../../state/openclaw-agent-reclamation-queue.js";
import {
  materializeSessionStateDeletePlans,
  type MaterializedSessionStateDeletePlan,
  type SessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
import type { SessionLifecycleArchivedTranscript } from "./session-accessor.sqlite-contract.js";
import {
  reconcileReclamationArchive,
  removeReclamationArchiveForDecline,
} from "./session-accessor.sqlite-reclamation-archive.js";
import type { ReclamationPhaseOperationsFactory } from "./session-accessor.sqlite-reclamation-executor.js";
import { evaluateSqliteReclamationTargetGuard } from "./session-accessor.sqlite-reclamation-guard.js";
import {
  ReclamationPhaseDeclinedError,
  type ReclamationPhase,
  type ReclamationPhaseOperations,
} from "./session-accessor.sqlite-reclamation-phases.js";
import {
  deserializeSqliteSessionReclamationPlan,
  type SerializedSessionStateDeletePlan,
  type SerializedSqliteSessionReclamationPlan,
} from "./session-accessor.sqlite-reclamation-plan.js";
import { isRecentHistoricalSessionId } from "./session-accessor.sqlite-references.js";
import { runExclusiveSqliteSessionWrite } from "./session-accessor.sqlite-scope.js";
import type { SqliteSessionWriteOperation } from "./session-accessor.sqlite-write-operation.js";

/** The archive Worker's own snapshot fence: a moved snapshot during materialization. */
const SNAPSHOT_MOVED_MESSAGE = "SQLite session state changed before archive materialization";

function isSnapshotMovedError(error: unknown): boolean {
  return error instanceof Error && error.message.includes(SNAPSHOT_MOVED_MESSAGE);
}

/**
 * Runs one reclamation phase's database transaction under the session's exclusive write
 * admission. Defaults to the ordinary runExclusiveSqliteSessionWrite queue every other session
 * write uses (Ruling 3: phases run in the parent, one short window per database transaction);
 * tests inject a passthrough so their in-memory handles stay authoritative.
 */
export type ReclamationPhaseExclusiveWrite = <T>(
  operation: SqliteSessionWriteOperation,
  run: () => Promise<T> | T,
) => Promise<T>;

function createDefaultExclusiveWrite(
  scope: SerializedSqliteSessionReclamationPlan["databaseOptions"],
): ReclamationPhaseExclusiveWrite {
  return (operation, run) => runExclusiveSqliteSessionWrite(scope, async () => run(), operation);
}

/** Strips the durable archive metadata back to the un-materialized plan the Worker consumes. */
export function toSessionStateDeletePlans(
  plans: readonly SerializedSessionStateDeletePlan[],
): SessionStateDeletePlan[] {
  return plans.map((plan) => ({
    agentId: plan.agentId,
    archiveDirectory: plan.archiveDirectory,
    archiveTranscript: plan.archiveTranscript,
    databasePath: plan.databasePath,
    reason: plan.reason,
    sessionId: plan.sessionId,
    snapshot: plan.snapshot,
  }));
}

/**
 * Phase 2's blob half: re-derive the archive bytes off-thread. No admission is held here --
 * this is pure compute (zstd in the materialize Worker), and holding the session writer for it
 * is exactly the hold the split exists to remove.
 *
 * The delete snapshot can move between the guard phase and here; materialization re-checks it
 * and its fence throws. That is a normal decline, so the caller maps it to
 * ReclamationPhaseDeclinedError rather than failed.
 */
export async function materializeReclamationArchivePlans(params: {
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  plan: SerializedSqliteSessionReclamationPlan;
}): Promise<MaterializedSessionStateDeletePlan[]> {
  const materialize = params.materializePlans ?? materializeSessionStateDeletePlans;
  try {
    return await materialize(toSessionStateDeletePlans(params.plan.materializedPlans));
  } catch (error) {
    if (isSnapshotMovedError(error)) {
      throw new ReclamationPhaseDeclinedError("delete-snapshot");
    }
    throw error;
  }
}

/**
 * Phase 2's database half: reconcile each re-materialized archive with its durable row. A
 * sha256 or byte-length conflict raises from reconcileReclamationArchive and stays a terminal
 * failure with no delete.
 */
export function persistReclamationArchiveRows(params: {
  database: OpenClawAgentDatabase;
  materialized: readonly MaterializedSessionStateDeletePlan[];
  plan: SerializedSqliteSessionReclamationPlan;
}): void {
  for (const materializedPlan of params.materialized) {
    const stored = params.plan.materializedPlans.find(
      (serialized) => serialized.sessionId === materializedPlan.sessionId,
    )?.archive;
    if (!stored) {
      continue;
    }
    reconcileReclamationArchive({
      database: params.database,
      plan: materializedPlan,
      stored,
    });
  }
}

/**
 * Runs the archive-append phase end to end. The phase operation itself splits this around the
 * exclusive-write window (bytes outside, reconcile inside); this composition is the
 * admission-free form the direct tests use.
 */
export async function reconcileReclamationArchivePhase(params: {
  database: OpenClawAgentDatabase;
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  plan: SerializedSqliteSessionReclamationPlan;
}): Promise<void> {
  const materialized = await materializeReclamationArchivePlans(params);
  persistReclamationArchiveRows({ database: params.database, materialized, plan: params.plan });
}

/**
 * What the delete phase reports back to the ledger executor (piece iv, part 2). The archived
 * transcripts are deliberately NOT durable -- the ledger row cannot carry them -- so the value
 * travels in-process from the delete operation to the completion signal the waiting call site
 * reads. A delete that removed nothing (a decline) returns an empty list, not undefined.
 */
export type ReclamationDeleteOutcome = {
  archivedTranscripts: SessionLifecycleArchivedTranscript[];
} | void;

export type ReclamationDeleteExecutor = (params: {
  database: OpenClawAgentDatabase;
  entry: ReclamationLedgerEntry;
  phase: ReclamationPhase;
  plan: SerializedSqliteSessionReclamationPlan;
  stepCursor: number;
}) => Promise<ReclamationDeleteOutcome> | ReclamationDeleteOutcome;

/** Builds the four operations for one phase against the live (or injected) primitives. */
export function createReclamationPhaseOperations(params: {
  database: OpenClawAgentDatabase;
  deleteReclaimedRows: ReclamationDeleteExecutor;
  entry: ReclamationLedgerEntry;
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  phase: ReclamationPhase;
  plan: SerializedSqliteSessionReclamationPlan;
  /**
   * Receives the transcripts the delete phase archived, so a multi-phase driver can carry them
   * across the phase boundary to the in-process completion signal. Optional: a direct caller that
   * never awaits a signal has no use for it.
   */
  recordDeletedArchivedTranscripts?: (transcripts: SessionLifecycleArchivedTranscript[]) => void;
  runExclusiveWrite?: ReclamationPhaseExclusiveWrite;
  stepCursor: number;
}): ReclamationPhaseOperations {
  const { database, entry, phase, plan, stepCursor } = params;
  const exclusive = params.runExclusiveWrite ?? createDefaultExclusiveWrite(plan.databaseOptions);
  // Ruling 1 (2026-10-01): `entry` and `lifecycle-artifacts` keep the atomic single-transaction
  // path -- their companion mutations cannot be compensated across phase boundaries, so the
  // split admits only the two bulk/historical kinds.
  if (plan.kind === "entry" || plan.kind === "lifecycle-artifacts") {
    throw new Error(`${plan.kind} plans keep their single-transaction path`);
  }

  const evaluateGuard = () => {
    if (plan.kind === "historical-generation") {
      const guard = evaluateSqliteReclamationTargetGuard(database, plan);
      return guard.passed
        ? ({ passed: true } as const)
        : ({ passed: false, reason: "target-guard" } as const);
    }
    // history-eviction: node activity can change after the parent dispatched the Worker.
    if (
      isRecentHistoricalSessionId({
        database,
        ...plan.diskBudget,
        sessionId: plan.sessionId,
      })
    ) {
      return { passed: false, reason: "recent-activity" } as const;
    }
    return { passed: true } as const;
  };

  return {
    // Phase 2 splits around the admission window: the blob re-materialization runs first with
    // no hold, then only the INSERT + readback reconcile enters the exclusive write.
    appendArchive: async () => {
      const materialized = await materializeReclamationArchivePlans({
        materializePlans: params.materializePlans,
        plan,
      });
      await exclusive("session.reclamation.phase-archive", () =>
        persistReclamationArchiveRows({ database, materialized, plan }),
      );
    },
    complete: () => {
      // Completion (including publication on the final phase) is the caller's job; mark is a no-op.
    },
    deleteReclaimedRows: async () => {
      const outcome = await exclusive("session.reclamation.phase-delete", () =>
        params.deleteReclaimedRows({ database, entry, phase, plan, stepCursor }),
      );
      if (outcome && outcome.archivedTranscripts) {
        params.recordDeletedArchivedTranscripts?.(outcome.archivedTranscripts);
      }
    },
    evaluateGuard: () => exclusive("session.reclamation.phase-guard", evaluateGuard),
    removeArchiveForDecline: () =>
      exclusive("session.reclamation.phase-cleanup", () => {
        for (const serialized of plan.materializedPlans) {
          const generation = serialized.snapshot.generation;
          if (!generation) {
            continue;
          }
          removeReclamationArchiveForDecline({
            database,
            generation,
            sessionId: serialized.sessionId,
            stepCursor,
          });
        }
      }),
  };
}

/**
 * Closure over the durable plan_json and the live database, returning the executor's per-phase
 * factory. The plan is deserialized afresh on every call because the transfer-list rule forbids
 * keeping bytes, and the deserialize validation is the guard against a malformed durable row.
 */
export function createReclamationPhaseOperationsFactory(params: {
  database: OpenClawAgentDatabase;
  deleteReclaimedRows: ReclamationDeleteExecutor;
  deserializePlan?: (planJson: string) => SerializedSqliteSessionReclamationPlan;
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  runExclusiveWrite?: ReclamationPhaseExclusiveWrite;
}): ReclamationPhaseOperationsFactory {
  const deserialize = params.deserializePlan ?? deserializeSqliteSessionReclamationPlan;
  // The holder lives on the factory, not on one phase's operations object: the delete phase writes
  // the archived transcripts and a LATER phase (complete) settles the completion signal, so the
  // value has to cross the phase boundary. Every phase's operations object exposes the same reader.
  let deletedArchivedTranscripts: SessionLifecycleArchivedTranscript[] = [];
  return ({ entry, phase, stepCursor }) => ({
    ...createReclamationPhaseOperations({
      database: params.database,
      deleteReclaimedRows: params.deleteReclaimedRows,
      entry,
      materializePlans: params.materializePlans,
      phase,
      plan: deserialize(entry.planJson),
      recordDeletedArchivedTranscripts: (transcripts) => {
        deletedArchivedTranscripts = transcripts;
      },
      runExclusiveWrite: params.runExclusiveWrite,
      stepCursor,
    }),
    readDeletedArchivedTranscripts: () => deletedArchivedTranscripts,
  });
}
