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
 * Materialization ownership (piece iv-3, 2026-10-03): the caller materializes ONCE and its
 * pre-materialized plans are the enqueue input; the INLINE drive reads those in-hand bytes in
 * phase 2 (re-validating the delete snapshot) instead of re-running the archive Worker, which
 * killed the duplicate materialize. A RECOVERY driver that did not enqueue the entry (boot sweep
 * / orphan finisher) has no in-hand plan and re-derives the bytes from plan_json -- that is what
 * the transfer-list rule reserves re-materialization for. The durable source of truth is still
 * plan_json; the inline in-hand plan is valid only because it is the same process and entry.
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
import {
  readSessionStateDeleteSnapshot,
  sqliteSessionStateDeleteSnapshotsEqual,
} from "./session-accessor.sqlite-delete-snapshot.js";
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

/**
 * Phase 2's snapshot fence for the caller's own inline drive (materialize-once, piece iv-3).
 *
 * With materialize-once the caller's pre-materialization IS this phase's bytes, so the inline
 * drive no longer runs the archive Worker and would otherwise lose the fence the Worker performed
 * before it encoded. Re-read the delete snapshot and raise the same normal decline the Worker's
 * fence raised when it moved -- the delete transaction re-checks anyway, but declining here keeps
 * the archive phase from persisting bytes against a stale snapshot. Tests inject a passthrough.
 */
export type InlineReclamationSnapshotValidator = (params: {
  database: OpenClawAgentDatabase;
  materializedPlans: readonly MaterializedSessionStateDeletePlan[];
}) => void;

export const revalidateInlineReclamationSnapshot: InlineReclamationSnapshotValidator = ({
  database,
  materializedPlans,
}) => {
  for (const materializedPlan of materializedPlans) {
    const current = readSessionStateDeleteSnapshot(database.db, materializedPlan.sessionId);
    if (!sqliteSessionStateDeleteSnapshotsEqual(current, materializedPlan.snapshot)) {
      throw new ReclamationPhaseDeclinedError("delete-snapshot");
    }
  }
};

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

export type ReclamationDeleteExecutor = (params: {
  database: OpenClawAgentDatabase;
  entry: ReclamationLedgerEntry;
  phase: ReclamationPhase;
  plan: SerializedSqliteSessionReclamationPlan;
  stepCursor: number;
}) => Promise<void> | void;

/** Builds the four operations for one phase against the live (or injected) primitives. */
export function createReclamationPhaseOperations(params: {
  database: OpenClawAgentDatabase;
  deleteReclaimedRows: ReclamationDeleteExecutor;
  entry: ReclamationLedgerEntry;
  /**
   * The caller's own pre-materialized plans (materialize-once, piece iv-3). Present only for the
   * inline drive: phase 2 then READS these bytes instead of re-running the archive Worker, so the
   * entry enqueues with exactly one materialization. Absent for a recovery driver, which re-derives.
   */
  inlineMaterializedPlans?: readonly MaterializedSessionStateDeletePlan[];
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  phase: ReclamationPhase;
  plan: SerializedSqliteSessionReclamationPlan;
  runExclusiveWrite?: ReclamationPhaseExclusiveWrite;
  stepCursor: number;
  /** Snapshot fence for the inline drive; defaults to the live read-and-compare re-check. */
  validateInlineSnapshot?: InlineReclamationSnapshotValidator;
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
    // Phase 2 splits around the admission window: the blob step runs first with no hold, then only
    // the INSERT + readback reconcile enters the exclusive write.
    appendArchive: async () => {
      const inline = params.inlineMaterializedPlans;
      if (inline) {
        // Materialize-once (piece iv-3): the caller's pre-materialization IS this phase's bytes, so
        // the inline drive reads them instead of re-running the archive Worker. Only the Worker's
        // snapshot fence stays, as its own normal decline.
        const validate = params.validateInlineSnapshot ?? revalidateInlineReclamationSnapshot;
        validate({ database, materializedPlans: inline });
        await exclusive("session.reclamation.phase-archive", () =>
          persistReclamationArchiveRows({ database, materialized: inline, plan }),
        );
        return;
      }
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
    deleteReclaimedRows: () =>
      exclusive("session.reclamation.phase-delete", () =>
        params.deleteReclaimedRows({ database, entry, phase, plan, stepCursor }),
      ),
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
  /** The caller's own pre-materialized plans, when this factory drives its own entry inline. */
  inlineMaterializedPlans?: readonly MaterializedSessionStateDeletePlan[];
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  runExclusiveWrite?: ReclamationPhaseExclusiveWrite;
  validateInlineSnapshot?: InlineReclamationSnapshotValidator;
}): ReclamationPhaseOperationsFactory {
  const deserialize = params.deserializePlan ?? deserializeSqliteSessionReclamationPlan;
  return ({ entry, phase, stepCursor }) =>
    createReclamationPhaseOperations({
      database: params.database,
      deleteReclaimedRows: params.deleteReclaimedRows,
      entry,
      inlineMaterializedPlans: params.inlineMaterializedPlans,
      materializePlans: params.materializePlans,
      phase,
      plan: deserialize(entry.planJson),
      runExclusiveWrite: params.runExclusiveWrite,
      stepCursor,
      validateInlineSnapshot: params.validateInlineSnapshot,
    });
}
