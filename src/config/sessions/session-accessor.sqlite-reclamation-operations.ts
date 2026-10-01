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
 * The batch-delete operation is injected because it is the one phase that still needs the live
 * per-kind transaction (and, in production, its own Worker commit gate). Wiring it to the live
 * primitives and the enqueue + push caller site is the next piece.
 */
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import type { ReclamationLedgerEntry } from "../../state/openclaw-agent-reclamation-queue.js";
import {
  materializeSessionStateDeletePlans,
  type MaterializedSessionStateDeletePlan,
  type SessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
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

/** The archive Worker's own snapshot fence: a moved snapshot during materialization. */
const SNAPSHOT_MOVED_MESSAGE = "SQLite session state changed before archive materialization";

function isSnapshotMovedError(error: unknown): boolean {
  return error instanceof Error && error.message.includes(SNAPSHOT_MOVED_MESSAGE);
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
 * Runs the archive-append phase: re-materialize the bytes and reconcile each with its durable
 * row. Both decline signals raise ReclamationPhaseDeclinedError (a normal, free outcome); a
 * sha256 conflict raises from reconcileReclamationArchive and stays a terminal failure.
 */
export async function reconcileReclamationArchivePhase(params: {
  database: OpenClawAgentDatabase;
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  plan: SerializedSqliteSessionReclamationPlan;
}): Promise<void> {
  const { database, plan } = params;
  const serializedPlans = plan.materializedPlans;
  // The delete snapshot can move between the guard phase and here; materialization re-checks it
  // and its fence throws. That is a normal decline, so the runner maps it to ReclamationPhase
  // declined rather than failed.
  const materialize = params.materializePlans ?? materializeSessionStateDeletePlans;
  let materialized: MaterializedSessionStateDeletePlan[];
  try {
    materialized = await materialize(toSessionStateDeletePlans(serializedPlans));
  } catch (error) {
    if (isSnapshotMovedError(error)) {
      throw new ReclamationPhaseDeclinedError("delete-snapshot");
    }
    throw error;
  }
  for (const materializedPlan of materialized) {
    const stored = serializedPlans.find(
      (serialized) => serialized.sessionId === materializedPlan.sessionId,
    )?.archive;
    if (!stored) {
      continue;
    }
    reconcileReclamationArchive({ database, plan: materializedPlan, stored });
  }
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
  materializePlans?: (
    plans: readonly SessionStateDeletePlan[],
  ) => Promise<MaterializedSessionStateDeletePlan[]>;
  phase: ReclamationPhase;
  plan: SerializedSqliteSessionReclamationPlan;
  stepCursor: number;
}): ReclamationPhaseOperations {
  const { database, entry, phase, plan, stepCursor } = params;
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
    appendArchive: () =>
      reconcileReclamationArchivePhase({
        database,
        materializePlans: params.materializePlans,
        plan,
      }),
    complete: () => {
      // Completion (including publication on the final phase) is the caller's job; mark is a no-op.
    },
    deleteReclaimedRows: () =>
      params.deleteReclaimedRows({ database, entry, phase, plan, stepCursor }),
    evaluateGuard,
    removeArchiveForDecline: () => {
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
    },
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
}): ReclamationPhaseOperationsFactory {
  const deserialize = params.deserializePlan ?? deserializeSqliteSessionReclamationPlan;
  return ({ entry, phase, stepCursor }) =>
    createReclamationPhaseOperations({
      database: params.database,
      deleteReclaimedRows: params.deleteReclaimedRows,
      entry,
      materializePlans: params.materializePlans,
      phase,
      plan: deserialize(entry.planJson),
      stepCursor,
    });
}
