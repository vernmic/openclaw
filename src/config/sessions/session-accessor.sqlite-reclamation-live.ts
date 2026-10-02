/**
 * Live binding for the reclamation batch-delete phase (phase-split piece c, part 5).
 *
 * The four-phase executor (session-accessor.sqlite-reclamation-executor.ts) and the operations
 * factory (session-accessor.sqlite-reclamation-operations.ts) are transport-free: the delete
 * operation is injected. This module is that injection for the two transcript kinds the ruling
 * assigns to the four-phase path (history-eviction / historical-generation); `entry` and
 * `lifecycle-artifacts` keep their atomic, single-transaction path (Ruling 1, 2026-10-01).
 *
 * The delete phase does NOT re-archive. Phase 2 (ARCHIVE-APPEND) already committed the archive
 * row, and the ledger's committed step_cursor is the proof that it did; re-materializing the blob
 * here would move the exact blob work the split exists to isolate back onto the delete hold. So
 * the durable plan_json is rebuilt into a DELETE-ONLY plan -- archive bytes and archived transcript
 * dropped to null -- and handed to the same per-kind transaction the atomic path already uses.
 * That keeps every per-kind rule (guard re-check, protected/excluded session ids, lifecycle-target
 * and board/delivery artifact removal, the session_windows disappearance check) in exactly one
 * place instead of a second copy inside the split.
 *
 * Decline mapping. The shared transaction already signals the two legitimate declines:
 *
 *  - a moved delete snapshot throws SqliteDeleteSnapshotMovedError, typed at its source, and
 *  - a target-snapshot mismatch returns { expectedEntryMismatch: true }.
 *
 * Both are normal, free outcomes -- the runner maps them to a terminal decline, not a failure.
 * Anything else propagates and becomes a terminal failure with no further delete.
 */
import { executeSqliteQuerySync } from "../../infra/kysely-sync.js";
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import type { MaterializedSessionStateDeletePlan } from "./session-accessor.sqlite-archive.js";
import { SqliteDeleteSnapshotMovedError } from "./session-accessor.sqlite-lifecycle-state.js";
import type { ReclamationDeleteExecutor } from "./session-accessor.sqlite-reclamation-operations.js";
import { ReclamationPhaseDeclinedError } from "./session-accessor.sqlite-reclamation-phases.js";
import type { SerializedSqliteSessionReclamationPlan } from "./session-accessor.sqlite-reclamation-plan.js";
import {
  type SqliteSessionReclamationPlan,
  reclaimSqliteSessionInTransaction,
} from "./session-accessor.sqlite-reclamation.js";
import { getSessionKysely } from "./session-accessor.sqlite-scope.js";

/**
 * Rebuilds the durable plan as a delete-only live plan: same shape, archive bytes and archived
 * transcript dropped. Phase 2 owns the archive; phase 3 must not re-run the blob step.
 */
export function toDeleteOnlyReclamationPlan(
  plan: SerializedSqliteSessionReclamationPlan,
): SqliteSessionReclamationPlan {
  if (plan.kind === "entry" || plan.kind === "lifecycle-artifacts") {
    throw new Error(`${plan.kind} plans keep their single-transaction path`);
  }
  const materializedPlans = plan.materializedPlans.map(
    (materialized) =>
      ({
        ...materialized,
        archive: null,
        archivedTranscript: null,
      }) as MaterializedSessionStateDeletePlan,
  );
  return { ...plan, materializedPlans } as SqliteSessionReclamationPlan;
}

/**
 * True when the session_windows row is gone after the delete -- the ledger's diagnostic
 * session_fully_deleted. Whole-session deletion is the norm; the flag is recorded, never gated.
 */
export function isReclamationSessionFullyDeleted(
  database: OpenClawAgentDatabase,
  sessionId: string,
): boolean {
  const db = getSessionKysely(database.db);
  return (
    executeSqliteQuerySync(
      database.db,
      db.selectFrom("session_windows").select("session_id").where("session_id", "=", sessionId),
    ).rows.length === 0
  );
}

/**
 * Builds the live delete operation for the four-phase path. The returned function is the
 * ReclamationDeleteExecutor the operations factory injects; it throws ReclamationPhaseDeclinedError
 * for the two legitimate declines and rethrows everything else.
 */
export function createLiveReclamationDeleteExecutor(): ReclamationDeleteExecutor {
  return ({ plan }) => {
    const livePlan = toDeleteOnlyReclamationPlan(plan);
    let result: ReturnType<typeof reclaimSqliteSessionInTransaction>;
    try {
      result = reclaimSqliteSessionInTransaction(livePlan);
    } catch (error) {
      // Discriminated by type, not message text: composed errors (an aggregate pairing a rollback
      // failure with a moved-snapshot cause) must fail the entry, never read as a decline.
      if (error instanceof SqliteDeleteSnapshotMovedError) {
        throw new ReclamationPhaseDeclinedError("delete-snapshot");
      }
      throw error;
    }
    if (result.kind === "historical-generation" && result.value.expectedEntryMismatch) {
      throw new ReclamationPhaseDeclinedError("target-guard");
    }
    // Surface the consumer value the atomic path returned synchronously (piece iv, part 2): the
    // ledger executor carries it into the in-process completion signal a waiting call site reads.
    return { archivedTranscripts: result.value.archivedTranscripts };
  };
}
