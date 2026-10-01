/**
 * The four-phase reclamation runner (phase-split piece c, part 3).
 *
 * Claude's 2026-10-01 ruling fixes four phases -- guard / archive-append / batch-delete /
 * complete -- and drops the conditional watermark step: reclamation deletes whole sessions, the
 * session_windows FK cascade removes the watermark row, and session_fully_deleted stays
 * diagnostic-only. This module is the protocol the live wiring drives. Given a ledger entry's
 * committed step_cursor it runs exactly the next phase over injected, per-phase operations and
 * returns the ledger transition the caller records.
 *
 * The runner is transport-free on purpose. Under the split each phase is its own durable boundary
 * (its own Worker commit gate, authorization and claim.assertCurrent() window), so every operation
 * handed in is already scoped to one transaction; the runner only sequences them and encodes the
 * takeover-safety rules that make idempotent takeover (spec v2) safe:
 *
 *  - a guard decline at the head of the delete phase is the ONLY place the step-2 archive row is
 *    removed, and removeReclamationArchiveForDecline gates that removal on the cursor, so an
 *    archive that belongs to a committed delete is never removed;
 *  - a phase-2 snapshot decline -- materializeSessionStateDeletePlans re-checks the delete
 *    snapshot -- is a normal terminal declined, distinct from a sha256 conflict, which is terminal
 *    failed with NO delete (the invariant's last line of defence).
 */
import {
  RECLAMATION_PHASE_CURSORS,
  RECLAMATION_PHASES,
  type ReclamationPhase,
} from "./session-accessor.sqlite-reclamation-plan.js";

/** Why a phase declined. Mirrors the ledger's free-text last_error; never a failure. */
export type ReclamationDeclineReason = string;

export type ReclamationGuardOutcome =
  | { passed: true }
  | { passed: false; reason: ReclamationDeclineReason };

/**
 * Thrown by a phase operation for a normal, non-failure decline (the delete snapshot moved, the
 * session became recently active). The runner maps it to a terminal declined; any other error from
 * the archive-append phase is a terminal failed with no delete.
 */
export class ReclamationPhaseDeclinedError extends Error {
  readonly reason: ReclamationDeclineReason;
  constructor(reason: ReclamationDeclineReason) {
    super(reason);
    this.name = "ReclamationPhaseDeclinedError";
    this.reason = reason;
  }
}

export type ReclamationPhaseOperations = {
  /** Phase 1, re-evaluated at the head of phase 3 (guard-2). */
  evaluateGuard: () => ReclamationGuardOutcome | Promise<ReclamationGuardOutcome>;
  /** Phase 2: re-materialize the archive bytes and reconcile them with the durable row. */
  appendArchive: () => Promise<void> | void;
  /** Phase 3: delete the archived rows. Must advance the ledger cursor inside its transaction. */
  deleteReclaimedRows: () => Promise<void> | void;
  /** Removes the un-consumed step-2 archive row for a guard-passed-then-declined entry. */
  removeArchiveForDecline: () => Promise<void> | void;
  /** Phase 4: mark the entry done and publish. Idempotent, so a resume may run it alone. */
  complete: () => Promise<void> | void;
};

export type ReclamationPhaseOutcome =
  | { kind: "advanced"; phase: ReclamationPhase; stepCursor: number }
  | {
      cleanupArchive: boolean;
      kind: "declined";
      phase: ReclamationPhase;
      reason: ReclamationDeclineReason;
    }
  | { error: unknown; kind: "failed"; phase: ReclamationPhase }
  | { kind: "complete" };

/** The phase a committed cursor says runs next. */
export function reclamationPhaseForStepCursor(stepCursor: number): ReclamationPhase {
  const next = RECLAMATION_PHASES.find((phase) => RECLAMATION_PHASE_CURSORS[phase] > stepCursor);
  return next ?? RECLAMATION_PHASES[RECLAMATION_PHASES.length - 1];
}

/**
 * Runs exactly the next phase for a committed step_cursor and reports the transition. Every branch
 * is resume-safe: the guard and the archive append are idempotent, the delete-phase decline cleanup
 * is cursor-gated, and a cursor at or past COMMIT runs complete() alone.
 */
export async function runSqliteSessionReclamationPhase(params: {
  operations: ReclamationPhaseOperations;
  stepCursor: number;
}): Promise<ReclamationPhaseOutcome> {
  const { operations, stepCursor } = params;
  if (stepCursor < RECLAMATION_PHASE_CURSORS.guard) {
    const guard = await operations.evaluateGuard();
    if (!guard.passed) {
      return { cleanupArchive: false, kind: "declined", phase: "guard", reason: guard.reason };
    }
    return { kind: "advanced", phase: "guard", stepCursor: RECLAMATION_PHASE_CURSORS.guard };
  }
  if (stepCursor < RECLAMATION_PHASE_CURSORS["archive-append"]) {
    try {
      await operations.appendArchive();
    } catch (error) {
      if (error instanceof ReclamationPhaseDeclinedError) {
        return {
          cleanupArchive: false,
          kind: "declined",
          phase: "archive-append",
          reason: error.reason,
        };
      }
      return { error, kind: "failed", phase: "archive-append" };
    }
    return {
      kind: "advanced",
      phase: "archive-append",
      stepCursor: RECLAMATION_PHASE_CURSORS["archive-append"],
    };
  }
  if (stepCursor < RECLAMATION_PHASE_CURSORS["batch-delete"]) {
    const guard = await operations.evaluateGuard();
    if (!guard.passed) {
      await operations.removeArchiveForDecline();
      return {
        cleanupArchive: true,
        kind: "declined",
        phase: "batch-delete",
        reason: guard.reason,
      };
    }
    await operations.deleteReclaimedRows();
    return {
      kind: "advanced",
      phase: "batch-delete",
      stepCursor: RECLAMATION_PHASE_CURSORS["batch-delete"],
    };
  }
  await operations.complete();
  return { kind: "complete" };
}
