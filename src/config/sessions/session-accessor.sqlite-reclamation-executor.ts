/**
 * The reclamation ledger executor (phase-split piece c, part 4a).
 *
 * The phase runner in session-accessor.sqlite-reclamation-phases.ts is transport-free: it drives
 * exactly one phase over injected operations and reports the transition. Something has to (1)
 * decide which phase runs next from the ledger's committed cursor, (2) build that phase's
 * operations against the live primitives, and (3) write the transition back to the ledger. This
 * module is (1) and (3); the live operations are injected as a per-phase factory because the
 * ruling requires the plan to be RE-MATERIALIZED per phase (the first Worker post consumed the
 * transfer list, so no in-memory plan survives) -- each phase builds its own operations against
 * the durable plan_json, one commit gate per phase.
 *
 * Every branch is resume-safe and matches the ledger's state machine exactly:
 *
 *  - advanced  -> advance the cursor (MONOTONIC, so a stale runner cannot rewind progress);
 *  - declined  -> terminal declined, free (guard-first means nothing was written, and a guard-2
 *                 decline has already removed its un-consumed archive row inside the operation);
 *  - failed    -> terminal failed. A sha256 conflict lands here and NO delete follows, which is
 *                 the invariant's last line of defence;
 *  - yielded   -> the loser of a concurrent per-phase claim re-queues quietly. A stop/start
 *                 overlap is the normal deploy sequence here, so a lost claim must never record
 *                 last_error or count toward failure (takeover-safety condition 3);
 *  - complete  -> done, recording whether the session itself was fully deleted (diagnostic only;
 *                 whole-session deletion is the norm and the watermark FK cascades with it).
 */
import type { DatabaseSync } from "node:sqlite";
import {
  advanceReclamationLedgerStep,
  completeReclamationLedgerEntry,
  declineReclamationLedgerEntry,
  failReclamationLedgerEntry,
  type ReclamationLedgerEntry,
  yieldReclamationLedgerEntry,
} from "../../state/openclaw-agent-reclamation-queue.js";
import {
  failReclamationLedgerCompletion,
  settleReclamationLedgerCompletion,
} from "./session-accessor.sqlite-reclamation-completion.js";
import {
  type ReclamationPhase,
  type ReclamationPhaseOperations,
  reclamationPhaseForStepCursor,
  runSqliteSessionReclamationPhase,
} from "./session-accessor.sqlite-reclamation-phases.js";

export type ReclamationEntryExecutionResult =
  | { kind: "done"; sessionFullyDeleted: boolean }
  | { kind: "declined"; phase: ReclamationPhase; reason: string }
  | { error: unknown; kind: "failed"; phase: ReclamationPhase }
  | { kind: "yielded"; phase: ReclamationPhase };

/**
 * Builds the operations for one phase against the live primitives. Called once per phase (not
 * once per entry) because the plan is re-materialized from plan_json at every phase boundary.
 */
export type ReclamationPhaseOperationsFactory = (params: {
  entry: ReclamationLedgerEntry;
  phase: ReclamationPhase;
  stepCursor: number;
}) => ReclamationPhaseOperations;

/**
 * Drives one claimed ledger entry through every remaining phase until it reaches a terminal state.
 * Each phase boundary commits its own cursor, so an interruption at any point leaves the entry
 * resumable at exactly the phase that had not yet been made durable.
 */
export async function executeReclamationLedgerEntry(params: {
  createOperations: ReclamationPhaseOperationsFactory;
  database: DatabaseSync;
  entry: ReclamationLedgerEntry;
  /** True when the error is a lost concurrent claim, which must yield rather than fail. */
  isBenignYield?: (error: unknown) => boolean;
  now: () => number;
  /** Whether the delete phase removed the whole session; diagnostic, recorded on completion. */
  readSessionFullyDeleted?: () => boolean;
}): Promise<ReclamationEntryExecutionResult> {
  const { createOperations, database, entry } = params;
  const recordOutcome = (
    error: unknown,
    phase: ReclamationPhase,
  ): ReclamationEntryExecutionResult => {
    if (params.isBenignYield?.(error)) {
      yieldReclamationLedgerEntry(database, { id: entry.id, now: params.now() });
      return { kind: "yielded", phase };
    }
    failReclamationLedgerEntry(database, { error, id: entry.id, now: params.now() });
    // A failure is not a value: reject every in-process waiter so a call site awaiting the entry
    // never hangs on a terminal outcome (piece iv, part 2).
    failReclamationLedgerCompletion(database, entry.id, error);
    return { error, kind: "failed", phase };
  };

  let stepCursor = entry.stepCursor;
  for (;;) {
    const phase = reclamationPhaseForStepCursor(stepCursor);
    const operations = createOperations({ entry, phase, stepCursor });
    let outcome;
    try {
      outcome = await runSqliteSessionReclamationPhase({ operations, stepCursor });
    } catch (error) {
      // Errors from the guard, the delete or complete escape the runner (only the archive-append
      // phase is mapped inside it), so a lost claim surfaces here.
      return recordOutcome(error, phase);
    }
    if (outcome.kind === "advanced") {
      advanceReclamationLedgerStep(database, {
        id: entry.id,
        now: params.now(),
        stepCursor: outcome.stepCursor,
      });
      stepCursor = outcome.stepCursor;
      continue;
    }
    if (outcome.kind === "declined") {
      declineReclamationLedgerEntry(database, {
        id: entry.id,
        now: params.now(),
        reason: outcome.phase + ":" + outcome.reason,
      });
      // A decline RESOLVES the signal, it does not reject it (the ruling has said so three times).
      // Nothing was deleted, and the historical-generation site reports this upward as a mismatch,
      // which its caller already understands.
      settleReclamationLedgerCompletion(database, entry.id, {
        archivedTranscripts: [],
        deleted: false,
        expectedEntryMismatch: true,
      });
      return { kind: "declined", phase: outcome.phase, reason: outcome.reason };
    }
    if (outcome.kind === "failed") {
      return recordOutcome(outcome.error, outcome.phase);
    }
    const sessionFullyDeleted = params.readSessionFullyDeleted?.() ?? false;
    completeReclamationLedgerEntry(database, {
      id: entry.id,
      now: params.now(),
      sessionFullyDeleted,
    });
    // Deliver the values the atomic path used to return synchronously to any in-process waiter.
    // They are not durable: a later pull, or a waiter registered after the row went terminal,
    // resolves from the ledger row instead and gets an empty archivedTranscripts by construction.
    settleReclamationLedgerCompletion(database, entry.id, {
      archivedTranscripts: operations.readDeletedArchivedTranscripts?.() ?? [],
      deleted: sessionFullyDeleted,
    });
    return { kind: "done", sessionFullyDeleted };
  }
}
