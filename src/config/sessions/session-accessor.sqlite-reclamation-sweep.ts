/**
 * The reclamation orphan-sweep recovery drive (phase-split piece iv-4).
 *
 * The queue runner (openclaw-agent-reclamation-queue.ts) owns the in-process drain and the
 * no-lost-wakeup stop; the phase executor (session-accessor.sqlite-reclamation-executor.ts) owns
 * the cursor state machine; the operations factory binds each phase to the live primitives. This
 * module is the missing PRODUCTION composition for the RECOVERY path: it turns one claimed ledger
 * entry into a completed drive without a caller's in-hand plan.
 *
 * Why a separate composition from the inline drive (session-accessor.sqlite-reclamation-phased.ts):
 * the inline drive is the caller's own fresh entry, so phase 2 READS the caller's pre-materialized
 * bytes (materialize-once, piece iv-3). A recovery driver did not enqueue the entry -- it found it
 * on disk after a crash -- so it has no in-hand plan and re-derives the archive bytes from the
 * durable plan_json, exactly what the transfer-list rule reserves re-materialization for. Both
 * routes share the same executor and the same live delete binding; only the materialization source
 * differs, which is the distinction the operations factory already models (inlineMaterializedPlans
 * absent => re-derive).
 *
 * Boot semantics (spec v2 runner lifecycle, Vern #3477): ON STARTUP THE RUNNER DEFAULTS TO
 * RUNNING. The boot pull IS the crash recovery -- interrupted entries sort first, idempotent
 * takeover applies, and resume-at-cursor proceeds with zero special boot code. An empty queue at
 * any pull is a no-op and the runner shuts down; there is no timer, no lease, no liveness
 * inference. The sweep is per-agent-DB and started only by the gateway process; two runners may
 * share the DB across a stop/start overlap and the terminal ledger row is the shared record of who
 * finished.
 */
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import {
  claimNextReclamationLedgerEntry,
  createReclamationLedgerRunner,
  hasClaimableReclamationLedgerEntry,
  type ReclamationLedgerEntry,
  type ReclamationLedgerRunner,
} from "../../state/openclaw-agent-reclamation-queue.js";
import { executeReclamationLedgerEntry } from "./session-accessor.sqlite-reclamation-executor.js";
import {
  createLiveReclamationDeleteExecutor,
  isReclamationSessionFullyDeleted,
} from "./session-accessor.sqlite-reclamation-live.js";
import { createReclamationPhaseOperationsFactory } from "./session-accessor.sqlite-reclamation-operations.js";

export type ReclamationOrphanSweepErrorHandler = (
  error: unknown,
  entry?: ReclamationLedgerEntry,
) => void;

/**
 * Builds the per-agent-DB orphan-sweep runner: a normal queue drain whose processEntry drives a
 * recovered entry through the four phases via the live delete binding and the durable plan_json.
 * Idempotent takeover makes the claim safe against the live inline drive or another process's
 * sweep; step idempotency (PK archive, idempotent delete, conditional watermark) makes the resume
 * safe.
 */
export function createReclamationOrphanSweepRunner(params: {
  database: OpenClawAgentDatabase;
  now?: () => number;
  onError?: ReclamationOrphanSweepErrorHandler;
}): ReclamationLedgerRunner {
  const now = params.now ?? Date.now;
  return createReclamationLedgerRunner({
    claimNext: () => claimNextReclamationLedgerEntry(params.database.db, { now: now() }),
    hasPending: () => hasClaimableReclamationLedgerEntry(params.database.db),
    onError: params.onError,
    processEntry: async (entry) => {
      await executeReclamationLedgerEntry({
        createOperations: createReclamationPhaseOperationsFactory({
          database: params.database,
          deleteReclaimedRows: createLiveReclamationDeleteExecutor(),
        }),
        database: params.database.db,
        entry,
        now,
        readSessionFullyDeleted: () =>
          isReclamationSessionFullyDeleted(params.database, entry.sessionId),
      });
    },
  });
}

/**
 * One boot pull for a single agent database: start the drain (which is the crash recovery) and
 * resolve once it has drained to empty. An empty queue is a no-op; the runner shuts itself down.
 * This is the unit the gateway boot sidecar calls once per configured agent database.
 */
export async function sweepReclamationOrphansForAgentDatabase(params: {
  database: OpenClawAgentDatabase;
  now?: () => number;
  onError?: ReclamationOrphanSweepErrorHandler;
}): Promise<void> {
  const runner = createReclamationOrphanSweepRunner(params);
  runner.start();
  await runner.whenIdle();
}
