import type { DatabaseSync } from "node:sqlite";
import { sql as kyselySql } from "kysely";
import { executeSqliteQuerySync, getNodeSqliteKysely } from "../infra/kysely-sync.js";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DB as OpenClawAgentKyselyDatabase } from "./openclaw-agent-db.generated.js";
import { ensureReclamationLedgerSchema } from "./openclaw-agent-reclamation-ledger-schema.js";

export const RECLAMATION_LEDGER_TABLE = "reclamation_ledger";

/**
 * The terminal cursor value: phase 4 COMPLETE. The conditional watermark step that once
 * occupied a fifth cursor is withdrawn (Claude's 2026-10-01 ruling), so the four phases
 * land on cursors 1..4.
 */
export const RECLAMATION_FINAL_STEP_CURSOR = 4;

/** Terminal states never re-enter the pull; the queue ignores them forever. */
export const TERMINAL_RECLAMATION_LEDGER_STATES = ["done", "failed", "declined"] as const;

export type ReclamationLedgerState = "queued" | "running" | "done" | "failed" | "declined";

export type ReclamationLedgerEntry = {
  attempts: number;
  generation: string;
  id: number;
  lastError: string | null;
  planJson: string;
  sessionFullyDeleted: boolean;
  sessionId: string;
  state: ReclamationLedgerState;
  stepCursor: number;
};

type LedgerKysely = Pick<OpenClawAgentKyselyDatabase, "reclamation_ledger">;

function ledgerDb(database: DatabaseSync) {
  return getNodeSqliteKysely<LedgerKysely>(database);
}

function toLedgerEntry(row: {
  attempts: number;
  generation: string;
  id: number;
  last_error: string | null;
  plan_json: string;
  session_fully_deleted: number;
  session_id: string;
  state: string;
  step_cursor: number;
}): ReclamationLedgerEntry {
  return {
    attempts: row.attempts,
    generation: row.generation,
    id: row.id,
    lastError: row.last_error,
    planJson: row.plan_json,
    sessionFullyDeleted: row.session_fully_deleted === 1,
    sessionId: row.session_id,
    state: row.state as ReclamationLedgerState,
    stepCursor: row.step_cursor,
  };
}

export type ReclamationLedgerEnqueue = {
  generation: string;
  planJson: string;
  sessionId: string;
};

/**
 * Inserts one ledger row. Dedup is per (session, generation) among LIVE entries only (the partial
 * index `idx_agent_reclamation_ledger_live`), so re-enqueueing a session already in flight is a
 * no-op while a re-plan after a terminal outcome inserts a fresh row -- terminal rows accumulate as
 * append-only history (Claude's 2026-10-01 enqueue/re-plan ruling). The conflict target is
 * deliberately omitted: a targetless `ON CONFLICT DO NOTHING` is the only form SQLite accepts
 * against a partial index, and the ledger's sole unique constraint is that index.
 */
function insertReclamationLedgerEntry(
  database: DatabaseSync,
  params: { generation: string; now: number; planJson: string; sessionId: string },
): { id: number; inserted: boolean } {
  const db = ledgerDb(database);
  const inserted = executeSqliteQuerySync(
    database,
    db
      .insertInto(RECLAMATION_LEDGER_TABLE)
      .values({
        created_at: params.now,
        generation: params.generation,
        plan_json: params.planJson,
        session_id: params.sessionId,
        updated_at: params.now,
      })
      .onConflict((conflict) => conflict.doNothing()),
  );
  // The partial index guarantees at most one LIVE row for the pair, so this read is deterministic:
  // it is either the row just inserted or the in-flight row that blocked the insert.
  const row = executeSqliteQuerySync(
    database,
    db
      .selectFrom(RECLAMATION_LEDGER_TABLE)
      .select("id")
      .where("session_id", "=", params.sessionId)
      .where("generation", "=", params.generation)
      .where("state", "not in", [...TERMINAL_RECLAMATION_LEDGER_STATES]),
  ).rows[0];
  if (!row) {
    throw new Error("Reclamation ledger enqueue lost its entry");
  }
  return { id: row.id, inserted: Number(inserted.numAffectedRows ?? 0n) === 1 };
}

/** Enqueues one ledger entry. */
export function enqueueReclamationLedgerEntry(
  database: DatabaseSync,
  params: { generation: string; now: number; planJson: string; sessionId: string },
): { id: number; inserted: boolean } {
  ensureReclamationLedgerSchema(database);
  return insertReclamationLedgerEntry(database, params);
}

/**
 * Fan-out enqueue: one per-session entry per reclaimed session, all-or-nothing in a single
 * transaction (Claude's 2026-10-01 ruling item 1). A crash mid-fan-out would otherwise queue only
 * part of the set, and nothing in the queue would know the set was incomplete.
 */
export function enqueueReclamationLedgerEntries(
  database: DatabaseSync,
  params: { entries: ReadonlyArray<ReclamationLedgerEnqueue>; now: number },
): Array<{ id: number; inserted: boolean }> {
  ensureReclamationLedgerSchema(database);
  return runSqliteImmediateTransactionSync(database, () =>
    params.entries.map((entry) =>
      insertReclamationLedgerEntry(database, { ...entry, now: params.now }),
    ),
  );
}

/**
 * Returns the generation most recently recorded for a session, or undefined. Terminal rows are
 * append-only, so a session may now hold several ledger rows; the newest row carries the most
 * recent generation observed for it (reclamation never rotates the generation -- only a transcript
 * rewrite does, and that always moves it forward). This drops the old single-row-per-pair
 * assumption (Claude's 2026-10-01 enqueue/re-plan ruling).
 */
export function findReclamationLedgerGeneration(
  database: DatabaseSync,
  sessionId: string,
): string | undefined {
  ensureReclamationLedgerSchema(database);
  const db = ledgerDb(database);
  return executeSqliteQuerySync(
    database,
    db
      .selectFrom(RECLAMATION_LEDGER_TABLE)
      .select("generation")
      .where("session_id", "=", sessionId)
      .orderBy("created_at", "desc")
      .orderBy("id", "desc")
      .limit(1),
  ).rows[0]?.generation;
}

function selectClaimableEntry(database: DatabaseSync) {
  const db = ledgerDb(database);
  return executeSqliteQuerySync(
    database,
    db
      .selectFrom(RECLAMATION_LEDGER_TABLE)
      .selectAll()
      .where("state", "not in", [...TERMINAL_RECLAMATION_LEDGER_STATES])
      /* kysely-allow-raw: in-progress-first pull order per the reclamation queue spec. */
      .orderBy(kyselySql`(step_cursor > 0 OR state = 'running') DESC`)
      .orderBy("created_at", "asc")
      .orderBy("id", "asc")
      .limit(1),
  ).rows[0];
}

/**
 * Idempotent-takeover claim (spec v2, review addendum 2026-10-01 — option 2). The UPDATE is
 * unconditional: a claim is a per-pull attempt, not ownership, so a row a crashed process left
 * state='running' is claimable by the next runner and requirement 5 (resume at step) holds.
 * Leases were rejected because lease expiry is timer-based liveness inference (idle != alive),
 * the same anti-pattern that caused the 2026-09-29 eviction incident. Safety comes from step
 * idempotency plus the four takeover-safety conditions, not from single ownership.
 *
 * Two consequences, both intended:
 * - Two runners sharing one DB (the normal stop/start deploy overlap) may BOTH claim the same
 *   entry; the ledger reaches `done` exactly once because COMPLETE is idempotent.
 * - `attempts` increments per pull, so a flapping restart inflates it. Nothing promotes a high
 *   `attempts` count to `failed` on its own — the condition that makes this safe.
 */
export function claimNextReclamationLedgerEntry(
  database: DatabaseSync,
  params: { now: number },
): ReclamationLedgerEntry | undefined {
  ensureReclamationLedgerSchema(database);
  const db = ledgerDb(database);
  const candidate = selectClaimableEntry(database);
  if (!candidate) {
    return undefined;
  }
  executeSqliteQuerySync(
    database,
    db
      .updateTable(RECLAMATION_LEDGER_TABLE)
      .set({
        /* kysely-allow-raw: per-pull attempts increment on the unconditional takeover claim. */
        attempts: kyselySql`attempts + 1`,
        state: "running",
        updated_at: params.now,
      })
      .where("id", "=", candidate.id),
  );
  return toLedgerEntry({ ...candidate, attempts: candidate.attempts + 1, state: "running" });
}

/** True when a row matching the pull predicate exists; a cheap re-check for the runner stop step. */
export function hasClaimableReclamationLedgerEntry(database: DatabaseSync): boolean {
  ensureReclamationLedgerSchema(database);
  const db = ledgerDb(database);
  return (
    executeSqliteQuerySync(
      database,
      db
        .selectFrom(RECLAMATION_LEDGER_TABLE)
        .select("id")
        .where("state", "not in", [...TERMINAL_RECLAMATION_LEDGER_STATES])
        .limit(1),
    ).rows.length > 0
  );
}

/**
 * Records a completed phase boundary. The cursor is committed only after the phase is durable,
 * and it is MONOTONIC: a stale runner finishing a phase late can never move the cursor backwards
 * past another runner's progress (takeover-safety condition 1).
 */
export function advanceReclamationLedgerStep(
  database: DatabaseSync,
  params: { id: number; now: number; stepCursor: number },
): void {
  const db = ledgerDb(database);
  executeSqliteQuerySync(
    database,
    db
      .updateTable(RECLAMATION_LEDGER_TABLE)
      .set({
        last_error: null,
        /* kysely-allow-raw: monotonic step cursor; a stale runner must not rewind progress. */
        step_cursor: kyselySql`MAX(step_cursor, ${params.stepCursor})`,
        updated_at: params.now,
      })
      .where("id", "=", params.id),
  );
}

export function completeReclamationLedgerEntry(
  database: DatabaseSync,
  params: { id: number; now: number; sessionFullyDeleted: boolean },
): void {
  const db = ledgerDb(database);
  executeSqliteQuerySync(
    database,
    db
      .updateTable(RECLAMATION_LEDGER_TABLE)
      .set({
        last_error: null,
        session_fully_deleted: params.sessionFullyDeleted ? 1 : 0,
        state: "done",
        step_cursor: RECLAMATION_FINAL_STEP_CURSOR,
        updated_at: params.now,
      })
      .where("id", "=", params.id),
  );
}

export function failReclamationLedgerEntry(
  database: DatabaseSync,
  params: { error?: unknown; id: number; now: number },
): void {
  const db = ledgerDb(database);
  executeSqliteQuerySync(
    database,
    db
      .updateTable(RECLAMATION_LEDGER_TABLE)
      .set({
        last_error: params.error === undefined ? null : String(params.error),
        state: "failed",
        updated_at: params.now,
      })
      .where("id", "=", params.id),
  );
}

export function declineReclamationLedgerEntry(
  database: DatabaseSync,
  params: { id: number; now: number; reason: string },
): void {
  const db = ledgerDb(database);
  executeSqliteQuerySync(
    database,
    db
      .updateTable(RECLAMATION_LEDGER_TABLE)
      .set({ last_error: params.reason, state: "declined", updated_at: params.now })
      .where("id", "=", params.id),
  );
}

/**
 * Benign yield for a phase that lost a concurrent claim (takeover-safety condition 3). Under
 * idempotent takeover the loser of a per-phase claim re-queues quietly, so the normal stop/start
 * deploy overlap never records last_error or a failed count against a healthy entry. `attempts`
 * is deliberately untouched — the takeover already counted its own pull.
 */
export function yieldReclamationLedgerEntry(
  database: DatabaseSync,
  params: { id: number; now: number },
): void {
  const db = ledgerDb(database);
  executeSqliteQuerySync(
    database,
    db
      .updateTable(RECLAMATION_LEDGER_TABLE)
      .set({ state: "queued", updated_at: params.now })
      .where("id", "=", params.id),
  );
}

export function readReclamationLedgerEntry(
  database: DatabaseSync,
  id: number,
): ReclamationLedgerEntry | undefined {
  ensureReclamationLedgerSchema(database);
  const db = ledgerDb(database);
  const row = executeSqliteQuerySync(
    database,
    db.selectFrom(RECLAMATION_LEDGER_TABLE).selectAll().where("id", "=", id),
  ).rows[0];
  return row ? toLedgerEntry(row) : undefined;
}

export type ReclamationLedgerRunner = {
  isRunning: () => boolean;
  push: () => void;
  /** Gateway-boot trigger: one pull, then shut down if the queue is empty. */
  start: () => void;
  whenIdle: () => Promise<void>;
};

/**
 * In-process singleton drain with no lost wakeup.
 *
 * push(): starts a drain if none is running; while one is running the owed work is picked
 * up by the loop's next claim. The drain concludes in ONE synchronous guarded step: the
 * running flag clears, the queue is re-checked with the flag already clear, and a non-empty
 * queue re-arms instead of stopping. That closes the window between a drain's emptiness
 * read and the flag clear, where a racing push would otherwise be stranded.
 */
export function createReclamationLedgerRunner(params: {
  claimNext: () => ReclamationLedgerEntry | undefined;
  hasPending: () => boolean;
  onError?: (error: unknown, entry?: ReclamationLedgerEntry) => void;
  processEntry: (entry: ReclamationLedgerEntry) => Promise<void> | void;
}): ReclamationLedgerRunner {
  let running = false;
  let idleResolvers: Array<() => void> = [];

  const settleIdle = (): void => {
    const resolvers = idleResolvers;
    idleResolvers = [];
    for (const resolve of resolvers) {
      resolve();
    }
  };

  const drainLoop = async (): Promise<void> => {
    try {
      for (;;) {
        const entry = params.claimNext();
        if (!entry) {
          running = false;
          if (!params.hasPending()) {
            return;
          }
          running = true;
          continue;
        }
        try {
          await params.processEntry(entry);
        } catch (error) {
          params.onError?.(error, entry);
        }
      }
    } catch (error) {
      params.onError?.(error);
    } finally {
      running = false;
      settleIdle();
    }
  };

  const startDrain = (): void => {
    running = true;
    void drainLoop();
  };

  const ensureRunning = (): void => {
    if (!running) {
      startDrain();
    }
  };

  return {
    isRunning: () => running,
    // Gateway boot and any enqueue both funnel into the same guarded drain, so this costs one
    // extra pull attempt per process lifetime and nothing when the queue is empty.
    start: ensureRunning,
    push: ensureRunning,
    whenIdle: () => {
      if (!running) {
        return Promise.resolve();
      }
      return new Promise<void>((resolve) => {
        idleResolvers.push(resolve);
      });
    },
  };
}
