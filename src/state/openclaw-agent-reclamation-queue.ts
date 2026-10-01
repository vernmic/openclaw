import type { DatabaseSync } from "node:sqlite";
import { sql as kyselySql } from "kysely";
import { executeSqliteQuerySync, getNodeSqliteKysely } from "../infra/kysely-sync.js";
import type { DB as OpenClawAgentKyselyDatabase } from "./openclaw-agent-db.generated.js";
import { ensureReclamationLedgerSchema } from "./openclaw-agent-reclamation-ledger-schema.js";

export const RECLAMATION_LEDGER_TABLE = "reclamation_ledger";

/** The terminal cursor value: step 5 COMPLETE. */
export const RECLAMATION_FINAL_STEP_CURSOR = 5;

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

/**
 * Enqueues one ledger entry. Re-planning a session MUST pass the generation already
 * recorded for that session; the UNIQUE(session_id, generation) key then makes the
 * insert a no-op and duplicate archives impossible by construction.
 */
export function enqueueReclamationLedgerEntry(
  database: DatabaseSync,
  params: { generation: string; now: number; planJson: string; sessionId: string },
): { id: number; inserted: boolean } {
  ensureReclamationLedgerSchema(database);
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
      .onConflict((conflict) => conflict.columns(["session_id", "generation"]).doNothing()),
  );
  const row = executeSqliteQuerySync(
    database,
    db
      .selectFrom(RECLAMATION_LEDGER_TABLE)
      .select("id")
      .where("session_id", "=", params.sessionId)
      .where("generation", "=", params.generation),
  ).rows[0];
  if (!row) {
    throw new Error("Reclamation ledger enqueue lost its entry");
  }
  return { id: row.id, inserted: Number(inserted.numAffectedRows ?? 0n) === 1 };
}

/** Reuses the generation already recorded for a session, or mints nothing and returns undefined. */
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
      .orderBy("created_at", "asc")
      .orderBy("id", "asc")
      .limit(1),
  ).rows[0]?.generation;
}

function selectClaimableEntry(database: DatabaseSync, excluded: readonly number[]) {
  const db = ledgerDb(database);
  let query = db
    .selectFrom(RECLAMATION_LEDGER_TABLE)
    .selectAll()
    .where("state", "not in", [...TERMINAL_RECLAMATION_LEDGER_STATES]);
  if (excluded.length > 0) {
    query = query.where("id", "not in", [...excluded]);
  }
  return executeSqliteQuerySync(
    database,
    query
      /* kysely-allow-raw: in-progress-first pull order per the reclamation queue spec. */
      .orderBy(kyselySql`(step_cursor > 0 OR state = 'running') DESC`)
      .orderBy("created_at", "asc")
      .orderBy("id", "asc")
      .limit(1),
  ).rows[0];
}

/**
 * Atomically claims the next claimable entry: an UPDATE gated on state<>'running', so a
 * queued row elects exactly one concurrent winner and attempts increments once. A row a
 * runner already holds cannot be re-claimed; a claim that loses the race advances to the
 * next candidate instead of stalling the queue.
 *
 * KNOWN GAP (flagged for review): an entry left state='running' by a crashed process is
 * never re-claimed by this predicate, so requirement 5's "grab the interrupted entry and
 * resume at its step" is NOT satisfiable without distinguishing a dead holder from a live
 * one. That needs an owner+expiry (a lease) or a takeover rule, neither of which this
 * schema carries. Reported to Vern/Claude rather than worked around here.
 */
export function claimNextReclamationLedgerEntry(
  database: DatabaseSync,
  params: { now: number },
): ReclamationLedgerEntry | undefined {
  ensureReclamationLedgerSchema(database);
  const db = ledgerDb(database);
  const excluded: number[] = [];
  for (;;) {
    const candidate = selectClaimableEntry(database, excluded);
    if (!candidate) {
      return undefined;
    }
    const claimed = executeSqliteQuerySync(
      database,
      db
        .updateTable(RECLAMATION_LEDGER_TABLE)
        .set({
          /* kysely-allow-raw: atomic attempts increment, spec-mandated claim shape. */
          attempts: kyselySql`attempts + 1`,
          state: "running",
          updated_at: params.now,
        })
        .where("id", "=", candidate.id)
        .where("state", "<>", "running"),
    );
    if (Number(claimed.numAffectedRows ?? 0n) === 1) {
      return toLedgerEntry({
        ...candidate,
        attempts: candidate.attempts + 1,
        state: "running",
      });
    }
    excluded.push(candidate.id);
  }
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

/** Records a completed phase boundary. The cursor is committed only after the phase is durable. */
export function advanceReclamationLedgerStep(
  database: DatabaseSync,
  params: { id: number; now: number; stepCursor: number },
): void {
  const db = ledgerDb(database);
  executeSqliteQuerySync(
    database,
    db
      .updateTable(RECLAMATION_LEDGER_TABLE)
      .set({ last_error: null, step_cursor: params.stepCursor, updated_at: params.now })
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

  return {
    isRunning: () => running,
    push: () => {
      if (!running) {
        startDrain();
      }
    },
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
