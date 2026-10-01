/**
 * The reclamation guard (phase 1 of the phase split).
 *
 * Both legitimate target declines live here, evaluated BEFORE any write so a decline is free:
 *
 *  - the prepared target snapshot no longer matches the live target snapshot, or
 *  - the entry no longer satisfies shouldDeleteSqliteSessionEntryLifecycle (an expected
 *    entry / session / revision / updated-at / transcript expectation moved).
 *
 * The old atomic in-transaction path and the phase-split factory share this one predicate, so
 * the guard the ledger's phase 1 re-evaluates is byte-for-byte the guard the atomic path used.
 * Both call sites evaluate it at the head of their write transaction, so the world cannot move
 * between the check and the mutation it protects.
 */
import { executeSqliteQuerySync } from "../../infra/kysely-sync.js";
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import type { DeleteSessionEntryLifecycleParams } from "./session-accessor.sqlite-contract.js";
import {
  sqliteLifecycleTargetSnapshotsEqual,
  sqliteSessionEntriesEqual,
  type SqliteLifecycleTargetSnapshot,
} from "./session-accessor.sqlite-entry-equality.js";
import { readLifecycleTargetSnapshot } from "./session-accessor.sqlite-entry-store.js";
import { getSessionKysely } from "./session-accessor.sqlite-scope.js";
import type { InternalSessionEntry as SessionEntry } from "./types.js";

export function shouldDeleteSqliteSessionEntryLifecycle(
  database: OpenClawAgentDatabase,
  entry: SessionEntry | undefined,
  params: DeleteSessionEntryLifecycleParams,
): entry is SessionEntry {
  if (!entry || (params.expectedEntry && !sqliteSessionEntriesEqual(entry, params.expectedEntry))) {
    return false;
  }
  if (
    params.expectedSessionId !== undefined &&
    (params.expectedSessionId === null
      ? entry.sessionId !== undefined
      : entry.sessionId !== params.expectedSessionId)
  ) {
    return false;
  }
  if (
    (params.expectedLifecycleRevision !== undefined &&
      entry.lifecycleRevision !== params.expectedLifecycleRevision) ||
    (params.expectedUpdatedAt !== undefined && entry.updatedAt !== params.expectedUpdatedAt)
  ) {
    return false;
  }
  const expectedTranscript = params.expectedTranscript;
  if (!expectedTranscript) {
    return true;
  }
  const rows = executeSqliteQuerySync(
    database.db,
    getSessionKysely(database.db)
      .selectFrom("transcript_events")
      .select("event_json")
      .where("session_id", "=", expectedTranscript.sessionId)
      .orderBy("seq", "asc"),
  ).rows;
  return (
    entry.sessionId === expectedTranscript.sessionId &&
    rows.length === expectedTranscript.eventJson.length &&
    rows.every((row, index) => row.event_json === expectedTranscript.eventJson[index])
  );
}

/** The guard's verdict plus the snapshot it read, so the delete phase can reuse it. */
export type SqliteReclamationTargetGuard =
  | { passed: true; snapshot: SqliteLifecycleTargetSnapshot }
  | { passed: false };

/**
 * Re-evaluates the two target declines against the live database and returns the snapshot it
 * read. The phase split calls this at the head of the guard phase and again at the head of the
 * batch-delete phase (guard-2); both run before any write, so a decline writes nothing.
 */
export function evaluateSqliteReclamationTargetGuard(
  database: OpenClawAgentDatabase,
  plan: {
    deleteParams: DeleteSessionEntryLifecycleParams;
    preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
  },
): SqliteReclamationTargetGuard {
  const snapshot = readLifecycleTargetSnapshot(database, plan.deleteParams.target);
  if (
    !sqliteLifecycleTargetSnapshotsEqual(plan.preparedTargetSnapshot, snapshot) ||
    !shouldDeleteSqliteSessionEntryLifecycle(database, snapshot[0]?.entry, plan.deleteParams)
  ) {
    return { passed: false };
  }
  return { passed: true, snapshot };
}
