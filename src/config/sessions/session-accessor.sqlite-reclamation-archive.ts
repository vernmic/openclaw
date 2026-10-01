/**
 * Phase 2 (ARCHIVE-APPEND) reconciliation for the reclamation phase split.
 *
 * The ledger's plan_json carries archive METADATA only -- name, createdAt, encoding, sha256 --
 * never the blob, because the first Worker post consumes the bytes' transfer list. So phase 2
 * re-derives the bytes from transcript_events and reconciles them against the durable row.
 *
 * Two outcomes are load-bearing for the invariant (an event may be deleted only if its archive
 * is durably committed):
 *
 *  - A PRIMARY KEY conflict whose stored sha256 matches the plan is "already done": advance.
 *  - A sha256 MISMATCH is terminal. It throws, and the caller must not delete.
 *
 * Re-materialization recomputes createdAt (Date.now() inside the archive Worker), so the derived
 * archive_name differs from the row a first attempt wrote while the sha256 stays stable. Persist's
 * full-row compare would read that legitimate re-run as a conflict, turning the crash-matrix
 * "resume at step 2" into a terminal failure. The fresh bytes are therefore persisted under the
 * STORED metadata, which makes a re-run byte-and-row idempotent.
 */
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
} from "../../infra/kysely-sync.js";
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import { ensureSessionTranscriptArchiveSchema } from "../../state/openclaw-agent-session-transcript-archive-schema.js";
import { persistSessionTranscriptArchive } from "./session-accessor.sqlite-archive-store.js";
import {
  hashSessionArchiveBytes,
  type MaterializedSessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
import {
  RECLAMATION_PHASE_CURSORS,
  type ReclamationArchiveMetadata,
} from "./session-accessor.sqlite-reclamation-plan.js";
import { getSessionKysely } from "./session-accessor.sqlite-scope.js";

/**
 * A stored archive that does not match the plan. Terminal: the caller marks the entry `failed`
 * and NEVER advances to a delete -- this is the invariant's last line of defence.
 */
export class ReclamationArchiveConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReclamationArchiveConflictError";
  }
}

export function readReclamationArchiveSha256(
  database: OpenClawAgentDatabase,
  params: { generation: string; sessionId: string },
): string | undefined {
  ensureSessionTranscriptArchiveSchema(database.db);
  const db = getSessionKysely(database.db);
  return executeSqliteQueryTakeFirstSync(
    database.db,
    db
      .selectFrom("session_transcript_archives")
      .select("archive_sha256")
      .where("session_id", "=", params.sessionId)
      .where("generation", "=", params.generation),
  )?.archive_sha256;
}

/**
 * Reconciles one re-materialized archive with the durable row for its (session_id, generation).
 * Returns "already-archived" when a matching row exists (the crash-resume case) and "archived"
 * when this call inserted it. Throws ReclamationArchiveConflictError on any sha256 mismatch --
 * the caller maps that to a terminal `failed` ledger state and must not delete.
 */
export function reconcileReclamationArchive(params: {
  database: OpenClawAgentDatabase;
  plan: MaterializedSessionStateDeletePlan;
  stored: ReclamationArchiveMetadata;
}): "already-archived" | "archived" {
  const { database, plan, stored } = params;
  const generation = plan.snapshot.generation;
  if (!generation) {
    throw new Error(`Reclamation archive plan is missing a generation for ${plan.sessionId}`);
  }
  const existing = readReclamationArchiveSha256(database, {
    generation,
    sessionId: plan.sessionId,
  });
  if (existing !== undefined) {
    if (existing !== stored.sha256) {
      throw new ReclamationArchiveConflictError(
        `Reclamation archive conflict for ${plan.sessionId}: stored ${existing} != planned ${stored.sha256}`,
      );
    }
    return "already-archived";
  }
  const archive = plan.archive;
  if (!archive) {
    throw new Error(`Reclamation archive bytes are missing for ${plan.sessionId}`);
  }
  const derived = hashSessionArchiveBytes(archive.bytes);
  if (derived !== stored.sha256) {
    throw new ReclamationArchiveConflictError(
      `Reclamation archive sha256 mismatch for ${plan.sessionId}: derived ${derived} != planned ${stored.sha256}`,
    );
  }
  persistSessionTranscriptArchive(database, {
    ...plan,
    archive: {
      archiveName: stored.archiveName,
      bytes: archive.bytes,
      createdAt: stored.createdAt,
      encoding: stored.encoding,
      sha256: stored.sha256,
    },
  });
  return "archived";
}

/**
 * Removes an un-consumed step-2 archive row when a later guard declines (takeover-safety
 * condition 2). The cleanup is gated on the cursor: once the entry has crossed the batch-delete
 * boundary its archive row is evidence, not garbage, and is kept. SQLite serializes write
 * transactions, so running this in the SAME transaction as the decline means the bound reads
 * either "no delete committed yet" (safe) or "delete committed" (cursor advanced, refuse).
 */
export function removeReclamationArchiveForDecline(params: {
  database: OpenClawAgentDatabase;
  generation: string;
  sessionId: string;
  stepCursor: number;
}): boolean {
  if (params.stepCursor >= RECLAMATION_PHASE_CURSORS["batch-delete"]) {
    return false;
  }
  ensureSessionTranscriptArchiveSchema(params.database.db);
  const db = getSessionKysely(params.database.db);
  const result = executeSqliteQuerySync(
    params.database.db,
    db
      .deleteFrom("session_transcript_archives")
      .where("session_id", "=", params.sessionId)
      .where("generation", "=", params.generation),
  );
  return Number(result.numAffectedRows ?? 0n) > 0;
}
