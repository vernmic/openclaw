import { executeSqliteQueryTakeFirstSync } from "../../infra/kysely-sync.js";
import {
  openOpenClawAgentDatabase,
  type OpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import {
  getSessionKysely,
  resolveSqliteTranscriptScope,
  toDatabaseOptions,
  type ResolvedTranscriptScope,
} from "./session-accessor.sqlite-scope.js";
import { readMessageIdempotencyKey } from "./session-accessor.sqlite-transcript-store.js";
import { sessionTranscriptIndexNeedsReconcile } from "./session-transcript-index.js";
import type { TranscriptEntryAnchor } from "./transcript-entry-anchor.js";

/** Reads one active message identity from the caller's current SQLite transaction. */
export function readActiveTranscriptEntryAnchorInTransaction(params: {
  database: Pick<OpenClawAgentDatabase, "db" | "path">;
  resolved: ResolvedTranscriptScope;
  entryId: string;
  message?: unknown;
}): TranscriptEntryAnchor | undefined {
  // Branch changes retain old projection rows until deferred reconciliation.
  // An anchor must never certify those rows as the current active path.
  if (sessionTranscriptIndexNeedsReconcile(params.database.db, params.resolved.sessionId)) {
    return undefined;
  }
  const db = getSessionKysely(params.database.db);
  const row = executeSqliteQueryTakeFirstSync(
    params.database.db,
    db
      .selectFrom("transcript_event_identities as identity")
      .innerJoin("session_transcript_active_events as active", (join) =>
        join
          .onRef("active.session_id", "=", "identity.session_id")
          .onRef("active.event_seq", "=", "identity.seq"),
      )
      .innerJoin("transcript_rewrite_watermarks as rewrite", (join) =>
        join.onRef("rewrite.session_id", "=", "identity.session_id"),
      )
      .select([
        "identity.seq",
        "identity.parent_id",
        "identity.message_idempotency_key",
        "active.message_position",
        "rewrite.generation",
      ])
      .where("identity.session_id", "=", params.resolved.sessionId)
      .where("identity.event_id", "=", params.entryId)
      .limit(1),
  );
  if (row?.message_position === null || row?.message_position === undefined) {
    return undefined;
  }
  const idempotencyKey = row.message_idempotency_key ?? readMessageIdempotencyKey(params.message);
  return Object.freeze({
    agentId: params.resolved.agentId,
    sessionId: params.resolved.sessionId,
    sessionKey: params.resolved.sessionKey,
    storePath: params.database.path,
    generation: row.generation,
    entryId: params.entryId,
    rawSeq: row.seq,
    effectiveParentId: row.parent_id,
    activeMessagePosition: row.message_position,
    ...(idempotencyKey ? { idempotencyKey } : {}),
  });
}

/** Reads one active message identity from the authoritative SQLite projection. */
export function readActiveTranscriptEntryAnchor(params: {
  agentId?: string;
  sessionId: string;
  sessionKey: string;
  storePath?: string;
  entryId: string;
}): TranscriptEntryAnchor | undefined {
  const resolved = resolveSqliteTranscriptScope(params);
  const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
  return readActiveTranscriptEntryAnchorInTransaction({
    database,
    resolved,
    entryId: params.entryId,
  });
}

/**
 * Fallback terminal-anchor resolution (2026-09-22, TERMINAL-ANCHOR-CACHE-TTL-FINDING +
 * CLAUDE-REVIEW-terminal-anchor): the transcript leaf can be a non-message record appended
 * after the terminal message (openclaw.cache-ttl pruning records, openclaw.nested-tool.v1,
 * lifecycle markers), which the leaf-based lookup cannot anchor. SELECT - never walk - the
 * newest active MESSAGE from the projection: the newest session_transcript_active_events
 * row with a non-null message_position, same identity/watermark joins as the primary
 * lookup, and the same dirty-reconcile guard. Returns undefined when the projection is
 * dirty or holds no message rows.
 */
export function readNewestActiveMessageTranscriptEntryAnchorInTransaction(params: {
  database: Pick<OpenClawAgentDatabase, "db" | "path">;
  resolved: ResolvedTranscriptScope;
}): TranscriptEntryAnchor | undefined {
  if (sessionTranscriptIndexNeedsReconcile(params.database.db, params.resolved.sessionId)) {
    return undefined;
  }
  const db = getSessionKysely(params.database.db);
  const row = executeSqliteQueryTakeFirstSync(
    params.database.db,
    db
      .selectFrom("session_transcript_active_events as active")
      .innerJoin("transcript_event_identities as identity", (join) =>
        join
          .onRef("identity.session_id", "=", "active.session_id")
          .onRef("identity.seq", "=", "active.event_seq"),
      )
      .innerJoin("transcript_rewrite_watermarks as rewrite", (join) =>
        join.onRef("rewrite.session_id", "=", "active.session_id"),
      )
      .select([
        "identity.seq",
        "identity.event_id",
        "identity.parent_id",
        "identity.message_idempotency_key",
        "active.message_position",
        "rewrite.generation",
      ])
      .where("active.session_id", "=", params.resolved.sessionId)
      .where("active.message_position", "is not", null)
      .orderBy("active.event_seq", "desc")
      .limit(1),
  );
  if (!row?.message_position) {
    return undefined;
  }
  return Object.freeze({
    agentId: params.resolved.agentId,
    sessionId: params.resolved.sessionId,
    sessionKey: params.resolved.sessionKey,
    storePath: params.database.path,
    generation: row.generation,
    entryId: row.event_id,
    rawSeq: row.seq,
    effectiveParentId: row.parent_id,
    activeMessagePosition: row.message_position,
    ...(row.message_idempotency_key ? { idempotencyKey: row.message_idempotency_key } : {}),
  });
}

/** Reads the newest active message identity from the authoritative SQLite projection. */
export function readNewestActiveMessageTranscriptEntryAnchor(params: {
  agentId?: string;
  sessionId: string;
  sessionKey: string;
  storePath?: string;
}): TranscriptEntryAnchor | undefined {
  const resolved = resolveSqliteTranscriptScope(params);
  const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
  return readNewestActiveMessageTranscriptEntryAnchorInTransaction({
    database,
    resolved,
  });
}
