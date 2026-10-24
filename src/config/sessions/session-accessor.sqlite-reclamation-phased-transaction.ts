import path from "node:path";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
} from "../../infra/kysely-sync.js";
import { readOpenClawAgentDatabaseIdentity } from "../../state/openclaw-agent-db-identity.js";
import {
  runOpenClawAgentWriteTransaction,
  type OpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import {
  claimReclamationJob,
  readPendingReclamationJobs,
  readReclamationJob,
  reclamationDb,
  type ReclamationJob,
} from "../../state/openclaw-agent-reclamation-queue.js";
import { hashSessionArchiveBytes } from "./session-accessor.sqlite-archive-artifact.js";
import { persistSessionTranscriptArchive } from "./session-accessor.sqlite-archive-store-kernel.js";
import type { MaterializedSessionStateDeletePlan } from "./session-accessor.sqlite-archive-types.js";
import {
  readSessionStateDeleteSnapshot,
  sqliteSessionStateDeleteSnapshotsEqual,
} from "./session-accessor.sqlite-delete-snapshot.js";
import { readValidatedSessionDeletionTarget } from "./session-accessor.sqlite-deletion-plan.js";
import {
  deleteMaterializedSessionStatePlans,
  readReferencedSessionIds,
} from "./session-accessor.sqlite-lifecycle-state.js";
import type {
  ReclamationPhaseReceipt,
  SqliteSessionReclamationCallbacks,
  SqliteSessionReclamationPlan,
  SqliteSessionReclamationResult,
} from "./session-accessor.sqlite-lifecycle-types.js";
import {
  deserializeReclamationPlan,
  serializeReclamationPlan,
  type ResumableReclamationPlan,
} from "./session-accessor.sqlite-reclamation-plan.js";
import { isRecentHistoricalSessionId } from "./session-accessor.sqlite-references.js";
import { getSessionKysely } from "./session-accessor.sqlite-scope.js";

function receipt(row: ReclamationJob): ReclamationPhaseReceipt {
  return {
    id: row.id,
    ownerId: row.owner_id,
    cursor: row.step_cursor,
    state: row.state,
    error: row.last_error,
  };
}
function result(
  plan: ResumableReclamationPlan,
  row: ReclamationJob,
  archivedTranscripts: NonNullable<MaterializedSessionStateDeletePlan["archivedTranscript"]>[] = [],
): SqliteSessionReclamationResult {
  const value = {
    archivedTranscripts,
    deleted: row.step_cursor >= 3 && row.state !== "failed",
    ...(plan.kind === "historical-generation" && row.state === "declined"
      ? { expectedEntryMismatch: true as const }
      : {}),
  };
  return { kind: plan.kind, value, reclamationReceipt: receipt(row) };
}
function validate(
  database: OpenClawAgentDatabase,
  plan: ResumableReclamationPlan,
): ReadonlySet<string> | false {
  const item = plan.materializedPlans[0];
  if (
    !item ||
    plan.protectedSessionIds.includes(plan.sessionId) ||
    !sqliteSessionStateDeleteSnapshotsEqual(
      readSessionStateDeleteSnapshot(database.db, plan.sessionId),
      item.snapshot,
    )
  ) {
    return false;
  }
  let excluded: ReadonlySet<string> | undefined;
  if (plan.kind === "historical-generation") {
    const current = readValidatedSessionDeletionTarget(database, {
      ...plan,
      scope: { kind: "historical-generation", phase: "commit", sessionId: plan.sessionId },
    });
    if (!current) {
      return false;
    }
    excluded = new Set([
      plan.deleteParams.target.canonicalKey,
      ...plan.deleteParams.target.storeKeys,
      ...current.snapshot.map((row) => row.sessionKey),
    ]);
  } else if (
    isRecentHistoricalSessionId({ database, ...plan.diskBudget, sessionId: plan.sessionId })
  ) {
    return false;
  }
  const referenced = readReferencedSessionIds(
    database,
    excluded,
    [plan.sessionId],
    plan.kind === "history-eviction" ? plan.diskBudget : undefined,
  );
  return referenced.has(plan.sessionId) ? false : (excluded ?? new Set());
}
function readArchivedPlan(
  database: OpenClawAgentDatabase,
  plan: ResumableReclamationPlan,
  expectedSha256: string,
): MaterializedSessionStateDeletePlan {
  const item = plan.materializedPlans[0];
  if (!item?.snapshot.generation) {
    throw new Error("Reclamation archive has no exact generation");
  }
  const stored = executeSqliteQueryTakeFirstSync(
    database.db,
    getSessionKysely(database.db)
      .selectFrom("session_transcript_archives")
      .selectAll()
      .where("session_id", "=", plan.sessionId)
      .where("generation", "=", item.snapshot.generation!),
  );
  if (
    !stored ||
    stored.archive_sha256 !== expectedSha256 ||
    hashSessionArchiveBytes(stored.archive_blob) !== stored.archive_sha256 ||
    stored.session_key !== item.snapshot.sessionKey ||
    (stored.encoding !== "identity" && stored.encoding !== "zstd") ||
    stored.reason !== item.reason
  ) {
    throw new Error("Reclamation canonical archive integrity mismatch");
  }
  return {
    ...item,
    archive: {
      archiveName: stored.archive_name,
      bytes: stored.archive_blob,
      createdAt: stored.created_at,
      encoding: stored.encoding,
      sha256: stored.archive_sha256,
    },
    archivedTranscript: {
      generation: stored.generation,
      sessionId: stored.session_id,
      sourcePath: path.join(item.archiveDirectory, `${item.sessionId}.jsonl`),
      archivedPath: path.join(item.archiveDirectory, stored.archive_name),
    },
  };
}

/** Each call is one ordinary native worker transaction, including its durable cursor receipt. */
export function reclaimResumableSessionInTransaction(
  plan:
    | ResumableReclamationPlan
    | Extract<SqliteSessionReclamationPlan, { kind: "reclamation-pending" }>,
  callbacks: SqliteSessionReclamationCallbacks,
): SqliteSessionReclamationResult {
  return runOpenClawAgentWriteTransaction(
    (database) => {
      callbacks.beforeMutation?.();
      const db = reclamationDb(database);
      if (plan.kind === "reclamation-pending") {
        if (plan.reject) {
          executeSqliteQuerySync(
            database.db,
            db
              .updateTable("session_reclamation_jobs")
              .set({
                state: plan.reject.state,
                last_error: plan.reject.error,
                updated_at: Date.now(),
              })
              .where("id", "=", plan.reject.id)
              .where("owner_id", "=", plan.reject.expectedOwnerId)
              .where("state", "=", "pending"),
          );
        }
        const value = readPendingReclamationJobs(database, plan.afterId).map((row) =>
          Object.assign({}, receipt(row), {
            planJson: row.plan_json,
            sourcePath: row.source_path,
            sourceIdentity: row.source_identity,
            sourceBirthtime: row.source_birthtime,
            archiveDirectory: row.archive_directory,
          }),
        );
        callbacks.onCommit?.(database);
        return { kind: plan.kind, value };
      }
      const request = plan.reclamation!;
      const item = plan.materializedPlans[0];
      if (!item || plan.materializedPlans.length !== 1) {
        throw new Error("Reclamation requires one exact session snapshot");
      }
      const identity = readOpenClawAgentDatabaseIdentity(database);
      let row = readReclamationJob(database, request.id);
      if (!row) {
        if (
          request.phase !== "guard" ||
          !item?.snapshot.generation ||
          plan.materializedPlans.length !== 1 ||
          !request.archiveSha256
        ) {
          throw new Error("Missing reclamation guard receipt");
        }
        executeSqliteQuerySync(
          database.db,
          db
            .insertInto("session_reclamation_jobs")
            .values({
              id: request.id,
              session_id: plan.sessionId,
              generation: item.snapshot.generation,
              source_path: identity.filename,
              source_identity: String(identity.identity),
              source_birthtime: identity.birthtime ?? null,
              archive_directory: path.resolve(item.archiveDirectory),
              expected_archive_sha256: request.archiveSha256,
              owner_id: request.ownerId,
              state: "pending",
              step_cursor: 1,
              plan_json: serializeReclamationPlan(plan),
              last_error: null,
              updated_at: Date.now(),
            })
            .onConflict((conflict) => conflict.doNothing()),
        );
        row = readReclamationJob(database, request.id);
        if (!row) {
          // A live drive already owns this immutable generation. It retains recovery responsibility.
          return { kind: plan.kind, value: { archivedTranscripts: [], deleted: false } };
        }
      }
      if (request.expectedOwnerId && row.state === "pending") {
        if (!claimReclamationJob(database, row.id, request.expectedOwnerId, request.ownerId)) {
          throw new Error("Reclamation recovery owner changed");
        }
        row = { ...row, owner_id: request.ownerId };
      }
      if (row.owner_id !== request.ownerId) {
        throw new Error("Reclamation phase lost its exact owner");
      }
      const update = (
        state: ReclamationJob["state"],
        cursor: number,
        error: string | null = null,
      ) => {
        const changed = executeSqliteQuerySync(
          database.db,
          db
            .updateTable("session_reclamation_jobs")
            .set({ state, step_cursor: cursor, last_error: error, updated_at: Date.now() })
            .where("id", "=", row!.id)
            .where("owner_id", "=", request.ownerId)
            .where("state", "=", "pending")
            .where("step_cursor", "=", row!.step_cursor),
        );
        if (changed.numAffectedRows !== 1n) {
          throw new Error("Reclamation phase cursor changed");
        }
        row = { ...row!, state, step_cursor: cursor, last_error: error };
      };
      let archives: NonNullable<MaterializedSessionStateDeletePlan["archivedTranscript"]>[] = [];
      if (row.state === "pending") {
        if (
          row.source_path !== identity.filename ||
          row.source_identity !== identity.identity ||
          row.source_birthtime !== (identity.birthtime ?? null) ||
          row.archive_directory !== path.resolve(item.archiveDirectory)
        ) {
          update(
            "declined",
            row.step_cursor,
            "Reclamation source or instance archive root changed; reconcile the copied ledger before retrying",
          );
        } else {
          let durable: ResumableReclamationPlan;
          try {
            durable = deserializeReclamationPlan(
              row.plan_json,
              plan.databaseOptions,
              item.archiveDirectory,
            );
          } catch (error) {
            update("failed", row.step_cursor, String(error));
            callbacks.onCommit?.(database);
            return result(plan, row);
          }
          if (
            durable.sessionId !== row.session_id ||
            durable.materializedPlans[0]?.snapshot.generation !== row.generation ||
            serializeReclamationPlan(plan) !== row.plan_json
          ) {
            update("failed", row.step_cursor, "Reclamation plan identity mismatch");
          } else if (row.step_cursor < 3 && validate(database, durable) === false) {
            update(
              "declined",
              row.step_cursor,
              "Reclamation snapshot, generation or current protection changed",
            );
          } else if (request.phase === "archive" && row.step_cursor === 1) {
            try {
              if (
                !item.archive ||
                item.archive.sha256 !== row.expected_archive_sha256 ||
                hashSessionArchiveBytes(item.archive.bytes) !== item.archive.sha256
              ) {
                throw new Error("Reclamation archive sha256 mismatch");
              }
              try {
                persistSessionTranscriptArchive(database, item);
              } catch {
                // Native persistence owns optional archive admission. A prior declined job may
                // have the same bytes under its already-stable name and creation timestamp.

                const canonical = readArchivedPlan(database, durable, row.expected_archive_sha256);
                if (
                  !canonical.archive ||
                  !Buffer.from(canonical.archive.bytes).equals(Buffer.from(item.archive.bytes))
                ) {
                  throw new Error("Reclamation archive byte identity mismatch");
                }
              }
              update("pending", 2);
            } catch (error) {
              update("failed", row.step_cursor, String(error));
            }
          } else if (request.phase === "delete" && row.step_cursor === 2) {
            let archived: MaterializedSessionStateDeletePlan;
            try {
              archived = readArchivedPlan(database, durable, row.expected_archive_sha256);
            } catch (error) {
              update("failed", row.step_cursor, String(error));
              callbacks.onCommit?.(database);
              return result(plan, row);
            }
            const excluded = validate(database, durable);
            if (excluded === false) {
              update("declined", row.step_cursor, "Reclamation protection changed before delete");
            } else {
              archives = deleteMaterializedSessionStatePlans(
                database,
                [archived],
                new Set(durable.protectedSessionIds),
                excluded,
                undefined,
                durable.kind === "history-eviction" ? durable.diskBudget : undefined,
              );
              const remains = executeSqliteQueryTakeFirstSync(
                database.db,
                getSessionKysely(database.db)
                  .selectFrom("session_windows")
                  .select("session_id")
                  .where("session_id", "=", durable.sessionId),
              );
              update(
                remains ? "declined" : "pending",
                remains ? 2 : 3,
                remains ? "Reclamation native owner preserved the generation" : null,
              );
            }
          } else if (request.phase === "complete" && row.step_cursor === 3) {
            update("done", 4);
          }
        }
      }
      if (row.step_cursor >= 3 && archives.length === 0) {
        try {
          const archived = readArchivedPlan(database, plan, row.expected_archive_sha256);
          if (archived.archivedTranscript) {
            archives = [archived.archivedTranscript];
          }
        } catch (error) {
          if (row.state === "pending") {
            update("failed", row.step_cursor, String(error));
          } else {
            throw error;
          }
        }
      }
      callbacks.onCommit?.(database);
      return result(plan, row, archives);
    },
    plan.databaseOptions,
    { operationLabel: "session.reclamation.phase" },
  );
}
