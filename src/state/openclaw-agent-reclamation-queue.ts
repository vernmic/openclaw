import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
} from "../infra/kysely-sync.js";
import type { OpenClawAgentDatabase } from "./openclaw-agent-db.js";

export type ReclamationJob = {
  id: string;
  session_id: string;
  generation: string;
  source_path: string;
  source_identity: string;
  source_birthtime: string | null;
  archive_directory: string;
  expected_archive_sha256: string;
  owner_id: string;
  state: "pending" | "done" | "declined" | "failed";
  step_cursor: number;
  plan_json: string;
  last_error: string | null;
  updated_at: number;
};
export function reclamationDb(database: Pick<OpenClawAgentDatabase, "db">) {
  return getNodeSqliteKysely<{ session_reclamation_jobs: ReclamationJob }>(database.db);
}
export function readReclamationJob(database: Pick<OpenClawAgentDatabase, "db">, id: string) {
  return executeSqliteQueryTakeFirstSync(
    database.db,
    reclamationDb(database).selectFrom("session_reclamation_jobs").selectAll().where("id", "=", id),
  );
}
export function readPendingReclamationJobs(
  database: Pick<OpenClawAgentDatabase, "db">,
  afterId = "",
) {
  return executeSqliteQuerySync(
    database.db,
    reclamationDb(database)
      .selectFrom("session_reclamation_jobs")
      .selectAll()
      .where("state", "=", "pending")
      .where("id", ">", afterId)
      .orderBy("id")
      .limit(64),
  ).rows;
}
/** A claim is an exact-owner CAS, never an elapsed-time or heartbeat inference. */
export function claimReclamationJob(
  database: Pick<OpenClawAgentDatabase, "db">,
  id: string,
  expectedOwner: string,
  owner: string,
): boolean {
  return (
    executeSqliteQuerySync(
      database.db,
      reclamationDb(database)
        .updateTable("session_reclamation_jobs")
        .set({ owner_id: owner })
        .where("id", "=", id)
        .where("state", "=", "pending")
        .where("owner_id", "=", expectedOwner),
    ).numAffectedRows === 1n
  );
}
export function archiveHeldByReclamation(
  database: Pick<OpenClawAgentDatabase, "db">,
  sessionId: string,
  generation: string,
): boolean {
  return (
    executeSqliteQueryTakeFirstSync(
      database.db,
      reclamationDb(database)
        .selectFrom("session_reclamation_jobs")
        .select("id")
        .where("session_id", "=", sessionId)
        .where("generation", "=", generation)
        .where("state", "=", "pending")
        .limit(1),
    ) !== undefined
  );
}
