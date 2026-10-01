/**
 * Durable reclamation plan codec and the four-phase model (phase-split piece c, part 1).
 *
 * The ledger's plan_json is the source of truth across phases; the in-memory plan is an
 * optimization. Materialized archive BYTES never enter plan_json -- the first Worker post
 * consumes their transfer list -- so this codec stores the un-materialized plan structure
 * plus per-plan archive METADATA only. Phase 2 re-derives the bytes from transcript_events
 * and re-verifies them against the stored sha256 before any delete.
 */
import type { SessionStateDeletePlan } from "./session-accessor.sqlite-archive.js";
import type { SqliteLifecycleTargetSnapshot } from "./session-accessor.sqlite-entry-equality.js";
import type { SessionEntryRemovalPlan } from "./session-accessor.sqlite-lifecycle-types.js";
import type { SqliteSessionReclamationPlan } from "./session-accessor.sqlite-reclamation.js";

/**
 * The four phases (Claude's 2026-10-01 ruling). The conditional watermark flip that once
 * occupied a fifth step is withdrawn: whole-session deletion cascades the
 * transcript_rewrite_watermarks FK, so `session_fully_deleted` stays diagnostic-only.
 */
export const RECLAMATION_PHASES = ["guard", "archive-append", "batch-delete", "complete"] as const;

export type ReclamationPhase = (typeof RECLAMATION_PHASES)[number];

/** step_cursor values are phase boundaries, committed only once the phase is durable. */
export const RECLAMATION_PHASE_CURSORS: Readonly<Record<ReclamationPhase, number>> = {
  "archive-append": 2,
  "batch-delete": 3,
  complete: 4,
  guard: 1,
};

/** Archive identity is durable metadata; the blob is re-derived per phase. */
export type ReclamationArchiveMetadata = {
  archiveName: string;
  createdAt: number;
  encoding: "identity" | "zstd";
  sha256: string;
};

/** One delete plan with its archive reduced to metadata (never bytes). */
export type SerializedSessionStateDeletePlan = SessionStateDeletePlan & {
  archive: ReclamationArchiveMetadata | null;
};

type SerializedPlanBase = {
  databaseOptions: SqliteSessionReclamationPlan["databaseOptions"];
  materializedPlans: SerializedSessionStateDeletePlan[];
};

/** JSON-safe mirror of SqliteSessionReclamationPlan for the ledger's plan_json column. */
export type SerializedSqliteSessionReclamationPlan =
  | (SerializedPlanBase & {
      deleteParams: Extract<SqliteSessionReclamationPlan, { kind: "entry" }>["deleteParams"];
      kind: "entry";
      preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
    })
  | (SerializedPlanBase & {
      agentId: string;
      entries: SessionEntryRemovalPlan[];
      kind: "lifecycle-artifacts";
    })
  | (SerializedPlanBase & {
      diskBudget: { preserveRecentMs?: number | null };
      kind: "history-eviction";
      protectedSessionIds: string[];
      sessionId: string;
    })
  | (SerializedPlanBase & {
      deleteParams: Extract<
        SqliteSessionReclamationPlan,
        { kind: "historical-generation" }
      >["deleteParams"];
      kind: "historical-generation";
      preparedTargetSnapshot: SqliteLifecycleTargetSnapshot;
      protectedSessionIds: string[];
      sessionId: string;
    });

type ReclamationPlanArchive = NonNullable<
  SqliteSessionReclamationPlan["materializedPlans"][number]["archive"]
>;

function toReclamationArchiveMetadata(
  archive: ReclamationPlanArchive | null,
): ReclamationArchiveMetadata | null {
  return archive
    ? {
        archiveName: archive.archiveName,
        createdAt: archive.createdAt,
        encoding: archive.encoding,
        sha256: archive.sha256,
      }
    : null;
}

function toSerializedDeletePlans(
  plans: SqliteSessionReclamationPlan["materializedPlans"],
): SerializedSessionStateDeletePlan[] {
  return plans.map((plan) => ({
    agentId: plan.agentId,
    archive: toReclamationArchiveMetadata(plan.archive),
    archiveDirectory: plan.archiveDirectory,
    archiveTranscript: plan.archiveTranscript,
    databasePath: plan.databasePath,
    reason: plan.reason,
    sessionId: plan.sessionId,
    snapshot: plan.snapshot,
  }));
}

/** Reduces a live plan to its durable JSON form, dropping every archive byte buffer. */
export function serializeSqliteSessionReclamationPlan(plan: SqliteSessionReclamationPlan): string {
  const materializedPlans = toSerializedDeletePlans(plan.materializedPlans);
  switch (plan.kind) {
    case "entry":
      return JSON.stringify({
        databaseOptions: plan.databaseOptions,
        deleteParams: plan.deleteParams,
        kind: plan.kind,
        materializedPlans,
        preparedTargetSnapshot: plan.preparedTargetSnapshot,
      });
    case "lifecycle-artifacts":
      return JSON.stringify({
        agentId: plan.agentId,
        databaseOptions: plan.databaseOptions,
        entries: plan.entries,
        kind: plan.kind,
        materializedPlans,
      });
    case "history-eviction":
      return JSON.stringify({
        databaseOptions: plan.databaseOptions,
        diskBudget: plan.diskBudget,
        kind: plan.kind,
        materializedPlans,
        protectedSessionIds: plan.protectedSessionIds,
        sessionId: plan.sessionId,
      });
    case "historical-generation":
      return JSON.stringify({
        databaseOptions: plan.databaseOptions,
        deleteParams: plan.deleteParams,
        kind: plan.kind,
        materializedPlans,
        preparedTargetSnapshot: plan.preparedTargetSnapshot,
        protectedSessionIds: plan.protectedSessionIds,
        sessionId: plan.sessionId,
      });
  }
}

const RECLAMATION_PLAN_KINDS = new Set<string>([
  "entry",
  "historical-generation",
  "history-eviction",
  "lifecycle-artifacts",
]);

/** Validates the durable shape before a phase trusts it. Throws on malformed input. */
export function deserializeSqliteSessionReclamationPlan(
  planJson: string,
): SerializedSqliteSessionReclamationPlan {
  const parsed: unknown = JSON.parse(planJson);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Reclamation plan JSON is not an object");
  }
  const kind = (parsed as { kind?: unknown }).kind;
  if (typeof kind !== "string" || !RECLAMATION_PLAN_KINDS.has(kind)) {
    throw new Error("Reclamation plan JSON has an unknown kind");
  }
  const plans = (parsed as { materializedPlans?: unknown }).materializedPlans;
  if (!Array.isArray(plans)) {
    throw new Error("Reclamation plan JSON is missing materializedPlans");
  }
  for (const planValue of plans) {
    if (!planValue || typeof planValue !== "object" || Array.isArray(planValue)) {
      throw new Error("Reclamation plan JSON has an invalid delete plan");
    }
    if (typeof (planValue as { sessionId?: unknown }).sessionId !== "string") {
      throw new Error("Reclamation plan JSON delete plan is missing a sessionId");
    }
  }
  return parsed as SerializedSqliteSessionReclamationPlan;
}
