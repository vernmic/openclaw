import path from "node:path";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import type { MaterializedSessionStateDeletePlan } from "./session-accessor.sqlite-archive-types.js";
import type {
  ReclamationDatabaseOptions,
  SqliteSessionReclamationPlan,
} from "./session-accessor.sqlite-lifecycle-types.js";

export type ResumableReclamationPlan = Extract<
  SqliteSessionReclamationPlan,
  { kind: "history-eviction" | "historical-generation" }
>;

/** Persist comparisons, never process capabilities, database locators, environment or byte buffers. */
export function serializeReclamationPlan(plan: ResumableReclamationPlan): string {
  const {
    databaseOptions: _databaseOptions,
    descendantRunBasis: _descendants,
    maintenanceRunBasis: _maintenance,
    reclamation: _reclamation,
    ...facts
  } = plan;
  const durableFacts =
    facts.kind === "historical-generation"
      ? {
          ...facts,
          deleteParams: {
            ...facts.deleteParams,
            storePath: undefined,
            expectedDatabaseIdentity: undefined,
          },
        }
      : facts;
  return JSON.stringify({
    ...durableFacts,
    materializedPlans: plan.materializedPlans.map((item) => ({
      ...item,
      databasePath: undefined,
      archiveDirectory: undefined,
      archive: null,
      archivedTranscript: null,
    })),
  });
}

function isSnapshot(value: unknown): boolean {
  if (!isRecord(value)) {
    return false;
  }
  return (
    [
      "acpParentStreamEventCount",
      "lastSeq",
      "sessionUpdatedAt",
      "trajectoryLastSeq",
      "transcriptUpdatedAt",
    ].every(
      (key) =>
        value[key] === null ||
        (typeof value[key] === "number" && Number.isSafeInteger(value[key]) && value[key] >= 0),
    ) &&
    (value.generation === null || typeof value.generation === "string") &&
    (value.sessionKey === null || typeof value.sessionKey === "string")
  );
}

/** JSON is untrusted comparison data; all locators come from the current admitted instance. */
export function deserializeReclamationPlan(
  json: string,
  databaseOptions: ReclamationDatabaseOptions,
  archiveDirectory: string,
): ResumableReclamationPlan {
  const value: unknown = JSON.parse(json);
  if (
    !isRecord(value) ||
    (value.kind !== "history-eviction" && value.kind !== "historical-generation") ||
    typeof value.sessionId !== "string" ||
    !value.sessionId ||
    !Array.isArray(value.protectedSessionIds) ||
    !value.protectedSessionIds.every((id) => typeof id === "string") ||
    !Array.isArray(value.materializedPlans) ||
    value.materializedPlans.length !== 1
  ) {
    throw new Error("Invalid reclamation recovery plan");
  }
  const materialized = value.materializedPlans[0];
  if (
    !isRecord(materialized) ||
    materialized.sessionId !== value.sessionId ||
    materialized.agentId !== databaseOptions.agentId ||
    typeof materialized.archiveTranscript !== "boolean" ||
    (materialized.reason !== "deleted" && materialized.reason !== "reset") ||
    !isSnapshot(materialized.snapshot) ||
    materialized.archive !== null ||
    materialized.archivedTranscript !== null
  ) {
    throw new Error("Invalid reclamation recovery snapshot");
  }
  if (value.kind === "history-eviction") {
    if (
      !isRecord(value.diskBudget) ||
      (value.diskBudget.preserveRecentMs !== undefined &&
        value.diskBudget.preserveRecentMs !== null &&
        (typeof value.diskBudget.preserveRecentMs !== "number" ||
          !Number.isFinite(value.diskBudget.preserveRecentMs)))
    ) {
      throw new Error("Invalid reclamation disk budget");
    }
  } else if (
    !isRecord(value.deleteParams) ||
    typeof value.deleteParams.archiveTranscript !== "boolean" ||
    !isRecord(value.deleteParams.target) ||
    typeof value.deleteParams.target.canonicalKey !== "string" ||
    !Array.isArray(value.deleteParams.target.storeKeys) ||
    !value.deleteParams.target.storeKeys.every((key) => typeof key === "string") ||
    !Array.isArray(value.preparedTargetSnapshot) ||
    !value.preparedTargetSnapshot.every(
      (row) =>
        isRecord(row) &&
        typeof row.sessionKey === "string" &&
        isRecord(row.entry) &&
        typeof row.entry.sessionId === "string" &&
        typeof row.entry.updatedAt === "number",
    )
  ) {
    throw new Error("Invalid historical reclamation comparison");
  }
  // The validated JSON contains comparison facts only. Native equality checks, current live
  // admission and transaction-held generation/reference checks still authorize every deletion.
  const plan = value as ResumableReclamationPlan;
  plan.databaseOptions = databaseOptions;
  plan.materializedPlans = [
    {
      ...materialized,
      databasePath: databaseOptions.path,
      archiveDirectory: path.resolve(archiveDirectory),
    } as MaterializedSessionStateDeletePlan,
  ];
  if (plan.kind === "historical-generation") {
    plan.deleteParams.storePath = databaseOptions.path;
  }
  return plan;
}
