import { describe, expect, it, vi } from "vitest";
import type { ReclamationLedgerEntry } from "../../state/openclaw-agent-reclamation-queue.js";
import { SqliteDeleteSnapshotMovedError } from "./session-accessor.sqlite-lifecycle-state.js";
import type { ReclamationDeleteOutcome } from "./session-accessor.sqlite-reclamation-operations.js";
import { ReclamationPhaseDeclinedError } from "./session-accessor.sqlite-reclamation-phases.js";
import type { SerializedSqliteSessionReclamationPlan } from "./session-accessor.sqlite-reclamation-plan.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";

const reclaimMock = vi.hoisted(() => ({
  reclaimSqliteSessionInTransaction: vi.fn(),
}));

// The live executor is a thin binding; pin the shared transaction so the decline mapping is
// asserted without standing up a real reclaimable database.
vi.mock("./session-accessor.sqlite-reclamation.js", () => ({
  reclaimSqliteSessionInTransaction: reclaimMock.reclaimSqliteSessionInTransaction,
}));

const { createLiveReclamationDeleteExecutor, toDeleteOnlyReclamationPlan } =
  await import("./session-accessor.sqlite-reclamation-live.js");

function entryPlan(
  overrides: { kind?: "entry" | "historical-generation" | "lifecycle-artifacts" } = {},
): SerializedSqliteSessionReclamationPlan {
  return {
    databaseOptions: {
      agentId: "main",
      env: { OPENCLAW_STATE_DIR: "C:\\state" },
      path: "C:\\state\\agent.sqlite",
    },
    deleteParams: {
      deleteDeliveryArtifacts: false,
      target: { canonicalKey: "agent:main:s1", storeKeys: [] },
    },
    kind: overrides.kind ?? "historical-generation",
    ...(overrides.kind === "lifecycle-artifacts"
      ? { agentId: "main", entries: [] }
      : { preparedTargetSnapshot: [] }),
    materializedPlans: [
      {
        agentId: "main",
        archive: {
          archiveName: "archive.jsonl.zst",
          createdAt: 1_700_000_500_000,
          encoding: "zstd",
          sha256: "f".repeat(64),
        },
        archiveDirectory: "C:\\state\\archives",
        archiveTranscript: true,
        databasePath: "C:\\state\\agent.sqlite",
        reason: "deleted",
        sessionId: "s1",
        snapshot: { generation: GENERATION, sessionKey: "agent:main:s1" },
      },
    ],
  } as unknown as SerializedSqliteSessionReclamationPlan;
}

function ledgerEntry(plan: SerializedSqliteSessionReclamationPlan): ReclamationLedgerEntry {
  return {
    attempts: 1,
    generation: GENERATION,
    id: 7,
    lastError: null,
    planJson: JSON.stringify(plan),
    sessionFullyDeleted: false,
    sessionId: "s1",
    state: "running",
    stepCursor: 2,
  };
}

async function deleteOnce(
  plan: SerializedSqliteSessionReclamationPlan,
): Promise<ReclamationDeleteOutcome> {
  const executor = createLiveReclamationDeleteExecutor();
  return await executor({
    database: {} as never,
    entry: ledgerEntry(plan),
    phase: "batch-delete",
    plan,
    stepCursor: 2,
  });
}

describe("live reclamation delete binding (part 5)", () => {
  it("rebuilds the durable plan with the archive bytes dropped", () => {
    const live = toDeleteOnlyReclamationPlan(entryPlan());
    const [plan] = live.materializedPlans;
    expect(plan?.archive).toBeNull();
    expect(plan?.archivedTranscript).toBeNull();
    // Everything the shared per-kind transaction needs is preserved.
    expect(plan?.sessionId).toBe("s1");
    expect(plan?.archiveDirectory).toBe("C:\\state\\archives");
    expect(live.kind).toBe("historical-generation");
  });

  it("refuses entry and lifecycle-artifacts plans before touching the shared transaction", () => {
    expect(() => toDeleteOnlyReclamationPlan(entryPlan({ kind: "lifecycle-artifacts" }))).toThrow(
      /lifecycle-artifacts/,
    );
    // Ruling 1 (2026-10-01): entry keeps the atomic path; the split must never see it.
    expect(() => toDeleteOnlyReclamationPlan(entryPlan({ kind: "entry" }))).toThrow(/entry/);
  });

  it("hands the shared transaction a delete-only plan and completes on success", async () => {
    reclaimMock.reclaimSqliteSessionInTransaction.mockReset();
    reclaimMock.reclaimSqliteSessionInTransaction.mockReturnValue({
      kind: "historical-generation",
      value: { archivedTranscripts: [], deleted: true },
    });
    // Piece iv, part 2: the executor now reports the consumer value the atomic path returned,
    // so a waiting call site can be settled with it.
    await expect(deleteOnce(entryPlan())).resolves.toEqual({ archivedTranscripts: [] });
    const passed = reclaimMock.reclaimSqliteSessionInTransaction.mock.calls[0]?.[0] as {
      materializedPlans: Array<{ archive: unknown }>;
    };
    expect(passed.materializedPlans[0]?.archive).toBeNull();
  });

  it("surfaces the archived transcripts the shared transaction reports", async () => {
    const archived = [
      {
        archivedPath: "archives/one.zst",
        generation: GENERATION,
        sessionId: "s1",
        sourcePath: "transcripts/one.jsonl",
      },
    ];
    reclaimMock.reclaimSqliteSessionInTransaction.mockReset();
    reclaimMock.reclaimSqliteSessionInTransaction.mockReturnValue({
      kind: "historical-generation",
      value: { archivedTranscripts: archived, deleted: true },
    });
    await expect(deleteOnce(entryPlan())).resolves.toEqual({ archivedTranscripts: archived });
  });

  it("maps a target-snapshot mismatch to a normal decline", async () => {
    reclaimMock.reclaimSqliteSessionInTransaction.mockReset();
    reclaimMock.reclaimSqliteSessionInTransaction.mockReturnValue({
      kind: "historical-generation",
      value: { archivedTranscripts: [], deleted: false, expectedEntryMismatch: true },
    });
    await expect(deleteOnce(entryPlan())).rejects.toBeInstanceOf(ReclamationPhaseDeclinedError);
  });

  it("maps a moved delete snapshot to a normal decline", async () => {
    reclaimMock.reclaimSqliteSessionInTransaction.mockReset();
    reclaimMock.reclaimSqliteSessionInTransaction.mockImplementation(() => {
      throw new SqliteDeleteSnapshotMovedError("s1");
    });
    await expect(deleteOnce(entryPlan())).rejects.toBeInstanceOf(ReclamationPhaseDeclinedError);
  });

  it("rethrows a prose look-alike so a rollback failure never reads as a decline", async () => {
    // The typed error is the contract. A plain Error that merely quotes the old message text --
    // e.g. an aggregate pairing a rollback failure with a moved-snapshot cause -- must fail the
    // entry (replan-able only via a fresh re-plan), never be classified as a benign decline.
    reclaimMock.reclaimSqliteSessionInTransaction.mockReset();
    reclaimMock.reclaimSqliteSessionInTransaction.mockImplementation(() => {
      throw new Error("SQLite session state changed before deletion for s1");
    });
    await expect(deleteOnce(entryPlan())).rejects.toBeInstanceOf(Error);
    await expect(deleteOnce(entryPlan())).rejects.not.toBeInstanceOf(ReclamationPhaseDeclinedError);
  });

  it("rethrows an unrelated failure so the entry fails, never declines", async () => {
    reclaimMock.reclaimSqliteSessionInTransaction.mockReset();
    reclaimMock.reclaimSqliteSessionInTransaction.mockImplementation(() => {
      throw new Error("disk exploded");
    });
    await expect(deleteOnce(entryPlan())).rejects.toThrow("disk exploded");
  });
});
