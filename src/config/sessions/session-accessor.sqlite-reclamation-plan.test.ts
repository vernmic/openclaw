import { describe, expect, it } from "vitest";
import { RECLAMATION_FINAL_STEP_CURSOR } from "../../state/openclaw-agent-reclamation-queue.js";
import {
  RECLAMATION_PHASE_CURSORS,
  RECLAMATION_PHASES,
  deserializeSqliteSessionReclamationPlan,
  serializeSqliteSessionReclamationPlan,
} from "./session-accessor.sqlite-reclamation-plan.js";
import type { SqliteSessionReclamationPlan } from "./session-accessor.sqlite-reclamation.js";

type DeletePlanShape = ReturnType<typeof deletePlan>;

function deletePlan(sessionId: string, archiveBytes: Uint8Array | null) {
  return {
    agentId: "agent-1",
    archive: archiveBytes
      ? {
          archiveName: "archive.jsonl.zst",
          bytes: archiveBytes,
          createdAt: 1_700_000_000_000,
          encoding: "zstd" as const,
          sha256: "a".repeat(64),
        }
      : null,
    archiveDirectory: "C:\\state\\archives",
    archiveTranscript: archiveBytes !== null,
    archivedTranscript: null,
    databasePath: "C:\\state\\agent.sqlite",
    reason: "deleted" as const,
    sessionId,
    snapshot: {
      acpParentStreamEventCount: 0,
      generation: "b".repeat(32),
      lastSeq: 12,
      sessionKey: `key:${sessionId}`,
      sessionUpdatedAt: 1_700_000_000_000,
      trajectoryLastSeq: null,
      transcriptUpdatedAt: 1_700_000_000_000,
    },
  };
}

const databaseOptions = {
  agentId: "agent-1",
  env: { OPENCLAW_STATE_DIR: "C:\\state" },
  path: "C:\\state\\agent.sqlite",
};

function entryPlan(sessionId: string): SqliteSessionReclamationPlan {
  return {
    databaseOptions,
    deleteParams: {
      archiveTranscript: true,
      storePath: "C:\\state",
      target: { canonicalKey: "key", storeKeys: [] },
    },
    kind: "entry",
    materializedPlans: [deletePlan(sessionId, new Uint8Array([1, 2, 3, 4]))],
    preparedTargetSnapshot: [],
  } as unknown as SqliteSessionReclamationPlan;
}

describe("reclamation plan codec", () => {
  it("declares the four ruling phases in order", () => {
    expect([...RECLAMATION_PHASES]).toEqual([
      "guard",
      "archive-append",
      "batch-delete",
      "complete",
    ]);
  });

  it("numbers phase cursors monotonically from the guard boundary", () => {
    expect(RECLAMATION_PHASE_CURSORS).toEqual({
      "archive-append": 2,
      "batch-delete": 3,
      complete: 4,
      guard: 1,
    });
  });

  it("keeps the ledger terminal cursor aligned with the complete phase", () => {
    expect(RECLAMATION_FINAL_STEP_CURSOR).toBe(RECLAMATION_PHASE_CURSORS.complete);
  });

  it("drops archive bytes and keeps metadata for an entry plan", () => {
    const plan = entryPlan("session-1");
    const json = serializeSqliteSessionReclamationPlan(plan);
    const parsed = JSON.parse(json) as { materializedPlans: DeletePlanShape[] };
    const archive = parsed.materializedPlans[0]!.archive;
    expect(archive).toEqual({
      archiveName: "archive.jsonl.zst",
      createdAt: 1_700_000_000_000,
      encoding: "zstd",
      sha256: "a".repeat(64),
    });
    expect(archive).not.toHaveProperty("bytes");
    expect(json).not.toContain("AQIDBA");

    const codec = deserializeSqliteSessionReclamationPlan(json);
    expect(codec.kind).toBe("entry");
    expect(codec.materializedPlans[0]!.archive?.sha256).toBe("a".repeat(64));
    expect(codec.materializedPlans[0]!.snapshot.generation).toBe("b".repeat(32));
    expect(codec.materializedPlans[0]!.sessionId).toBe("session-1");
  });

  it("preserves a null archive without inventing metadata", () => {
    const plan = entryPlan("session-2");
    (plan.materializedPlans[0] as { archive: unknown }).archive = null;
    const codec = deserializeSqliteSessionReclamationPlan(
      serializeSqliteSessionReclamationPlan(plan),
    );
    expect(codec.materializedPlans[0]!.archive).toBeNull();
  });

  it("round-trips a history-eviction plan with its budget and protected ids", () => {
    const plan = {
      databaseOptions,
      diskBudget: { preserveRecentMs: 600_000 },
      kind: "history-eviction",
      materializedPlans: [deletePlan("session-3", new Uint8Array([9]))],
      protectedSessionIds: ["session-3", "session-9"],
      sessionId: "session-3",
    } as unknown as SqliteSessionReclamationPlan;
    const codec = deserializeSqliteSessionReclamationPlan(
      serializeSqliteSessionReclamationPlan(plan),
    );
    expect(codec.kind).toBe("history-eviction");
    expect(codec.protectedSessionIds).toEqual(["session-3", "session-9"]);
    expect(codec.sessionId).toBe("session-3");
  });

  it("round-trips a lifecycle-artifacts plan with its entries", () => {
    const plan = {
      agentId: "agent-1",
      databaseOptions,
      entries: [{ sessionKey: "key:4" }],
      kind: "lifecycle-artifacts",
      materializedPlans: [deletePlan("session-4", null)],
    } as unknown as SqliteSessionReclamationPlan;
    const codec = deserializeSqliteSessionReclamationPlan(
      serializeSqliteSessionReclamationPlan(plan),
    );
    expect(codec.kind).toBe("lifecycle-artifacts");
    expect(codec.entries).toEqual([{ sessionKey: "key:4" }]);
    expect(codec.materializedPlans[0]!.archive).toBeNull();
  });

  it("rejects malformed durable plans", () => {
    expect(() => deserializeSqliteSessionReclamationPlan("[]")).toThrow(/not an object/);
    expect(() =>
      deserializeSqliteSessionReclamationPlan('{"kind":"bogus","materializedPlans":[]}'),
    ).toThrow(/unknown kind/);
    expect(() => deserializeSqliteSessionReclamationPlan('{"kind":"entry"}')).toThrow(
      /missing materializedPlans/,
    );
    expect(() =>
      deserializeSqliteSessionReclamationPlan(
        '{"kind":"entry","materializedPlans":[{"archive":null}]}',
      ),
    ).toThrow(/missing a sessionId/);
  });
});
