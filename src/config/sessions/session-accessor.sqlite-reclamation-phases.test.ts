import { describe, expect, it } from "vitest";
import { ReclamationArchiveConflictError } from "./session-accessor.sqlite-reclamation-archive.js";
import {
  type ReclamationGuardOutcome,
  ReclamationPhaseDeclinedError,
  type ReclamationPhaseOperations,
  reclamationPhaseForStepCursor,
  runSqliteSessionReclamationPhase,
} from "./session-accessor.sqlite-reclamation-phases.js";

function fakeOperations(params: { appendThrows?: unknown; guard?: ReclamationGuardOutcome } = {}): {
  calls: string[];
  operations: ReclamationPhaseOperations;
} {
  const calls: string[] = [];
  const operations: ReclamationPhaseOperations = {
    appendArchive: () => {
      calls.push("appendArchive");
      if (params.appendThrows !== undefined) {
        throw params.appendThrows;
      }
    },
    complete: () => {
      calls.push("complete");
    },
    deleteReclaimedRows: () => {
      calls.push("deleteReclaimedRows");
    },
    evaluateGuard: () => {
      calls.push("guard");
      return params.guard ?? { passed: true };
    },
    removeArchiveForDecline: () => {
      calls.push("removeArchiveForDecline");
    },
  };
  return { calls, operations };
}

describe("reclamation four-phase runner", () => {
  it("runs the guard as phase 1 and advances to the guard cursor", async () => {
    const { calls, operations } = fakeOperations();
    await expect(runSqliteSessionReclamationPhase({ operations, stepCursor: 0 })).resolves.toEqual({
      kind: "advanced",
      phase: "guard",
      stepCursor: 1,
    });
    expect(calls).toEqual(["guard"]);
  });

  it("declines at the guard without touching the archive or the delete", async () => {
    const { calls, operations } = fakeOperations({
      guard: { passed: false, reason: "target-mismatch" },
    });
    await expect(runSqliteSessionReclamationPhase({ operations, stepCursor: 0 })).resolves.toEqual({
      cleanupArchive: false,
      kind: "declined",
      phase: "guard",
      reason: "target-mismatch",
    });
    expect(calls).toEqual(["guard"]);
  });

  it("appends the archive as phase 2 and advances to the delete boundary", async () => {
    const { calls, operations } = fakeOperations();
    await expect(runSqliteSessionReclamationPhase({ operations, stepCursor: 1 })).resolves.toEqual({
      kind: "advanced",
      phase: "archive-append",
      stepCursor: 2,
    });
    expect(calls).toEqual(["appendArchive"]);
  });

  it("maps a phase-2 snapshot decline to a terminal declined without deleting", async () => {
    const { calls, operations } = fakeOperations({
      appendThrows: new ReclamationPhaseDeclinedError("target-mismatch"),
    });
    await expect(runSqliteSessionReclamationPhase({ operations, stepCursor: 1 })).resolves.toEqual({
      cleanupArchive: false,
      kind: "declined",
      phase: "archive-append",
      reason: "target-mismatch",
    });
    // The snapshot decline precedes persistSessionTranscriptArchive, so no cleanup is owed.
    expect(calls).toEqual(["appendArchive"]);
  });

  it("maps a phase-2 sha256 conflict to a terminal failure with no delete", async () => {
    const { calls, operations } = fakeOperations({
      appendThrows: new ReclamationArchiveConflictError("stored != planned"),
    });
    const outcome = await runSqliteSessionReclamationPhase({ operations, stepCursor: 1 });
    expect(outcome.kind).toBe("failed");
    expect(outcome).toMatchObject({ phase: "archive-append" });
    if (outcome.kind === "failed") {
      expect(outcome.error).toBeInstanceOf(ReclamationArchiveConflictError);
    }
    expect(calls).toEqual(["appendArchive"]);
  });

  it("re-checks the guard before deleting and advances on a pass", async () => {
    const { calls, operations } = fakeOperations();
    await expect(runSqliteSessionReclamationPhase({ operations, stepCursor: 2 })).resolves.toEqual({
      kind: "advanced",
      phase: "batch-delete",
      stepCursor: 3,
    });
    expect(calls).toEqual(["guard", "deleteReclaimedRows"]);
  });

  it("removes the un-consumed archive when guard-2 declines, and never deletes", async () => {
    const { calls, operations } = fakeOperations({
      guard: { passed: false, reason: "recent-activity" },
    });
    await expect(runSqliteSessionReclamationPhase({ operations, stepCursor: 2 })).resolves.toEqual({
      cleanupArchive: true,
      kind: "declined",
      phase: "batch-delete",
      reason: "recent-activity",
    });
    expect(calls).toEqual(["guard", "removeArchiveForDecline"]);
  });

  it("completes from the delete cursor without replaying earlier phases", async () => {
    const { calls, operations } = fakeOperations();
    await expect(runSqliteSessionReclamationPhase({ operations, stepCursor: 3 })).resolves.toEqual({
      kind: "complete",
    });
    expect(calls).toEqual(["complete"]);
  });

  it("treats a cursor at or past complete as an idempotent completion", async () => {
    const { calls, operations } = fakeOperations();
    await expect(runSqliteSessionReclamationPhase({ operations, stepCursor: 4 })).resolves.toEqual({
      kind: "complete",
    });
    expect(calls).toEqual(["complete"]);
  });

  it("maps a committed cursor to its next phase", () => {
    expect([0, 1, 2, 3, 4].map(reclamationPhaseForStepCursor)).toEqual([
      "guard",
      "archive-append",
      "batch-delete",
      "complete",
      "complete",
    ]);
  });
});
