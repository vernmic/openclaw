import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { ensureReclamationLedgerSchema } from "../../state/openclaw-agent-reclamation-ledger-schema.js";
import {
  advanceReclamationLedgerStep,
  claimNextReclamationLedgerEntry,
  enqueueReclamationLedgerEntry,
  hasClaimableReclamationLedgerEntry,
  readReclamationLedgerEntry,
  type ReclamationLedgerEntry,
} from "../../state/openclaw-agent-reclamation-queue.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "../../state/openclaw-agent-schema.js";
import { ReclamationArchiveConflictError } from "./session-accessor.sqlite-reclamation-archive.js";
import {
  executeReclamationLedgerEntry,
  type ReclamationPhaseOperationsFactory,
} from "./session-accessor.sqlite-reclamation-executor.js";
import { ReclamationPhaseDeclinedError } from "./session-accessor.sqlite-reclamation-phases.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";

type FakeState = {
  appendThrows?: unknown;
  calls: string[];
  deleteThrows?: unknown;
  guard2Pass?: boolean;
  guardPass?: boolean;
};

function openLedgerDatabase(): DatabaseSync {
  const database = new DatabaseSync(":memory:");
  database.exec(OPENCLAW_AGENT_SCHEMA_SQL);
  ensureReclamationLedgerSchema(database);
  return database;
}

function enqueueAndClaim(database: DatabaseSync, sessionId = "session-1"): ReclamationLedgerEntry {
  enqueueReclamationLedgerEntry(database, {
    generation: GENERATION,
    now: 1_000,
    planJson: "{}",
    sessionId,
  });
  const entry = claimNextReclamationLedgerEntry(database, { now: 1_100 });
  if (!entry) {
    throw new Error("expected the executor fixture to claim its entry");
  }
  return entry;
}

function fakeOperations(state: FakeState): ReclamationPhaseOperationsFactory {
  return ({ phase }) => ({
    appendArchive: () => {
      state.calls.push("appendArchive");
      if (state.appendThrows !== undefined) {
        throw state.appendThrows;
      }
    },
    complete: () => {
      state.calls.push("complete");
    },
    deleteReclaimedRows: () => {
      state.calls.push("deleteReclaimedRows");
      if (state.deleteThrows !== undefined) {
        throw state.deleteThrows;
      }
    },
    evaluateGuard: () => {
      state.calls.push("guard:" + phase);
      const passed =
        phase === "batch-delete" ? (state.guard2Pass ?? true) : (state.guardPass ?? true);
      return passed
        ? { passed: true }
        : {
            passed: false,
            reason: phase === "guard" ? "target-mismatch" : "recent-activity",
          };
    },
    removeArchiveForDecline: () => {
      state.calls.push("removeArchiveForDecline");
    },
  });
}

function counts(): () => number {
  let tick = 2_000;
  return () => (tick += 1);
}

describe("reclamation ledger executor", () => {
  it("drives the entry through every phase, committing each boundary", async () => {
    const database = openLedgerDatabase();
    try {
      const entry = enqueueAndClaim(database);
      const state: FakeState = { calls: [] };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        now: counts(),
      });

      expect(result).toEqual({ kind: "done", sessionFullyDeleted: false });
      expect(state.calls).toEqual([
        "guard:guard",
        "appendArchive",
        "guard:batch-delete",
        "deleteReclaimedRows",
        "complete",
      ]);
      expect(readReclamationLedgerEntry(database, entry.id)).toMatchObject({
        state: "done",
        stepCursor: 4,
      });
      expect(hasClaimableReclamationLedgerEntry(database)).toBe(false);
    } finally {
      database.close();
    }
  });

  it("resumes from the committed cursor without replaying earlier phases", async () => {
    const database = openLedgerDatabase();
    try {
      const claimed = enqueueAndClaim(database);
      advanceReclamationLedgerStep(database, { id: claimed.id, now: 1_500, stepCursor: 2 });
      const entry = readReclamationLedgerEntry(database, claimed.id);
      if (!entry) {
        throw new Error("expected the advanced entry");
      }
      const state: FakeState = { calls: [] };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        now: counts(),
      });

      expect(result).toEqual({ kind: "done", sessionFullyDeleted: false });
      expect(state.calls).toEqual(["guard:batch-delete", "deleteReclaimedRows", "complete"]);
    } finally {
      database.close();
    }
  });

  it("records a guard decline terminally and writes nothing else", async () => {
    const database = openLedgerDatabase();
    try {
      const entry = enqueueAndClaim(database);
      const state: FakeState = { calls: [], guardPass: false };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        now: counts(),
      });

      expect(result).toEqual({
        kind: "declined",
        phase: "guard",
        reason: "target-mismatch",
      });
      expect(state.calls).toEqual(["guard:guard"]);
      expect(readReclamationLedgerEntry(database, entry.id)).toMatchObject({
        lastError: "guard:target-mismatch",
        state: "declined",
        stepCursor: 0,
      });
      expect(hasClaimableReclamationLedgerEntry(database)).toBe(false);
    } finally {
      database.close();
    }
  });

  it("maps a phase-2 archive decline to a terminal decline with no cleanup", async () => {
    const database = openLedgerDatabase();
    try {
      const claimed = enqueueAndClaim(database);
      advanceReclamationLedgerStep(database, { id: claimed.id, now: 1_500, stepCursor: 1 });
      const entry = readReclamationLedgerEntry(database, claimed.id);
      if (!entry) {
        throw new Error("expected the advanced entry");
      }
      const state: FakeState = {
        appendThrows: new ReclamationPhaseDeclinedError("target-mismatch"),
        calls: [],
      };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        now: counts(),
      });

      expect(result).toEqual({
        kind: "declined",
        phase: "archive-append",
        reason: "target-mismatch",
      });
      // The snapshot decline precedes the persist, so there is no archive row to clean up.
      expect(state.calls).toEqual(["appendArchive"]);
      expect(readReclamationLedgerEntry(database, entry.id)).toMatchObject({
        lastError: "archive-append:target-mismatch",
        state: "declined",
        stepCursor: 1,
      });
    } finally {
      database.close();
    }
  });

  it("is terminal on a sha256 conflict and never reaches the delete", async () => {
    const database = openLedgerDatabase();
    try {
      const claimed = enqueueAndClaim(database);
      advanceReclamationLedgerStep(database, { id: claimed.id, now: 1_500, stepCursor: 1 });
      const entry = readReclamationLedgerEntry(database, claimed.id);
      if (!entry) {
        throw new Error("expected the advanced entry");
      }
      const state: FakeState = {
        appendThrows: new ReclamationArchiveConflictError("stored != planned"),
        calls: [],
      };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        now: counts(),
      });

      expect(result).toMatchObject({ kind: "failed", phase: "archive-append" });
      expect(state.calls).toEqual(["appendArchive"]);
      expect(state.calls).not.toContain("deleteReclaimedRows");
      expect(readReclamationLedgerEntry(database, entry.id)).toMatchObject({ state: "failed" });
      expect(readReclamationLedgerEntry(database, entry.id)?.lastError).toContain(
        "stored != planned",
      );
      expect(hasClaimableReclamationLedgerEntry(database)).toBe(false);
    } finally {
      database.close();
    }
  });

  it("removes the un-consumed archive when guard-2 declines and never deletes", async () => {
    const database = openLedgerDatabase();
    try {
      const claimed = enqueueAndClaim(database);
      advanceReclamationLedgerStep(database, { id: claimed.id, now: 1_500, stepCursor: 2 });
      const entry = readReclamationLedgerEntry(database, claimed.id);
      if (!entry) {
        throw new Error("expected the advanced entry");
      }
      const state: FakeState = { calls: [], guard2Pass: false };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        now: counts(),
      });

      expect(result).toEqual({
        kind: "declined",
        phase: "batch-delete",
        reason: "recent-activity",
      });
      expect(state.calls).toEqual(["guard:batch-delete", "removeArchiveForDecline"]);
      expect(state.calls).not.toContain("deleteReclaimedRows");
      expect(readReclamationLedgerEntry(database, entry.id)).toMatchObject({ state: "declined" });
    } finally {
      database.close();
    }
  });

  it("yields a lost concurrent claim quietly instead of failing the entry", async () => {
    const database = openLedgerDatabase();
    try {
      const claimed = enqueueAndClaim(database);
      advanceReclamationLedgerStep(database, { id: claimed.id, now: 1_500, stepCursor: 2 });
      const entry = readReclamationLedgerEntry(database, claimed.id);
      if (!entry) {
        throw new Error("expected the advanced entry");
      }
      const state: FakeState = {
        calls: [],
        deleteThrows: new Error("reclamation claim is stale"),
      };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        isBenignYield: (error) => /claim/.test(String(error)),
        now: counts(),
      });

      expect(result).toEqual({ kind: "yielded", phase: "batch-delete" });
      expect(readReclamationLedgerEntry(database, entry.id)).toMatchObject({
        attempts: 1,
        lastError: null,
        state: "queued",
      });
    } finally {
      database.close();
    }
  });

  it("fails a genuine delete error that is not a claim conflict", async () => {
    const database = openLedgerDatabase();
    try {
      const claimed = enqueueAndClaim(database);
      advanceReclamationLedgerStep(database, { id: claimed.id, now: 1_500, stepCursor: 2 });
      const entry = readReclamationLedgerEntry(database, claimed.id);
      if (!entry) {
        throw new Error("expected the advanced entry");
      }
      const state: FakeState = { calls: [], deleteThrows: new Error("boom") };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        isBenignYield: (error) => /claim/.test(String(error)),
        now: counts(),
      });

      expect(result).toMatchObject({ kind: "failed", phase: "batch-delete" });
      expect(readReclamationLedgerEntry(database, entry.id)?.lastError).toContain("boom");
    } finally {
      database.close();
    }
  });

  it("records a fully deleted session on completion", async () => {
    const database = openLedgerDatabase();
    try {
      const entry = enqueueAndClaim(database);
      const state: FakeState = { calls: [] };

      const result = await executeReclamationLedgerEntry({
        createOperations: fakeOperations(state),
        database,
        entry,
        now: counts(),
        readSessionFullyDeleted: () => true,
      });

      expect(result).toEqual({ kind: "done", sessionFullyDeleted: true });
      expect(readReclamationLedgerEntry(database, entry.id)).toMatchObject({
        sessionFullyDeleted: true,
        state: "done",
      });
    } finally {
      database.close();
    }
  });

  it("completes idempotently from a cursor already past the delete boundary", async () => {
    const database = openLedgerDatabase();
    try {
      const claimed = enqueueAndClaim(database);
      advanceReclamationLedgerStep(database, { id: claimed.id, now: 1_500, stepCursor: 3 });
      const entry = readReclamationLedgerEntry(database, claimed.id);
      if (!entry) {
        throw new Error("expected the advanced entry");
      }
      const state: FakeState = { calls: [] };

      await expect(
        executeReclamationLedgerEntry({
          createOperations: fakeOperations(state),
          database,
          entry,
          now: counts(),
        }),
      ).resolves.toEqual({ kind: "done", sessionFullyDeleted: false });
      expect(state.calls).toEqual(["complete"]);
      expect(readReclamationLedgerEntry(database, entry.id)).toMatchObject({
        state: "done",
        stepCursor: 4,
      });
    } finally {
      database.close();
    }
  });
});
