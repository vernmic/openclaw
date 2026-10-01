import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ensureReclamationLedgerSchema } from "./openclaw-agent-reclamation-ledger-schema.js";
import {
  advanceReclamationLedgerStep,
  claimNextReclamationLedgerEntry,
  completeReclamationLedgerEntry,
  createReclamationLedgerRunner,
  declineReclamationLedgerEntry,
  enqueueReclamationLedgerEntry,
  failReclamationLedgerEntry,
  findReclamationLedgerGeneration,
  hasClaimableReclamationLedgerEntry,
  readReclamationLedgerEntry,
  type ReclamationLedgerEntry,
  yieldReclamationLedgerEntry,
} from "./openclaw-agent-reclamation-queue.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "./openclaw-agent-schema.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";

afterEach(() => {
  vi.restoreAllMocks();
});

function openLedgerDatabase(): DatabaseSync {
  const database = new DatabaseSync(":memory:");
  database.exec(OPENCLAW_AGENT_SCHEMA_SQL);
  ensureReclamationLedgerSchema(database);
  return database;
}

function enqueue(
  database: DatabaseSync,
  params: { generation?: string; now?: number; sessionId?: string } = {},
) {
  return enqueueReclamationLedgerEntry(database, {
    generation: params.generation ?? GENERATION,
    now: params.now ?? 1_000,
    planJson: JSON.stringify({ sessionId: params.sessionId ?? "session-1" }),
    sessionId: params.sessionId ?? "session-1",
  });
}

describe("reclamation ledger queue", () => {
  it("round-trips a real 32-hex generation and reuses it on re-plan", () => {
    const database = openLedgerDatabase();
    try {
      const first = enqueue(database);
      expect(first.inserted).toBe(true);
      expect(findReclamationLedgerGeneration(database, "session-1")).toBe(GENERATION);

      // A re-plan of the same session must reuse the recorded generation.
      const replan = enqueue(database, {
        generation: findReclamationLedgerGeneration(database, "session-1"),
      });
      expect(replan.inserted).toBe(false);
      expect(replan.id).toBe(first.id);
      expect(readReclamationLedgerEntry(database, first.id)?.generation).toBe(GENERATION);
    } finally {
      database.close();
    }
  });

  it("idempotent takeover: both runners may claim one entry, which reaches done exactly once", () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);

      // Spec v2 / review addendum: the claim is unconditional, so the normal stop/start deploy
      // overlap legitimately has both runners claim the same entry. Single ownership is NOT the
      // guarantee; step idempotency is. Both pulls count as attempts.
      const first = claimNextReclamationLedgerEntry(database, { now: 2_000 });
      const second = claimNextReclamationLedgerEntry(database, { now: 2_000 });

      expect(first?.id).toBe(id);
      expect(second?.id).toBe(id);
      expect(readReclamationLedgerEntry(database, id)?.attempts).toBe(2);

      // COMPLETE is idempotent, so a doubled finish still lands on done exactly once.
      completeReclamationLedgerEntry(database, { id, now: 3_000, sessionFullyDeleted: false });
      completeReclamationLedgerEntry(database, { id, now: 3_000, sessionFullyDeleted: false });
      expect(readReclamationLedgerEntry(database, id)?.state).toBe("done");
      expect(hasClaimableReclamationLedgerEntry(database)).toBe(false);
    } finally {
      database.close();
    }
  });

  it("pulls an in-progress entry before a newer queued one, then FIFO", () => {
    const database = openLedgerDatabase();
    try {
      const older = enqueue(database, {
        generation: "a".repeat(32),
        now: 1_000,
        sessionId: "s-older",
      });
      const newer = enqueue(database, {
        generation: "b".repeat(32),
        now: 5_000,
        sessionId: "s-newer",
      });
      // The older entry is partially done (cursor=3) and must sort ahead of the newer queued one.
      advanceReclamationLedgerStep(database, { id: older.id, now: 1_100, stepCursor: 3 });

      const first = claimNextReclamationLedgerEntry(database, { now: 9_000 });
      expect(first?.id).toBe(older.id);
      expect(first?.stepCursor).toBe(3);
      completeReclamationLedgerEntry(database, {
        id: older.id,
        now: 9_000,
        sessionFullyDeleted: false,
      });

      const second = claimNextReclamationLedgerEntry(database, { now: 9_000 });
      expect(second?.id).toBe(newer.id);
    } finally {
      database.close();
    }
  });

  it("re-claims a running entry a crashed process left behind and resumes at its step", () => {
    const database = openLedgerDatabase();
    try {
      const crashed = enqueue(database, {
        generation: "1".repeat(32),
        now: 1_000,
        sessionId: "s-crashed",
      });
      // A crashed process leaves the entry state=running at cursor 2.
      expect(claimNextReclamationLedgerEntry(database, { now: 2_000 })?.id).toBe(crashed.id);
      advanceReclamationLedgerStep(database, { id: crashed.id, now: 2_100, stepCursor: 2 });

      // The fresh runner takes the entry over and resumes from the recorded cursor.
      const resumed = claimNextReclamationLedgerEntry(database, { now: 3_000 });
      expect(resumed?.id).toBe(crashed.id);
      expect(resumed?.stepCursor).toBe(2);
      expect(readReclamationLedgerEntry(database, crashed.id)?.attempts).toBe(2);
    } finally {
      database.close();
    }
  });

  it("keeps the step cursor monotonic so a stale runner cannot rewind progress", () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      advanceReclamationLedgerStep(database, { id, now: 2_000, stepCursor: 3 });
      // A stale runner that only reached cursor 1 must not move the boundary backwards.
      advanceReclamationLedgerStep(database, { id, now: 2_100, stepCursor: 1 });
      expect(readReclamationLedgerEntry(database, id)?.stepCursor).toBe(3);
    } finally {
      database.close();
    }
  });

  it("requeues a benign yield without recording an error or a failed count", () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      claimNextReclamationLedgerEntry(database, { now: 2_000 });
      yieldReclamationLedgerEntry(database, { id, now: 2_100 });
      expect(readReclamationLedgerEntry(database, id)).toMatchObject({
        attempts: 1,
        lastError: null,
        state: "queued",
      });
    } finally {
      database.close();
    }
  });

  it("excludes terminal entries from the queue and never re-claims them", () => {
    const database = openLedgerDatabase();
    try {
      const done = enqueue(database, { generation: "c".repeat(32), sessionId: "s-done" });
      const failed = enqueue(database, { generation: "d".repeat(32), sessionId: "s-failed" });
      const declined = enqueue(database, { generation: "e".repeat(32), sessionId: "s-declined" });
      completeReclamationLedgerEntry(database, {
        id: done.id,
        now: 2_000,
        sessionFullyDeleted: true,
      });
      failReclamationLedgerEntry(database, { error: new Error("boom"), id: failed.id, now: 2_000 });
      declineReclamationLedgerEntry(database, {
        id: declined.id,
        now: 2_000,
        reason: "snapshot changed",
      });

      expect(hasClaimableReclamationLedgerEntry(database)).toBe(false);
      expect(claimNextReclamationLedgerEntry(database, { now: 3_000 })).toBeUndefined();
      expect(readReclamationLedgerEntry(database, done.id)).toMatchObject({
        sessionFullyDeleted: true,
        state: "done",
      });
      expect(readReclamationLedgerEntry(database, failed.id)?.lastError).toContain("boom");
      expect(readReclamationLedgerEntry(database, declined.id)?.state).toBe("declined");
    } finally {
      database.close();
    }
  });

  it("does not lose a push that lands while the drain is concluding empty", async () => {
    const processed: string[] = [];
    const queue = ["A"];
    let raceArmed = true;
    const claimNext = () => queue.shift();
    // Models a push landing after the drain read the queue as empty but before the
    // running flag cleared: the re-check must observe the new work and re-arm.
    const hasPending = () => {
      if (raceArmed && queue.length === 0) {
        queue.push("B");
        raceArmed = false;
      }
      return queue.length > 0;
    };
    const runner = createReclamationLedgerRunner({
      claimNext: () => {
        const sessionId = claimNext();
        return sessionId
          ? ({
              attempts: 1,
              generation: GENERATION,
              id: 1,
              lastError: null,
              planJson: "{}",
              sessionFullyDeleted: false,
              sessionId,
              state: "running",
              stepCursor: 0,
            } satisfies ReclamationLedgerEntry)
          : undefined;
      },
      hasPending,
      processEntry: (entry) => {
        processed.push(entry.sessionId);
      },
    });

    runner.push();
    await runner.whenIdle();

    expect(processed).toEqual(["A", "B"]);
    expect(runner.isRunning()).toBe(false);
  });

  it("drains a burst from the boot trigger and stops when empty", async () => {
    const database = openLedgerDatabase();
    try {
      enqueue(database, { generation: "f".repeat(32), sessionId: "s-1" });
      enqueue(database, { generation: "0".repeat(32), sessionId: "s-2" });
      const processed: string[] = [];
      const runner = createReclamationLedgerRunner({
        claimNext: () => claimNextReclamationLedgerEntry(database, { now: 4_000 }),
        hasPending: () => hasClaimableReclamationLedgerEntry(database),
        processEntry: (entry) => {
          processed.push(entry.sessionId);
          completeReclamationLedgerEntry(database, {
            id: entry.id,
            now: 4_000,
            sessionFullyDeleted: false,
          });
        },
      });

      runner.start();
      expect(runner.isRunning()).toBe(true);
      await runner.whenIdle();

      expect(processed).toHaveLength(2);
      expect(runner.isRunning()).toBe(false);
      expect(
        database.prepare("SELECT COUNT(*) AS n FROM reclamation_ledger WHERE state = 'done'").get(),
      ).toEqual({ n: 2 });
    } finally {
      database.close();
    }
  });
});
