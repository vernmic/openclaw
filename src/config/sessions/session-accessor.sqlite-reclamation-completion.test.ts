import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ensureReclamationLedgerSchema } from "../../state/openclaw-agent-reclamation-ledger-schema.js";
import {
  completeReclamationLedgerEntry,
  declineReclamationLedgerEntry,
  enqueueReclamationLedgerEntry,
  failReclamationLedgerEntry,
  yieldReclamationLedgerEntry,
} from "../../state/openclaw-agent-reclamation-queue.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "../../state/openclaw-agent-schema.js";
import {
  awaitReclamationLedgerCompletion,
  failReclamationLedgerCompletion,
  hasReclamationLedgerCompletionWatchers,
  settleReclamationLedgerCompletion,
  type ReclamationLedgerCompletion,
} from "./session-accessor.sqlite-reclamation-completion.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";

const ARCHIVED = [
  {
    archivedPath: "archive/one.zst",
    generation: GENERATION,
    sessionId: "session-1",
    sourcePath: "transcript/one.jsonl",
  },
];

afterEach(() => {
  vi.restoreAllMocks();
});

function openLedgerDatabase(): DatabaseSync {
  const database = new DatabaseSync(":memory:");
  database.exec(OPENCLAW_AGENT_SCHEMA_SQL);
  ensureReclamationLedgerSchema(database);
  return database;
}

function enqueue(database: DatabaseSync, params: { now?: number; sessionId?: string } = {}) {
  return enqueueReclamationLedgerEntry(database, {
    generation: GENERATION,
    now: params.now ?? 1_000,
    planJson: JSON.stringify({ sessionId: params.sessionId ?? "session-1" }),
    sessionId: params.sessionId ?? "session-1",
  });
}

describe("reclamation ledger completion signals", () => {
  it("carries the driver values to a waiter registered before terminal", async () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      const completion = awaitReclamationLedgerCompletion(database, id);
      expect(hasReclamationLedgerCompletionWatchers(database, id)).toBe(true);

      settleReclamationLedgerCompletion(database, id, {
        archivedTranscripts: ARCHIVED,
        deleted: true,
      });

      await expect(completion).resolves.toEqual({ archivedTranscripts: ARCHIVED, deleted: true });
      expect(hasReclamationLedgerCompletionWatchers(database, id)).toBe(false);
    } finally {
      database.close();
    }
  });

  it("resolves a decline instead of rejecting it", async () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      const completion = awaitReclamationLedgerCompletion(database, id);
      settleReclamationLedgerCompletion(database, id, {
        archivedTranscripts: [],
        deleted: false,
        expectedEntryMismatch: true,
      });
      await expect(completion).resolves.toEqual({
        archivedTranscripts: [],
        deleted: false,
        expectedEntryMismatch: true,
      });
    } finally {
      database.close();
    }
  });

  it("resolves immediately from an already-done row", async () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      completeReclamationLedgerEntry(database, { id, now: 2_000, sessionFullyDeleted: true });

      await expect(awaitReclamationLedgerCompletion(database, id)).resolves.toEqual({
        archivedTranscripts: [],
        deleted: true,
      });
    } finally {
      database.close();
    }
  });

  it("resolves an already-declined row as a mismatched entry", async () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      declineReclamationLedgerEntry(database, { id, now: 2_000, reason: "guard:snapshot moved" });

      await expect(awaitReclamationLedgerCompletion(database, id)).resolves.toEqual({
        archivedTranscripts: [],
        deleted: false,
        expectedEntryMismatch: true,
      });
    } finally {
      database.close();
    }
  });

  it("rejects immediately from an already-failed row", async () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      failReclamationLedgerEntry(database, { error: new Error("sha256 conflict"), id, now: 2_000 });

      await expect(awaitReclamationLedgerCompletion(database, id)).rejects.toThrow(
        "sha256 conflict",
      );
    } finally {
      database.close();
    }
  });

  it("resolves a late waiter from the terminal row (registered-too-late)", async () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      completeReclamationLedgerEntry(database, { id, now: 2_000, sessionFullyDeleted: false });

      // Settling with no waiter is a no-op; the row is the durable record the waiter reads.
      settleReclamationLedgerCompletion(database, id, {
        archivedTranscripts: ARCHIVED,
        deleted: true,
      });
      await expect(awaitReclamationLedgerCompletion(database, id)).resolves.toEqual({
        archivedTranscripts: [],
        deleted: false,
      });
    } finally {
      database.close();
    }
  });

  it("resolves every waiter on one entry exactly once", async () => {
    const database = openLedgerDatabase();
    try {
      const { id } = enqueue(database);
      const first = awaitReclamationLedgerCompletion(database, id);
      const second = awaitReclamationLedgerCompletion(database, id);

      const completion: ReclamationLedgerCompletion = {
        archivedTranscripts: ARCHIVED,
        deleted: true,
      };
      settleReclamationLedgerCompletion(database, id, completion);
      // The second settle arrives after the entry is terminal and must not double-resolve.
      settleReclamationLedgerCompletion(database, id, { archivedTranscripts: [], deleted: false });

      await expect(first).resolves.toEqual(completion);
      await expect(second).resolves.toEqual(completion);
    } finally {
      database.close();
    }
  });

  it("keeps entries independent and ignores a non-terminal yield", async () => {
    const database = openLedgerDatabase();
    try {
      const one = enqueue(database, { sessionId: "session-1" });
      const two = enqueue(database, {
        generation: "b".repeat(32),
        now: 1_100,
        sessionId: "session-2",
      });
      const first = awaitReclamationLedgerCompletion(database, one.id);
      const second = awaitReclamationLedgerCompletion(database, two.id);

      // A yield is a re-queue, not a terminal outcome, so the waiter stays registered.
      yieldReclamationLedgerEntry(database, { id: one.id, now: 2_000 });
      expect(hasReclamationLedgerCompletionWatchers(database, one.id)).toBe(true);

      settleReclamationLedgerCompletion(database, one.id, {
        archivedTranscripts: [],
        deleted: false,
      });
      failReclamationLedgerCompletion(database, two.id, new Error("other entry failed"));

      await expect(first).resolves.toEqual({ archivedTranscripts: [], deleted: false });
      await expect(second).rejects.toThrow("other entry failed");
    } finally {
      database.close();
    }
  });

  it("separates signals per database handle", async () => {
    const first = openLedgerDatabase();
    const second = openLedgerDatabase();
    try {
      const one = enqueue(first);
      const two = enqueue(second);
      expect(one.id).toBe(two.id);

      const completion = awaitReclamationLedgerCompletion(first, one.id);
      settleReclamationLedgerCompletion(second, two.id, {
        archivedTranscripts: ARCHIVED,
        deleted: true,
      });
      expect(hasReclamationLedgerCompletionWatchers(first, one.id)).toBe(true);

      settleReclamationLedgerCompletion(first, one.id, { archivedTranscripts: [], deleted: false });
      await expect(completion).resolves.toEqual({ archivedTranscripts: [], deleted: false });
    } finally {
      first.close();
      second.close();
    }
  });
});
