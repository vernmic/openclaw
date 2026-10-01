import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "../../state/openclaw-agent-schema.js";
import {
  hashSessionArchiveBytes,
  type MaterializedSessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
import {
  ReclamationArchiveConflictError,
  readReclamationArchiveSha256,
  reconcileReclamationArchive,
  removeReclamationArchiveForDecline,
} from "./session-accessor.sqlite-reclamation-archive.js";
import type { ReclamationArchiveMetadata } from "./session-accessor.sqlite-reclamation-plan.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";
const SESSION_ID = "session-1";

function openDatabase(): OpenClawAgentDatabase {
  const db = new DatabaseSync(":memory:");
  db.exec(OPENCLAW_AGENT_SCHEMA_SQL);
  return {
    agentId: "agent-1",
    db,
    path: ":memory:",
    walMaintenance: {},
  } as unknown as OpenClawAgentDatabase;
}

function planWithBytes(
  bytes: Uint8Array,
  overrides: { createdAt?: number; generation?: string; sessionId?: string } = {},
): MaterializedSessionStateDeletePlan {
  const sessionId = overrides.sessionId ?? SESSION_ID;
  return {
    agentId: "agent-1",
    archive: {
      archiveName: "derived-name.jsonl.zst",
      bytes,
      createdAt: overrides.createdAt ?? 1_700_000_500_000,
      encoding: "zstd",
      sha256: hashSessionArchiveBytes(bytes),
    },
    archiveDirectory: ".",
    archiveTranscript: true,
    archivedTranscript: null,
    databasePath: ":memory:",
    reason: "deleted",
    sessionId,
    snapshot: {
      acpParentStreamEventCount: 0,
      generation: overrides.generation ?? GENERATION,
      lastSeq: 3,
      sessionKey: `key:${sessionId}`,
      sessionUpdatedAt: 1_700_000_000_000,
      trajectoryLastSeq: null,
      transcriptUpdatedAt: 1_700_000_000_000,
    },
  } as unknown as MaterializedSessionStateDeletePlan;
}

function storedMetadata(
  bytes: Uint8Array,
  overrides: Partial<ReclamationArchiveMetadata> = {},
): ReclamationArchiveMetadata {
  return {
    archiveName: "stored-name.jsonl.zst",
    createdAt: 1_699_000_000_000,
    encoding: "zstd",
    sha256: hashSessionArchiveBytes(bytes),
    ...overrides,
  };
}

function archiveRowSha(database: OpenClawAgentDatabase): string | undefined {
  return readReclamationArchiveSha256(database, { generation: GENERATION, sessionId: SESSION_ID });
}

describe("reclamation archive-append reconciliation", () => {
  it("archives fresh bytes under the stored metadata and advances", () => {
    const database = openDatabase();
    try {
      const bytes = new Uint8Array([1, 2, 3, 4]);
      const stored = storedMetadata(bytes);
      expect(reconcileReclamationArchive({ database, plan: planWithBytes(bytes), stored })).toBe(
        "archived",
      );
      const row = database.db
        .prepare(
          "SELECT archive_name, archive_sha256, created_at FROM session_transcript_archives WHERE session_id = ? AND generation = ?",
        )
        .get(SESSION_ID, GENERATION) as { archive_name: string; created_at: number };
      // The derived name is discarded; the row carries the planned identity so a re-run matches.
      expect(row.archive_name).toBe("stored-name.jsonl.zst");
      expect(row.created_at).toBe(stored.createdAt);
      expect(archiveRowSha(database)).toBe(stored.sha256);
    } finally {
      database.db.close();
    }
  });

  it("treats a re-run with a new derived name as already-archived and leaves the row untouched", () => {
    const database = openDatabase();
    try {
      const bytes = new Uint8Array([9, 8, 7]);
      const stored = storedMetadata(bytes);
      expect(reconcileReclamationArchive({ database, plan: planWithBytes(bytes), stored })).toBe(
        "archived",
      );
      // Re-materialization recomputes createdAt; the sha256 is stable.
      const rerun = planWithBytes(bytes, { createdAt: 1_800_000_000_000 });
      expect(reconcileReclamationArchive({ database, plan: rerun, stored })).toBe(
        "already-archived",
      );
      const row = database.db
        .prepare(
          "SELECT archive_name, created_at FROM session_transcript_archives WHERE session_id = ? AND generation = ?",
        )
        .get(SESSION_ID, GENERATION) as { archive_name: string; created_at: number };
      expect(row.archive_name).toBe("stored-name.jsonl.zst");
      expect(row.created_at).toBe(stored.createdAt);
    } finally {
      database.db.close();
    }
  });

  it("is terminal on a derived-byte sha256 mismatch and writes nothing", () => {
    const database = openDatabase();
    try {
      const bytes = new Uint8Array([1, 1, 1]);
      const stored = storedMetadata(bytes, { sha256: "f".repeat(64) });
      expect(() =>
        reconcileReclamationArchive({ database, plan: planWithBytes(bytes), stored }),
      ).toThrow(ReclamationArchiveConflictError);
      expect(archiveRowSha(database)).toBeUndefined();
    } finally {
      database.db.close();
    }
  });

  it("is terminal when an existing row's sha256 differs from the plan", () => {
    const database = openDatabase();
    try {
      const bytes = new Uint8Array([4, 4]);
      const stored = storedMetadata(bytes);
      expect(reconcileReclamationArchive({ database, plan: planWithBytes(bytes), stored })).toBe(
        "archived",
      );
      const conflicting = storedMetadata(bytes, { sha256: "a".repeat(64) });
      expect(() =>
        reconcileReclamationArchive({ database, plan: planWithBytes(bytes), stored: conflicting }),
      ).toThrow(ReclamationArchiveConflictError);
      // The original row is intact; no second row and no overwrite happened.
      expect(archiveRowSha(database)).toBe(stored.sha256);
      const count = database.db
        .prepare(
          "SELECT COUNT(*) AS n FROM session_transcript_archives WHERE session_id = ? AND generation = ?",
        )
        .get(SESSION_ID, GENERATION) as { n: number };
      expect(count.n).toBe(1);
    } finally {
      database.db.close();
    }
  });

  it("gates decline cleanup on the cursor so a delete-phase archive is never removed", () => {
    const database = openDatabase();
    try {
      const bytes = new Uint8Array([2, 2]);
      const stored = storedMetadata(bytes);
      const seed = () => {
        if (archiveRowSha(database) === undefined) {
          reconcileReclamationArchive({ database, plan: planWithBytes(bytes), stored });
        }
      };

      seed();
      expect(
        removeReclamationArchiveForDecline({
          database,
          generation: GENERATION,
          sessionId: SESSION_ID,
          stepCursor: 1,
        }),
      ).toBe(true);
      expect(archiveRowSha(database)).toBeUndefined();

      seed();
      expect(
        removeReclamationArchiveForDecline({
          database,
          generation: GENERATION,
          sessionId: SESSION_ID,
          stepCursor: 2,
        }),
      ).toBe(true);

      seed();
      expect(
        removeReclamationArchiveForDecline({
          database,
          generation: GENERATION,
          sessionId: SESSION_ID,
          stepCursor: 3,
        }),
      ).toBe(false);
      expect(archiveRowSha(database)).toBe(stored.sha256);
    } finally {
      database.db.close();
    }
  });
});
