import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "../../state/openclaw-agent-schema.js";
import type { DeleteSessionEntryLifecycleParams } from "./session-accessor.sqlite-contract.js";
import type { SqliteLifecycleTargetSnapshot } from "./session-accessor.sqlite-entry-equality.js";
import {
  evaluateSqliteReclamationTargetGuard,
  shouldDeleteSqliteSessionEntryLifecycle,
} from "./session-accessor.sqlite-reclamation-guard.js";
import type { InternalSessionEntry as SessionEntry } from "./types.js";

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

function deleteParams(
  overrides: Partial<DeleteSessionEntryLifecycleParams> = {},
): DeleteSessionEntryLifecycleParams {
  return {
    archiveTranscript: true,
    storePath: ":memory:",
    target: {
      canonicalKey: "key:session-1",
      storeKeys: [],
    } as DeleteSessionEntryLifecycleParams["target"],
    ...overrides,
  };
}

const ENTRY = { sessionId: "session-1", updatedAt: 42 } as unknown as SessionEntry;

describe("reclamation guard", () => {
  it("declines when the target entry is gone", () => {
    const database = openDatabase();
    try {
      expect(shouldDeleteSqliteSessionEntryLifecycle(database, undefined, deleteParams())).toBe(
        false,
      );
    } finally {
      database.db.close();
    }
  });

  it("passes an unconstrained entry", () => {
    const database = openDatabase();
    try {
      expect(shouldDeleteSqliteSessionEntryLifecycle(database, ENTRY, deleteParams())).toBe(true);
    } finally {
      database.db.close();
    }
  });

  it("declines on an expected-session mismatch, including the null expectation", () => {
    const database = openDatabase();
    try {
      expect(
        shouldDeleteSqliteSessionEntryLifecycle(
          database,
          ENTRY,
          deleteParams({ expectedSessionId: "session-other" }),
        ),
      ).toBe(false);
      expect(
        shouldDeleteSqliteSessionEntryLifecycle(
          database,
          ENTRY,
          deleteParams({ expectedSessionId: null }),
        ),
      ).toBe(false);
    } finally {
      database.db.close();
    }
  });

  it("declines on updated-at or lifecycle-revision mismatches and passes an exact match", () => {
    const database = openDatabase();
    try {
      expect(
        shouldDeleteSqliteSessionEntryLifecycle(
          database,
          ENTRY,
          deleteParams({ expectedUpdatedAt: 7 }),
        ),
      ).toBe(false);
      expect(
        shouldDeleteSqliteSessionEntryLifecycle(
          database,
          ENTRY,
          deleteParams({ expectedLifecycleRevision: "rev-2" }),
        ),
      ).toBe(false);
      expect(
        shouldDeleteSqliteSessionEntryLifecycle(
          database,
          ENTRY,
          deleteParams({ expectedUpdatedAt: 42 }),
        ),
      ).toBe(true);
    } finally {
      database.db.close();
    }
  });

  it("declines when the live target vanished and the plan expected nothing", () => {
    const database = openDatabase();
    try {
      expect(
        evaluateSqliteReclamationTargetGuard(database, {
          deleteParams: deleteParams(),
          preparedTargetSnapshot: [] as unknown as SqliteLifecycleTargetSnapshot,
        }),
      ).toEqual({ passed: false });
    } finally {
      database.db.close();
    }
  });

  it("declines when the prepared snapshot no longer matches the live target", () => {
    const database = openDatabase();
    try {
      const prepared = [
        { entry: ENTRY, sessionKey: "key:session-1" },
      ] as unknown as SqliteLifecycleTargetSnapshot;
      expect(
        evaluateSqliteReclamationTargetGuard(database, {
          deleteParams: deleteParams(),
          preparedTargetSnapshot: prepared,
        }),
      ).toEqual({ passed: false });
    } finally {
      database.db.close();
    }
  });
});
