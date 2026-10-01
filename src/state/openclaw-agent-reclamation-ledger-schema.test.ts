import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "./openclaw-agent-db.js";
import {
  ensureReclamationLedgerSchema,
  RECLAMATION_LEDGER_TABLE,
} from "./openclaw-agent-reclamation-ledger-schema.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "./openclaw-agent-schema.js";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

// A real generation token: 32-char hex, opaque, never castable to an integer.
const GENERATION = "443a91f600f748d2a8052cb1fbafa327";

afterEach(() => {
  closeOpenClawAgentDatabasesForTest();
});

function insertLedgerRow(
  db: DatabaseSync,
  overrides: { generation?: string; sessionId?: string; state?: string } = {},
): void {
  const now = Date.now();
  db.prepare(
    `INSERT INTO ${RECLAMATION_LEDGER_TABLE} (
       session_id, generation, plan_json, state, created_at, updated_at
     ) VALUES (?, ?, ?, ?, ?, ?)`,
  ).run(
    overrides.sessionId ?? "session-1",
    overrides.generation ?? GENERATION,
    "{}",
    overrides.state ?? "queued",
    now,
    now,
  );
}

describe("reclamation ledger schema", () => {
  it("round-trips a real 32-hex generation through the ledger", () => {
    const database = new DatabaseSync(":memory:");
    try {
      database.exec(OPENCLAW_AGENT_SCHEMA_SQL);
      ensureReclamationLedgerSchema(database);

      insertLedgerRow(database);

      expect(
        database
          .prepare(`SELECT generation, state, step_cursor FROM ${RECLAMATION_LEDGER_TABLE}`)
          .get(),
      ).toEqual({ generation: GENERATION, state: "queued", step_cursor: 0 });
    } finally {
      database.close();
    }
  });

  it("keeps a current database table-free until first reclamation use without changing its version", () => {
    const stateDir = tempDirs.make("openclaw-reclamation-ledger-schema-");
    const options = { agentId: "main", env: { OPENCLAW_STATE_DIR: stateDir } };
    const initial = openOpenClawAgentDatabase(options);
    const databasePath = initial.path;
    closeOpenClawAgentDatabasesForTest();

    const shipped = new DatabaseSync(databasePath);
    shipped.exec(`DROP TABLE ${RECLAMATION_LEDGER_TABLE};`);
    const versionBefore = shipped.prepare("PRAGMA user_version").get();
    const metadataBefore = shipped
      .prepare("SELECT schema_version, updated_at FROM schema_meta WHERE meta_key = 'primary'")
      .get();
    shipped.close();

    const reopened = openOpenClawAgentDatabase(options);
    expect(
      reopened.db
        .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")
        .get(RECLAMATION_LEDGER_TABLE),
    ).toBeUndefined();

    ensureReclamationLedgerSchema(reopened.db);

    expect(
      reopened.db
        .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")
        .get(RECLAMATION_LEDGER_TABLE),
    ).toEqual({ 1: 1 });
    expect(reopened.db.prepare("PRAGMA user_version").get()).toEqual(versionBefore);
    expect(
      reopened.db
        .prepare("SELECT schema_version, updated_at FROM schema_meta WHERE meta_key = 'primary'")
        .get(),
    ).toEqual(metadataBefore);
  });

  it("enforces the (session_id, generation) uniqueness and the state check", () => {
    const database = new DatabaseSync(":memory:");
    try {
      database.exec(OPENCLAW_AGENT_SCHEMA_SQL);
      ensureReclamationLedgerSchema(database);

      insertLedgerRow(database);
      expect(() => insertLedgerRow(database)).toThrow(/UNIQUE/i);

      expect(() => insertLedgerRow(database, { generation: "other", state: "bogus" })).toThrow(
        /CHECK/i,
      );
    } finally {
      database.close();
    }
  });
});
