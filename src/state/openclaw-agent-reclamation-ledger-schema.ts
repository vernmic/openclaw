import type { DatabaseSync } from "node:sqlite";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "./openclaw-agent-schema.js";

export const RECLAMATION_LEDGER_TABLE = "reclamation_ledger";

const LEDGER_SCHEMA_START = `CREATE TABLE IF NOT EXISTS ${RECLAMATION_LEDGER_TABLE} (`;
const LEDGER_SCHEMA_END = "CREATE TABLE IF NOT EXISTS trajectory_runtime_events (";
const ENSURED_DATABASES = new WeakSet<DatabaseSync>();

function reclamationLedgerSchemaSql(): string {
  const start = OPENCLAW_AGENT_SCHEMA_SQL.indexOf(LEDGER_SCHEMA_START);
  const end = OPENCLAW_AGENT_SCHEMA_SQL.indexOf(LEDGER_SCHEMA_END, start);
  if (start === -1 || end === -1) {
    throw new Error("OpenClaw reclamation ledger schema markers are missing.");
  }
  return OPENCLAW_AGENT_SCHEMA_SQL.slice(start, end);
}

/** Lazily installs the additive reclamation ledger on first reclamation use. */
export function ensureReclamationLedgerSchema(db: DatabaseSync): void {
  if (ENSURED_DATABASES.has(db)) {
    return;
  }
  const ensure = () => {
    db.exec(reclamationLedgerSchemaSql()); // sqlite-allow-raw -- Canonical additive DDL only.
  };
  if (db.isTransaction) {
    ensure();
    return;
  }
  runSqliteImmediateTransactionSync(db, ensure);
  ENSURED_DATABASES.add(db);
}
