import { afterEach, describe, expect, it } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
  type OpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import { ensureReclamationLedgerSchema } from "../../state/openclaw-agent-reclamation-ledger-schema.js";
import {
  enqueueReclamationLedgerEntry,
  hasClaimableReclamationLedgerEntry,
  readReclamationLedgerEntry,
} from "../../state/openclaw-agent-reclamation-queue.js";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import { sweepReclamationOrphansForAgentDatabase } from "./session-accessor.sqlite-reclamation-sweep.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

function openAgentDatabase(stateDir: string): OpenClawAgentDatabase {
  const database = openOpenClawAgentDatabase({
    agentId: "main",
    env: { OPENCLAW_STATE_DIR: stateDir },
  });
  ensureReclamationLedgerSchema(database.db);
  return database;
}

function evictionPlanJson(database: OpenClawAgentDatabase, sessionId: string): string {
  return JSON.stringify({
    databaseOptions: {
      agentId: "main",
      env: { OPENCLAW_STATE_DIR: database.path },
      path: database.path,
    },
    diskBudget: { preserveRecentMs: null },
    kind: "history-eviction",
    materializedPlans: [],
    protectedSessionIds: [],
    sessionId,
  });
}

describe("reclamation orphan-sweep recovery drive (piece iv-4)", () => {
  afterEach(() => {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
  });

  it("is a no-op and settles when the queue is empty", async () => {
    const database = openAgentDatabase(tempDirs.make("openclaw-reclamation-sweep-empty-"));
    expect(hasClaimableReclamationLedgerEntry(database.db)).toBe(false);
    await expect(
      sweepReclamationOrphansForAgentDatabase({ database, now: () => 1_000 }),
    ).resolves.toBeUndefined();
  });

  it("resumes a crash-left queued entry to done through the live delete", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-sweep-resume-");
    const database = openAgentDatabase(stateDir);
    const sessionId = "session-sweep-resume";
    const planJson = evictionPlanJson(database, sessionId);
    enqueueReclamationLedgerEntry(database.db, {
      generation: GENERATION,
      now: 1_000,
      planJson,
      sessionId,
    });
    // A crash left the entry queued (state='queued', cursor=0); the boot pull is the recovery.
    expect(hasClaimableReclamationLedgerEntry(database.db)).toBe(true);

    await sweepReclamationOrphansForAgentDatabase({ database, now: () => 1_100 });

    const row = readReclamationLedgerEntry(
      database.db,
      readReclamationLedgerRowId(database, sessionId),
    );
    expect(row).toMatchObject({ state: "done", stepCursor: 4 });
    expect(hasClaimableReclamationLedgerEntry(database.db)).toBe(false);
  });

  it("terminates a decline and never re-pulls it", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-sweep-decline-");
    const database = openAgentDatabase(stateDir);
    const sessionId = "session-sweep-decline";
    const planJson = JSON.stringify({
      databaseOptions: {
        agentId: "main",
        env: { OPENCLAW_STATE_DIR: stateDir },
        path: database.path,
      },
      deleteParams: {
        deleteDeliveryArtifacts: false,
        target: { canonicalKey: `agent:main:${sessionId}`, storeKeys: [] },
      },
      kind: "historical-generation",
      materializedPlans: [],
      preparedTargetSnapshot: [],
      protectedSessionIds: [],
      sessionId,
    });
    enqueueReclamationLedgerEntry(database.db, {
      generation: GENERATION,
      now: 1_000,
      planJson,
      sessionId,
    });

    await sweepReclamationOrphansForAgentDatabase({ database, now: () => 1_100 });

    const row = readReclamationLedgerEntry(
      database.db,
      readReclamationLedgerRowId(database, sessionId),
    );
    expect(row).toMatchObject({ lastError: "guard:target-guard", state: "declined" });
    // Terminal states never re-enter the pull, so a second sweep is still a no-op.
    expect(hasClaimableReclamationLedgerEntry(database.db)).toBe(false);
    await expect(
      sweepReclamationOrphansForAgentDatabase({ database, now: () => 1_200 }),
    ).resolves.toBeUndefined();
  });
});

function readReclamationLedgerRowId(database: OpenClawAgentDatabase, sessionId: string): number {
  const row = database.db
    .prepare("SELECT id FROM reclamation_ledger WHERE session_id = ? ORDER BY id DESC LIMIT 1")
    .get(sessionId) as { id: number } | undefined;
  if (!row) {
    throw new Error("expected a reclamation ledger row for " + sessionId);
  }
  return row.id;
}
