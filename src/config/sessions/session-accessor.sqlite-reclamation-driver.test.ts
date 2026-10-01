import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
  type OpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import { ensureReclamationLedgerSchema } from "../../state/openclaw-agent-reclamation-ledger-schema.js";
import {
  claimNextReclamationLedgerEntry,
  enqueueReclamationLedgerEntry,
  readReclamationLedgerEntry,
  type ReclamationLedgerEntry,
} from "../../state/openclaw-agent-reclamation-queue.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "../../state/openclaw-agent-schema.js";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import {
  hashSessionArchiveBytes,
  type MaterializedSessionStateDeletePlan,
  type SessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
import { readReclamationArchiveSha256 } from "./session-accessor.sqlite-reclamation-archive.js";
import { executeReclamationLedgerEntry } from "./session-accessor.sqlite-reclamation-executor.js";
import {
  createLiveReclamationDeleteExecutor,
  isReclamationSessionFullyDeleted,
} from "./session-accessor.sqlite-reclamation-live.js";
import {
  createReclamationPhaseOperationsFactory,
  type ReclamationPhaseExclusiveWrite,
} from "./session-accessor.sqlite-reclamation-operations.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";
const SESSION_ID = "session-driver";
const BYTES = new Uint8Array([7, 7, 1, 4]);

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

function openInMemoryAgentDatabase(): OpenClawAgentDatabase {
  const db = new DatabaseSync(":memory:");
  db.exec(OPENCLAW_AGENT_SCHEMA_SQL);
  ensureReclamationLedgerSchema(db);
  return {
    agentId: "agent-1",
    db,
    path: ":memory:",
    walMaintenance: {},
  } as unknown as OpenClawAgentDatabase;
}

function evictionPlanJson(databasePath = ":memory:"): string {
  return JSON.stringify({
    databaseOptions: {
      agentId: "agent-1",
      env: { OPENCLAW_STATE_DIR: "C:\\state" },
      path: databasePath,
    },
    diskBudget: { preserveRecentMs: null },
    kind: "history-eviction",
    materializedPlans: [
      {
        agentId: "agent-1",
        archive: {
          archiveName: "archive.jsonl.zst",
          createdAt: 1_700_000_500_000,
          encoding: "zstd",
          sha256: hashSessionArchiveBytes(BYTES),
        },
        archiveDirectory: "C:\\state\\archives",
        archiveTranscript: true,
        databasePath,
        reason: "deleted",
        sessionId: SESSION_ID,
        snapshot: { generation: GENERATION, sessionKey: `key:${SESSION_ID}` },
      },
    ],
    protectedSessionIds: [],
    sessionId: SESSION_ID,
  });
}

function materializeTo(bytes: Uint8Array) {
  return async (
    plans: readonly SessionStateDeletePlan[],
  ): Promise<MaterializedSessionStateDeletePlan[]> =>
    plans.map((plan) => ({
      ...plan,
      archive: {
        archiveName: "derived.jsonl.zst",
        bytes,
        createdAt: 1_800_000_000_000,
        encoding: "zstd",
        sha256: hashSessionArchiveBytes(bytes),
      },
      archivedTranscript: null,
    }));
}

function enqueueAndClaim(
  database: DatabaseSync,
  planJson: string,
  sessionId = SESSION_ID,
): ReclamationLedgerEntry {
  enqueueReclamationLedgerEntry(database, {
    generation: GENERATION,
    now: 1_000,
    planJson,
    sessionId,
  });
  const entry = claimNextReclamationLedgerEntry(database, { now: 1_100 });
  if (!entry) {
    throw new Error("expected the driver fixture to claim its entry");
  }
  return entry;
}

function countingClock(): () => number {
  let tick = 2_000;
  return () => (tick += 1);
}

describe("reclamation ledger parent driver (piece c, part 11)", () => {
  // Runs before the module-level temp-dir cleanup: an open agent DB handle turns that
  // rmSync into an EPERM on Windows.
  afterEach(() => {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
  });

  it("opens one exclusive-write window per phase and materializes bytes outside the archive window", async () => {
    const database = openInMemoryAgentDatabase();
    try {
      const events: string[] = [];
      const exclusive: ReclamationPhaseExclusiveWrite = async (operation, run) => {
        events.push(">" + operation);
        try {
          return await run();
        } finally {
          events.push("<" + operation);
        }
      };
      const entry = enqueueAndClaim(database.db, evictionPlanJson());

      const result = await executeReclamationLedgerEntry({
        createOperations: createReclamationPhaseOperationsFactory({
          database,
          deleteReclaimedRows: () => {
            events.push("delete");
          },
          materializePlans: async (plans) => {
            events.push("materialize");
            return await materializeTo(BYTES)(plans);
          },
          runExclusiveWrite: exclusive,
        }),
        database: database.db,
        entry,
        now: countingClock(),
      });

      expect(result).toEqual({ kind: "done", sessionFullyDeleted: false });
      // The blob re-materialization deliberately happens BEFORE the archive window opens:
      // holding the writer for off-thread zstd is the hold this split exists to remove.
      expect(events).toEqual([
        ">session.reclamation.phase-guard",
        "<session.reclamation.phase-guard",
        "materialize",
        ">session.reclamation.phase-archive",
        "<session.reclamation.phase-archive",
        ">session.reclamation.phase-guard",
        "<session.reclamation.phase-guard",
        ">session.reclamation.phase-delete",
        "delete",
        "<session.reclamation.phase-delete",
      ]);
      expect(
        readReclamationArchiveSha256(database, { generation: GENERATION, sessionId: SESSION_ID }),
      ).toBe(hashSessionArchiveBytes(BYTES));
      expect(readReclamationLedgerEntry(database.db, entry.id)).toMatchObject({
        state: "done",
        stepCursor: 4,
      });
    } finally {
      database.db.close();
    }
  });

  it("drives a real file database to done through the default exclusive write and the live delete", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-driver-");
    const options = { agentId: "main", env: { OPENCLAW_STATE_DIR: stateDir } };
    const database = openOpenClawAgentDatabase(options);
    ensureReclamationLedgerSchema(database.db);
    const planJson = JSON.stringify({
      databaseOptions: {
        agentId: "main",
        env: { OPENCLAW_STATE_DIR: stateDir },
        path: database.path,
      },
      diskBudget: { preserveRecentMs: null },
      kind: "history-eviction",
      materializedPlans: [],
      protectedSessionIds: [],
      sessionId: SESSION_ID,
    });
    const entry = enqueueAndClaim(database.db, planJson);

    const result = await executeReclamationLedgerEntry({
      createOperations: createReclamationPhaseOperationsFactory({
        database,
        deleteReclaimedRows: createLiveReclamationDeleteExecutor(),
      }),
      database: database.db,
      entry,
      now: countingClock(),
      readSessionFullyDeleted: () => isReclamationSessionFullyDeleted(database, SESSION_ID),
    });

    expect(result).toEqual({ kind: "done", sessionFullyDeleted: true });
    expect(readReclamationLedgerEntry(database.db, entry.id)).toMatchObject({
      sessionFullyDeleted: true,
      state: "done",
    });
  });

  it("declines terminally through the shared guard before anything is written", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-driver-decline-");
    const options = { agentId: "main", env: { OPENCLAW_STATE_DIR: stateDir } };
    const database = openOpenClawAgentDatabase(options);
    ensureReclamationLedgerSchema(database.db);
    const sessionId = "session-decline";
    // The target matches no live entry, so the shared guard fails both its checks' second leg:
    // the prepared snapshot (empty) matches, but there is no entry to delete. Guard-first means
    // the decline writes nothing and stays free.
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
    const entry = enqueueAndClaim(database.db, planJson, sessionId);

    const result = await executeReclamationLedgerEntry({
      createOperations: createReclamationPhaseOperationsFactory({
        database,
        deleteReclaimedRows: createLiveReclamationDeleteExecutor(),
      }),
      database: database.db,
      entry,
      now: countingClock(),
    });

    expect(result).toEqual({ kind: "declined", phase: "guard", reason: "target-guard" });
    expect(readReclamationLedgerEntry(database.db, entry.id)).toMatchObject({
      lastError: "guard:target-guard",
      state: "declined",
      stepCursor: 0,
    });
    expect(
      readReclamationArchiveSha256(database, { generation: GENERATION, sessionId }),
    ).toBeUndefined();
  });
});
