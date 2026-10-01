import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db.js";
import type { ReclamationLedgerEntry } from "../../state/openclaw-agent-reclamation-queue.js";
import { OPENCLAW_AGENT_SCHEMA_SQL } from "../../state/openclaw-agent-schema.js";
import {
  hashSessionArchiveBytes,
  type MaterializedSessionStateDeletePlan,
  type SessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
import {
  ReclamationArchiveConflictError,
  readReclamationArchiveSha256,
} from "./session-accessor.sqlite-reclamation-archive.js";
import {
  createReclamationPhaseOperations,
  createReclamationPhaseOperationsFactory,
  type ReclamationPhaseExclusiveWrite,
  reconcileReclamationArchivePhase,
  toSessionStateDeletePlans,
} from "./session-accessor.sqlite-reclamation-operations.js";
import { ReclamationPhaseDeclinedError } from "./session-accessor.sqlite-reclamation-phases.js";
import type { SerializedSqliteSessionReclamationPlan } from "./session-accessor.sqlite-reclamation-plan.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";
const SESSION_ID = "session-1";
const BYTES = new Uint8Array([1, 2, 3, 4]);

// The default exclusive write resolves a store path from the plan and would bypass these
// tests' in-memory handles; the passthrough keeps the operations on the fixture database.
const passthrough: ReclamationPhaseExclusiveWrite = (_operation, run) => run();

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

function snapshot(generation = GENERATION) {
  return {
    acpParentStreamEventCount: 0,
    generation,
    lastSeq: 3,
    sessionKey: `key:${SESSION_ID}`,
    sessionUpdatedAt: 1_700_000_000_000,
    trajectoryLastSeq: null,
    transcriptUpdatedAt: 1_700_000_000_000,
  };
}

/** A durable four-phase plan (history-eviction) whose archive is metadata-only. */
function evictionPlan(params: { sha256?: string } = {}): SerializedSqliteSessionReclamationPlan {
  return {
    databaseOptions: {
      agentId: "agent-1",
      env: { OPENCLAW_STATE_DIR: "C:\\state" },
      path: ":memory:",
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
          sha256: params.sha256 ?? hashSessionArchiveBytes(BYTES),
        },
        archiveDirectory: "C:\\state\\archives",
        archiveTranscript: true,
        databasePath: ":memory:",
        reason: "deleted",
        sessionId: SESSION_ID,
        snapshot: snapshot(),
      },
    ],
    protectedSessionIds: [],
    sessionId: SESSION_ID,
  } as unknown as SerializedSqliteSessionReclamationPlan;
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

function ledgerEntry(planJson: string): ReclamationLedgerEntry {
  return {
    attempts: 1,
    generation: GENERATION,
    id: 1,
    lastError: null,
    planJson,
    sessionFullyDeleted: false,
    sessionId: SESSION_ID,
    state: "running",
    stepCursor: 0,
  };
}

function archiveSha(database: OpenClawAgentDatabase): string | undefined {
  return readReclamationArchiveSha256(database, { generation: GENERATION, sessionId: SESSION_ID });
}

describe("reclamation phase operations (part 4b-2)", () => {
  it("strips the durable archive metadata back to the un-materialized plan", () => {
    const [plan] = toSessionStateDeletePlans(evictionPlan().materializedPlans);
    expect(plan).toBeDefined();
    expect("archive" in plan!).toBe(false);
    expect(plan!.sessionId).toBe(SESSION_ID);
    expect(plan!.archiveTranscript).toBe(true);
  });

  it("re-materializes phase 2 and persists the row under the stored metadata", async () => {
    const database = openDatabase();
    try {
      await reconcileReclamationArchivePhase({
        database,
        materializePlans: materializeTo(BYTES),
        plan: evictionPlan(),
      });
      expect(archiveSha(database)).toBe(hashSessionArchiveBytes(BYTES));
      const row = database.db
        .prepare(
          "SELECT archive_name, created_at FROM session_transcript_archives WHERE session_id = ? AND generation = ?",
        )
        .get(SESSION_ID, GENERATION) as { archive_name: string; created_at: number };
      expect(row.archive_name).toBe("archive.jsonl.zst");
      expect(row.created_at).toBe(1_700_000_500_000);
    } finally {
      database.db.close();
    }
  });

  it("is terminal on a sha256 mismatch and writes nothing (no delete follows)", async () => {
    const database = openDatabase();
    try {
      await expect(
        reconcileReclamationArchivePhase({
          database,
          materializePlans: materializeTo(BYTES),
          plan: evictionPlan({ sha256: "f".repeat(64) }),
        }),
      ).rejects.toBeInstanceOf(ReclamationArchiveConflictError);
      expect(archiveSha(database)).toBeUndefined();
    } finally {
      database.db.close();
    }
  });

  it("maps the archive Worker's snapshot fence to a normal decline", async () => {
    const database = openDatabase();
    try {
      const materializePlans = async (): Promise<MaterializedSessionStateDeletePlan[]> => {
        throw new Error(
          "SQLite session state changed before archive materialization for session-1",
        );
      };
      await expect(
        reconcileReclamationArchivePhase({ database, materializePlans, plan: evictionPlan() }),
      ).rejects.toBeInstanceOf(ReclamationPhaseDeclinedError);
      expect(archiveSha(database)).toBeUndefined();
    } finally {
      database.db.close();
    }
  });

  it("passes the guard for the eviction kind and invokes the injected delete", async () => {
    const database = openDatabase();
    try {
      const deleted: string[] = [];
      const planJson = JSON.stringify(evictionPlan());
      const operations = createReclamationPhaseOperations({
        database,
        deleteReclaimedRows: ({ phase }) => {
          deleted.push(phase);
        },
        entry: ledgerEntry(planJson),
        materializePlans: materializeTo(BYTES),
        phase: "batch-delete",
        plan: evictionPlan(),
        runExclusiveWrite: passthrough,
        stepCursor: 2,
      });
      expect(await operations.evaluateGuard()).toEqual({ passed: true });
      await operations.deleteReclaimedRows();
      expect(deleted).toEqual(["batch-delete"]);
    } finally {
      database.db.close();
    }
  });

  it("gates the decline cleanup on the cursor through the operation", async () => {
    const database = openDatabase();
    try {
      const seed = async () => {
        if (archiveSha(database) === undefined) {
          await reconcileReclamationArchivePhase({
            database,
            materializePlans: materializeTo(BYTES),
            plan: evictionPlan(),
          });
        }
      };
      const removeWithCursor = async (stepCursor: number) => {
        const operations = createReclamationPhaseOperations({
          database,
          deleteReclaimedRows: () => undefined,
          entry: ledgerEntry(JSON.stringify(evictionPlan())),
          phase: "batch-delete",
          plan: evictionPlan(),
          runExclusiveWrite: passthrough,
          stepCursor,
        });
        await operations.removeArchiveForDecline();
      };

      await seed();
      await removeWithCursor(2);
      expect(archiveSha(database)).toBeUndefined();

      await seed();
      await removeWithCursor(3);
      expect(archiveSha(database)).toBe(hashSessionArchiveBytes(BYTES));
    } finally {
      database.db.close();
    }
  });

  it("refuses the four-phase path for entry and lifecycle-artifacts plans", () => {
    const database = openDatabase();
    try {
      const build = (plan: SerializedSqliteSessionReclamationPlan) => () =>
        createReclamationPhaseOperations({
          database,
          deleteReclaimedRows: () => undefined,
          entry: ledgerEntry(JSON.stringify(plan)),
          phase: "guard",
          plan,
          stepCursor: 0,
        });
      const lifecyclePlan = {
        ...evictionPlan(),
        agentId: "agent-1",
        entries: [],
        kind: "lifecycle-artifacts",
      } as unknown as SerializedSqliteSessionReclamationPlan;
      expect(build(lifecyclePlan)).toThrow(/lifecycle-artifacts/);
      // Ruling 1 (2026-10-01): entry keeps its atomic single-transaction path.
      const entryPlan = {
        ...evictionPlan(),
        kind: "entry",
      } as unknown as SerializedSqliteSessionReclamationPlan;
      expect(build(entryPlan)).toThrow(/entry/);
    } finally {
      database.db.close();
    }
  });

  it("builds per-phase operations from the ledger entry's durable plan_json", async () => {
    const database = openDatabase();
    try {
      const factory = createReclamationPhaseOperationsFactory({
        database,
        deleteReclaimedRows: () => undefined,
        materializePlans: materializeTo(BYTES),
        runExclusiveWrite: passthrough,
      });
      const entry = ledgerEntry(JSON.stringify(evictionPlan()));
      const guard = factory({ entry, phase: "guard", stepCursor: 0 });
      expect(await guard.evaluateGuard()).toEqual({ passed: true });
      await guard.appendArchive();
      expect(archiveSha(database)).toBe(hashSessionArchiveBytes(BYTES));
    } finally {
      database.db.close();
    }
  });
});
