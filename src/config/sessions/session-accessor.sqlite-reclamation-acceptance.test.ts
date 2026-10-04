/**
 * Phase-split piece (v): the acceptance rows.
 *
 * The per-phase behaviours are already pinned in the executor / operations / phases / queue
 * suites. These rows assert the composition those phases add up to, which is what Claude's
 * acceptance list is about:
 *
 *  - the crash matrix resumed from EVERY committed cursor (0..4), never replaying a phase;
 *  - the invariant asserted at terminal states, not only clean runs: an archive is durably
 *    committed before any delete, and the archive set is a superset of the deleted set
 *    (the queue-completion key: the ledger row reaching done IS the completion signal);
 *  - the sha256-mismatch branch -- terminal failed with NO delete, the invariant's last line
 *    of defence;
 *  - full-session deletion: the session_windows cascade leaves no watermark row and no FK error.
 *
 * The queue-completion-keyed replacement assertions that used to live inline in the heavy
 * suites fold in here (piece v, per the 2026-10-04 record section 4).
 */
import path from "node:path";
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
  advanceReclamationLedgerStep,
  claimNextReclamationLedgerEntry,
  createReclamationLedgerRunner,
  enqueueReclamationLedgerEntry,
  hasClaimableReclamationLedgerEntry,
  readReclamationLedgerEntry,
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
import {
  type PhasedSqliteSessionReclamationPlan,
  runPhasedSqliteSessionReclamation,
} from "./session-accessor.sqlite-reclamation-phased.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";
const SESSION_ID = "session-acceptance";
const BYTES = new Uint8Array([7, 7, 1, 4]);
const OTHER_BYTES = new Uint8Array([9, 9, 2, 5]);

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

function countingClock(): () => number {
  let tick = 2_000;
  return () => (tick += 1);
}

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

function evictionPlanJson(databasePath: string, archiveDirectory: string): string {
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
        archiveDirectory,
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
        encoding: "zstd" as const,
        sha256: hashSessionArchiveBytes(bytes),
      },
      archivedTranscript: null,
    }));
}

function phasedEvictionPlan(params: {
  archiveDirectory: string;
  bytes: Uint8Array;
  database: OpenClawAgentDatabase;
  stateDir: string;
}): PhasedSqliteSessionReclamationPlan {
  return {
    databaseOptions: {
      agentId: "main",
      env: { OPENCLAW_STATE_DIR: params.stateDir },
      path: params.database.path,
    },
    diskBudget: { preserveRecentMs: null },
    kind: "history-eviction",
    materializedPlans: [
      {
        agentId: "main",
        archive: {
          archiveName: "archive.jsonl.zst",
          bytes: params.bytes,
          createdAt: 1_700_000_500_000,
          encoding: "zstd",
          sha256: hashSessionArchiveBytes(params.bytes),
        },
        archiveDirectory: params.archiveDirectory,
        archiveTranscript: true,
        databasePath: params.database.path,
        reason: "deleted",
        sessionId: SESSION_ID,
        snapshot: {
          acpParentStreamEventCount: 0,
          generation: GENERATION,
          lastSeq: 1,
          sessionKey: "key:" + SESSION_ID,
          sessionUpdatedAt: 1,
          trajectoryLastSeq: null,
          transcriptUpdatedAt: 1,
        },
      },
    ],
    protectedSessionIds: [],
    sessionId: SESSION_ID,
  };
}

function rowCount(database: DatabaseSync, table: string, sessionId: string): number {
  const row = database
    .prepare(`SELECT COUNT(*) AS n FROM ${table} WHERE session_id = ?`)
    .get(sessionId) as { n: number };
  return row.n;
}

describe("reclamation acceptance (piece v)", () => {
  // Runs before the module-level temp-dir cleanup: an open agent DB handle turns that
  // rmSync into an EPERM on Windows.
  afterEach(() => {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
  });

  const CRASH_MATRIX: Array<{ label: string; cursor: number; phases: string[] }> = [
    {
      cursor: 0,
      label: "before/during the guard",
      phases: [
        "session.reclamation.phase-guard",
        "session.reclamation.phase-archive",
        "session.reclamation.phase-guard",
        "session.reclamation.phase-delete",
      ],
    },
    {
      cursor: 1,
      label: "after the archive boundary",
      phases: [
        "session.reclamation.phase-archive",
        "session.reclamation.phase-guard",
        "session.reclamation.phase-delete",
      ],
    },
    {
      cursor: 2,
      label: "mid batch-delete",
      phases: ["session.reclamation.phase-guard", "session.reclamation.phase-delete"],
    },
    { cursor: 3, label: "after the delete, before complete", phases: [] },
    { cursor: 4, label: "after complete", phases: [] },
  ];

  it.each(CRASH_MATRIX)(
    "resumes a crash at cursor $cursor ($label) without replaying earlier phases",
    async ({ cursor, phases }) => {
      const database = openInMemoryAgentDatabase();
      try {
        enqueueReclamationLedgerEntry(database.db, {
          generation: GENERATION,
          now: 1_000,
          planJson: evictionPlanJson(":memory:", "C:\\state\\archives"),
          sessionId: SESSION_ID,
        });
        const claimed = claimNextReclamationLedgerEntry(database.db, { now: 1_100 });
        if (!claimed) {
          throw new Error("expected the acceptance fixture to claim its entry");
        }
        if (cursor > 0) {
          advanceReclamationLedgerStep(database.db, {
            id: claimed.id,
            now: 1_200,
            stepCursor: cursor,
          });
        }
        const entry = readReclamationLedgerEntry(database.db, claimed.id);
        if (!entry) {
          throw new Error("expected the advanced entry");
        }

        const events: string[] = [];
        const exclusive: ReclamationPhaseExclusiveWrite = async (operation, run) => {
          events.push(operation);
          return await run();
        };

        const result = await executeReclamationLedgerEntry({
          createOperations: createReclamationPhaseOperationsFactory({
            database,
            deleteReclaimedRows: () => {},
            materializePlans: materializeTo(BYTES),
            runExclusiveWrite: exclusive,
          }),
          database: database.db,
          entry,
          now: countingClock(),
        });

        expect(result).toMatchObject({ kind: "done", sessionFullyDeleted: false });
        // Resume runs exactly the phases a committed cursor leaves, in order.
        expect(events).toEqual(phases);
        expect(readReclamationLedgerEntry(database.db, claimed.id)).toMatchObject({
          state: "done",
          stepCursor: 4,
        });
        // When the archive ran, the committed row is the identity the completion resolves.
        if (cursor <= 1) {
          expect(
            readReclamationArchiveSha256(database, {
              generation: GENERATION,
              sessionId: SESSION_ID,
            }),
          ).toBe(hashSessionArchiveBytes(BYTES));
        }
      } finally {
        database.db.close();
      }
    },
  );

  it("commits the archive before any delete and reaches done once through the queue", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-acceptance-invariant-");
    const options = { agentId: "main", env: { OPENCLAW_STATE_DIR: stateDir } };
    const database = openOpenClawAgentDatabase(options);
    ensureReclamationLedgerSchema(database.db);
    const archiveDirectory = path.join(stateDir, "archives");
    enqueueReclamationLedgerEntry(database.db, {
      generation: GENERATION,
      now: 1_000,
      planJson: evictionPlanJson(database.path, archiveDirectory),
      sessionId: SESSION_ID,
    });

    // The invariant, sampled at the exact moment of the irreversible op: the archive must
    // already be durable when the delete phase runs.
    const archiveAtDelete: Array<string | undefined> = [];
    const processed: string[] = [];
    const runner = createReclamationLedgerRunner({
      claimNext: () => claimNextReclamationLedgerEntry(database.db, { now: 2_000 }),
      hasPending: () => hasClaimableReclamationLedgerEntry(database.db),
      processEntry: async (entry) => {
        processed.push(entry.sessionId);
        await executeReclamationLedgerEntry({
          createOperations: createReclamationPhaseOperationsFactory({
            database,
            deleteReclaimedRows: () => {
              archiveAtDelete.push(
                readReclamationArchiveSha256(database, {
                  generation: GENERATION,
                  sessionId: SESSION_ID,
                }),
              );
            },
            materializePlans: materializeTo(BYTES),
          }),
          database: database.db,
          entry,
          now: countingClock(),
        });
      },
    });

    runner.push();
    await runner.whenIdle();

    expect(processed).toEqual([SESSION_ID]);
    // Archive set superset of deleted set: the delete never ran without a committed archive.
    expect(archiveAtDelete).toEqual([hashSessionArchiveBytes(BYTES)]);
    expect(
      readReclamationArchiveSha256(database, { generation: GENERATION, sessionId: SESSION_ID }),
    ).toBe(hashSessionArchiveBytes(BYTES));
    // The queue-completion key is the terminal ledger row and an empty queue.
    expect(readReclamationLedgerEntry(database.db, 1)).toMatchObject({ state: "done" });
    expect(hasClaimableReclamationLedgerEntry(database.db)).toBe(false);
    expect(runner.isRunning()).toBe(false);
  });

  it("is terminal failed and deletes nothing when the stored archive sha256 differs from the plan", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-acceptance-sha-");
    const options = { agentId: "main", env: { OPENCLAW_STATE_DIR: stateDir } };
    const database = openOpenClawAgentDatabase(options);
    ensureReclamationLedgerSchema(database.db);
    const archiveDirectory = path.join(stateDir, "archives");
    const deleteCalls: number[] = [];

    // First run commits BYTES under the generation.
    await runPhasedSqliteSessionReclamation({
      deleteReclaimedRows: () => {
        deleteCalls.push(1);
      },
      now: countingClock(),
      plan: phasedEvictionPlan({ archiveDirectory, bytes: BYTES, database, stateDir }),
      validateInlineSnapshot: () => {},
    });
    expect(deleteCalls).toEqual([1]);

    // A re-plan whose bytes do not match the stored row is the one branch where advancing the
    // cursor would delete events this plan never archived. It must be terminal failed, no delete.
    await expect(
      runPhasedSqliteSessionReclamation({
        deleteReclaimedRows: () => {
          deleteCalls.push(2);
        },
        now: countingClock(),
        plan: phasedEvictionPlan({ archiveDirectory, bytes: OTHER_BYTES, database, stateDir }),
        validateInlineSnapshot: () => {},
      }),
    ).rejects.toThrow();

    expect(deleteCalls).toEqual([1]);
    expect(
      readReclamationArchiveSha256(database, { generation: GENERATION, sessionId: SESSION_ID }),
    ).toBe(hashSessionArchiveBytes(BYTES));
    expect(readReclamationLedgerEntry(database.db, 2)).toMatchObject({
      state: "failed",
      stepCursor: 1,
    });
  });

  it("completes a fully deleted session and leaves no watermark behind", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-acceptance-full-");
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
    enqueueReclamationLedgerEntry(database.db, {
      generation: GENERATION,
      now: 1_000,
      planJson,
      sessionId: SESSION_ID,
    });
    const entry = claimNextReclamationLedgerEntry(database.db, { now: 1_100 });
    if (!entry) {
      throw new Error("expected the acceptance fixture to claim its entry");
    }

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

    expect(result).toEqual({
      archivedTranscripts: [],
      kind: "done",
      sessionFullyDeleted: true,
    });
    expect(readReclamationLedgerEntry(database.db, entry.id)).toMatchObject({
      sessionFullyDeleted: true,
      state: "done",
    });
    // The four-phase ruling drops the conditional watermark step: the session_windows FK cascade
    // removed the row, so no FK error and no resurrected watermark.
    expect(rowCount(database.db, "session_windows", SESSION_ID)).toBe(0);
    expect(rowCount(database.db, "transcript_rewrite_watermarks", SESSION_ID)).toBe(0);
  });
});
