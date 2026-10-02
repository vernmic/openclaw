import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
  type OpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import { ensureReclamationLedgerSchema } from "../../state/openclaw-agent-reclamation-ledger-schema.js";
import { readReclamationLedgerEntry } from "../../state/openclaw-agent-reclamation-queue.js";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import {
  hashSessionArchiveBytes,
  type MaterializedSessionStateDeletePlan,
  type SessionStateDeletePlan,
} from "./session-accessor.sqlite-archive.js";
import { readReclamationArchiveSha256 } from "./session-accessor.sqlite-reclamation-archive.js";
import {
  runPhasedSqliteSessionReclamation,
  type PhasedSqliteSessionReclamationPlan,
} from "./session-accessor.sqlite-reclamation-phased.js";

const GENERATION = "443a91f600f748d2a8052cb1fbafa327";
const SESSION_ID = "session-phased";
const BYTES = new Uint8Array([7, 7, 1, 4]);

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

function countingClock(): () => number {
  let tick = 2_000;
  return () => (tick += 1);
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

function evictionPlan(params: {
  archiveDirectory: string;
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
          bytes: BYTES,
          createdAt: 1_700_000_500_000,
          encoding: "zstd",
          sha256: hashSessionArchiveBytes(BYTES),
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

describe("phased reclamation helper (piece iv, step 2)", () => {
  // Runs before the module-level temp-dir cleanup: an open agent DB handle turns that
  // rmSync into an EPERM on Windows.
  afterEach(() => {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
  });

  it("enqueues and drives a history-eviction plan to done, reconstructing the committed archive identity", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-phased-");
    const options = { agentId: "main", env: { OPENCLAW_STATE_DIR: stateDir } };
    const database = openOpenClawAgentDatabase(options);
    ensureReclamationLedgerSchema(database.db);
    const archiveDirectory = path.join(stateDir, "archives");
    const plan = evictionPlan({ archiveDirectory, database, stateDir });

    const result = await runPhasedSqliteSessionReclamation({
      deleteReclaimedRows: () => {},
      materializePlans: materializeTo(BYTES),
      now: countingClock(),
      plan,
    });

    expect(result).toEqual({
      kind: "history-eviction",
      value: {
        archivedTranscripts: [
          {
            archivedPath: path.join(archiveDirectory, "archive.jsonl.zst"),
            generation: GENERATION,
            sessionId: SESSION_ID,
            sourcePath: path.join(archiveDirectory, SESSION_ID + ".jsonl"),
          },
        ],
        deleted: true,
      },
    });
    expect(readReclamationLedgerEntry(database.db, 1)).toMatchObject({
      state: "done",
      stepCursor: 4,
    });
    expect(
      readReclamationArchiveSha256(database, { generation: GENERATION, sessionId: SESSION_ID }),
    ).toBe(hashSessionArchiveBytes(BYTES));
    // The delete phase runs on a delete-only plan: the archive identity is reconstructed from the
    // committed row, never re-materialized into the caller's return by the delete step.
    expect(result.kind === "history-eviction" && result.value.deleted).toBe(true);
  });

  it("re-plans the same session through the same generation without a duplicate archive row", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-phased-replan-");
    const options = { agentId: "main", env: { OPENCLAW_STATE_DIR: stateDir } };
    const database = openOpenClawAgentDatabase(options);
    ensureReclamationLedgerSchema(database.db);
    const archiveDirectory = path.join(stateDir, "archives");
    const plan = evictionPlan({ archiveDirectory, database, stateDir });
    const materializePlans = materializeTo(BYTES);

    const first = await runPhasedSqliteSessionReclamation({
      deleteReclaimedRows: () => {},
      materializePlans,
      now: countingClock(),
      plan,
    });
    const second = await runPhasedSqliteSessionReclamation({
      deleteReclaimedRows: () => {},
      materializePlans,
      now: countingClock(),
      plan,
    });

    expect(first).toEqual(second);
    // Append-only history: a terminal row is not revived, a fresh row carries the re-plan, and the
    // live-uniqueness index plus (session, generation) PK keep exactly one archive for the pair.
    expect(readReclamationLedgerEntry(database.db, 1)).toMatchObject({ state: "done" });
    expect(readReclamationLedgerEntry(database.db, 2)).toMatchObject({ state: "done" });
    expect(
      readReclamationArchiveSha256(database, { generation: GENERATION, sessionId: SESSION_ID }),
    ).toBe(hashSessionArchiveBytes(BYTES));
  });

  it("resolves a guarded historical-generation decline as the atomic decline shape", async () => {
    const stateDir = tempDirs.make("openclaw-reclamation-phased-decline-");
    const options = { agentId: "main", env: { OPENCLAW_STATE_DIR: stateDir } };
    const database = openOpenClawAgentDatabase(options);
    ensureReclamationLedgerSchema(database.db);
    const sessionId = "session-phased-decline";
    // The target matches no live entry, so the shared guard declines before anything is written.
    const plan = {
      databaseOptions: {
        agentId: "main",
        env: { OPENCLAW_STATE_DIR: stateDir },
        path: database.path,
      },
      deleteParams: {
        deleteDeliveryArtifacts: false,
        target: { canonicalKey: "agent:main:" + sessionId, storeKeys: [] },
      },
      kind: "historical-generation",
      materializedPlans: [
        {
          agentId: "main",
          archive: null,
          archiveDirectory: path.join(stateDir, "archives"),
          archiveTranscript: false,
          databasePath: database.path,
          reason: "deleted",
          sessionId,
          snapshot: {
            acpParentStreamEventCount: 0,
            generation: GENERATION,
            lastSeq: null,
            sessionKey: "key:" + sessionId,
            sessionUpdatedAt: 1,
            trajectoryLastSeq: null,
            transcriptUpdatedAt: 1,
          },
        },
      ],
      preparedTargetSnapshot: [],
      protectedSessionIds: [],
      sessionId,
    } as unknown as PhasedSqliteSessionReclamationPlan;

    const result = await runPhasedSqliteSessionReclamation({ now: countingClock(), plan });

    expect(result).toEqual({
      kind: "historical-generation",
      value: { archivedTranscripts: [], deleted: false, expectedEntryMismatch: true },
    });
    expect(readReclamationLedgerEntry(database.db, 1)).toMatchObject({
      state: "declined",
      stepCursor: 0,
    });
  });
});
