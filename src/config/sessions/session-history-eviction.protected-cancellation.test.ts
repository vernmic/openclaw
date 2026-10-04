import fs from "node:fs";
import path from "node:path";
import type { Worker } from "node:worker_threads";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resetAgentRunRegistryForTest } from "../../infra/agent-run-registry.js";
import { executeSqliteQuerySync } from "../../infra/kysely-sync.js";
import * as lifecycle from "../../sessions/session-lifecycle-admission.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../../test-utils/openclaw-test-state.js";
import {
  appendTranscriptMessage,
  loadTranscriptEventsSync,
  replaceSessionEntry,
  replaceSessionEntrySync,
  resetSessionEntryLifecycle,
} from "./session-accessor.js";
import { getSessionKysely } from "./session-accessor.sqlite-scope.js";
import { enforceSqliteSessionHistoryDiskBudget } from "./session-history-eviction.js";
import { resolveSqliteTargetFromSessionStorePath } from "./session-sqlite-target.js";

describe("protected historical session cancellation", () => {
  let testState: OpenClawTestState;
  let tempDir: string;
  let storePath: string;

  beforeEach(async () => {
    testState = await createOpenClawTestState({
      prefix: "openclaw-protected-history-cancellation-",
      layout: "state-only",
    });
    tempDir = testState.sessionsDir();
    fs.mkdirSync(tempDir, { recursive: true });
    storePath = path.join(tempDir, "sessions.json");
  });

  afterEach(async () => {
    resetAgentRunRegistryForTest();
    vi.restoreAllMocks();
    await enforceSqliteSessionHistoryDiskBudget({
      storePath,
      mode: "warn",
      maintenance: { maxDiskBytes: null, highWaterBytes: null },
    });
    closeOpenClawAgentDatabasesForTest();
    await testState.cleanup();
  });

  it.each([
    ["planning", false],
    ["planning", true],
    ["materialization", false],
    ["materialization", true],
    ["worker", false],
    ["worker", true],
    ["archived entry", false],
    ["archived entry", true],
  ] as const)(
    "retains history after %s cancellation releases pressure (measurement fails: %s)",
    async (stage, measurementFails) => {
      const dayMs = 24 * 60 * 60 * 1000;
      const oldestAt = Date.now() - 8 * dayMs;
      const histories = ["protected", "next"].map((name, index) => ({
        sessionKey: `agent:main:${name}-history`,
        sessionId: `${name}-old`,
        nextSessionId: `${name}-live`,
        updatedAt: oldestAt + index,
        content: `retain ${name} history`,
      }));
      for (const history of histories) {
        await createCandidateTranscript(history, stage === "archived entry");
      }
      const protectedHistory = histories[0]!;
      const beforeEvents = histories.map((history) =>
        loadTranscriptEventsSync({ ...history, storePath }),
      );
      expect(beforeEvents.every((events) => events.length > 0)).toBe(true);
      settlePhysicalUsage();
      // This live peer artifact counts toward pressure but is not an archive-pruning target.
      const peerArtifact = path.join(tempDir, "peer-live.jsonl");
      const peerBytes = 2 * 1024 * 1024;
      fs.writeFileSync(peerArtifact, Buffer.alloc(peerBytes));
      const diskBudget = await import("./disk-budget.js");
      const measure = diskBudget.measureSessionPhysicalDiskUsage;
      const before = await measure(storePath);
      const highWaterBytes = before.totalBytes - peerBytes / 2;
      const measurementFailure = new Error("synthetic post-cancellation measurement failure");
      let protectionChanged = false;
      vi.spyOn(diskBudget, "measureSessionPhysicalDiskUsage").mockImplementation(
        async (pathname) => {
          if (protectionChanged) {
            expect(
              lifecycle.isSessionLifecycleMutationActive(storePath, [protectedHistory.sessionId]),
            ).toBe(false);
            if (measurementFails) {
              throw measurementFailure;
            }
          }
          return await measure(pathname);
        },
      );
      const releasePressure = () => {
        expect(protectionChanged).toBe(false);
        replaceSessionEntrySync(
          { sessionKey: protectedHistory.sessionKey, storePath },
          stage === "archived entry"
            ? {
                sessionId: protectedHistory.sessionId,
                updatedAt: Date.now(),
                archivedAt: protectedHistory.updatedAt,
                archiveReason: "active-session-cap",
              }
            : { sessionId: protectedHistory.nextSessionId, updatedAt: Date.now() },
        );
        fs.unlinkSync(peerArtifact);
        protectionChanged = true;
      };
      if (stage === "planning") {
        const mutate = lifecycle.runExclusiveSessionLifecycleMutation;
        vi.spyOn(lifecycle, "runExclusiveSessionLifecycleMutation").mockImplementation((params) =>
          mutate({
            ...params,
            run: async () => {
              if (
                !protectionChanged &&
                "scope" in params &&
                params.scope === storePath &&
                Array.from(params.identities).includes(protectedHistory.sessionId)
              ) {
                releasePressure();
              }
              return await params.run();
            },
          }),
        );
      }
      if (stage === "materialization") {
        const archive = await import("./session-accessor.sqlite-archive.js");
        const materialize = archive.materializeSessionStateDeletePlans;
        vi.spyOn(archive, "materializeSessionStateDeletePlans").mockImplementationOnce(
          async (plans) => {
            expect(plans.map((plan) => plan.sessionId)).toEqual([protectedHistory.sessionId]);
            const prepared = await materialize(plans);
            releasePressure();
            return prepared;
          },
        );
      }
      const reclamation = await import("./session-accessor.sqlite-reclamation.js");
      const reclaimedHistories: Array<{ sessionId: string; deleted: boolean }> = [];
      const reclaimedEntries: Array<{ sessionKey: string; deleted: boolean }> = [];
      if (stage === "worker") {
        // The phased inline drive has no archive-Worker admission request, so the mid-drive
        // cancellation that releases pressure is injected at the drive head; the piece-(v)
        // acceptance set keys this to queue completion.
        const phased = await import("./session-accessor.sqlite-reclamation-phased.js");
        const drivePhased = phased.runPhasedSqliteSessionReclamation;
        vi.spyOn(phased, "runPhasedSqliteSessionReclamation").mockImplementation(async (params) => {
          if (!protectionChanged) {
            releasePressure();
          }
          return await drivePhased(params);
        });
      }
      // Path-agnostic observation (piece iv-3, delta 3): the entry kind stays atomic while
      // history-eviction goes phased, but both routes drive through the one shared
      // exclusive-reclamation wrapper, so one seam covers the mixed routing.
      const exclusive = reclamation.runExclusiveSqliteSessionReclamation;
      vi.spyOn(reclamation, "runExclusiveSqliteSessionReclamation").mockImplementation(
        async (run) => {
          const result = await exclusive(run);
          if (stage === "worker" && result === null) {
            reclaimedHistories.push({ sessionId: protectedHistory.sessionId, deleted: false });
          } else if (
            stage === "archived entry" &&
            (result as { deleted?: boolean } | null)?.deleted === false
          ) {
            reclaimedEntries.push({ sessionKey: protectedHistory.sessionKey, deleted: false });
          }
          return result;
        },
      );
      const admissions: Array<{ workerThreadId: number; admissionId: number | undefined }> = [];
      const detachWorkerListeners: Array<() => void> = [];
      const observeWorker = (worker: Worker) => {
        const workerThreadId = worker.threadId;
        const observeMessage = (message: { type?: string; admissionId?: number }) => {
          // Archive materialization and disk scans do not request reclamation write admission.
          if (message.type === "admission-request") {
            admissions.push({ workerThreadId, admissionId: message.admissionId });
            if ((stage === "worker" || stage === "archived entry") && !protectionChanged) {
              releasePressure();
            }
          }
        };
        worker.on("message", observeMessage);
        detachWorkerListeners.push(() => worker.off("message", observeMessage));
      };
      process.on("worker", observeWorker);
      try {
        const sweep = enforceSqliteSessionHistoryDiskBudget({
          storePath,
          mode: "enforce",
          maintenance: {
            maxDiskBytes: before.totalBytes - 1,
            highWaterBytes,
            preserveRecentMs: 7 * dayMs,
          },
        });
        let result: Awaited<ReturnType<typeof enforceSqliteSessionHistoryDiskBudget>> | undefined;
        if (measurementFails) {
          await expect(sweep).rejects.toBe(measurementFailure);
        } else {
          result = await sweep;
        }
        expect(protectionChanged).toBe(true);
        expect(fs.existsSync(peerArtifact)).toBe(false);
        if (stage === "worker") {
          // Queue-completion-keyed replacement (piece iv-3 delta 3): the phased inline drive has
          // no archive-Worker admission request, so instead of counting worker admissions we
          // assert the durable outcome -- the protected candidate's ledger entry terminalised as
          // declined while its history stayed intact (asserted below).
          expect(
            database()
              .db.prepare(
                "SELECT state FROM reclamation_ledger WHERE session_id = ? ORDER BY id DESC LIMIT 1",
              )
              .get(protectedHistory.sessionId),
          ).toEqual({ state: "declined" });
          expect(reclaimedHistories[0]).toEqual({
            sessionId: protectedHistory.sessionId,
            deleted: false,
          });
        } else if (stage === "archived entry") {
          expect(admissions.length).toBeGreaterThan(0);
          expect(reclaimedEntries[0]).toEqual({
            sessionKey: protectedHistory.sessionKey,
            deleted: false,
          });
        }
        for (const [index, history] of histories.entries()) {
          expect(sessionExists(history.sessionId), history.sessionId).toBe(true);
          expect(loadTranscriptEventsSync({ ...history, storePath })).toEqual(beforeEvents[index]);
          expect(readArchiveNames(history.sessionId)).toEqual([]);
        }
        if (stage === "worker") {
          expect(reclaimedHistories).toEqual([
            { sessionId: protectedHistory.sessionId, deleted: false },
          ]);
        } else if (stage === "archived entry") {
          expect(reclaimedEntries).toEqual([
            { sessionKey: protectedHistory.sessionKey, deleted: false },
          ]);
          expect(reclaimedHistories).toEqual([]);
        } else {
          expect(admissions).toEqual([]);
          expect(reclaimedHistories).toEqual([]);
        }
        if (!measurementFails) {
          expect(result).toMatchObject({ removedEntries: 0, removedFiles: 0 });
          expect(result?.totalBytesAfter).toBeLessThanOrEqual(highWaterBytes);
          expect(result?.totalBytesAfter).toBe((await measure(storePath)).totalBytes);
        }
      } finally {
        process.off("worker", observeWorker);
        detachWorkerListeners.forEach((detach) => detach());
      }
    },
  );

  async function createCandidateTranscript(
    params: {
      content: string;
      nextSessionId: string;
      sessionId: string;
      sessionKey: string;
      updatedAt: number;
    },
    archivedEntry: boolean,
  ): Promise<void> {
    await replaceSessionEntry(
      { sessionKey: params.sessionKey, storePath },
      { sessionId: params.sessionId, updatedAt: params.updatedAt },
    );
    await appendTranscriptMessage(
      { sessionId: params.sessionId, sessionKey: params.sessionKey, storePath },
      { message: { role: "user", content: params.content } },
    );
    if (archivedEntry) {
      await replaceSessionEntry(
        { sessionKey: params.sessionKey, storePath },
        {
          sessionId: params.sessionId,
          updatedAt: params.updatedAt,
          archivedAt: params.updatedAt,
          archiveReason: "active-session-cap",
        },
      );
    } else {
      await resetSessionEntryLifecycle({
        storePath,
        target: { canonicalKey: params.sessionKey, storeKeys: [params.sessionKey] },
        buildNextEntry: () => ({
          sessionId: params.nextSessionId,
          updatedAt: params.updatedAt + 1,
        }),
      });
    }
    setSessionUpdatedAt(params.sessionId, params.updatedAt);
  }

  function database() {
    const target = resolveSqliteTargetFromSessionStorePath(storePath);
    if (!target.path) {
      throw new Error("expected SQLite database path");
    }
    return openOpenClawAgentDatabase({ agentId: target.agentId ?? "main", path: target.path });
  }

  function settlePhysicalUsage(): void {
    const owner = database();
    owner.walMaintenance.checkpoint();
    const row = owner.db.prepare("PRAGMA freelist_count").get() as
      | { freelist_count?: unknown }
      | undefined;
    const freePages = Number(row?.freelist_count ?? 0);
    if (Number.isSafeInteger(freePages) && freePages > 0) {
      owner.db.exec(`PRAGMA incremental_vacuum(${freePages});`);
    }
    owner.walMaintenance.checkpoint();
  }

  function setSessionUpdatedAt(sessionId: string, updatedAt: number): void {
    const owner = database();
    const db = getSessionKysely(owner.db);
    executeSqliteQuerySync(
      owner.db,
      db
        .updateTable("session_windows")
        .set({ updated_at: updatedAt })
        .where("session_id", "=", sessionId),
    );
  }

  function sessionExists(sessionId: string): boolean {
    const owner = database();
    const db = getSessionKysely(owner.db);
    return (
      executeSqliteQuerySync(
        owner.db,
        db.selectFrom("session_windows").select("session_id").where("session_id", "=", sessionId),
      ).rows.length === 1
    );
  }

  function readArchiveNames(sessionId: string): string[] {
    return fs.readdirSync(tempDir).filter((name) => name.startsWith(`${sessionId}.jsonl.deleted.`));
  }
});
