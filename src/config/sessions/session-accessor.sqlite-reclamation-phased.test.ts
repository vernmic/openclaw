import { createHash } from "node:crypto";
import { expect, test } from "vitest";
import { observeHostDataSql } from "../../../test/helpers/sqlite-statement-execution-counter.js";
import {
  closeOpenClawAgentDatabaseByPathAsync,
  openOpenClawAgentDatabase,
  runOpenClawAgentWriteTransaction,
} from "../../state/openclaw-agent-db.js";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import { resolveSessionArtifactDirectory } from "./paths.js";
import { prepareSessionTranscriptArchivePublishPlans } from "./session-accessor.sqlite-archive-store-kernel.js";
import { materializeSessionStateDeletePlans } from "./session-accessor.sqlite-archive.js";
import { readSessionStateDeleteSnapshot } from "./session-accessor.sqlite-delete-snapshot.js";
import type { ReclamationPhaseRequest } from "./session-accessor.sqlite-lifecycle-types.js";
import { recoverPendingSessionReclamations } from "./session-accessor.sqlite-reclamation-phased.js";
import type { ResumableReclamationPlan } from "./session-accessor.sqlite-reclamation-plan.js";
import { runSqliteSessionReclamation } from "./session-accessor.sqlite-reclamation-run.js";
import {
  createHistoryEvictionReclamationPlan,
  resolveSessionReclamationDatabaseOptions,
} from "./session-accessor.sqlite-reclamation.js";

// One isolated store and archive batch exercise actual native worker boundaries without per-case boots.
test("native worker resumes committed cursors and fences takeover, integrity, generation and copied-plan failures", async () => {
  await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
    const databaseOptions = resolveSessionReclamationDatabaseOptions({
      agentId: "main",
      env: state.env,
    });
    let database = openOpenClawAgentDatabase(databaseOptions);
    const archiveDirectory = resolveSessionArtifactDirectory(databaseOptions.path);
    const ids = [
      "cursor1",
      "cursor2",
      "cursor3",
      "takeover",
      "generation",
      "sha",
      "copied",
      "wrongroot",
      "corrupt",
      "facade",
    ];
    runOpenClawAgentWriteTransaction((owner) => {
      for (const id of ids) {
        const sessionKey = `agent:main:${id}`;
        const currentId = `current-${id}`;
        owner.db
          .prepare(
            "INSERT INTO session_nodes(session_key,current_session_id,entry_json,updated_at) VALUES (?,?,?,1)",
          )
          .run(sessionKey, currentId, JSON.stringify({ sessionId: currentId, updatedAt: 1 }));
        owner.db
          .prepare(
            "INSERT INTO session_windows(session_id,session_key,created_at,updated_at) VALUES (?,?,1,1)",
          )
          .run(currentId, sessionKey);
        owner.db
          .prepare(
            "INSERT INTO session_windows(session_id,session_key,created_at,updated_at) VALUES (?,?,1,1)",
          )
          .run(id, `agent:main:${id}`);
        owner.db
          .prepare(
            "INSERT INTO transcript_rewrite_watermarks(session_id,generation,updated_at) VALUES (?,?,1)",
          )
          .run(id, `generation-${id}`);
        owner.db
          .prepare(
            "INSERT INTO transcript_events(session_id,seq,event_json,created_at) VALUES (?,0,?,1)",
          )
          .run(
            id,
            JSON.stringify({
              type: "message",
              id: "m1",
              message: { role: "assistant", content: [{ type: "text", text: id }], timestamp: 1 },
            }),
          );
      }
    }, databaseOptions);
    const materialized = await materializeSessionStateDeletePlans(
      ids.map((sessionId) => ({
        agentId: "main",
        databasePath: databaseOptions.path,
        archiveDirectory,
        archiveTranscript: true,
        reason: "deleted" as const,
        sessionId,
        snapshot: readSessionStateDeleteSnapshot(database.db, sessionId),
      })),
    );
    const plans = new Map(
      ids.map((sessionId, index) => [
        sessionId,
        createHistoryEvictionReclamationPlan({
          databaseOptions,
          diskBudget: { preserveRecentMs: null },
          materializedPlans: materialized.slice(index, index + 1),
          protectedSessionIds: new Set(),
          sessionId,
        }),
      ]),
    );
    const get = (id: string) => {
      const plan = plans.get(id);
      if (!plan) {
        throw new Error("Missing fixture");
      }
      return plan;
    };
    const phase = (
      id: string,
      step: ReclamationPhaseRequest["phase"],
      ownerId = "owner-a",
      expectedOwnerId?: string,
    ) => {
      const original = get(id);
      const plan: ResumableReclamationPlan = {
        ...original,
        materializedPlans: original.materializedPlans.map((item) => ({
          ...item,
          archive:
            step === "archive" && item.archive
              ? { ...item.archive, bytes: item.archive.bytes.slice() }
              : null,
          archivedTranscript: null,
        })),
        reclamation: {
          id: `job-${id}`,
          ownerId,
          expectedOwnerId,
          phase: step,
          archiveSha256: original.materializedPlans[0]?.archive?.sha256,
        },
      };
      return runSqliteSessionReclamation({ plan, forceInProcess: false });
    };
    for (const id of ids.filter((candidateId) => candidateId !== "facade")) {
      expect((await phase(id, "guard")).reclamationReceipt?.cursor).toBe(1);
      if (!["cursor1", "copied", "wrongroot", "corrupt"].includes(id)) {
        expect((await phase(id, "archive")).reclamationReceipt?.cursor).toBe(2);
      }
    }
    expect((await phase("cursor3", "delete")).reclamationReceipt?.cursor).toBe(3);
    // Even the already-deleted generation retains its canonical recovery bytes until COMPLETE.
    expect(
      prepareSessionTranscriptArchivePublishPlans(database, { archiveDirectory, requested: [] }),
    ).toEqual([]);
    await expect(phase("takeover", "delete", "owner-b")).rejects.toThrow(/exact owner/);
    expect(
      (await phase("takeover", "delete", "owner-b", "owner-a")).reclamationReceipt?.cursor,
    ).toBe(3);
    await expect(phase("takeover", "complete", "owner-a")).rejects.toThrow(/exact owner/);
    runOpenClawAgentWriteTransaction((owner) => {
      owner.db
        .prepare(
          "UPDATE transcript_rewrite_watermarks SET generation = 'changed' WHERE session_id = 'generation'",
        )
        .run();
      const forged = Buffer.from("forged archive");
      owner.db
        .prepare(
          "UPDATE session_transcript_archives SET archive_blob = ?, archive_sha256 = ? WHERE session_id = 'sha'",
        )
        .run(forged, createHash("sha256").update(forged).digest("hex"));
      owner.db
        .prepare(
          "UPDATE session_reclamation_jobs SET source_identity = 'copied-production-source' WHERE session_id = 'copied'",
        )
        .run();
      owner.db
        .prepare(
          "UPDATE session_reclamation_jobs SET archive_directory = 'Z:/synthetic-other-instance/sessions' WHERE session_id = 'wrongroot'",
        )
        .run();
      owner.db
        .prepare(
          "UPDATE session_reclamation_jobs SET plan_json = '{broken' WHERE session_id = 'corrupt'",
        )
        .run();
    }, databaseOptions);
    expect((await phase("generation", "delete")).reclamationReceipt?.state).toBe("declined");
    expect((await phase("sha", "delete")).reclamationReceipt?.state).toBe("failed");
    await closeOpenClawAgentDatabaseByPathAsync(databaseOptions.path);
    const reports: string[] = [];
    const observation = observeHostDataSql();
    try {
      expect(
        await recoverPendingSessionReclamations(
          {
            databaseOptions,
            archiveDirectory,
            assertCurrent() {},
            reportDeclined: (message) => reports.push(message),
          },
          runSqliteSessionReclamation,
        ),
      ).toBe(4);
      expect(observation.queries).toEqual([]);
    } finally {
      observation.restore();
    }
    database = openOpenClawAgentDatabase(databaseOptions);
    expect(reports.some((message) => message.includes("copied source"))).toBe(true);
    expect(reports.some((message) => message.includes("SyntaxError"))).toBe(true);
    const rows = database.db
      .prepare(
        "SELECT session_id,state,step_cursor,plan_json FROM session_reclamation_jobs ORDER BY session_id",
      )
      .all();
    expect(rows.filter((row) => row.state === "done").map((row) => row.session_id)).toEqual([
      "cursor1",
      "cursor2",
      "cursor3",
      "takeover",
    ]);
    expect(
      rows.every(
        (row) =>
          !String(row.plan_json).includes('"bytes"') &&
          !String(row.plan_json).includes("OPENCLAW_STATE_DIR"),
      ),
    ).toBe(true);
    for (const id of ["cursor1", "cursor2", "cursor3", "takeover"]) {
      expect(
        database.db.prepare("SELECT session_id FROM session_windows WHERE session_id = ?").get(id),
      ).toBeUndefined();
      expect(
        database.db
          .prepare("SELECT session_id FROM transcript_rewrite_watermarks WHERE session_id = ?")
          .get(id),
      ).toBeUndefined();
    }
    for (const id of ["generation", "sha", "copied", "wrongroot", "corrupt"]) {
      expect(
        database.db.prepare("SELECT session_id FROM session_windows WHERE session_id = ?").get(id),
      ).toBeDefined();
    }
    const original = get("facade");
    const finished = await runSqliteSessionReclamation({ plan: original, forceInProcess: false });
    expect(finished).not.toHaveProperty("reclamationReceipt");
    expect(
      database.db
        .prepare("SELECT state FROM session_reclamation_jobs WHERE session_id = 'facade'")
        .get(),
    ).toEqual({ state: "done" });
    if (finished.kind !== "history-eviction") {
      throw new Error("Wrong native facade result");
    }
    expect(finished.value.deleted).toBe(true);
    expect(finished.value.archivedTranscripts).toHaveLength(1);
  });
});
