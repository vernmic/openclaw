// Terminal-anchor fallback: the transcript leaf can be a non-message record appended after
// the terminal message (openclaw.cache-ttl pruning records, openclaw.nested-tool.v1, lifecycle
// markers). The fallback selector must resolve the newest active MESSAGE from the projection.
// See suits/astinus/analysis/TERMINAL-ANCHOR-CACHE-TTL-FINDING-2026-09-22.md.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import { closeOpenClawStateDatabaseForTest } from "../../state/openclaw-state-db.js";
import {
  persistSessionTranscriptTurn,
  readActiveTranscriptEntryAnchor,
  readNewestActiveMessageTranscriptEntryAnchor,
} from "./session-accessor.js";
import { resolveSqliteTranscriptScope } from "./session-accessor.sqlite-scope.js";
import { appendTranscriptEventsInTransaction } from "./session-accessor.sqlite-transcript-store.js";
import { waitForSessionTranscriptIndexReconcile } from "./session-transcript-reconcile.js";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

describe("terminal anchor fallback (newest active message)", () => {
  let scope: {
    agentId: string;
    env: NodeJS.ProcessEnv;
    sessionId: string;
    sessionKey: string;
  };

  beforeEach(() => {
    const stateDir = tempDirs.make("openclaw-anchor-fallback-");
    scope = {
      agentId: "main",
      env: { ...process.env, OPENCLAW_STATE_DIR: stateDir },
      sessionId: "anchor-fallback-test",
      sessionKey: "agent:main:anchor-fallback-test",
    };
  });

  afterEach(() => {
    closeOpenClawAgentDatabasesForTest();
    closeOpenClawStateDatabaseForTest();
  });

  it("selects the newest active message when the leaf is a non-message record", async () => {
    await persistSessionTranscriptTurn(scope, {
      messages: [
        { eventId: "root", parentId: null, message: { role: "user", content: "root" } },
        {
          eventId: "terminal",
          parentId: "root",
          message: { role: "assistant", content: "terminal" },
        },
      ],
      touchSessionEntry: false,
    });
    const database = openOpenClawAgentDatabase({ agentId: scope.agentId, env: scope.env });
    const resolved = resolveSqliteTranscriptScope(scope);

    // The leaf record: a cache-ttl-style custom event appended after the terminal message.
    appendTranscriptEventsInTransaction(database, resolved, [
      {
        type: "custom",
        customType: "openclaw.cache-ttl",
        data: { prunedToolResults: [] },
        id: "cache-ttl-leaf",
        parentId: "terminal",
        timestamp: new Date().toISOString(),
      } as never,
    ]);
    await waitForSessionTranscriptIndexReconcile({ agentId: scope.agentId, env: scope.env });

    // The leaf-based lookup cannot anchor the custom record (message_position is null).
    expect(
      readActiveTranscriptEntryAnchor({
        agentId: scope.agentId,
        sessionId: scope.sessionId,
        sessionKey: scope.sessionKey,
        storePath: database.path,
        entryId: "cache-ttl-leaf",
      }),
    ).toBeUndefined();

    // The fallback SELECTs the terminal message underneath the custom leaf.
    const anchor = readNewestActiveMessageTranscriptEntryAnchor({
      agentId: scope.agentId,
      sessionId: scope.sessionId,
      sessionKey: scope.sessionKey,
      storePath: database.path,
    });
    expect(anchor?.entryId).toBe("terminal");
    expect(anchor?.activeMessagePosition).toBe(1);
    expect(anchor?.rawSeq).toBeGreaterThan(0);
  });

  it("returns undefined while the projection needs reconcile", async () => {
    // Branch-style seeding (a sibling that goes inactive) is what leaves needs_rebuild set
    // until the deferred reconcile runs - a simple linear append settles inline.
    await persistSessionTranscriptTurn(scope, {
      messages: [
        { eventId: "root", parentId: null, message: { role: "user", content: "root" } },
        {
          eventId: "inactive",
          parentId: "root",
          message: { role: "assistant", content: "inactive" },
        },
        {
          eventId: "terminal",
          parentId: "root",
          message: { role: "assistant", content: "terminal" },
        },
      ],
      touchSessionEntry: false,
    });
    const database = openOpenClawAgentDatabase({ agentId: scope.agentId, env: scope.env });
    const stateRow = database.db
      .prepare("SELECT needs_rebuild FROM session_transcript_index_state WHERE session_id = ?")
      .get(scope.sessionId) as { needs_rebuild: number } | undefined;
    expect(stateRow?.needs_rebuild).toBe(1);
    // Dirty projection: the selector must refuse to answer.
    expect(
      readNewestActiveMessageTranscriptEntryAnchor({
        agentId: scope.agentId,
        sessionId: scope.sessionId,
        sessionKey: scope.sessionKey,
        storePath: database.path,
      }),
    ).toBeUndefined();
  });
});
