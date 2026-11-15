import { afterEach, beforeEach, expect, it } from "vitest";
import { resolveAgentRunSessionTarget } from "../agents/run-session-target.js";
import { SessionManager } from "../agents/sessions/index.js";
import {
  makeAgentAssistantMessage,
  makeAgentUserMessage,
} from "../agents/test-helpers/agent-message-fixtures.js";
import { closeOpenClawAgentDatabasesForTest } from "../state/openclaw-agent-db.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../test-utils/openclaw-test-state.js";
import {
  recordPluginForkedTurnSession,
  recordPluginForkedTurnSessionIfRequested,
  type ForkTurnMessage,
} from "./plugin-forked-turn-visible-session.js";

let state: OpenClawTestState;

beforeEach(async () => {
  state = await createOpenClawTestState({ layout: "state-only" });
});
afterEach(async () => {
  closeOpenClawAgentDatabasesForTest();
  await state.cleanup();
});

const turns: ForkTurnMessage[] = [
  makeAgentUserMessage({ content: "Review the finished turn." }),
  makeAgentAssistantMessage({ content: "Recorded one change." }),
];

async function readRecordedMessages(sessionKey: string, sessionId: string) {
  const target = await resolveAgentRunSessionTarget({
    agentId: "main",
    config: {},
    sessionId,
    sessionKey,
    missingSessionKey: "resolve-existing",
  });
  const session = await SessionManager.openAsync(target, state.workspaceDir);
  return session
    .getEntries()
    .filter((entry) => entry.type === "message")
    .map((entry) => entry.message);
}

it("records one visible session with the fork's own turns", async () => {
  const first = await recordPluginForkedTurnSession({
    config: {},
    agentId: "main",
    workspaceDir: state.workspaceDir,
    category: "workshop-review",
    label: "Skill review",
    messages: turns,
  });
  expect(first?.sessionKey).toMatch(/^agent:main:workshop-review:[0-9a-f-]{36}$/u);
  if (!first) {
    throw new Error("the record was not written");
  }
  const messages = await readRecordedMessages(first.sessionKey, first.sessionId);
  expect(messages).toHaveLength(2);
  expect(messages[0]).toMatchObject({ role: "user", content: "Review the finished turn." });
  expect(messages[1]).toMatchObject({ role: "assistant", content: "Recorded one change." });

  // Every settlement gets its own session: a later record never reuses an earlier one.
  const second = await recordPluginForkedTurnSession({
    config: {},
    agentId: "main",
    workspaceDir: state.workspaceDir,
    category: "workshop-review",
    label: "Skill review",
    messages: turns,
  });
  expect(second?.sessionKey).not.toBe(first.sessionKey);
});

it("writes nothing without turns or without a request", async () => {
  await expect(
    recordPluginForkedTurnSession({
      config: {},
      agentId: "main",
      workspaceDir: state.workspaceDir,
      category: "workshop-review",
      messages: [],
    }),
  ).resolves.toBeUndefined();
  await expect(
    recordPluginForkedTurnSessionIfRequested({
      record: undefined,
      config: {},
      agentId: "main",
      workspaceDir: state.workspaceDir,
      messages: turns,
    }),
  ).resolves.toBeUndefined();
});

it("refuses a category that cannot be a session-key segment", async () => {
  await expect(
    recordPluginForkedTurnSession({
      config: {},
      agentId: "main",
      workspaceDir: state.workspaceDir,
      category: "  ",
      messages: turns,
    }),
  ).rejects.toThrow("A visible fork session needs a category.");
});
