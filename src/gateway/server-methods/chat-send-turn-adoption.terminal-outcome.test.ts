import { describe, expect, it, vi } from "vitest";
import type { QueuedFollowupReplyBatch } from "../../auto-reply/reply/queue/types.js";
import {
  beginSessionWorkAdmission,
  getTerminalSessionWorkAdmissionRelease,
} from "../../sessions/session-lifecycle-admission.js";
import type { ChatAbortControllerEntry } from "../chat-abort.js";
import type { QueuedChatTurnMap } from "../chat-queued-turns.js";
import { createChatSendTurnAdoptionLifecycle } from "./chat-send-turn-adoption.js";
import type { GatewayRequestContext } from "./types.js";

const sessionKey = "agent:work:terminal-outcome";

function createSource() {
  const chatQueuedTurns: QueuedChatTurnMap = new Map();
  const controller = new AbortController();
  const sessionBinding: Pick<
    ChatAbortControllerEntry,
    | "sessionKey"
    | "sessionId"
    | "agentId"
    | "lifecycleGeneration"
    | "abortDiagnosticReason"
    | "terminalOutcomeObserved"
  > = {
    sessionKey,
    sessionId: "terminal-outcome-session",
    agentId: "work",
    lifecycleGeneration: "test-generation",
  };
  const context = {
    chatAbortControllers: new Map(),
    chatQueuedTurns,
    dedupe: new Map(),
    logGateway: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
  } as unknown as GatewayRequestContext;
  const adoption = createChatSendTurnAdoptionLifecycle({
    accountId: undefined,
    chatQueuedTurns,
    context,
    runId: "source-run",
    controller,
    sessionBinding,
    sessionKey,
    agentId: "work",
    originatingChannel: "webchat",
    session: {
      agentId: "work",
      backingSessionId: "terminal-outcome-session",
      cfg: {},
      clientRunId: "source-run",
      sessionKey,
      sessionLoadOptions: { agentId: "work" },
    },
    hasCronCreatorAuthority: false,
    // The mark runs before visible finalization; suppression keeps the fixture free of it.
    suppressReplies: true,
    releaseSourceWorkAdmission: () => {},
    retainWorkAdmission: () => () => {},
  });
  return { adoption, chatQueuedTurns, controller, sessionBinding };
}

const batch = (
  runId: string,
  kind: QueuedFollowupReplyBatch["completion"]["kind"] = "completed",
): QueuedFollowupReplyBatch => ({
  kind: "queued-followup",
  runId,
  originatingChannel: "webchat",
  payloads: [],
  completion: kind === "failed" ? { kind, error: "failed" } : { kind },
});

/** Whether a compact/fork/rewind mutation would join (true) or refuse (false) this source. */
async function mutationJoinsSourceAdmission(
  sessionBinding: Pick<ChatAbortControllerEntry, "terminalOutcomeObserved">,
) {
  const scope = `terminal-outcome-${Math.random()}`;
  const admission = await beginSessionWorkAdmission({
    scope,
    identities: [sessionKey],
    isSettling: () => sessionBinding.terminalOutcomeObserved === true,
    assertAllowed: () => {},
  });
  try {
    return getTerminalSessionWorkAdmissionRelease({ scope, identities: [sessionKey] }) !== false;
  } finally {
    admission.release();
  }
}

describe("queued chat.send terminal outcome ownership", () => {
  it("marks the source only for its own non-progress queued terminal", async () => {
    const { adoption, sessionBinding } = createSource();
    expect(adoption.lifecycle.onDeferred?.()).toBe(true);
    await adoption.onQueuedFollowupReplyBatch(batch("followup-run", "progress"));
    expect(sessionBinding.terminalOutcomeObserved).toBeUndefined();
    expect(await mutationJoinsSourceAdmission(sessionBinding)).toBe(false);
    await adoption.onQueuedFollowupReplyBatch(batch("followup-run"));
    expect(sessionBinding.terminalOutcomeObserved).toBe(true);
    expect(await mutationJoinsSourceAdmission(sessionBinding)).toBe(true);
  });

  it("does not mark the original source for a recovery run delivered through its retry", async () => {
    const { adoption, sessionBinding } = createSource();
    const retry = adoption.onQueuedFollowupReplyBatch.createSourceRetry?.();
    expect(retry).toBeDefined();
    await retry?.(batch("recovery-run"));
    expect(sessionBinding.terminalOutcomeObserved).toBeUndefined();
    // The original source admission stays non-settling, so a mutation refuses instead of waiting.
    expect(await mutationJoinsSourceAdmission(sessionBinding)).toBe(false);
  });

  it("does not mark the source once a same-ID queued turn owns the queue identity", async () => {
    const { adoption, chatQueuedTurns, sessionBinding } = createSource();
    expect(adoption.lifecycle.onDeferred?.()).toBe(true);
    const replacement = chatQueuedTurns.get("source-run");
    expect(replacement).toBeDefined();
    chatQueuedTurns.set("source-run", { ...replacement!, controller: new AbortController() });
    await adoption.onQueuedFollowupReplyBatch(batch("followup-run"));
    expect(sessionBinding.terminalOutcomeObserved).toBeUndefined();
    expect(await mutationJoinsSourceAdmission(sessionBinding)).toBe(false);
  });
});
