import { afterEach, describe, expect, it } from "vitest";
import { clearFollowupQueue, getFollowupQueue } from "./state.js";
import type { FollowupRun } from "./types.js";
import {
  classifyWrapperGateFollowupSource,
  noteFollowupEnqueuedForWrapperGate,
  resolveWrapperGateDrainBoundary,
  waitForWrapperGateDrainBoundary,
} from "./wrapper-gate.js";

const KEY = "agent:test:wrapper-gate-queue";

function makeQueue(params?: {
  mode?: string;
  enabled?: boolean;
  collectMs?: number;
  warnMs?: number;
}) {
  return getFollowupQueue(KEY, {
    mode: (params?.mode ?? "collect") as "collect",
    wrapperGate: {
      enabled: params?.enabled ?? true,
      ...(params?.collectMs === undefined ? {} : { collectMs: params.collectMs }),
      ...(params?.warnMs === undefined ? {} : { warnMs: params.warnMs }),
    },
  });
}

function subagentRun(marker?: "handoff" | "provenance"): Pick<FollowupRun, "run"> {
  if (marker === "provenance") {
    return {
      run: {
        inputProvenance: { kind: "inter_session", sourceTool: "subagent_announce" },
      } as FollowupRun["run"],
    };
  }
  return {
    run: {
      trustedInternalHandoff: { kind: "subagent-completion" },
    } as FollowupRun["run"],
  };
}

function userRun(): Pick<FollowupRun, "run"> {
  return { run: { inputProvenance: { kind: "external_user" } } as FollowupRun["run"] };
}

afterEach(() => {
  clearFollowupQueue(KEY);
});

describe("classifyWrapperGateFollowupSource", () => {
  it("classifies trusted handoff and provenance markers as subagent sources", () => {
    expect(classifyWrapperGateFollowupSource(subagentRun("handoff"))).toBe("subagent");
    expect(classifyWrapperGateFollowupSource(subagentRun("provenance"))).toBe("subagent");
  });

  // SETTLED (s1c4, Vern's qualifier ruling in the amended Codex s1c2 review):
  // classify by a WHITELIST OF QUALIFIERS - never channel names or exclusion.
  // S-qualifiers (delivered work) open the collection window; U-qualifiers (a
  // waiting party) open the two-minute backstop; no qualifier -> no window.
  // Run shapes below use the host's own real fields: currentInboundEventKind
  // (set on the followup run at get-reply-run-execute:397) and
  // gatewayLocalUserIngress (:399); the S2 shapes are the host's
  // AGENT_MEDIATED_COMPLETION_SOURCE_TOOLS names via its own predicate.
  it("SETTLED (s1c4): qualifier whitelist - U-qualifiers open the backstop, S-qualifiers the collection window, no qualifier no window", () => {
    // U2: Telegram-shaped run (a real user request from a channel).
    const telegramRun = {
      run: {},
      currentInboundEventKind: "user_request",
    } as Pick<FollowupRun, "run" | "currentInboundEventKind" | "gatewayLocalUserIngress">;
    expect(classifyWrapperGateFollowupSource(telegramRun)).toBe("user");
    // U3: webchat / Control UI / user chat.send run (local-user ingress
    // evidence on a USER turn - no provenance).
    const webchatRun = {
      run: {},
      gatewayLocalUserIngress: { kind: "gateway-local-user-ingress" },
    } as Pick<FollowupRun, "run" | "currentInboundEventKind" | "gatewayLocalUserIngress">;
    expect(classifyWrapperGateFollowupSource(webchatRun)).toBe("user");
    // W5 (Codex s1c5 review): the ingress belongs to the CONNECTION, not the
    // input - admin/system chat.send turns (chat-send-request.ts:148 lets
    // admin callers send systemInputProvenance) carry BOTH real ingress and
    // system provenance. U3 requires a user turn (no provenance), so an
    // admin/system send on an ingressed connection opens nothing.
    const adminSystemSendOnIngress = {
      run: { inputProvenance: { kind: "internal_system", sourceTool: "exec" } },
      gatewayLocalUserIngress: { kind: "gateway-local-user-ingress" },
    } as Pick<FollowupRun, "run" | "currentInboundEventKind" | "gatewayLocalUserIngress">;
    expect(classifyWrapperGateFollowupSource(adminSystemSendOnIngress)).toBe("internal");
    // U4: inter-session traffic that is NOT a completion report (sessions_send).
    const sessionsSendRun = {
      run: { inputProvenance: { kind: "inter_session", sourceTool: "sessions_send" } },
    } as Pick<FollowupRun, "run">;
    expect(classifyWrapperGateFollowupSource(sessionsSendRun)).toBe("user");
    // S2 (via the host predicate): an agent-mediated completion (image_generate)
    // opens the COLLECTION window, not the backstop - and S evaluates before U4.
    const mediatedCompletionRun = {
      run: { inputProvenance: { kind: "inter_session", sourceTool: "image_generate" } },
    } as Pick<FollowupRun, "run">;
    expect(classifyWrapperGateFollowupSource(mediatedCompletionRun)).toBe("subagent");
    // No qualifier: internal_system (heartbeat / cron-event / exec-completion)
    // opens NO window - W4 cannot arise in this design.
    const heartbeatRun = {
      run: { inputProvenance: { kind: "internal_system", sourceTool: "heartbeat" } },
    } as Pick<FollowupRun, "run">;
    expect(classifyWrapperGateFollowupSource(heartbeatRun)).toBe("internal");
    const cronRun = {
      run: { inputProvenance: { kind: "internal_system", sourceTool: "cron-event" } },
    } as Pick<FollowupRun, "run">;
    expect(classifyWrapperGateFollowupSource(cronRun)).toBe("internal");
    // No qualifier at all: an unmarked run (NO inbound kind) opens no window.
    const unmarkedRun = { run: {} } as Pick<FollowupRun, "run">;
    expect(classifyWrapperGateFollowupSource(unmarkedRun)).toBe("internal");
    // s1c5 (Claude's catch): production runs groupChat.unmentionedInbound
    // 'room_event', so EVERY unmentioned group-topic message - the owner's
    // main traffic - classifies room_event. U2 fires on the inbound kind SET
    // AT ALL: the group-topic room_event Telegram shape opens the backstop.
    const groupTopicRoomEventRun = {
      run: {},
      currentInboundEventKind: "room_event",
    } as Pick<FollowupRun, "run" | "currentInboundEventKind" | "gatewayLocalUserIngress">;
    expect(classifyWrapperGateFollowupSource(groupTopicRoomEventRun)).toBe("user");
  });

  // s1c4 U4: inter-session traffic that is NOT a completion report is a
  // WAITING PARTY (another agent awaits the reply) - it opens the backstop
  // where the pre-s1c2 design sent it internal. Unmarked runs stay internal.
  it("classifies external users, waiting inter-session parties, and unmarked runs", () => {
    expect(classifyWrapperGateFollowupSource(userRun())).toBe("user");
    expect(
      classifyWrapperGateFollowupSource({
        run: {
          inputProvenance: { kind: "inter_session", sourceTool: "other" },
        } as FollowupRun["run"],
      }),
    ).toBe("user");
    expect(classifyWrapperGateFollowupSource({ run: {} as FollowupRun["run"] })).toBe("internal");
  });
});

describe("noteFollowupEnqueuedForWrapperGate", () => {
  it("opens a fixed window at the first subagent delivery and never extends it", () => {
    const queue = makeQueue({ collectMs: 1000, warnMs: 2000 });
    noteFollowupEnqueuedForWrapperGate(queue, subagentRun(), 10_000);
    expect(queue.wrapperGateWindow).toEqual({
      firstQueuedAt: 10_000,
      collectUntil: 11_000,
      forceHandoffAt: 13_000,
      // MF3: the drain boundary is the collection close, not the handoff
      // deadline - an idle caller drains at five minutes, not seven.
      drainAt: 11_000,
    });
    noteFollowupEnqueuedForWrapperGate(queue, subagentRun("provenance"), 12_500);
    expect(queue.wrapperGateWindow?.forceHandoffAt).toBe(13_000);
    expect(queue.wrapperGateWindow?.firstQueuedAt).toBe(10_000);
  });

  it("user input bypasses remaining collection and clamps the deadline to its own backstop", () => {
    const queue = makeQueue({ collectMs: 1000, warnMs: 2000 });
    noteFollowupEnqueuedForWrapperGate(queue, subagentRun(), 10_000);
    noteFollowupEnqueuedForWrapperGate(queue, userRun(), 10_400);
    expect(queue.wrapperGateWindow?.collectUntil).toBe(10_400);
    expect(queue.wrapperGateWindow?.drainAt).toBe(10_400);
    expect(queue.wrapperGateWindow?.forceHandoffAt).toBe(12_400);
  });

  it("a user message without an open window opens only the two-minute backstop", () => {
    const queue = makeQueue({ warnMs: 3000 });
    noteFollowupEnqueuedForWrapperGate(queue, userRun(), 50_000);
    expect(queue.wrapperGateWindow).toEqual({
      firstQueuedAt: 50_000,
      collectUntil: 50_000,
      forceHandoffAt: 53_000,
      drainAt: 50_000,
    });
  });

  it("stays idle when disabled, in non-collect modes, or for internal sources", () => {
    const disabled = makeQueue({ enabled: false, collectMs: 1000 });
    noteFollowupEnqueuedForWrapperGate(disabled, subagentRun(), 1);
    expect(disabled.wrapperGateWindow).toBeUndefined();

    const followupMode = getFollowupQueue(KEY, {
      mode: "followup",
      wrapperGate: { enabled: true },
    });
    noteFollowupEnqueuedForWrapperGate(followupMode, subagentRun(), 1);
    expect(followupMode.wrapperGateWindow).toBeUndefined();

    const collect = makeQueue({ enabled: true });
    noteFollowupEnqueuedForWrapperGate(collect, { run: {} as FollowupRun["run"] }, 1);
    expect(collect.wrapperGateWindow).toBeUndefined();
  });
});

describe("drain boundary wait", () => {
  it("resolves undefined boundary without a window and reports the fixed drainAt with one", () => {
    const queue = makeQueue({ enabled: true, collectMs: 1000, warnMs: 500 });
    expect(resolveWrapperGateDrainBoundary(queue, 5)).toBeUndefined();
    noteFollowupEnqueuedForWrapperGate(queue, subagentRun(), 10_000);
    // MF3: the boundary reported is the collection close (10_000 + collectMs
    // 1000), not the handoff deadline.
    expect(resolveWrapperGateDrainBoundary(queue, 10_500)).toEqual({ drainAt: 11_000 });
  });

  it("holds until the fixed boundary and reports the hold", async () => {
    const queue = makeQueue({ enabled: true, collectMs: 60, warnMs: 30 });
    noteFollowupEnqueuedForWrapperGate(queue, subagentRun(), Date.now());
    const startedAt = Date.now();
    const held = await waitForWrapperGateDrainBoundary(queue);
    expect(held).toBe(true);
    // MF3: the hold ends at the collection close (collectMs), not the handoff
    // deadline (collectMs + warnMs) - ~60ms here, bounded loosely for the
    // <=250ms poll cadence.
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(50);
  });

  it("wakes early when a user bypass pulls the boundary to now", async () => {
    const queue = makeQueue({ enabled: true, collectMs: 1_000, warnMs: 30 });
    noteFollowupEnqueuedForWrapperGate(queue, subagentRun(), Date.now());
    const wait = waitForWrapperGateDrainBoundary(queue);
    await new Promise((resolve) => setTimeout(resolve, 40));
    noteFollowupEnqueuedForWrapperGate(queue, userRun(), Date.now());
    const startedAt = Date.now();
    await wait;
    expect(Date.now() - startedAt).toBeLessThan(900);
  });
});
