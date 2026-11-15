import { afterEach, describe, expect, it, vi } from "vitest";
import { clearFollowupQueue, getFollowupQueue } from "../auto-reply/reply/queue/state.js";
import { noteFollowupEnqueuedForWrapperGate } from "../auto-reply/reply/queue/wrapper-gate.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { resetDiagnosticEventsForTest } from "../infra/diagnostic-events.js";
import { resetDiagnosticRunActivityForTest } from "../logging/diagnostic-run-activity.js";
import { resetDiagnosticSessionStateForTest } from "../logging/diagnostic-session-state.js";
import {
  appendWrapperGateWarning,
  resolveWrapperGateCodeModeBoundary,
  resolveWrapperGateToolDecision,
  wrapperGatePostExecNote,
} from "./agent-tools.before-tool-call.wrapper-gate.js";
import { wrapToolWithBeforeToolCallHook } from "./agent-tools.before-tool-call.wrapper.js";
import type { AnyAgentTool } from "./tools/common.js";

const SESSION_KEY = "agent:test:wrapper-gate-tools";

function enabledConfig(): OpenClawConfig {
  return { messages: { queue: { wrapperGate: { enabled: true } } } } as OpenClawConfig;
}

function openWindow(params: { collectMs: number; warnMs: number }) {
  const queue = getFollowupQueue(SESSION_KEY, {
    mode: "collect",
    wrapperGate: { enabled: true, ...params },
  });
  noteFollowupEnqueuedForWrapperGate(
    queue,
    { run: { trustedInternalHandoff: { kind: "subagent-completion" } } as never },
    Date.now(),
  );
  return queue;
}

function makeTool(executed: () => void): AnyAgentTool {
  return {
    name: "noop",
    label: "Noop",
    description: "Synthetic no-op tool",
    execute: async () => {
      executed();
      return { content: [{ type: "text", text: "done" }], details: { status: "completed" } };
    },
  } as AnyAgentTool;
}

afterEach(() => {
  // Fake timers freeze setImmediate, and vi.useRealTimers() discards a drain that
  // was already scheduled under them: asyncDrainScheduled stays true with events
  // stuck in the async queue, so waitForDiagnosticEventsDrained() would spin
  // forever (the 180s hook timeouts in the integration tests). Resetting the
  // dispatcher first clears the async queue and the stuck drain flag.
  vi.useRealTimers();
  resetDiagnosticEventsForTest();
  resetDiagnosticRunActivityForTest();
  resetDiagnosticSessionStateForTest();
  clearFollowupQueue(SESSION_KEY);
});

describe("resolveWrapperGateToolDecision", () => {
  it("passes with the flag off, no session, or no window", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 60_000, warnMs: 120_000 });
    const window = queue.wrapperGateWindow!;
    const now = window.forceHandoffAt + 5_000;
    expect(
      resolveWrapperGateToolDecision({ config: undefined, sessionKey: SESSION_KEY, now }),
    ).toEqual({ action: "pass" });
    expect(
      resolveWrapperGateToolDecision({
        config: { messages: { queue: { wrapperGate: { enabled: false } } } } as OpenClawConfig,
        sessionKey: SESSION_KEY,
        now,
      }),
    ).toEqual({ action: "pass" });
    expect(
      resolveWrapperGateToolDecision({ config: enabledConfig(), sessionKey: undefined, now }),
    ).toEqual({ action: "pass" });
    expect(
      resolveWrapperGateToolDecision({
        config: enabledConfig(),
        sessionKey: "agent:test:no-such-queue",
        now,
      }),
    ).toEqual({ action: "pass" });
  });

  it("passes while collecting, warns once during the warning phase, refuses at the deadline", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 300_000, warnMs: 120_000 });
    const window = queue.wrapperGateWindow!;
    expect(
      resolveWrapperGateToolDecision({
        config: enabledConfig(),
        sessionKey: SESSION_KEY,
        now: window.firstQueuedAt + 1_000,
      }),
    ).toEqual({ action: "pass" });
    const warn = resolveWrapperGateToolDecision({
      config: enabledConfig(),
      sessionKey: SESSION_KEY,
      now: window.collectUntil + 1_000,
    });
    expect(warn.action).toBe("warn");
    // SF4: warned is committed only where the note is actually appended. An
    // un-committed window warns again rather than silently consuming the shot.
    expect(
      resolveWrapperGateToolDecision({
        config: enabledConfig(),
        sessionKey: SESSION_KEY,
        now: window.collectUntil + 2_000,
      }).action,
    ).toBe("warn");
    if (warn.action === "warn") {
      warn.commit(window.collectUntil + 1_000);
    }
    expect(
      resolveWrapperGateToolDecision({
        config: enabledConfig(),
        sessionKey: SESSION_KEY,
        now: window.collectUntil + 3_000,
      }),
    ).toEqual({
      action: "pass",
      note: expect.stringContaining("[tool_denial countdown"),
    });
    // #6278: the refusal clock runs from warning receipt, so at the original
    // deadline the dispatch still passes inside the notice grace...
    expect(
      resolveWrapperGateToolDecision({
        config: enabledConfig(),
        sessionKey: SESSION_KEY,
        now: window.forceHandoffAt,
      }),
    ).toEqual({
      action: "pass",
      note: expect.stringContaining("[tool_denial countdown"),
    });
    // ...and refuses only past warnedAt + warnMs (default 120s).
    const refused = resolveWrapperGateToolDecision({
      config: enabledConfig(),
      sessionKey: SESSION_KEY,
      now: window.collectUntil + 1_000 + 600_000,
    });
    expect(refused.action).toBe("refuse");
  });

  // REGRESSION (W2, Codex final review): two concurrent warning-phase calls
  // both resolve to warn before either completes. The claim is a compare-and-
  // set AT THE APPEND POINT: only the first completion claims the one-per-window
  // warning; the loser ships without the note and spends nothing.
  it("REGRESSION (W2): two concurrent warning-phase claims - only the first completion wins", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 300_000, warnMs: 120_000 });
    const window = queue.wrapperGateWindow!;
    const first = resolveWrapperGateToolDecision({
      config: enabledConfig(),
      sessionKey: SESSION_KEY,
      now: window.collectUntil + 1_000,
    });
    const second = resolveWrapperGateToolDecision({
      config: enabledConfig(),
      sessionKey: SESSION_KEY,
      now: window.collectUntil + 2_000,
    });
    expect(first.action).toBe("warn");
    expect(second.action).toBe("warn");
    expect(first.action === "warn" && first.commit(window.collectUntil + 1_000)).toBe(true);
    expect(second.action === "warn" && second.commit(window.collectUntil + 2_000)).toBe(false);
    // The winner claimed: a later call passes without a second warning.
    expect(
      resolveWrapperGateToolDecision({
        config: enabledConfig(),
        sessionKey: SESSION_KEY,
        now: window.collectUntil + 3_000,
      }),
    ).toEqual({
      action: "pass",
      note: expect.stringContaining("[tool_denial countdown"),
    });
  });

  it("sees no window after the queue clears, so dispatch passes again", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1, warnMs: 1 });
    const window = queue.wrapperGateWindow!;
    // #6278: first contact past the deadline delivers the warning, not a
    // cold refusal.
    const warned = resolveWrapperGateToolDecision({
      config: enabledConfig(),
      sessionKey: SESSION_KEY,
      now: window.forceHandoffAt + 1,
    });
    expect(warned.action).toBe("warn");
    if (warned.action === "warn") {
      warned.commit(window.forceHandoffAt + 1);
    }
    // Past the notice grace (default warnMs from warning receipt) it refuses.
    expect(
      resolveWrapperGateToolDecision({
        config: enabledConfig(),
        sessionKey: SESSION_KEY,
        now: window.forceHandoffAt + 1 + 600_000,
      }).action,
    ).toBe("refuse");
    clearFollowupQueue(SESSION_KEY);
    expect(
      resolveWrapperGateToolDecision({
        config: enabledConfig(),
        sessionKey: SESSION_KEY,
        now: window.forceHandoffAt + 2 + 600_000,
      }),
    ).toEqual({ action: "pass" });
  });
});

describe("appendWrapperGateWarning", () => {
  it("appends the note without touching existing content or details", () => {
    const result = appendWrapperGateWarning(
      { content: [{ type: "text", text: "done" }], details: { status: "completed" } },
      "wrap up",
    );
    expect(result.content).toHaveLength(2);
    expect(result.content?.[1]).toEqual({ type: "text", text: "wrap up" });
    // MF2: the append is content-only via copyInternalToolResultState; existing
    // details are preserved untouched (no wrapperGateWarning marker is added).
    expect(result.details).toEqual({ status: "completed" });
  });
});

describe("D2: visible-reply tool passes the wrapper gate at the deadline", () => {
  const GROUP_KEY = "agent:main:telegram:group:-1004327615866:topic:2549";

  function messageToolOnlyConfig(): OpenClawConfig {
    return {
      messages: {
        queue: { wrapperGate: { enabled: true } },
        groupChat: { visibleReplies: "message_tool" },
      },
    } as OpenClawConfig;
  }

  function expiredWindow() {
    const queue = getFollowupQueue(GROUP_KEY, {
      mode: "collect",
      wrapperGate: { enabled: true, collectMs: 1, warnMs: 1 },
    });
    noteFollowupEnqueuedForWrapperGate(
      queue,
      { run: { trustedInternalHandoff: { kind: "subagent-completion" } } } as never,
      Date.now() - 10_000,
    );
    // The warning was delivered long ago and its notice grace has passed, so
    // decisions reach the refusal boundary directly (#6278 semantics).
    const window = queue.wrapperGateWindow!;
    window.warned = true;
    window.warnedAt = Date.now() - 600_000;
    return queue;
  }

  afterEach(() => {
    clearFollowupQueue(GROUP_KEY);
  });

  it("passes the message tool for a message-tool-only group session", () => {
    expiredWindow();
    const decision = resolveWrapperGateToolDecision({
      config: messageToolOnlyConfig(),
      sessionKey: GROUP_KEY,
      now: Date.now(),
      toolName: "message",
    });
    expect(decision.action).toBe("pass");
  });

  it("still refuses other tools for the same session", () => {
    expiredWindow();
    const decision = resolveWrapperGateToolDecision({
      config: messageToolOnlyConfig(),
      sessionKey: GROUP_KEY,
      now: Date.now(),
      toolName: "exec",
    });
    expect(decision.action).toBe("refuse");
  });

  it("refuses the message tool when replies are not message-tool-only", () => {
    expiredWindow();
    const decision = resolveWrapperGateToolDecision({
      config: enabledConfig(),
      sessionKey: GROUP_KEY,
      now: Date.now(),
      toolName: "message",
    });
    expect(decision.action).toBe("refuse");
  });
});

describe("cold-refusal grace (#6278)", () => {
  it("delivers the warning at the deadline instead of refusing cold, then refuses only after the notice grace", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 5_000 });
    vi.setSystemTime(queue.wrapperGateWindow!.forceHandoffAt + 10);
    const cfg = {
      messages: { queue: { wrapperGate: { enabled: true, warnMs: 5_000 } } },
    } as OpenClawConfig;
    const at = () =>
      resolveWrapperGateToolDecision({
        config: cfg,
        sessionKey: SESSION_KEY,
        now: Date.now(),
        toolName: "exec",
      });
    const first = at();
    // Past the deadline with the warning never delivered: warn, never refuse.
    expect(first.action).toBe("warn");
    if (first.action === "warn") {
      expect(first.commit(Date.now())).toBe(true);
    }
    // Within the notice grace the dispatch passes.
    expect(at().action).toBe("pass");
    // Past the grace the refusal finally fires — never before the warning.
    vi.setSystemTime(Date.now() + 5_001);
    expect(at().action).toBe("refuse");
    vi.useRealTimers();
  });
});

describe("countdown and turn-end exemption (#6294, #6295)", () => {
  const countdownConfig = (): OpenClawConfig =>
    ({ messages: { queue: { wrapperGate: { enabled: true, warnMs: 5_000 } } } }) as OpenClawConfig;

  it("names what is waiting in the warning (#6294)", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 5_000 });
    // Upstream mailbox items are derived from controller entries; stub the view the gate reads.
    vi.spyOn(queue, "items", "get").mockReturnValue([
      { prompt: "Please summarize the deploy ticket" },
      { prompt: "Second message" },
    ] as never);
    vi.setSystemTime(queue.wrapperGateWindow!.collectUntil + 10);
    const warn = resolveWrapperGateToolDecision({
      config: countdownConfig(),
      sessionKey: SESSION_KEY,
      now: Date.now(),
      toolName: "exec",
    });
    expect(warn.action).toBe("warn");
    if (warn.action === "warn") {
      expect(warn.note).toContain("Please summarize the deploy ticket");
      expect(warn.note).toContain("2 messages waiting");
    }
  });

  it("carries a live countdown on every passing dispatch after the warning (#6294)", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 5_000 });
    vi.setSystemTime(queue.wrapperGateWindow!.collectUntil + 10);
    const at = (toolName: string) =>
      resolveWrapperGateToolDecision({
        config: countdownConfig(),
        sessionKey: SESSION_KEY,
        now: Date.now(),
        toolName,
      });
    const warn = at("exec");
    expect(warn.action).toBe("warn");
    if (warn.action === "warn") {
      warn.commit(Date.now());
    }
    // Deadline = warnedAt + warnMs = collectUntil + 5_010; every passing
    // dispatch carries the live countdown in Vern's exact format.
    const first = at("exec");
    expect(first.action).toBe("pass");
    if (first.action === "pass") {
      expect(first.note).toContain(
        "[tool_denial countdown 5s - end turn and ingest queued traffic]",
      );
    }
    vi.setSystemTime(Date.now() + 3_000);
    const later = at("exec");
    expect(later.action).toBe("pass");
    if (later.action === "pass") {
      expect(later.note).toContain("countdown 2s");
    }
    vi.setSystemTime(Date.now() + 3_000);
    expect(at("exec").action).toBe("refuse");
  });

  it("never blocks the host turn-ending tool at the deadline (#6295)", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 5_000 });
    const window = queue.wrapperGateWindow!;
    window.warned = true;
    window.warnedAt = Date.now() - 600_000;
    vi.setSystemTime(window.forceHandoffAt + 1);
    const at = (toolName: string) =>
      resolveWrapperGateToolDecision({
        config: countdownConfig(),
        sessionKey: SESSION_KEY,
        now: Date.now(),
        toolName,
      });
    expect(at("exec").action).toBe("refuse");
    const yielded = at("sessions_yield");
    expect(yielded.action).toBe("pass");
    if (yielded.action === "pass") {
      // The exemption is a clean pass - no countdown noise on the way out.
      expect(yielded.note).toBeUndefined();
    }
  });
});

describe("code-mode boundary (D4)", () => {
  const boundaryConfig = (): OpenClawConfig =>
    ({ messages: { queue: { wrapperGate: { enabled: true, warnMs: 5_000 } } } }) as OpenClawConfig;

  it("refuses the outer code-mode exec past the deadline", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 5_000 });
    const window = queue.wrapperGateWindow!;
    window.warned = true;
    window.warnedAt = Date.now() - 600_000;
    vi.setSystemTime(window.forceHandoffAt + 1);
    const decision = resolveWrapperGateCodeModeBoundary({
      config: boundaryConfig(),
      sessionKey: SESSION_KEY,
      now: Date.now(),
    });
    // A script may swallow nested refusals; the outer exec itself must be
    // refused so the gate reaches the model.
    expect(decision.action).toBe("refuse");
    if (decision.action === "refuse") {
      expect(decision.reason).toContain("Wrapper gate");
    }
  });

  it("passes the outer code-mode exec during the notice grace", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 5_000 });
    vi.setSystemTime(queue.wrapperGateWindow!.collectUntil + 10);
    // Warning not yet delivered: the outer exec runs; the nested calls
    // inside it deliver the warning themselves.
    expect(
      resolveWrapperGateCodeModeBoundary({
        config: boundaryConfig(),
        sessionKey: SESSION_KEY,
        now: Date.now(),
      }).action,
    ).toBe("pass");
  });

  it("surfaces a warning delivered inside this exec, once", () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 5_000 });
    const window = queue.wrapperGateWindow!;
    const execStartedAt = window.collectUntil + 10;
    vi.setSystemTime(execStartedAt);
    window.warned = true;
    window.warnedAt = execStartedAt + 5;
    const note = wrapperGatePostExecNote({
      config: boundaryConfig(),
      sessionKey: SESSION_KEY,
      execStartedAt,
      now: Date.now(),
    });
    // The script may have swallowed the nested result; the note still
    // reaches the model on the outer result, with the live countdown.
    expect(note).toBeDefined();
    expect(note).toContain("[tool_denial countdown");
    // A later exec (started after the delivery) gets no repeat note.
    vi.setSystemTime(Date.now() + 1_000);
    expect(
      wrapperGatePostExecNote({
        config: boundaryConfig(),
        sessionKey: SESSION_KEY,
        execStartedAt: window.warnedAt + 1,
        now: Date.now(),
      }),
    ).toBeUndefined();
  });
});

describe("wrapToolWithBeforeToolCallHook integration", () => {
  it("warns on first deadline contact, executes within the grace, refuses only after it (#6278)", async () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 1_000 });
    vi.setSystemTime(queue.wrapperGateWindow!.forceHandoffAt + 10);
    let executed = 0;
    const tool = wrapToolWithBeforeToolCallHook(
      makeTool(() => {
        executed += 1;
      }),
      {
        sessionKey: SESSION_KEY,
        config: enabledConfig(),
      },
    );
    // Never a cold refusal: the first dispatch past the deadline carries the
    // warning and executes.
    const first = await tool.execute("gate-warn-call", {});
    expect(executed).toBe(1);
    expect(JSON.stringify(first.content)).toContain("Wrapper gate");
    expect(first).toMatchObject({ details: { status: "completed" } });
    // Within the notice grace the tool still executes (no second warning).
    await tool.execute("gate-grace-call", {});
    expect(executed).toBe(2);
    // Past the grace (default warnMs from warning receipt) it refuses.
    vi.setSystemTime(Date.now() + 600_000);
    const third = await tool.execute("gate-refuse-call", {});
    expect(executed).toBe(2);
    expect(third).toMatchObject({
      details: { status: "blocked", deniedReason: "wrapper-gate-deadline" },
    });
  });

  it("executes during the warning phase and appends the one-time warning to the result", async () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 1_000, warnMs: 5_000 });
    vi.setSystemTime(queue.wrapperGateWindow!.collectUntil + 10);
    let executed = 0;
    const tool = wrapToolWithBeforeToolCallHook(
      makeTool(() => {
        executed += 1;
      }),
      {
        sessionKey: SESSION_KEY,
        config: enabledConfig(),
      },
    );
    const result = await tool.execute("warn-call", {});
    expect(executed).toBe(1);
    const text = JSON.stringify(result.content);
    expect(text).toContain("Wrapper gate");
    // MF2: content-only append; details stay the tool's own.
    expect(result).toMatchObject({ details: { status: "completed" } });
    // The window has spent its one warning (committed at the append); later calls carry no second note.
    const second = await tool.execute("warn-call-2", {});
    expect(second).toMatchObject({ details: { status: "completed" } });
    expect(JSON.stringify(second.content)).not.toContain("Wrapper gate");
  });

  it("leaves dispatch untouched while the window is still collecting", async () => {
    vi.useFakeTimers();
    const queue = openWindow({ collectMs: 300_000, warnMs: 120_000 });
    vi.setSystemTime(queue.wrapperGateWindow!.firstQueuedAt + 1_000);
    let executed = 0;
    const tool = wrapToolWithBeforeToolCallHook(
      makeTool(() => {
        executed += 1;
      }),
      {
        sessionKey: SESSION_KEY,
        config: enabledConfig(),
      },
    );
    const result = await tool.execute("collect-call", {});
    expect(executed).toBe(1);
    expect(result).toMatchObject({ details: { status: "completed" } });
  });
});
