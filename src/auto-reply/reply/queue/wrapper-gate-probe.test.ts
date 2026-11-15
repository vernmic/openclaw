import { afterEach, describe, expect, it, vi } from "vitest";
import { wrapToolWithBeforeToolCallHook } from "../../../agents/agent-tools.before-tool-call.wrapper.js";
import type { AnyAgentTool } from "../../../agents/agent-tools/tools/common.js";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import { scheduleFollowupDrain } from "./drain.js";
import { enqueueFollowupRun } from "./enqueue.js";
import type { QueueSettings } from "./settings.js";
import { clearFollowupQueue, getFollowupQueue } from "./state.js";
import type { FollowupRun } from "./types.js";

/**
 * Unit-level port of Codex's built-wrapper probe (research/
 * wrapper-slice1-codex-review-2026-11-03/built-wrapper-probe.test.mjs), which
 * reproduced W1 and W2 against the completed build. The probe imported the
 * dist bundles; this port imports the same real code from source so the
 * regressions run in the slice suites without a build.
 */

const ROOT = "unused-probe-state-root";
const keys: string[] = [];

function key(label: string): string {
  const k = "agent:review:wrapper-" + label;
  keys.push(k);
  return k;
}

function config(enabled: boolean): OpenClawConfig {
  return { messages: { queue: { mode: "collect", wrapperGate: { enabled } } } } as OpenClawConfig;
}

function settings(enabled = true, collectMs = 0, warnMs = 0): QueueSettings {
  return {
    mode: "collect",
    debounceMs: 0,
    cap: 50,
    dropPolicy: "summarize",
    wrapperGate: { enabled, collectMs, warnMs },
  } as QueueSettings;
}

function run(label: string): FollowupRun {
  return {
    prompt: "Synthetic child completion " + label,
    messageId: "m-" + label,
    enqueuedAt: Date.now(),
    run: {
      agentId: "review",
      agentDir: ROOT,
      sessionId: "synthetic-session",
      sessionFile: ROOT + "/unused-session.json",
      workspaceDir: ROOT,
      config: config(true),
      provider: "openai",
      model: "synthetic",
      timeoutMs: 10_000,
      blockReplyBreak: "text_end",
      inputProvenance: { kind: "inter_session", sourceTool: "subagent_announce" },
    },
  } as unknown as FollowupRun;
}

function tool(execute: () => Promise<unknown>): AnyAgentTool {
  return {
    name: "noop",
    label: "Noop",
    description: "Synthetic no-op tool",
    execute,
  } as AnyAgentTool;
}

const result = () => ({
  content: [{ type: "text" as const, text: "done" }],
  details: { status: "completed" },
});

afterEach(() => {
  vi.restoreAllMocks();
  for (const k of keys.splice(0)) {
    // The mailbox owns its drain callback upstream; clearing the queue clears both.
    clearFollowupQueue(k);
  }
});

describe("wrapper gate (Codex probe port)", () => {
  it("OFF control: queue opens no gate window and dispatch returns unchanged content", async () => {
    const k = key("off");
    enqueueFollowupRun(k, run("off1"), settings(false), "none", undefined, false);
    expect(getFollowupQueue(k, settings(false)).wrapperGateWindow).toBeUndefined();
    let count = 0;
    const wrapped = wrapToolWithBeforeToolCallHook(
      tool(async () => {
        count++;
        return result();
      }),
      { sessionKey: k, config: config(false) },
    );
    const out = await wrapped.execute("off-call", {});
    expect(count).toBe(1);
    expect(out).toEqual(result());
  });

  it("ON control: an overdue busy caller gets the warning first, then is refused after the notice grace (#6278)", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const k = key("refuse");
    const q = getFollowupQueue(k, settings());
    const { noteFollowupEnqueuedForWrapperGate } = await import("./wrapper-gate.js");
    noteFollowupEnqueuedForWrapperGate(q, run("refuse"), now - 1000);
    let count = 0;
    const gateConfig = {
      messages: { queue: { mode: "collect", wrapperGate: { enabled: true, warnMs: 1_000 } } },
    } as OpenClawConfig;
    const wrapped = wrapToolWithBeforeToolCallHook(
      tool(async () => {
        count++;
        return result();
      }),
      { sessionKey: k, config: gateConfig },
    );
    // Never a cold refusal (#6278): the first overdue dispatch carries the
    // one-time warning and still executes.
    const first = await wrapped.execute("refuse-call", {});
    expect(count).toBe(1);
    expect(
      (first as { details?: { deniedReason?: string } }).details?.deniedReason,
    ).toBeUndefined();
    // Only a dispatch past warnedAt + warnMs (the notice grace) is refused.
    now += 1_001;
    const second = await wrapped.execute("refuse-call", {});
    expect(count).toBe(1);
    expect((second as { details?: { deniedReason?: string } }).details?.deniedReason).toBe(
      "wrapper-gate-deadline",
    );
  });

  // W1 (Codex final review): the ordinary collected-bundle admission consumes
  // its window, so the drained bundle's own tool dispatch passes the gate.
  it("REGRESSION (W1): ordinary collected bundle consumes the expired window before its own tool dispatch", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const k = key("bundle");
    const s = settings(true, 300_000, 120_000);
    enqueueFollowupRun(k, run("bundle1"), s, "none", undefined, false);
    enqueueFollowupRun(k, run("bundle2"), s, "none", undefined, false);
    // The busy caller finished after the real 5+2-minute deadline: advance only
    // the clock; no seven-minute real wait.
    now += 420_001;
    let executed = 0;
    let windowAfterAdmission: unknown = "not-observed";
    const done = Promise.withResolvers<void>();
    const wrapped = wrapToolWithBeforeToolCallHook(
      tool(async () => {
        executed++;
        return result();
      }),
      { sessionKey: k, config: config(true) },
    );
    scheduleFollowupDrain(k, async (collected) => {
      try {
        expect(
          (collected as { turnAdoptionLifecycle?: unknown }).turnAdoptionLifecycle,
          "fixture must use real grouped admission",
        ).toBeTruthy();
        await (
          collected as {
            turnAdoptionLifecycle: { onAdopted: () => Promise<void> };
          }
        ).turnAdoptionLifecycle.onAdopted();
        windowAfterAdmission = getFollowupQueue(k, s)?.wrapperGateWindow;
        await wrapped.execute("bundle-call", {});
        done.resolve();
      } catch (error) {
        done.reject(error as Error);
      }
    });
    await done.promise;
    await new Promise((resolve) => setImmediate(resolve));
    expect(windowAfterAdmission, "SEEN admission must consume the prior window").toBeUndefined();
    expect(executed, "the newly admitted bundle must be allowed to run its own tool").toBe(1);
  });

  // W2 (Codex final review): concurrent warning-phase calls append only one
  // warning across the whole window.
  it("REGRESSION (W2): two concurrent warning-phase tool calls append only one warning", async () => {
    const k = key("parallel");
    const q = getFollowupQueue(k, settings(true, 0, 60_000));
    const { noteFollowupEnqueuedForWrapperGate } = await import("./wrapper-gate.js");
    noteFollowupEnqueuedForWrapperGate(q, run("parallel"), Date.now());
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let executed = 0;
    const wrapped = wrapToolWithBeforeToolCallHook(
      tool(async () => {
        if (++executed === 2) {
          entered.resolve();
        }
        await release.promise;
        return result();
      }),
      { sessionKey: k, config: config(true) },
    );
    const a = wrapped.execute("parallel-a", {});
    const b = wrapped.execute("parallel-b", {});
    const completed = Promise.all([a, b]);
    await Promise.race([
      entered.promise,
      completed.then(() => {
        throw new Error("calls completed before both entered");
      }),
    ]);
    release.resolve();
    const outs = await completed;
    const warnings = outs
      .flatMap((o) => (o as { content?: Array<{ type: string; text: string }> }).content ?? [])
      .filter((c) => c.type === "text" && c.text.includes("Wrapper gate:"));
    expect(executed).toBe(2);
    expect(
      warnings.length,
      "one warning is committed across the whole window, including parallel calls",
    ).toBe(1);
  });
});

// Codex final review 2026-11-04 (final-followup-codex-evidence-20261103/host-probe.mjs), ported
// from the built-dist probe to the source modules.
describe("wrapper gate (Codex final review N1/N2)", () => {
  const gateConfig = (warnMs: number) =>
    ({
      messages: { queue: { mode: "collect", wrapperGate: { enabled: true, warnMs } } },
    }) as OpenClawConfig;

  async function drainOnce(k: string, onRun: (collected: FollowupRun) => Promise<void>) {
    const done = Promise.withResolvers<void>();
    scheduleFollowupDrain(k, async (collected) => {
      try {
        await onRun(collected);
        done.resolve();
      } catch (error) {
        done.reject(error as Error);
      }
    });
    await done.promise;
    await new Promise((resolve) => setImmediate(resolve));
  }

  it("REGRESSION (N1): a single ordinary collected item consumes its inherited window before its own tool dispatch", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const k = key("single");
    const s = settings();
    enqueueFollowupRun(k, run("one"), s, "none", undefined, false);
    const q = getFollowupQueue(k, s);
    q.wrapperGateWindow!.warned = true;
    q.wrapperGateWindow!.warnedAt = now - 10_000;
    now += 1;
    let executed = 0;
    let denied: unknown;
    let hadLifecycle: unknown;
    const wrapped = wrapToolWithBeforeToolCallHook(
      tool(async () => {
        executed++;
        return result();
      }),
      { sessionKey: k, config: gateConfig(1_000) },
    );
    await drainOnce(k, async (collected) => {
      hadLifecycle = (collected as { turnAdoptionLifecycle?: unknown }).turnAdoptionLifecycle;
      const out = await wrapped.execute("drained", {});
      denied = (out as { details?: { deniedReason?: string } }).details?.deniedReason;
    });
    expect(
      hadLifecycle,
      "the fixture is the one-item path with no adoption lifecycle",
    ).toBeUndefined();
    expect(denied).toBeUndefined();
    expect(executed, "the admitted single collect turn must run its own tool").toBe(1);
  });

  it("REGRESSION (N1): a window opened by a later arrival during the admitted run is kept", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const k = key("later");
    const s = settings(true, 300_000, 120_000);
    enqueueFollowupRun(k, run("first"), s, "none", undefined, false);
    now += 420_001;
    let laterWindow: unknown;
    await drainOnce(k, async () => {
      // the admitted window is gone at run start; a new completion arrives mid-run and opens
      // the next bundle's window, which then holds that item for its own collection
      expect(getFollowupQueue(k, s).wrapperGateWindow).toBeUndefined();
      enqueueFollowupRun(k, run("later"), s, "none", undefined, false);
      laterWindow = getFollowupQueue(k, s).wrapperGateWindow;
    });
    expect(laterWindow).toBeDefined();
    expect(
      getFollowupQueue(k, s).items.length,
      "the later item is waiting for its own window",
    ).toBe(1);
    expect(
      getFollowupQueue(k, s).wrapperGateWindow,
      "the next bundle's window survives the admission",
    ).toBe(laterWindow);
  });

  it("REGRESSION (N1): a protected-priority item consumes the window it bypasses the helpers with", async () => {
    let now = 1_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const k = key("priority");
    const s = settings();
    // position "front" is how the host marks an item protected (enqueue.ts)
    const priority = run("priority");
    enqueueFollowupRun(k, priority, s, "none", undefined, false, { position: "front" });
    expect(priority.protectFromQueueOverflow).toBe(true);
    const q = getFollowupQueue(k, s);
    expect(q.wrapperGateWindow).toBeDefined();
    let windowAtRun: unknown = "not-observed";
    await drainOnce(k, async () => {
      windowAtRun = getFollowupQueue(k, s).wrapperGateWindow;
    });
    expect(windowAtRun).toBeUndefined();
  });

  it("REGRESSION (N2): a slow warning tool starts the notice grace when its result returns", async () => {
    let now = 10_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const k = key("slow");
    const q = getFollowupQueue(k, settings());
    const { noteFollowupEnqueuedForWrapperGate } = await import("./wrapper-gate.js");
    noteFollowupEnqueuedForWrapperGate(q, run("slow"), now - 100);
    let executed = 0;
    const wrapped = wrapToolWithBeforeToolCallHook(
      tool(async () => {
        executed++;
        if (executed === 1) {
          now += 2_000; // the tool outlasts warnMs (1,000)
        }
        return result();
      }),
      { sessionKey: k, config: gateConfig(1_000) },
    );
    const first = await wrapped.execute("first", {});
    expect(JSON.stringify(first)).toContain("Wrapper gate");
    expect(
      q.wrapperGateWindow?.warnedAt,
      "warnedAt is the receipt time, not the dispatch time",
    ).toBe(12_000);
    const second = await wrapped.execute("second", {});
    expect(
      (second as { details?: { deniedReason?: string } }).details?.deniedReason,
    ).toBeUndefined();
    now = 12_000 + 1_001;
    const third = await wrapped.execute("third", {});
    expect((third as { details?: { deniedReason?: string } }).details?.deniedReason).toBe(
      "wrapper-gate-deadline",
    );
    expect(executed).toBe(2);
  });
});
