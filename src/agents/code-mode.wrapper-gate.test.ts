import { afterEach, describe, expect, it, vi } from "vitest";
import { clearFollowupQueue, getFollowupQueue } from "../auto-reply/reply/queue/state.js";
import { noteFollowupEnqueuedForWrapperGate } from "../auto-reply/reply/queue/wrapper-gate.js";
import { createCodeModeTools } from "./code-mode.js";
import {
  createHeadlessCodeModeHarness,
  resetCodeModeTestState,
  resultDetails,
} from "./code-mode.test-support.js";
import { fakeTool } from "./test-helpers/fake-tool.js";
import { jsonResult, type AnyAgentTool } from "./tools/common.js";

// D4 through the real code-mode exec tool: the gate must read the OpenClaw
// config the session runs with, not the resolved Code Mode limits.
const SESSION_KEY = "agent:test:code-mode-wrapper-gate";

function openWindow() {
  const queue = getFollowupQueue(SESSION_KEY, {
    mode: "collect",
    wrapperGate: { enabled: true, collectMs: 1, warnMs: 5_000 },
  });
  noteFollowupEnqueuedForWrapperGate(
    queue,
    { run: { trustedInternalHandoff: { kind: "subagent-completion" } } as never },
    Date.now() - 1_000,
  );
  return queue.wrapperGateWindow!;
}

function execTool(tools: AnyAgentTool[]) {
  const base = createHeadlessCodeModeHarness(tools);
  const config = {
    ...(base.config as object),
    messages: { queue: { wrapperGate: { enabled: true, warnMs: 5_000 } } },
  } as never;
  return createCodeModeTools({
    ...base,
    config,
    runtimeConfig: config,
    sessionKey: SESSION_KEY,
  })[0]!;
}

afterEach(async () => {
  await resetCodeModeTestState();
  clearFollowupQueue(SESSION_KEY);
});

describe("code-mode exec and the wrapper gate (D4)", () => {
  it("refuses the outer exec past the deadline without running the script", async () => {
    const window = openWindow();
    window.forceHandoffAt = Date.now() - 1;
    window.warned = true;
    window.warnedAt = Date.now() - 60_000;
    const probe = { ...fakeTool("probe", "Probe"), execute: vi.fn(async () => jsonResult({})) };

    const result = await execTool([probe]).execute("gate-refuse", {
      code: "await probe({}); return 1;",
    });

    expect(resultDetails(result)).toMatchObject({
      status: "blocked",
      deniedReason: "wrapper-gate-deadline",
    });
    expect(probe.execute).not.toHaveBeenCalled();
  });

  it("puts a warning a nested call delivered on the outer result when the script drops it", async () => {
    const window = openWindow();
    window.forceHandoffAt = Date.now() + 60_000;
    // The nested call is where the gate delivers the warning; the script
    // ignores that call's result.
    const probe = {
      ...fakeTool("probe", "Probe"),
      execute: vi.fn(async () => {
        window.warned = true;
        window.warnedAt = Date.now();
        return jsonResult({});
      }),
    };

    const result = await execTool([probe]).execute("gate-warn", {
      code: 'await probe({}); return "done";',
    });

    expect(probe.execute).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(result.content)).toContain("Wrapper gate");
    expect(JSON.stringify(result.content)).toContain("[tool_denial countdown");
  });

  it("adds nothing when no nested call delivered the warning", async () => {
    openWindow().forceHandoffAt = Date.now() + 60_000;
    const result = await execTool([]).execute("gate-quiet", { code: "return 1;" });
    expect(resultDetails(result)).toMatchObject({ status: "completed" });
    expect(JSON.stringify(result.content)).not.toContain("Wrapper gate");
  });
});
