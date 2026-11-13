import { beforeEach, expect, it, vi } from "vitest";
import { isAbortTrigger } from "./abort-trigger-text.js";
import { handleNextCommand } from "./commands-session-abort.js";
import { buildPluginsCommandParams } from "./commands.test-harness.js";

const mocks = vi.hoisted(() => ({
  abort: vi.fn(),
  capture: vi.fn(),
  controller: vi.fn(),
  select: vi.fn(),
  kill: vi.fn(),
}));

vi.mock("../../globals.js", () => ({ logVerbose: vi.fn() }));
vi.mock("./abort-operation.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./abort-operation.js")>()),
  captureChannelSessionStop: mocks.capture,
  abortSessionRunTargetWithOutcome: mocks.abort,
  stopSubagentsForRequester: vi.fn(async () => ({ stopped: 0, failed: 0 })),
}));
vi.mock("../../agents/subagents/registry/subagent-control-scope.js", async (importOriginal) => ({
  ...(await importOriginal<
    typeof import("../../agents/subagents/registry/subagent-control-scope.js")
  >()),
  resolveSubagentController: mocks.controller,
  captureExecRequestSubagentSelection: mocks.select,
}));
vi.mock("../../agents/subagents/registry/subagent-control.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../agents/subagents/registry/subagent-control.js")>()),
  killAllControlledSubagentRuns: mocks.kill,
}));
vi.mock("./commands-session-store.js", () => ({
  persistAbortTargetEntry: vi.fn(),
  resolveCommandSessionEntryForKey: vi.fn(() => ({ entry: undefined, key: "agent:main:main" })),
}));

function paramsFor(body = "/next") {
  const params = buildPluginsCommandParams({
    commandBodyNormalized: body,
    cfg: { commands: { text: true }, channels: { whatsapp: { allowFrom: ["*"] } } },
  });
  params.command.isAuthorizedSender = true;
  params.command.senderIsOwner = true;
  params.sessionKey = "agent:main:main";
  return params;
}

beforeEach(() => {
  vi.clearAllMocks();
  mocks.capture.mockImplementation((p: object) => ({
    ...p,
    controller: { queuedInputs: [] },
    requestRunIds: new Set(["run-a"]),
  }));
  mocks.controller.mockReturnValue({ controlScope: "children" });
  mocks.select.mockReturnValue({ runs: [], prepare: undefined });
  mocks.kill.mockResolvedValue({ killed: 0 });
  mocks.abort.mockImplementation(() => ({
    completed: Promise.resolve({
      aborted: true,
      alreadyFinalizing: false,
      childrenStopped: 0,
      childFailures: 0,
    }),
  }));
});

it.each(["/next", "/interrupt"])("routes %s through source talk", async (body) => {
  const result = await handleNextCommand(paramsFor(body), true);
  expect(mocks.abort).toHaveBeenCalledWith(expect.objectContaining({ source: "talk" }));
  expect(result).toMatchObject({ shouldContinue: false });
});

it("reports idle when nothing is queued", async () => {
  const result = await handleNextCommand(paramsFor(), true);
  expect(result?.reply).toEqual({ text: "⚙️ Turn ended. Agent is idle, nothing queued." });
});

it("reports busy with the queued count", async () => {
  mocks.capture.mockImplementation((p: object) => ({
    ...p,
    controller: { queuedInputs: [{}, {}] },
    requestRunIds: new Set(["run-a"]),
  }));
  const result = await handleNextCommand(paramsFor(), true);
  expect(result?.reply).toEqual({ text: "⚙️ Turn ended. Busy again with 2 queued items." });
});

it("reports a finalizing turn without waiting", async () => {
  mocks.abort.mockReturnValue({ completed: Promise.resolve({ alreadyFinalizing: true }) });
  const result = await handleNextCommand(paramsFor(), true);
  expect(result?.reply).toEqual({
    text: "Agent reply is already finalizing and can no longer be interrupted.",
  });
});

it("aborts nothing when no turn is active", async () => {
  mocks.abort.mockReturnValue({
    completed: Promise.resolve({ aborted: false, alreadyFinalizing: false }),
  });
  const result = await handleNextCommand(paramsFor(), true);
  expect(result?.reply).toEqual({ text: "No active turn to end. Agent is idle, nothing queued." });
});

it("stops no subagent when the turn has no run id", async () => {
  mocks.capture.mockImplementation((p: object) => ({
    ...p,
    controller: { queuedInputs: [] },
    requestRunIds: new Set(),
  }));
  await handleNextCommand(paramsFor(), true);
  expect(mocks.select).not.toHaveBeenCalled();
  expect(mocks.kill).not.toHaveBeenCalled();
});

it("scopes child selection to the captured turn run id", async () => {
  await handleNextCommand(paramsFor(), true);
  expect(mocks.select).toHaveBeenCalledWith(
    expect.objectContaining({ requesterTurnRunId: "run-a" }),
  );
});

it("refuses when the selected session changed before the abort", async () => {
  const params = paramsFor();
  params.opts = { ...params.opts, isCommandTargetCurrent: () => false };
  mocks.abort.mockImplementation((p: { assertCurrent?: () => void }) => {
    p.assertCurrent?.();
    return { completed: Promise.resolve({ aborted: true }) };
  });
  await expect(handleNextCommand(params, true)).rejects.toThrow("selected session changed");
});

it("is never taken by the stop recognizer", () => {
  expect(isAbortTrigger("/next")).toBe(false);
  expect(isAbortTrigger("/interrupt")).toBe(false);
});
