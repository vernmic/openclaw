import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  readActiveTranscriptEntryAnchorAsync: vi.fn(),
  runAgentEndSideEffects: vi.fn(async () => undefined),
  waitForSessionTranscriptProjection: vi.fn(async () => undefined),
  warn: vi.fn(),
}));

vi.mock("../../../config/sessions/session-transcript-anchor-read.js", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return {
    ...actual,
    readActiveTranscriptEntryAnchorAsync: mocks.readActiveTranscriptEntryAnchorAsync,
  };
});
vi.mock("../../../config/sessions/session-transcript-reconcile.js", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return {
    ...actual,
    waitForSessionTranscriptProjection: mocks.waitForSessionTranscriptProjection,
  };
});
vi.mock("../../harness/agent-end-side-effects.js", () => ({
  runAgentEndSideEffects: mocks.runAgentEndSideEffects,
  runAgentEndSideEffectsAsync: mocks.runAgentEndSideEffects,
}));
vi.mock("./agent-end-context.js", () => ({
  buildEmbeddedAgentEndContext: () => ({}),
  buildEmbeddedForegroundPromptContext: () => ({}),
}));
vi.mock("../logger.js", () => ({ log: { debug: vi.fn(), error: vi.fn(), warn: mocks.warn } }));
vi.mock("../runs.js", () => ({
  markActiveEmbeddedRunAbandoned: vi.fn(),
}));

import { sessionManagerReadMessageAnchor } from "../../sessions/session-manager-message-anchor.js";
import { completeEmbeddedAttemptAfterTurn } from "./attempt-finalize.js";
import { createAttemptNestedToolActivityState } from "./attempt-nested-tool-activity.js";

const admission = {
  agentId: "main",
  sessionId: "session-1",
  sessionKey: "agent:main:main",
  storePath: "store.sqlite",
  generation: "gen-1",
  entryId: "entry-1",
  rawSeq: 10,
  effectiveParentId: "parent-1",
  activeMessagePosition: 4,
  idempotencyKey: "turn:user",
  logicalTurnId: "logical-turn-1",
  role: "user",
};

const terminalAnchor = {
  agentId: "main",
  sessionId: "session-1",
  sessionKey: "agent:main:main",
  storePath: "store.sqlite",
  generation: "gen-terminal",
  entryId: "terminal-entry-1",
  rawSeq: 12,
  effectiveParentId: "entry-1",
  activeMessagePosition: 5,
};

const terminalEntryId = "terminal-entry-1";

function makeDurableEngine() {
  return {
    info: {
      id: "fixture-context-engine",
      name: "Fixture Context Engine",
      transcriptSemantics: {
        currentTurnFence: "before-current-turn-entry-v1",
        turnAdvancementIdempotency: "atomic-idempotent-v1",
      },
    },
    commitTurn: vi.fn(),
  } as never;
}

function makeLegacyEngine() {
  return { info: { id: "legacy", name: "Legacy" } } as never;
}

function makeInput(options?: {
  engine?: unknown;
  pluginId?: string;
  terminal?: { kind: "ok" } | { kind: "aborted"; source: "runtime" };
  admission?: typeof admission;
  degradedReason?: string;
}) {
  const onContextEngineTurnCandidate = vi.fn();
  const receipt = options && "admission" in options ? options.admission : admission;
  const input = {
    runAbortController: new AbortController(),
    attempt: {
      onContextEngineTurnCandidate,
      userTurnTranscriptRecorder: {
        getAdmissionReceipt: () => receipt,
      },
      sessionKey: "agent:main:main",
      sessionTarget: "current",
      runId: "run-1",
      sessionId: "session-1",
      provider: "fixture-provider",
      modelId: "fixture-model",
      model: { contextWindow: 200000 },
      bootstrapContextRunKind: "chat",
      config: {},
      contextTokenBudget: 180000,
      ...(options?.degradedReason ? { degradedReason: options.degradedReason } : {}),
    },
    activeContextEngine: options?.engine ?? makeDurableEngine(),
    agentDir: "agent-dir",
    resolveActiveContextEnginePluginId: () => options?.pluginId,
    state: { terminal: options?.terminal ?? { kind: "ok" } },
    sessionLock: { withOwnedTranscriptWrite: (run: () => unknown) => run() },
    setup: { effectiveWorkspace: "workspace", sessionAgentId: "main" },
    prepared: {
      promptToolPolicy: { current: { uncompactedEffectiveTools: [] } },
      sessionRuntime: {
        agentSession: { hookRunner: {} },
        contextGuards: { getAfterTurnCheckpoint: () => undefined },
        sessionManager: {
          getEntry: (id: string) =>
            id === terminalEntryId
              ? { id: terminalEntryId, type: "message", parentId: null }
              : undefined,
          getLeafEntry: () => undefined,
          getLeafId: () => terminalEntryId,
          getSessionTarget: () => undefined,
          [sessionManagerReadMessageAnchor]: () => undefined,
        },
        state: { prePromptMessageCount: 0 },
      },
      bootstrap: { shouldRecordCompletedBootstrapTurn: false },
      bundleTools: { uncompactedEffectiveTools: [] },
      toolBase: { nestedToolActivityState: createAttemptNestedToolActivityState() },
    },
    diagnostics: { diagnosticTrace: {} },
  } as never;
  return { input, onContextEngineTurnCandidate };
}

function settled(overrides?: { promptError?: unknown }) {
  return {
    sessionIdUsed: "session-1",
    promptError: overrides?.promptError,
    messagesSnapshot: [],
    lastCallUsage: undefined,
    promptCache: undefined,
    compactionOccurredThisAttempt: false,
  } as never;
}

function prompt(overrides?: { yieldAborted?: boolean }) {
  return {
    yieldAborted: overrides?.yieldAborted ?? false,
    transcriptLeafId: "leaf-1",
    promptStartedAt: Date.now(),
  } as never;
}

function contextEngineTurnAt(index: number) {
  const call = mocks.runAgentEndSideEffects.mock.calls[index]?.[0] as
    | {
        ctx?: { contextEngineTurn?: { logicalTurnId: string; enginePluginId?: string } };
      }
    | undefined;
  return call?.ctx?.contextEngineTurn;
}

describe("completeEmbeddedAttemptAfterTurn durable context-engine turn", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.readActiveTranscriptEntryAnchorAsync.mockResolvedValue(terminalAnchor);
    mocks.waitForSessionTranscriptProjection.mockResolvedValue(undefined);
  });

  it("announces the recorded candidate, including the plugin id for a plugin engine", async () => {
    const { input, onContextEngineTurnCandidate } = makeInput({
      pluginId: "fixture-engine-plugin",
    });
    await completeEmbeddedAttemptAfterTurn(input, settled(), prompt());
    expect(contextEngineTurnAt(0)).toEqual({
      logicalTurnId: "logical-turn-1",
      enginePluginId: "fixture-engine-plugin",
    });
    expect(onContextEngineTurnCandidate).toHaveBeenCalledOnce();
  });

  it("announces logicalTurnId without enginePluginId when the engine has no plugin owner", async () => {
    const { input } = makeInput();
    await completeEmbeddedAttemptAfterTurn(input, settled(), prompt());
    expect(contextEngineTurnAt(0)).toEqual({ logicalTurnId: "logical-turn-1" });
  });

  it.each([
    {
      label: "prompt error",
      promptError: new Error("prompt failed") as Error | undefined,
      terminal: undefined as { kind: "aborted"; source: "runtime" } | undefined,
      yieldAborted: false,
      missingAnchor: false,
      missingAdmission: false,
    },
    {
      label: "abort",
      promptError: undefined,
      terminal: { kind: "aborted" as const, source: "runtime" as const },
      yieldAborted: false,
      missingAnchor: false,
      missingAdmission: false,
    },
    {
      label: "yield abort",
      promptError: undefined,
      terminal: undefined,
      yieldAborted: true,
      missingAnchor: false,
      missingAdmission: false,
    },
    {
      label: "missing terminal anchor",
      promptError: undefined,
      terminal: undefined,
      yieldAborted: false,
      missingAnchor: true,
      missingAdmission: false,
    },
    {
      label: "missing admission",
      promptError: undefined,
      terminal: undefined,
      yieldAborted: false,
      missingAnchor: false,
      missingAdmission: true,
    },
  ])("omits contextEngineTurn on $label", async (scenario) => {
    if (scenario.missingAnchor) {
      mocks.readActiveTranscriptEntryAnchorAsync.mockResolvedValueOnce(undefined);
    }
    const { input, onContextEngineTurnCandidate } = makeInput({
      pluginId: "fixture-engine-plugin",
      terminal: scenario.terminal,
      admission: scenario.missingAdmission ? undefined : admission,
    });
    await completeEmbeddedAttemptAfterTurn(
      input,
      settled(scenario.promptError ? { promptError: scenario.promptError } : undefined),
      prompt(scenario.yieldAborted ? { yieldAborted: true } : undefined),
    );
    expect(contextEngineTurnAt(0)).toBeUndefined();
    expect(onContextEngineTurnCandidate).not.toHaveBeenCalled();
  });

  it("omits contextEngineTurn for a degraded lease running the legacy engine", async () => {
    const { input, onContextEngineTurnCandidate } = makeInput({
      engine: makeLegacyEngine(),
      pluginId: "fixture-engine-plugin",
      degradedReason: "pending durable advancement could not be checked",
    });
    await completeEmbeddedAttemptAfterTurn(input, settled(), prompt());
    expect(contextEngineTurnAt(0)).toBeUndefined();
    expect(onContextEngineTurnCandidate).toHaveBeenCalledOnce();
  });

  it("gives the field only to the attempt that reaches the candidate after a failed attempt", async () => {
    const { input, onContextEngineTurnCandidate } = makeInput({
      pluginId: "fixture-engine-plugin",
    });
    await completeEmbeddedAttemptAfterTurn(
      input,
      settled({ promptError: new Error("prompt failed") }),
      prompt(),
    );
    await completeEmbeddedAttemptAfterTurn(input, settled(), prompt());
    expect(contextEngineTurnAt(0)).toBeUndefined();
    expect(contextEngineTurnAt(1)).toEqual({
      logicalTurnId: "logical-turn-1",
      enginePluginId: "fixture-engine-plugin",
    });
    expect(onContextEngineTurnCandidate).toHaveBeenCalledOnce();
  });

  it("omits contextEngineTurn when the record callback throws", async () => {
    const { input, onContextEngineTurnCandidate } = makeInput({
      pluginId: "fixture-engine-plugin",
    });
    onContextEngineTurnCandidate.mockImplementation(() => {
      throw new Error("record failed");
    });
    await completeEmbeddedAttemptAfterTurn(input, settled(), prompt()).catch(() => undefined);
    expect(onContextEngineTurnCandidate).toHaveBeenCalledOnce();
    expect(contextEngineTurnAt(0)).toBeUndefined();
  });
});
