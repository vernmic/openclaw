import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  discardIntent: vi.fn(),
  finalizeHarnessContextEngineTurn: vi.fn(async () => undefined),
  markActiveEmbeddedRunAbandoned: vi.fn(),
  readActiveTranscriptEntryAnchor: vi.fn(),
  readNewestActiveMessageTranscriptEntryAnchor: vi.fn(),
  resolveTerminalMessageEntryId: vi.fn(),
  runAgentEndSideEffects: vi.fn(async () => undefined),
  warn: vi.fn(),
}));

vi.mock("../../../config/sessions/session-accessor.js", () => ({
  readActiveTranscriptEntryAnchor: mocks.readActiveTranscriptEntryAnchor,
  readNewestActiveMessageTranscriptEntryAnchor: mocks.readNewestActiveMessageTranscriptEntryAnchor,
}));
vi.mock("../../../context-engine/registry.js", () => ({
  resolveContextEngineRegisteredId: (engine: unknown) =>
    engine ? "hindsight-openclaw" : undefined,
}));
vi.mock("../../agent-run-terminal-outcome.js", () => ({
  projectAgentRunAttemptTerminal: vi.fn(() => ({
    aborted: false,
    timedOutDuringCompaction: false,
  })),
  mergeAgentRunAttemptTerminal: vi.fn((terminal) => terminal),
}));
vi.mock("../../harness/agent-end-side-effects.js", () => ({
  runAgentEndSideEffects: mocks.runAgentEndSideEffects,
}));
vi.mock("../../harness/context-engine-lifecycle.js", () => ({
  finalizeHarnessContextEngineTurn: mocks.finalizeHarnessContextEngineTurn,
}));
vi.mock("../../harness/context-engine-turn-outbox-store.js", () => ({
  openContextEngineTurnOutboxWorkerStore: vi.fn(() => ({
    discardIntent: mocks.discardIntent,
  })),
}));
vi.mock("./agent-end-context.js", () => ({ buildEmbeddedAgentEndContext: () => ({}) }));
vi.mock("./attempt-terminal-anchor.js", () => ({
  resolveTerminalMessageEntryId: mocks.resolveTerminalMessageEntryId,
}));
vi.mock("../logger.js", () => ({ log: { debug: vi.fn(), error: vi.fn(), warn: mocks.warn } }));
vi.mock("../runs.js", () => ({
  markActiveEmbeddedRunAbandoned: mocks.markActiveEmbeddedRunAbandoned,
}));

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
} as never;

function makeDurableEngine() {
  return {
    info: {
      id: "hindsight-openclaw",
      transcriptSemantics: {
        currentTurnFence: "before-current-turn-entry-v1",
        turnAdvancementIdempotency: "atomic-idempotent-v1",
      },
    },
    commitTurn: vi.fn(),
  } as never;
}

function makeInput(engine: unknown, pluginId: string | undefined) {
  return {
    attempt: {
      onContextEngineTurnCandidate: vi.fn(),
      userTurnTranscriptRecorder: { getAdmissionReceipt: () => admission },
      sessionKey: "agent:main:main",
      sessionTarget: "current",
      runId: "run-1",
      sessionId: "session-1",
      provider: "zai",
      modelId: "glm-5.3",
      bootstrapContextRunKind: "chat",
      config: {},
    },
    activeContextEngine: engine,
    agentDir: "agent-dir",
    resolveActiveContextEnginePluginId: () => pluginId,
    state: { terminal: { kind: "ok" } },
    sessionLock: { withOwnedTranscriptWrite: (run: () => unknown) => run() },
    setup: { effectiveWorkspace: "workspace", sessionAgentId: "main" },
    prepared: {
      sessionRuntime: {
        agentSession: { hookRunner: {} },
        contextGuards: { getAfterTurnCheckpoint: () => undefined },
        sessionManager: {
          getEntry: () => undefined,
          getLeafEntry: () => undefined,
          getSessionTarget: () => undefined,
        },
        state: { prePromptMessageCount: 0 },
      },
      bootstrap: { shouldRecordCompletedBootstrapTurn: false },
      bundleTools: { uncompactedEffectiveTools: [] },
      toolBase: { nestedToolActivityState: createAttemptNestedToolActivityState() },
    },
    diagnostics: { diagnosticTrace: {} },
  } as never;
}

const settled = {
  sessionIdUsed: "session-1",
  promptError: false,
  messagesSnapshot: [],
  lastCallUsage: undefined,
  promptCache: undefined,
  compactionOccurredThisAttempt: false,
} as never;

const prompt = {
  yieldAborted: false,
  transcriptLeafId: "leaf-1",
  promptStartedAt: Date.now(),
} as never;

describe("completeEmbeddedAttemptAfterTurn durable context-engine turn", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.resolveTerminalMessageEntryId.mockReturnValue("leaf-1");
  });

  it("announces contextEngineTurn with the owner plugin id from the active engine", async () => {
    mocks.readActiveTranscriptEntryAnchor.mockReturnValue({ rawSeq: 12 });
    await completeEmbeddedAttemptAfterTurn(
      makeInput(makeDurableEngine(), "hindsight-openclaw"),
      settled,
      prompt,
    );
    const call = mocks.runAgentEndSideEffects.mock.calls.at(-1)?.[0] as {
      ctx?: { contextEngineTurn?: { logicalTurnId: string; enginePluginId?: string } };
    };
    expect(call?.ctx?.contextEngineTurn).toEqual({
      logicalTurnId: "logical-turn-1",
      enginePluginId: "hindsight-openclaw",
    });
  });

  it("omits contextEngineTurn when the engine does not support durable advancement", async () => {
    mocks.readActiveTranscriptEntryAnchor.mockReturnValue({ rawSeq: 12 });
    const engineWithoutCommit = {
      info: {
        id: "hindsight-openclaw",
        transcriptSemantics: makeDurableEngine().info.transcriptSemantics,
      },
    };
    await completeEmbeddedAttemptAfterTurn(
      makeInput(engineWithoutCommit, "hindsight-openclaw"),
      settled,
      prompt,
    );
    const call = mocks.runAgentEndSideEffects.mock.calls.at(-1)?.[0] as {
      ctx?: { contextEngineTurn?: unknown };
    };
    expect(call?.ctx?.contextEngineTurn).toBeUndefined();
  });

  it("discards the turn intent when the terminal anchor cannot resolve", async () => {
    mocks.readActiveTranscriptEntryAnchor.mockReturnValue(undefined);
    mocks.readNewestActiveMessageTranscriptEntryAnchor.mockReturnValue(undefined);
    await completeEmbeddedAttemptAfterTurn(
      makeInput(makeDurableEngine(), "hindsight-openclaw"),
      settled,
      prompt,
    );
    expect(mocks.discardIntent).toHaveBeenCalledWith(
      expect.objectContaining({
        admission,
        engineId: "hindsight-openclaw",
        ownerPluginId: "hindsight-openclaw",
        onlyAdmitted: true,
      }),
    );
  });
});
