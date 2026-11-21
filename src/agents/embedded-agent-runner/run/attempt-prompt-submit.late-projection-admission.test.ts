// Regression for openclaw/openclaw#172031: a host producer that enriches the model
// prompt projection after durable admission rotates the transcript generation that a
// custom durable context engine bound at the earlier boundary. The durable outbox
// admits the turn with the pre-capture admission, the recorder's refreshed receipt
// replaces it at accept time, and commitTurn is dispatched with a generation the
// engine never admitted - so a generation-bound engine rejects the durable commit of
// the already-finished turn. This drives the current runner path: durable admission,
// enrichment capture at the LLM boundary, first tool execution, turn completion, and
// durable finalization.
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it, vi } from "vitest";
import { setRuntimeConfigSnapshot } from "../../../config/runtime-snapshot.js";
import {
  readActiveTranscriptEntryAnchor,
  upsertSessionEntryCore,
} from "../../../config/sessions/session-accessor.js";
import { loadSessionEntryForAdmission } from "../../../config/sessions/session-accessor.sqlite-entry-admission.js";
import { createSessionActorFactory } from "../../../config/sessions/session-actor-durable.js";
import type { TranscriptTurnAdmission } from "../../../config/sessions/transcript-entry-anchor.js";
import { withOwnedSessionTranscriptWrites } from "../../../config/sessions/transcript-write-context.js";
import type { ContextEngine } from "../../../context-engine/types.js";
import { readDatabasePathIdentitySync } from "../../../infra/sqlite-worker-identity.js";
import { createUserTurnTranscriptRecorder } from "../../../sessions/user-turn-transcript.js";
import { readModelPromptProjection } from "../../../sessions/user-turn-transcript.message.js";
import { withOpenClawTestState } from "../../../test-utils/openclaw-test-state.js";
import { toToolDefinitions } from "../../agent-tool-definition-adapter.js";
import {
  drainPendingContextEngineTurnsBeforeRun,
  finalizeAcceptedContextEngineTurn,
} from "../../harness/context-engine-turn-attempt.js";
import {
  createTurnEngine,
  createTurnFacts,
  createTurnLease,
} from "../../harness/context-engine-turn.test-support.js";
import { guardSessionManager } from "../../session-tool-result-guard-wrapper.js";
import {
  createAssistant,
  createAssistantResultStream,
  createTestSession,
  registerAgentSessionLoopTestLifecycle,
  streamMocks,
} from "../../sessions/agent-session-loop-correctness.test-support.js";
import { createResourceLoader } from "../../sessions/agent-session-loop-resource-loader.test-support.js";
import { withSessionManagerWrite } from "../../sessions/session-manager-write-admission.js";
import { SessionManager } from "../../sessions/session-manager.js";
import { normalizeMessagesForLlmBoundary } from "./attempt-llm-boundary.js";
import { submitEmbeddedAttemptPrompt } from "./attempt-prompt-submit.js";
import {
  createBaseInput,
  sessionId as modelPromptSessionId,
} from "./attempt-prompt-submit.test-support.js";
import { createUserTranscriptContextRegistry } from "./attempt-user-transcript-context-registry.js";
import { buildRuntimeContextCustomMessage } from "./runtime-context-prompt.js";

registerAgentSessionLoopTestLifecycle();
const sessionId = modelPromptSessionId;

type Session = Awaited<ReturnType<typeof createTestSession>>["session"];
type Submission = Parameters<typeof submitEmbeddedAttemptPrompt>[0];
function submit(session: Session, input: Partial<Submission>) {
  return submitEmbeddedAttemptPrompt({
    ...createBaseInput(),
    activeSession: session,
    withTranscriptWrite: (write) => withSessionManagerWrite(session.sessionManager, write),
    promptActiveSession: (prompt, options) => session.prompt(prompt, options),
    ...input,
  });
}

describe("late model prompt projection vs durable context-engine admission (#172031)", () => {
  it("commits the admission the engine durably bound when the enriched projection is captured after admission", async () => {
    await withOpenClawTestState({ label: "late-projection-admission" }, async (state) => {
      const target = {
        agentId: "main",
        sessionId,
        sessionKey: "agent:main:late-projection-admission",
        storePath: path.join(state.agentDir("main"), "openclaw-agent.sqlite"),
      };
      setRuntimeConfigSnapshot({});
      await upsertSessionEntryCore(target, { sessionId, updatedAt: 1 });
      const { databaseClaim } = await loadSessionEntryForAdmission(target);
      if (!("kind" in databaseClaim) || databaseClaim.kind !== "worker" || !databaseClaim.reader) {
        throw new Error("Expected an admitted durable reader");
      }
      const reader = databaseClaim.reader;
      const identity = readDatabasePathIdentitySync(reader.database.path);
      if (!identity.key.startsWith("file:")) {
        throw new Error("Expected a physical actor database");
      }
      const assertCurrent = () => reader.assertCurrent();
      const actor = await createSessionActorFactory(reader.database).acquire(
        {
          sessionKey: target.sessionKey,
          database: {
            kind: "file",
            physicalIdentity: identity.key.slice(5),
            birthtime: identity.birthtime,
            nativeLocation: reader.database.path,
          },
        },
        { assertCurrent, assertReadable: assertCurrent },
      );
      if ("kind" in actor) {
        throw new Error("Expected a durable session actor");
      }
      try {
        await actor.read({ assertCurrent, authorize: assertCurrent });
        await withOwnedSessionTranscriptWrites(
          {
            sessionTarget: target,
            sessionReader: reader,
            sessionActor: { actor, database: reader.database },
            withTranscriptWrite: async (write) => write(),
          },
          async () => {
            const manager = await SessionManager.openAsync(target, state.workspaceDir);
            const contexts = createUserTranscriptContextRegistry();
            const recorder = createUserTurnTranscriptRecorder({
              input: { text: "original", timestamp: 1, idempotencyKey: "late-projection:user" },
              target: { ...target, sessionEntry: { sessionId, updatedAt: 1 } },
            });

            // The custom durable context engine: it records the admission handed to
            // commitTurn and enforces the generation it bound at the admission
            // boundary (the generation-bound consumer guard from #172031).
            const bound: { admission?: TranscriptTurnAdmission } = {};
            const committedAdmissions: TranscriptTurnAdmission[] = [];
            const generationMismatch: { message: string | null } = { message: null };
            const commitTurn = vi.fn<NonNullable<ContextEngine["commitTurn"]>>(async (turn) => {
              committedAdmissions.push(structuredClone(turn.admission));
              if (bound.admission && turn.admission.generation !== bound.admission.generation) {
                generationMismatch.message = `native_owner_generation_changed: bound ${bound.admission.generation} committed ${turn.admission.generation}`;
                throw new Error(generationMismatch.message);
              }
              return { status: "committed" as const };
            });
            const engine = createTurnEngine(commitTurn);
            const lease = createTurnLease(engine);
            const warn = vi.fn();

            const toolExecutions: string[] = [];
            const { session } = await createTestSession({
              sessionManager: manager,
              customTools: toToolDefinitions([
                {
                  name: "late_projection_probe",
                  description: "Executes once during the regression turn.",
                  parameters: { type: "object", properties: {}, additionalProperties: false },
                  execute: async () => {
                    toolExecutions.push("late_projection_probe");
                    return { title: "Probe", details: { status: "completed", value: "probe-ok" } };
                  },
                },
              ]),
              resourceLoader: await createResourceLoader(),
            });
            const convert = session.agent.convertToLlm;
            session.agent.convertToLlm = async (messages) =>
              convert(
                normalizeMessagesForLlmBoundary(messages, {
                  sessionVersion: manager.getHeader()?.version,
                  userTranscriptContexts: contexts.list(),
                  timezone: "UTC",
                }),
              );
            guardSessionManager(manager, {
              preparedUserTurnMessage: recorder.message,
              preparedUserTurnTranscriptRecorder: recorder,
              onUserMessagePersisted: (message, runtimeMessage) => {
                if (runtimeMessage) {
                  contexts.record(runtimeMessage, message);
                }
              },
            });

            // First round: the model requests the tool. Second round: the turn completes.
            const modelRequests: unknown[] = [];
            streamMocks.streamSimple.mockImplementation((model, context) => {
              modelRequests.push(structuredClone(context.messages));
              if (modelRequests.length === 1) {
                return createAssistantResultStream(
                  createAssistant(
                    model,
                    [
                      { type: "text", text: "Calling the probe." },
                      {
                        type: "toolCall",
                        id: "call_probe",
                        name: "late_projection_probe",
                        arguments: {},
                      },
                    ],
                    "toolUse",
                  ),
                );
              }
              return createAssistantResultStream(createAssistant(model, "Done."));
            });

            // The runner's begin: install the durable admission handler before the
            // run, exactly as beginContextEngineLogicalTurn does for the current turn.
            await drainPendingContextEngineTurnsBeforeRun({
              recorder,
              sessionTarget: target,
              lease,
              warn,
            });

            // The host producer enriches the prompt after durable admission: runtime
            // context plus prelude/postlude reach transformContext, and the LLM
            // boundary captures them as the model prompt projection.
            await submit(session, {
              attempt: { sessionId, userTurnTranscriptRecorder: recorder },
              appendOnlyRuntimeContext: true,
              runtimeContextMessage: buildRuntimeContextCustomMessage("Synthetic runtime"),
              transcriptPrompt: "original",
              modelPrompt: "",
              prependContext: "before",
              appendContext: "after",
              getUserTranscriptContexts: () => contexts.list(),
            });

            // The turn ran through first tool execution and durable completion.
            expect(toolExecutions).toEqual(["late_projection_probe"]);
            expect(modelRequests).toHaveLength(2);
            expect(session.getLastAssistantText()).toBe("Done.");
            // The enrichment producer stored a projection after admission.
            expect(readModelPromptProjection(recorder.getPersistedMessage?.())).toContain(
              "original",
            );

            // The admission the durable outbox bound at the earlier boundary.
            const db = new DatabaseSync(target.storePath, { readOnly: true });
            let admittedPayload: { state?: string; admission?: TranscriptTurnAdmission };
            try {
              const row = db
                .prepare("SELECT payload_json FROM context_engine_turn_outbox")
                .get() as { payload_json: string } | undefined;
              expect(row).toBeDefined();
              admittedPayload = JSON.parse(row!.payload_json);
            } finally {
              db.close();
            }
            expect(admittedPayload.state).toBe("admitted");
            expect(admittedPayload.admission).toBeDefined();
            bound.admission = admittedPayload.admission;

            // Finalize the accepted turn the way attempt-finalize does: the boundary
            // carries the recorder's current (refreshed) admission receipt.
            const finalReceipt = recorder.getAdmissionReceipt();
            expect(finalReceipt).toBeDefined();
            let entryId = manager.getLeafId();
            let terminalEntryId: string | null = null;
            while (entryId) {
              const entry = manager.getEntry(entryId);
              if (!entry || entry.type !== "custom" || entry.customType !== "openclaw.cache-ttl") {
                terminalEntryId = entry ? entryId : null;
                break;
              }
              entryId = entry.parentId;
            }
            expect(terminalEntryId).toBeTruthy();
            const terminal = readActiveTranscriptEntryAnchor({
              ...target,
              entryId: terminalEntryId!,
            });
            expect(terminal).toBeDefined();
            const facts = createTurnFacts(
              { admission: finalReceipt!, terminal: terminal! },
              target,
            );
            await finalizeAcceptedContextEngineTurn({ facts, lease, warn });

            // The regression (#172031): on current main the accepted boundary's
            // refreshed admission replaces the generation the engine durably bound,
            // so the engine's guard rejects and the finished turn never durably
            // commits. The fixed contract commits the bound admission instead.
            expect(generationMismatch.message).toBeNull();
            expect(committedAdmissions).toHaveLength(1);
            expect(committedAdmissions[0]).toEqual(bound.admission);
            expect(warn).not.toHaveBeenCalledWith(
              expect.stringMatching(/durable turn advancement remains queued/),
            );
          },
        );
      } finally {
        await actor.release();
        await databaseClaim.release();
      }
    });
  });
});
