// Adapted onto v2026.10.3 (cohort 1): upstream this file was created by #165469 ("settle restart
// recovery in one terminal operation"), which is not part of this candidate. Only the case added
// by #166597 (df4bf5295a7, "queue follow-ups behind settling image completions") is carried; it
// runs against stable's own command owner and durable cleanup, with no #165469 production code.
import { expect, it, vi } from "vitest";
import { awaitGateBeforeSettlement, createDeferred } from "../../test/helpers/promise.js";
import * as sessionAdmission from "../sessions/session-lifecycle-admission.js";
import { createAdmittedRunOperatorAuthority } from "./admitted-run-context.js";
import {
  agentCommandFromGatewayIngress,
  compactionTestRuntime,
  compactionTestState as state,
  GATEWAY_INGRESS_ARGS,
  makeCompactionResult,
  registerAgentCommandCompactionTestHooks,
  requireCompactionStorePath,
} from "./agent-command.compaction.test-support.js";

const { loadSessionEntry, replaceSessionEntry } = compactionTestRuntime;

registerAgentCommandCompactionTestHooks();

it.each([false, true])(
  "queues image follow-up and revalidates session replacement=%s",
  async (replace) => {
    const sessionKey = "agent:main:image-roundtrip";
    const sessionId = "image-roundtrip-session";
    const target = { agentId: "main", sessionKey, storePath: requireCompactionStorePath() };
    await replaceSessionEntry(target, { sessionId, updatedAt: Date.now() });
    const completionEntered = createDeferred();
    const finishCompletion = createDeferred();
    const followupQueued = createDeferred();
    const runs: string[] = [];
    state.runAgentAttemptMock.mockImplementation(async (params) => {
      runs.push(params.runId);
      if (params.runId === "image-completion") {
        completionEntered.resolve();
        await finishCompletion.promise;
      }
      return makeCompactionResult({ sessionId, text: "A lighthouse", runner: "embedded" });
    });
    const completion = agentCommandFromGatewayIngress(
      {
        sessionKey,
        sessionId,
        runId: "image-completion",
        message: "Describe the completed image",
        allowModelOverride: false,
        internalDeliveryMediaUrls: ["/synthetic/lighthouse.png"],
        forceRestartSafeTools: true,
        disableMessageTool: true,
        sourceReplyDeliveryMode: "automatic",
        inputProvenance: { kind: "inter_session", sourceTool: "image_generate" },
        beforeTerminalDelivery: async () => {
          if (replace) {
            await replaceSessionEntry(target, {
              sessionId: "replacement-session",
              updatedAt: Date.now(),
            });
          }
        },
      },
      ...GATEWAY_INGRESS_ARGS,
    );
    let followup: Promise<unknown> | undefined;
    try {
      await awaitGateBeforeSettlement(
        completionEntered.promise,
        completion,
        "completion did not run",
      );
      expect(loadSessionEntry(target)?.restartRecoveryDeliveryRunId).toBe("image-completion");
      const beginAdmission = sessionAdmission.beginSessionWorkAdmission;
      using _ = vi
        .spyOn(sessionAdmission, "beginSessionWorkAdmission")
        .mockImplementation((params) => {
          const pending = beginAdmission(params);
          followupQueued.resolve();
          return pending;
        });
      followup = agentCommandFromGatewayIngress(
        {
          sessionKey,
          sessionId,
          runId: "inspect-image",
          message: "Describe this lighthouse attachment",
          allowModelOverride: false,
          operatorAuthority: createAdmittedRunOperatorAuthority({
            profileId: "synthetic-operator",
            scopes: ["operator.admin"],
            assertCurrent() {},
          }),
        },
        ...GATEWAY_INGRESS_ARGS,
      );
      void followup.catch(() => {});
      await awaitGateBeforeSettlement(followupQueued.promise, followup, "follow-up did not queue");
      const barrier = await beginAdmission({
        scope: target.storePath,
        identities: [sessionKey, sessionId],
        assertAllowed() {},
      });
      barrier.release();
      expect(runs).toEqual(["image-completion"]);
      finishCompletion.resolve();
      await completion;
      if (replace) {
        await expect(followup).rejects.toMatchObject({ code: "SESSION_WORK_START_CHANGED" });
        expect(runs).toEqual(["image-completion"]);
        expect(loadSessionEntry(target)?.sessionId).toBe("replacement-session");
      } else {
        await followup;
        expect(runs).toEqual(["image-completion", "inspect-image"]);
      }
      expect(loadSessionEntry(target)?.restartRecoveryDeliveryRunId).toBeUndefined();
    } finally {
      finishCompletion.resolve();
      await Promise.allSettled([completion, ...(followup ? [followup] : [])]);
    }
  },
);
