// Implements session abort commands and active-run stop targeting.
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { resolveSessionAgentId } from "../../agents/agent-scope.js";
import {
  captureExecRequestSubagentSelection,
  resolveSubagentController,
} from "../../agents/subagents/registry/subagent-control-scope.js";
import { killAllControlledSubagentRuns } from "../../agents/subagents/registry/subagent-control.js";
import { readChannelContextGatewayContextResolver } from "../../channels/message-access/admission-evidence.js";
import { resolveSessionStorePathCore, type SessionEntry } from "../../config/sessions.js";
import { getPluginRuntimeGatewayRequestScope } from "../../plugins/runtime/gateway-request-scope.js";
import { resolveAbortCutoffFromContext, shouldPersistAbortCutoff } from "./abort-cutoff.js";
import {
  abortSessionRunTargetWithOutcome,
  captureChannelSessionStop,
  stopSubagentsForRequester,
} from "./abort-operation.js";
import { completeAbortRetirements } from "./abort-retirements.js";
import { isAbortTrigger } from "./abort-trigger-text.js";
import { formatAbortReplyText } from "./abort.js";
import { commandReply, defineAuthorizedTextCommand } from "./command-gates.js";
import {
  persistAbortTargetEntry,
  resolveCommandSessionEntryForKey,
} from "./commands-session-store.js";
import type { CommandHandler } from "./commands-types.js";

type AbortTarget = {
  agentId: string;
  entry?: SessionEntry;
  key?: string;
  sessionId?: string;
};

function resolveAbortTarget(params: Parameters<CommandHandler>[0]): AbortTarget {
  const targetSessionKey =
    normalizeOptionalString(params.ctx.CommandTargetSessionKey) || params.sessionKey;
  const resolved = resolveCommandSessionEntryForKey(params.sessionStore, targetSessionKey);
  const entry =
    resolved.entry ??
    (targetSessionKey && targetSessionKey === params.sessionKey ? params.sessionEntry : undefined);
  const key = resolved.key ?? targetSessionKey;
  const agentId = resolveSessionAgentId({
    config: params.cfg,
    sessionKey: key,
    fallbackAgentId: params.agentId,
  });
  return {
    agentId,
    entry: entry ? { ...entry } : undefined,
    key,
    sessionId: entry?.sessionId,
  };
}

function captureAbortTarget(params: Parameters<CommandHandler>[0], abortTarget: AbortTarget) {
  const agentId = resolveSessionAgentId({
    config: params.cfg,
    sessionKey: abortTarget.key ?? params.sessionKey ?? "",
    fallbackAgentId: params.agentId,
  });
  return captureChannelSessionStop({
    key: abortTarget.key,
    sessionId: abortTarget.sessionId,
    agentId,
    resolveGatewayContext:
      readChannelContextGatewayContextResolver(params.rootCtx ?? params.ctx) ??
      getPluginRuntimeGatewayRequestScope()?.resolveGatewayContext,
    storePath:
      params.storePath ?? resolveSessionStorePathCore(params.cfg.session?.store, { agentId }),
  });
}

async function executeChannelUserStop(
  params: Parameters<CommandHandler>[0],
  source: "channel-user" | "talk" = "channel-user",
) {
  const abortTarget = resolveAbortTarget(params);
  const capture = captureAbortTarget(params, abortTarget);
  abortTarget.sessionId = capture.sessionId;
  const retirements: Promise<void>[] = [];
  const assertCurrent = () => {
    if (params.opts?.isCommandTargetCurrent?.sessionSource) {
      params.opts.isCommandTargetCurrent.sessionSource();
      return;
    }
    if (params.opts?.isCommandTargetCurrent?.() === false) {
      throw new Error("The selected session changed before it could be stopped.");
    }
  };
  const abortCutoff = shouldPersistAbortCutoff({
    commandSessionKey: params.sessionKey,
    targetSessionKey: abortTarget.key,
  })
    ? resolveAbortCutoffFromContext(params.ctx)
    : undefined;
  const current = params.opts?.isCommandTargetCurrent?.();
  if ((typeof current === "boolean" || current === undefined ? current : await current) === false) {
    throw new Error("The selected session changed before it could be stopped.");
  }
  const stop = abortSessionRunTargetWithOutcome({
    source,
    capture,
    retirements,
    assertCurrent,
    hookContext: {
      sessionKey: abortTarget.key ?? params.sessionKey ?? "",
      sessionEntry: abortTarget.entry,
      sessionId: abortTarget.sessionId,
      commandSource: params.command.surface,
      senderId: params.command.senderId,
    },
    messageIdentity: abortCutoff,
    recordAbortTarget: async ({ recordCutoff }) => {
      assertCurrent();
      await persistAbortTargetEntry({
        isCurrent: params.opts?.isCommandTargetCurrent,
        entry: abortTarget.entry,
        key: abortTarget.key,
        sessionStore: params.sessionStore,
        storePath: params.storePath,
        abortCutoff: recordCutoff ? abortCutoff : undefined,
      });
    },
    // The controller invokes this adapter only for sources whose policy stops children.
    stopChildren: (applyParentStop) =>
      stopSubagentsForRequester({
        cfg: params.cfg,
        requesterSessionKey: abortTarget.key ?? params.sessionKey,
        requesterAgentId: params.agentId,
        requesterSession:
          abortTarget.entry && abortTarget.sessionId
            ? {
                storePath: resolveSessionStorePathCore(params.cfg.session?.store, {
                  agentId: abortTarget.agentId,
                }),
                sessionId: abortTarget.sessionId,
                lifecycleRevision: abortTarget.entry.lifecycleRevision ?? null,
              }
            : undefined,
        assertCurrent,
        beforeKill: async (sealRootSelection) => {
          sealRootSelection();
          return await applyParentStop();
        },
      }),
  });
  return await completeAbortRetirements(async () => {
    const outcome = await stop.completed;
    return commandReply(
      formatAbortReplyText(
        outcome.childrenStopped,
        outcome.alreadyFinalizing ? "finalizing" : undefined,
        outcome.childFailures,
        outcome.localModelMayStillBeFinishing,
      ),
    );
  }, retirements);
}

export const handleStopCommand: CommandHandler = defineAuthorizedTextCommand(
  { label: "/stop", match: (body) => (body === "/stop" ? true : null) },
  (params) => executeChannelUserStop(params),
);

async function executeNextCommand(params: Parameters<CommandHandler>[0]) {
  const abortTarget = resolveAbortTarget(params);
  const capture = captureAbortTarget(params, abortTarget);
  abortTarget.sessionId = capture.sessionId;
  const retirements: Promise<void>[] = [];
  const assertCurrent = () => {
    if (params.opts?.isCommandTargetCurrent?.sessionSource) {
      params.opts.isCommandTargetCurrent.sessionSource();
      return;
    }
    if (params.opts?.isCommandTargetCurrent?.() === false) {
      throw new Error("The selected session changed before it could be stopped.");
    }
  };
  const stop = abortSessionRunTargetWithOutcome({
    source: "talk",
    capture,
    assertCurrent,
    retirements,
    hookContext: {
      sessionKey: abortTarget.key ?? params.sessionKey ?? "",
      sessionEntry: abortTarget.entry,
      sessionId: abortTarget.sessionId,
      commandSource: params.command.surface,
      senderId: params.command.senderId,
    },
  });
  const outcome = await completeAbortRetirements(async () => await stop.completed, retirements);
  if (outcome.alreadyFinalizing) {
    return commandReply("Agent reply is already finalizing and can no longer be interrupted.");
  }
  let killed = 0;
  const runId = [...capture.requestRunIds][0];
  if (runId) {
    const controller = resolveSubagentController({
      cfg: params.cfg,
      agentSessionKey: abortTarget.key ?? params.sessionKey,
      agentId: abortTarget.agentId,
    });
    const selection = captureExecRequestSubagentSelection({
      cfg: params.cfg,
      controller,
      requesterTurnRunId: runId,
      owners: [],
    });
    if (selection.runs.length > 0) {
      const result = await killAllControlledSubagentRuns({
        cfg: params.cfg,
        controller,
        runs: selection.runs,
        requestSelection: selection,
        assertCurrent,
      });
      killed = "killed" in result ? result.killed : 0;
    }
  }
  const queued = capture.controller.queuedInputs.length;
  if (!outcome.aborted && queued === 0) {
    return commandReply("No active turn to end. Agent is idle, nothing queued.");
  }
  const suffix = killed > 0 ? ` Stopped ${killed} sub-agent${killed === 1 ? "" : "s"}.` : "";
  return commandReply(
    queued > 0
      ? `⚙️ Turn ended. Busy again with ${queued} queued items.${suffix}`
      : `⚙️ Turn ended. Agent is idle, nothing queued.${suffix}`,
  );
}

export const handleNextCommand: CommandHandler = defineAuthorizedTextCommand(
  {
    label: "/next",
    match: (body) => (body === "/next" || body === "/interrupt" ? true : null),
  },
  executeNextCommand,
);

export const handleAbortTrigger: CommandHandler = defineAuthorizedTextCommand(
  {
    label: "abort trigger",
    match: (_body, params) => (isAbortTrigger(params.command.rawBodyNormalized) ? true : null),
  },
  (params) => executeChannelUserStop(params),
);
