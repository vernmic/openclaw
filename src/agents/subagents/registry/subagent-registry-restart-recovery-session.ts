import { getRuntimeConfig } from "../../../config/config.js";
import {
  resolveAgentIdFromSessionKey,
  resolveSessionStorePathCore,
} from "../../../config/sessions.js";
import { applySessionEntryExactReplacements } from "../../../config/sessions/session-accessor.sqlite-replacement-projection.js";
import { captureSessionEntryCurrentRead } from "../../../config/sessions/session-entry-current-runtime.js";
import { withSessionEntryReadOnlyInWorker } from "../../../config/sessions/session-entry-read-runtime.js";
import type { InternalSessionEntry } from "../../../config/sessions/types.js";
import { listAgentRunsForSession } from "../../../infra/agent-run-registry.js";
import {
  getSessionWorkAdmissionRelease,
  isSessionWorkAdmissionActive,
} from "../../../sessions/session-lifecycle-admission.js";
import {
  isRetiredSubagentExecution,
  isRetiredSubagentSessionOwner,
} from "./subagent-registry-restart-recovery-helpers.js";
import type { RestartRecoveryResult } from "./subagent-registry-restart-recovery-types.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

// Capture only identity, never the session's saved prompts or the recovery stack.
function retainSessionOwner(storePath: string, sessionKey: string, sessionId: string | undefined) {
  const isCurrent = () =>
    listAgentRunsForSession({ sessionKey, sessionId }).length > 0 ||
    isSessionWorkAdmissionActive(storePath, [sessionKey, sessionId]);
  return isCurrent()
    ? {
        isCurrent,
        released: getSessionWorkAdmissionRelease({
          scope: storePath,
          identities: [sessionKey, sessionId],
        }),
      }
    : undefined;
}

export async function loadSubagentRecoverySession(params: {
  entry: SubagentRunRecord;
  isOwnerCurrent: () => boolean;
}): Promise<{
  agentId: string;
  storePath: string;
  sessionEntry: InternalSessionEntry | undefined;
  currentRead: ReturnType<typeof captureSessionEntryCurrentRead>;
  retained?: Extract<RestartRecoveryResult, { status: "handled" }>["retained"];
  replacementReason?: string;
} | null> {
  const sessionKey = params.entry.childSessionKey.trim();
  const agentId = resolveAgentIdFromSessionKey(sessionKey);
  const storePath = resolveSessionStorePathCore(getRuntimeConfig().session?.store, { agentId });
  const scope = { storePath, sessionKey, projection: "list" as const };
  const { sessionEntry, currentRead } = await withSessionEntryReadOnlyInWorker(
    scope,
    () => {
      if (!params.isOwnerCurrent()) {
        throw new Error("subagent recovery owner changed during session read");
      }
    },
    async (read, owner) => {
      if (!read.ok) {
        throw read.error;
      }
      return {
        sessionEntry: read.value,
        currentRead: captureSessionEntryCurrentRead(scope, owner),
      };
    },
  );
  // Fork adaptation of #170513 without #167966's recorded-owner contract: the incarnation
  // recorded at launch identifies a replaced child, the same rule as upstream's
  // "original session incarnation changed" mismatch. A replacement session cannot own
  // effects for the retained task, but the registry can still settle that task and notify
  // its original requester.
  const original = params.entry.childSessionIdentity;
  if (
    sessionEntry &&
    ((original?.sessionId && original.sessionId !== sessionEntry.sessionId) ||
      (original?.lifecycleRevision && original.lifecycleRevision !== sessionEntry.lifecycleRevision))
  ) {
    return {
      agentId,
      storePath,
      sessionEntry,
      currentRead,
      replacementReason:
        "Cannot operate on this subagent because its original session incarnation changed. " +
        "No child work was changed. Inspect the retained record and original execution evidence before retrying.",
    };
  }
  const retained = retainSessionOwner(storePath, sessionKey, sessionEntry?.sessionId);
  if (
    retained ||
    params.entry.execution.restartRecovery ||
    sessionEntry?.abortedLastRun === true ||
    !isRetiredSubagentSessionOwner(params.entry, sessionEntry)
  ) {
    return { agentId, storePath, sessionEntry, currentRead, retained };
  }
  const { sessionId, lifecycleRevision, updatedAt } = sessionEntry;
  const target = { sessionKey, sessionId };
  const isCurrent = () =>
    params.isOwnerCurrent() &&
    isRetiredSubagentExecution(params.entry) &&
    listAgentRunsForSession(target).length === 0 &&
    !isSessionWorkAdmissionActive(storePath, [sessionKey, sessionId]);
  const interrupted = await applySessionEntryExactReplacements<InternalSessionEntry | null>({
    agentId,
    storePath,
    sessionKeys: [sessionKey],
    update: (entries) => {
      const current = entries[0]?.entry;
      if (
        !isCurrent() ||
        !current ||
        current.sessionId !== sessionId ||
        current.lifecycleRevision !== lifecycleRevision ||
        current.updatedAt !== updatedAt ||
        !isRetiredSubagentSessionOwner(params.entry, current)
      ) {
        return { result: null };
      }
      // Keep the last observed timestamp: restart must not make an old orphan fresh.
      const interruptedEntry = { ...current, abortedLastRun: true };
      return { result: interruptedEntry, replacements: [{ sessionKey, entry: interruptedEntry }] };
    },
    assertCommitAllowed: () => {
      currentRead.assertSourceCurrent();
      if (!isCurrent()) {
        throw new Error("subagent orphan ownership changed before interruption commit");
      }
    },
    skipMaintenance: true,
  });
  return interrupted ? { agentId, storePath, sessionEntry: interrupted, currentRead } : null;
}
