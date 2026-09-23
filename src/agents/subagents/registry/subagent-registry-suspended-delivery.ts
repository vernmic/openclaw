import { isDeliverySuspended } from "./subagent-delivery-state.js";
import {
  SUBAGENT_ENDED_REASON_COMPLETE,
  type SubagentLifecycleEndedReason,
} from "./subagent-lifecycle-events.js";
import { shouldSuppressSubagentRecoverySessionEffects } from "./subagent-recovery-state.js";
import { safeRemoveAttachmentsDir } from "./subagent-registry-helpers.js";
import type { SubagentLifecycleController } from "./subagent-registry-lifecycle.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

const SUBAGENT_SUSPENDED_DELIVERY_RETENTION_MS = 7 * 24 * 60 * 60_000;
// A delivery suspended as a PERMANENT failure is never retried: the sink is
// gone, not busy. Holding one for the full week only feeds the pressure cap -
// 27 such entries (all "sink_unavailable"/"delivery path no longer exists",
// created inside one incident window on 2026-09-22) warned once a minute for
// hours and sat 23 short of the hard cap that blocks subagent admission. Keep
// them long enough to be read during an incident, not long enough to BE one:
// six hours, so a next-morning post-mortem still sees an overnight cluster.
const SUBAGENT_PERMANENT_FAILURE_RETENTION_MS = 6 * 60 * 60_000;
export const SUBAGENT_SUSPENDED_DELIVERY_WARNING_COUNT = 25;
export const SUBAGENT_SUSPENDED_DELIVERY_HARD_CAP = 50;

export function isSuspendedPendingFinalDelivery(entry: SubagentRunRecord): boolean {
  return typeof entry.execution.endedAt === "number" && isDeliverySuspended(entry);
}

export function resolveSuspendedDeliveryExpiryMs(entry?: SubagentRunRecord): number {
  // Two different fields carry "this will never deliver", and the live store
  // proves they disagree: all 29 stuck rows on 2026-09-23 read
  // disposition="permanent_failure" with suspendedReason="expiry" (attempts
  // exhausted). `disposition` is the delivery OUTCOME and the marker the rest
  // of the registry treats as unrecoverable (subagent-delivery-state.ts:155);
  // `suspendedReason` is only why the run was parked. Keying on suspendedReason
  // alone matched nothing in production.
  const delivery = entry?.delivery;
  const permanentlyFailed =
    delivery?.disposition === "permanent_failure" ||
    delivery?.suspendedReason === "permanent_failure";
  return permanentlyFailed
    ? SUBAGENT_PERMANENT_FAILURE_RETENTION_MS
    : SUBAGENT_SUSPENDED_DELIVERY_RETENTION_MS;
}

export async function discardSuspendedPendingFinalDelivery(params: {
  runId: string;
  entry: SubagentRunRecord;
  now: number;
  reason: "expired";
  resumedRuns: Set<string>;
  clearPendingLifecycleError: (runId: string) => void;
  clearPendingLifecycleTimeout: (runId: string) => void;
  discardTerminalDelivery: typeof SubagentLifecycleController.discardTerminalDelivery;
  completeCleanupBookkeeping: (params: {
    runId: string;
    entry: SubagentRunRecord;
    cleanup: "delete" | "keep";
    completedAt: number;
    skipRequesterSettleWake: true;
  }) => void;
  shouldEmitEndedHookForRun: (params: {
    entry: SubagentRunRecord;
    reason: SubagentLifecycleEndedReason;
  }) => boolean;
  emitSubagentEndedHookForRun: (params: {
    entry: SubagentRunRecord;
    reason: SubagentLifecycleEndedReason;
    sendFarewell: true;
  }) => Promise<void>;
  warn: (message: string, meta?: Record<string, unknown>) => void;
}): Promise<void> {
  const { runId, entry, now, reason, resumedRuns } = params;
  const snapshot = structuredClone(entry);
  const wasResumed = resumedRuns.has(runId);
  params.discardTerminalDelivery(entry, now, reason);
  const suppressSessionEffects = shouldSuppressSubagentRecoverySessionEffects(entry);
  const completionReason = entry.endedReason ?? SUBAGENT_ENDED_REASON_COMPLETE;
  try {
    params.completeCleanupBookkeeping({
      runId,
      entry,
      cleanup: entry.cleanup,
      completedAt: now,
      skipRequesterSettleWake: true,
    });
  } catch (error) {
    for (const key of Object.keys(entry)) {
      Reflect.deleteProperty(entry, key);
    }
    Object.assign(entry, snapshot);
    if (wasResumed) {
      resumedRuns.add(runId);
    }
    throw error;
  }
  resumedRuns.delete(runId);
  params.clearPendingLifecycleError(runId);
  params.clearPendingLifecycleTimeout(runId);
  params.warn("subagent suspended delivery discarded", {
    reason,
    runId: entry.runId,
    childSessionKey: entry.childSessionKey,
    requesterSessionKey: entry.requesterSessionKey,
  });
  if (entry.cleanup === "delete" || !entry.retainAttachmentsOnKeep) {
    await safeRemoveAttachmentsDir(entry);
  }
  if (
    !suppressSessionEffects &&
    entry.expectsCompletionMessage === true &&
    params.shouldEmitEndedHookForRun({ entry, reason: completionReason })
  ) {
    await params.emitSubagentEndedHookForRun({
      entry,
      reason: completionReason,
      sendFarewell: true,
    });
  }
}
