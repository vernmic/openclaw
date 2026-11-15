import { copyInternalToolResultState } from "../../packages/agent-core/src/internal-hooks.js";
import { completionRequiresMessageToolDelivery } from "../auto-reply/reply/completion-delivery-policy.js";
import {
  resolveWrapperGateWarnMs,
  type WrapperGateWindow,
} from "../auto-reply/reply/queue/wrapper-gate.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { getExistingSessionControllerMailbox } from "../sessions/session-controller.mailbox.js";
import type { AgentToolResult } from "./runtime/index.js";

/**
 * Tool-dispatch side of the wrapper gate (slice 1).
 *
 * Reads the caller session's fixed collection window from the shared followup
 * queue registry (same Symbol-keyed map the queue owner uses, so one process
 * observes one window per session) and decides, per tool call:
 *   - pass while the window is still collecting;
 *   - one model-visible warning per window during the warning phase;
 *   - refusal of NEW dispatches at forceHandoffAt so the busy caller reaches
 *     the drain boundary (in-flight execution is never cancelled here);
 *   - never a cold refusal: the deadline is enforced only after the warning
 *     has been delivered — the notice clock runs from warning receipt
 *     (#6278), not from input arrival;
 *   - the warning names what is waiting, and every passing dispatch after it
 *     carries a live tool-denial countdown (#6294); the host's turn-ending
 *     and visible-reply tools are never blocked (#6295, D2).
 * Default OFF: no config flag or no window means pass with zero behavior
 * change. SEEN for the drained bundle is the collected turn's admission.
 */
type WrapperGateQueueSnapshot = {
  wrapperGateWindow?: WrapperGateWindow;
  /**
   * Waiting followups, oldest first (#6294): the warning names what is queued
   * instead of burying it. Structurally typed - the queue owner's FollowupRun
   * satisfies this without importing the full queue type here.
   */
  items?: Array<{ prompt?: string }>;
};

// Upstream keeps each session's followup queue on its session-controller mailbox (#175837);
// the gate reads the same mailbox the queue owner writes.
const readQueueSnapshot = (sessionKey: string): WrapperGateQueueSnapshot | undefined =>
  getExistingSessionControllerMailbox(sessionKey);

export type WrapperGateToolDecision =
  | { action: "pass"; note?: string }
  | { action: "warn"; note: string; commit: (deliveredAt: number) => boolean }
  | { action: "refuse"; reason: string };

export function isWrapperGateEnabled(config: OpenClawConfig | undefined): boolean {
  return config?.messages?.queue?.wrapperGate?.enabled === true;
}

const WRAPPER_GATE_WARNING_BASE =
  "Wrapper gate: new input is queued for this session and its collection window has closed. " +
  "Finish the current step and wrap up this turn; do not start new multi-step work. " +
  "The queued bundle will be admitted when this turn ends.";

/**
 * #6294 (Vern, 2026-11-04): the warning says WHAT is waiting, not just that
 * something is queued. Names the waiting count and a short preview of the
 * oldest queued input so the model can see the cost of continuing.
 */
function wrapperGateWaitingNote(queue: WrapperGateQueueSnapshot | undefined): string {
  const prompts = (queue?.items ?? [])
    .map((item) => (typeof item.prompt === "string" ? item.prompt.trim() : ""))
    .filter((prompt) => prompt.length > 0);
  const oldest = prompts[0];
  if (oldest === undefined) {
    return WRAPPER_GATE_WARNING_BASE;
  }
  const preview = oldest.replace(/\s+/g, " ").slice(0, 140);
  const ellipsis = oldest.length > 140 ? "..." : "";
  const count = prompts.length > 1 ? " (" + prompts.length + " messages waiting; first shown)" : "";
  return WRAPPER_GATE_WARNING_BASE + " Waiting" + count + ': "' + preview + ellipsis + '"';
}

const WRAPPER_GATE_REFUSAL_REASON =
  "Wrapper gate: the queued-input handoff deadline has been reached. " +
  "This tool dispatch is refused; end this turn now so the queued messages can be admitted.";

/**
 * D2: true when the dispatching tool is this session's visible-reply tool.
 * Named from the host's own reply-route contract: group desks configured with
 * visibleReplies "message_tool" resolve through resolveSourceReplyDeliveryMode
 * to "message_tool_only" — for such sessions the message tool IS the reply
 * channel. The name comparison matches the host's own identification pattern
 * (agent-tools.ts compares the normalized tool name to "message"); callers
 * pass the already-normalized name.
 */
function isWrapperGateVisibleReplyTool(params: {
  config?: OpenClawConfig;
  sessionKey?: string;
  toolName?: string;
}): boolean {
  if (params.toolName !== "message" || !params.config || !params.sessionKey?.trim()) {
    return false;
  }
  return completionRequiresMessageToolDelivery({
    cfg: params.config,
    requesterSessionKey: params.sessionKey,
  });
}

/** Resolve the wrapper-gate decision for one new tool dispatch. */
export function resolveWrapperGateToolDecision(params: {
  config?: OpenClawConfig;
  sessionKey?: string;
  now: number;
  /** Already-normalized name of the dispatching tool (D2 reply exemption). */
  toolName?: string;
}): WrapperGateToolDecision {
  if (!isWrapperGateEnabled(params.config) || !params.sessionKey?.trim()) {
    return { action: "pass" };
  }
  const queue = readQueueSnapshot(params.sessionKey.trim());
  const window = queue?.wrapperGateWindow;
  if (!window) {
    return { action: "pass" };
  }
  const warnMs = resolveWrapperGateWarnMs(params.config?.messages?.queue?.wrapperGate ?? {});
  // The deadline the model is told about (#6294): the arrival deadline, or
  // warning receipt plus the notice grace, whichever is later.
  const refusalDeadline =
    window.warnedAt !== undefined
      ? Math.max(window.forceHandoffAt, window.warnedAt + warnMs)
      : window.forceHandoffAt;
  const secondsLeft = Math.max(0, Math.ceil((refusalDeadline - params.now) / 1000));
  // #6294 (Vern, 2026-11-04): every tool response after the warning carries a
  // live countdown so the model always knows the cost of continuing and the
  // way out.
  const countdownNote =
    "[tool_denial countdown " + secondsLeft + "s - end turn and ingest queued traffic]";
  const warningDecision = (): WrapperGateToolDecision => ({
    action: "warn",
    note: wrapperGateWaitingNote(queue),
    // N2 (Codex final review, 2026-11-04): the caller passes the time the warning is actually
    // appended to the successful result. params.now was sampled before policy awaits and tool
    // execution, so a tool slower than warnMs used to spend the whole notice grace first.
    commit: (deliveredAt: number) => {
      if (window.warned) {
        return false;
      }
      window.warned = true;
      // #6278 (Vern, 2026-11-03): the refusal clock starts when the warning
      // is actually delivered to the model, not when the input arrived.
      window.warnedAt = deliveredAt;
      return true;
    },
  });
  if (params.now >= window.forceHandoffAt) {
    if (isWrapperGateVisibleReplyTool(params)) {
      // D2 (live defect, 2026-11-03): a message-tool-only session's visible
      // reply IS the message tool (the host's own reply-route contract —
      // resolveSourceReplyDeliveryMode "message_tool_only"). Refusing it at
      // the deadline mutes the desk at the exact moment it must wrap up, so
      // the refusal defeats its own handoff: the wrap-up reply can never be
      // sent. The visible-reply tool passes; every other dispatch refuses.
      return { action: "pass" };
    }
    if (params.toolName === "sessions_yield") {
      // #6295 (Vern, 2026-11-03): the refusal tells the model to end its
      // turn; the gate must never block the call that does exactly that.
      return { action: "pass" };
    }
    // Cold-refusal rule (#6278, Vern 2026-11-03): the deadline must not be
    // enforced before the model has SEEN the warning. If the warning has not
    // been delivered yet, this dispatch carries it and starts the notice
    // clock; only dispatches past warnedAt + warnMs are refused.
    if (window.warnedAt !== undefined && params.now < window.warnedAt + warnMs) {
      // Notice grace: the warning was delivered at warnedAt; the refusal
      // clock runs from its receipt. The countdown rides every passing
      // dispatch (#6294).
      return { action: "pass", note: countdownNote };
    }
    if (window.warned) {
      return { action: "refuse", reason: WRAPPER_GATE_REFUSAL_REASON };
    }
    // First gate contact already past the deadline: deliver the warning
    // instead of refusing the caller cold.
    return warningDecision();
  }
  if (params.now >= window.collectUntil) {
    if (window.warned) {
      return { action: "pass", note: countdownNote };
    }
    // Warned is committed only where the note is actually appended (the
    // wrapper's success return). A call blocked by policy or approval after
    // this decision leaves the window un-warned so a later call can still
    // surface the note instead of refusing the caller cold at the deadline.
    // W2 (Codex final review): the claim is an atomic compare-and-set AT THE
    // APPEND POINT - concurrent warning-phase calls each hold a warn decision,
    // but only the first to complete claims it; the loser ships without the
    // note. Blocked or failed calls never reach commit, so they never spend it.
    return warningDecision();
  }
  return { action: "pass" };
}

/**
 * Attach the wrapper-gate warning note to a completed tool result. Mirrors
 * appendToolLoopWarning: the warned result keeps the source result's internal
 * state (provenance, acknowledgement, batch lifecycle) via
 * copyInternalToolResultState, and loop hashes and diagnostics stay on the
 * raw outcome upstream.
 */
export function appendWrapperGateWarning<T extends AgentToolResult<unknown>>(
  result: T,
  note: string,
): T {
  return copyInternalToolResultState(result, {
    ...result,
    content: [
      // Match transcript normalization for tools that omit display content.
      ...(result.content ?? []),
      { type: "text" as const, text: note },
    ],
  });
}

/**
 * D4 (live defect, 2026-11-04): in code mode every real tool call is a
 * NESTED call inside the outer exec script. Nested warnings and refusals
 * were invisible to the model when the script swallowed them, and the outer
 * exec itself ran past the deadline unrefused (call 26 started 10:47:25.8
 * after the 10:47:15 deadline). This boundary applies the gate to the OUTER
 * code-mode exec dispatch: refuse past the deadline, pass otherwise - the
 * nested calls inside the script deliver the warning themselves.
 */
export function resolveWrapperGateCodeModeBoundary(params: {
  config?: OpenClawConfig;
  sessionKey?: string;
  now: number;
}): { action: "pass" } | { action: "refuse"; reason: string } {
  const decision = resolveWrapperGateToolDecision({
    ...params,
    toolName: CODE_MODE_BOUNDARY_TOOL_NAME,
  });
  if (decision.action === "refuse") {
    return { action: "refuse", reason: decision.reason };
  }
  return { action: "pass" };
}

/**
 * The outer exec tool name for the wrapper-gate boundary check. Not the
 * visible-reply or turn-end tool, so no exemption applies - which is the
 * point: the outer code-mode dispatch is a work dispatch.
 */
const CODE_MODE_BOUNDARY_TOOL_NAME = "exec";

/**
 * D4: after an outer code-mode exec completes, surface a warning that a
 * nested call inside it delivered - the script may have swallowed the
 * nested result. Appends once per delivering exec: a later exec (started
 * after warnedAt) gets nothing, so the note cannot repeat.
 */
export function wrapperGatePostExecNote(params: {
  config?: OpenClawConfig;
  sessionKey?: string;
  execStartedAt: number;
  now: number;
}): string | undefined {
  if (!isWrapperGateEnabled(params.config) || !params.sessionKey?.trim()) {
    return undefined;
  }
  const queue = readQueueSnapshot(params.sessionKey.trim());
  const window = queue?.wrapperGateWindow;
  if (!window?.warned || window.warnedAt === undefined) {
    return undefined;
  }
  // Delivered before this exec started: an earlier exec already surfaced it.
  if (window.warnedAt < params.execStartedAt) {
    return undefined;
  }
  const warnMs = resolveWrapperGateWarnMs(params.config?.messages?.queue?.wrapperGate ?? {});
  const deadline = Math.max(window.forceHandoffAt, window.warnedAt + warnMs);
  const secondsLeft = Math.max(0, Math.ceil((deadline - params.now) / 1000));
  return (
    wrapperGateWaitingNote(queue) +
    " [tool_denial countdown " +
    secondsLeft +
    "s - end turn and ingest queued traffic]"
  );
}
