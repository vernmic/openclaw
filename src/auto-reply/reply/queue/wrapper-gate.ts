import { isCompletionReportInputProvenance } from "../../../sessions/input-provenance.js";
import type { FollowupRun } from "./types.js";

/**
 * Structural view of the followup queue the wrapper gate needs. Defined
 * locally rather than imported from state.js so the wrapper-gate <- state
 * dependency stays one-directional, and so the queue's OPTIONAL gate
 * properties satisfy these parameters directly (no Pick optionality clash).
 */
export type WrapperGateQueueView = {
  mode?: string;
  wrapperGateSettings?: WrapperGateSettings;
  wrapperGateWindow?: WrapperGateWindow;
};

/**
 * Wrapper-gate fixed collection window (slice 1 of the native wrapper build).
 *
 * One timing owner per caller session queue: the first queued subagent-source
 * delivery opens a fixed collection window that later deliveries only join.
 * The queue holds its collect drain until the window's handoff deadline; a
 * user-source delivery bypasses the remaining collection delay and clamps the
 * deadline to its own two-minute backstop. The tool gate
 * (agent-tools.before-tool-call.wrapper-gate.ts) reads the same window to warn
 * and then refuse new tool dispatches at the deadline so a busy caller reaches
 * the drain boundary. State is in-memory for this slice; persistence lands with
 * the ownership-spine slice.
 */
export type WrapperGateWindow = {
  /** Absolute ms when the first subagent-source delivery queued. */
  firstQueuedAt: number;
  /** Absolute ms when collection closed (firstQueuedAt + collectMs). */
  collectUntil: number;
  /** Absolute ms when new tool dispatch must be refused (collectUntil + warnMs). */
  forceHandoffAt: number;
  /** Absolute ms when the queue may drain the collected bundle. */
  drainAt: number;
  /** One model-visible warning per window has been attached by the tool gate. */
  warned?: boolean;
  /**
   * Absolute ms when the warning was actually delivered (#6278): the refusal
   * clock runs from warning receipt, not from input arrival.
   */
  warnedAt?: number;
};

export type WrapperGateSettings = {
  enabled?: boolean;
  collectMs?: number;
  warnMs?: number;
};

export const DEFAULT_WRAPPER_GATE_COLLECT_MS = 300_000;
export const DEFAULT_WRAPPER_GATE_WARN_MS = 120_000;

export type WrapperGateFollowupSource = "subagent" | "user" | "internal";

/**
 * s1c4 (Vern's qualifier ruling, appended to the Codex s1c2 review): classify
 * by a WHITELIST OF QUALIFIERS - positive markers for the inputs we want
 * warnings on - never by channel names or exclusion. s1c2/s1c3 classified by
 * exclusion (provenance kinds, then a channel-name fallback, then W4's
 * provenance-absent patch), each review adding a branch; the channel list
 * also missed inputs we DO want warned on (webchat, chat.send - no channel
 * origin) and needed hand maintenance. Anything matching no qualifier opens
 * no window at all.
 *
 * SUBAGENT / WRAPPER-DELIVERY qualifiers -> the fixed collection window (N20):
 *   S1: run.trustedInternalHandoff.kind === "subagent-completion"
 *   S2: the host predicate isCompletionReportInputProvenance - every
 *       agent-mediated completion in AGENT_MEDIATED_COMPLETION_SOURCE_TOOLS
 *       (subagent_announce, subagent_settle, agent_harness_task,
 *       agent_harness_completion, image_generate, music_generate,
 *       video_generate). Call the host predicate; never copy its names. When
 *       the wrapper's own delivery (N04/N06, later slices) stamps a new
 *       sourceTool, add it once to AGENT_MEDIATED_COMPLETION_SOURCE_TOOLS and
 *       this gate covers it with no gate change.
 *   S evaluates BEFORE U4: completion reports are also inter_session.
 *
 * WAITING-PARTY qualifiers -> the two-minute backstop (N07). A person or
 * another agent is waiting on this input:
 *   U1: inputProvenance.kind === "external_user" (ACP, voice transcripts)
 *   U2: currentInboundEventKind SET AT ALL (user_request OR room_event) -
 *       the host's own inbound event kind (channels/inbound-event/kind.ts).
 *       Covers channel users such as Telegram AND unmentioned group-topic
 *       traffic (production sets groupChat.unmentionedInbound 'room_event';
 *       s1c5). (Set on the followup run at get-reply-run-execute :397 from
 *       the inbound event kind.)
 *   U3: gatewayLocalUserIngress present on a USER turn - ingress present
 *       AND no provenance (the host's requester-profile guard; Codex W5:
 *       the ingress belongs to the connection, so it also rides on
 *       admin/system chat.send turns, which carry systemInputProvenance).
 *       Covers webchat, the Control UI and user chat.send turns. (Set at
 *       get-reply-run-execute :399.)
 *   U4: inputProvenance.kind === "inter_session" that is NOT a completion
 *       report under S2 (for example sessions_send) - another agent waits.
 *
 * NO QUALIFIER -> no window. Heartbeats, cron events, exec completion and
 * other system work carry internal_system and none of the markers above, so
 * W4 cannot arise. s1c3 (6ba812b15b3) is superseded by this rule.
 */
export function classifyWrapperGateFollowupSource(
  run: Pick<FollowupRun, "run" | "currentInboundEventKind" | "gatewayLocalUserIngress">,
): WrapperGateFollowupSource {
  const provenance = run.run.inputProvenance;
  // S1: delivered subagent work.
  if (run.run.trustedInternalHandoff?.kind === "subagent-completion") {
    return "subagent";
  }
  // S2 (before U4): every agent-mediated completion report opens the fixed
  // collection window via the host's own predicate.
  if (provenance !== undefined && isCompletionReportInputProvenance(provenance)) {
    return "subagent";
  }
  // U1: explicit external user (ACP, voice transcripts).
  if (provenance?.kind === "external_user") {
    return "user";
  }
  // U2 (s1c5, Claude's catch + the updated addendum): the host's
  // inbound-event kind SET AT ALL (user_request OR room_event) marks real
  // channel inbound traffic. Production runs groupChat.unmentionedInbound
  // 'room_event' (openclaw.json:803), so classifyChannelInboundEvent marks
  // every unmentioned group message room_event - which is all of the group
  // topic traffic - and requiring user_request missed all of it. Any
  // host-classified inbound kind opens the backstop; runs with NO inbound
  // kind (internal machinery, exec completion, cron) do not.
  if (
    run.currentInboundEventKind === "user_request" ||
    run.currentInboundEventKind === "room_event"
  ) {
    return "user";
  }
  // U3: local-user ingress on a USER turn. The ingress belongs to the
  // connection, not the input (chat-s...turn.ts:284-285 transfers it for
  // every client; chat-send-request.ts:148 lets admin callers send
  // systemInputProvenance), so it also rides on admin/system chat.send
  // turns (Codex W5). Use the host's own user-turn test (the requester-
  // profile guard): no system provenance. external_user is already U1.
  if (run.gatewayLocalUserIngress !== undefined && provenance === undefined) {
    return "user";
  }
  // U4: inter-session traffic that is not a completion report - another
  // agent is waiting for a reply.
  if (provenance?.kind === "inter_session") {
    return "user";
  }
  // No qualifier: no window (system work, unmarked runs). Group-topic
  // chatter is U2 since s1c5; admin/system chat.send turns are excluded
  // from U3 by the user-turn test (W5).
  return "internal";
}

export function resolveWrapperGateCollectMs(settings: WrapperGateSettings): number {
  const raw = settings.collectMs;
  return typeof raw === "number" && Number.isFinite(raw) && raw >= 0
    ? Math.floor(raw)
    : DEFAULT_WRAPPER_GATE_COLLECT_MS;
}

export function resolveWrapperGateWarnMs(settings: WrapperGateSettings): number {
  const raw = settings.warnMs;
  return typeof raw === "number" && Number.isFinite(raw) && raw >= 0
    ? Math.floor(raw)
    : DEFAULT_WRAPPER_GATE_WARN_MS;
}

export function isWrapperGateEnabledForQueue(params: {
  mode: unknown;
  settings?: WrapperGateSettings;
}): boolean {
  if (params.settings?.enabled !== true) {
    return false;
  }
  // The fixed bundle window is a collect-mode behavior: it holds one drained
  // turn for the whole bundle. Other modes keep their existing scheduling.
  return params.mode === "collect";
}

/**
 * Record one enqueue against the queue's wrapper-gate window.
 * Subagent sources open (or join) the fixed window; user sources bypass any
 * remaining collection delay and clamp the handoff deadline to their own
 * backstop. Later arrivals never extend an open window's absolute times.
 */
export function noteFollowupEnqueuedForWrapperGate(
  queue: WrapperGateQueueView,
  run: Pick<FollowupRun, "run">,
  now: number,
): void {
  if (!isWrapperGateEnabledForQueue({ mode: queue.mode, settings: queue.wrapperGateSettings })) {
    return;
  }
  const source = classifyWrapperGateFollowupSource(run);
  if (source === "internal") {
    return;
  }
  const settings = queue.wrapperGateSettings ?? {};
  const warnMs = resolveWrapperGateWarnMs(settings);
  if (source === "user") {
    const backstopAt = now + warnMs;
    const window = queue.wrapperGateWindow;
    if (window) {
      // User input preempts any remaining subagent collection delay but never
      // relaxes an earlier deadline.
      window.collectUntil = Math.min(window.collectUntil, now);
      window.drainAt = Math.min(window.drainAt, now);
      window.forceHandoffAt = Math.min(window.forceHandoffAt, backstopAt);
    } else {
      queue.wrapperGateWindow = {
        firstQueuedAt: now,
        collectUntil: now,
        forceHandoffAt: backstopAt,
        drainAt: now,
      };
    }
    return;
  }
  const existing = queue.wrapperGateWindow;
  if (existing) {
    // Fixed from first arrival: later subagent deliveries only queue.
    return;
  }
  const collectMs = resolveWrapperGateCollectMs(settings);
  const collectUntil = now + collectMs;
  const forceHandoffAt = collectUntil + warnMs;
  queue.wrapperGateWindow = {
    firstQueuedAt: now,
    collectUntil,
    forceHandoffAt,
    // The bundle drains when collection closes: an idle caller hands off at
    // collectUntil; a still-busy caller is warned, refused at the forceHandoffAt
    // deadline by the tool gate, and drains when its own turn ends. Both times
    // derive from firstQueuedAt -- still one schedule, no competing timer.
    drainAt: collectUntil,
  };
}

/**
 * Resolve the boundary the collect drain must respect before its normal
 * trailing debounce. Undefined means no window hold applies.
 */
export function resolveWrapperGateDrainBoundary(
  queue: WrapperGateQueueView,
  now: number,
): { drainAt: number } | undefined {
  if (!isWrapperGateEnabledForQueue({ mode: queue.mode, settings: queue.wrapperGateSettings })) {
    return undefined;
  }
  const window = queue.wrapperGateWindow;
  if (!window) {
    return undefined;
  }
  return { drainAt: Math.max(window.drainAt, now) };
}

/** Max poll cadence while holding for a wrapper-gate window boundary. */
const WRAPPER_GATE_HOLD_POLL_MS = 250;

/**
 * Hold the collect drain until the queue's wrapper-gate window reaches its
 * drain boundary. Wakes early when a user-source bypass pulls the boundary to
 * now, when the window disappears, or when the queue aborts. Resolves false
 * when no hold applied; true after an actual hold, letting the caller skip the
 * trailing quiet-window debounce so handoff still happens at the fixed deadline.
 */
export function waitForWrapperGateDrainBoundary(
  queue: WrapperGateQueueView,
  abortSignal?: AbortSignal,
): Promise<boolean> {
  const signal = abortSignal;
  const initial = resolveWrapperGateDrainBoundary(queue, Date.now());
  if (!initial || initial.drainAt <= Date.now() || signal?.aborted) {
    return Promise.resolve(false);
  }
  return new Promise<boolean>((resolve) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let held = false;
    const finish = () => {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      signal?.removeEventListener("abort", finish);
      resolve(held);
    };
    const check = () => {
      if (signal?.aborted) {
        finish();
        return;
      }
      const boundary = resolveWrapperGateDrainBoundary(queue, Date.now());
      if (!boundary) {
        finish();
        return;
      }
      const remaining = boundary.drainAt - Date.now();
      if (remaining <= 0) {
        finish();
        return;
      }
      held = true;
      timer = setTimeout(check, Math.min(remaining, WRAPPER_GATE_HOLD_POLL_MS));
      if (typeof timer === "object" && timer && "unref" in timer) {
        (timer as { unref: () => void }).unref();
      }
    };
    signal?.addEventListener("abort", finish, { once: true });
    check();
  });
}
