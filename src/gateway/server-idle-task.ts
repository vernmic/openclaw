import {
  getGatewayRestartDrainSignal,
  isGatewayRestartDrainError,
  tryBeginGatewayIndependentRootWorkAdmission,
} from "../process/gateway-work-admission.js";

export type GatewayIdleTaskHandle = {
  stop: () => void | Promise<void>;
};

/**
 * Schedules one low-priority task, retrying until the gateway has no active request roots.
 *
 * Pass `repeatDelayMs` to make it RECUR instead. Without it the task is one-shot by construction:
 * the initial `schedule(delayMs)` arms a single attempt, `schedule(retryDelayMs)` re-arms only while
 * busy, and nothing re-arms after a run completes. A caller that wants an hourly pass and supplies
 * only `delayMs: 3_600_000` gets one pass per process lifetime -- which is exactly what happened to
 * the agent-db snapshot sidecar (2026-10-06: one certification burst per gateway start, while the
 * databases it watches changed every hour).
 */
export function scheduleGatewayIdleTask(params: {
  delayMs: number;
  retryDelayMs: number;
  /** Set to re-arm after each completed run. Omit to keep one-shot semantics. */
  repeatDelayMs?: number;
  isClosing: () => boolean;
  isBusy: () => boolean;
  run: () => Promise<void>;
  log: { warn: (message: string) => void };
  errorMessage: string;
}): GatewayIdleTaskHandle {
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let running: Promise<void> | undefined;
  const isClosing = () => stopped || params.isClosing() || getGatewayRestartDrainSignal().aborted;
  const run = async () => {
    if (isClosing()) {
      return;
    }
    // Newly admitted request work takes priority over maintenance. The busy branch returns before
    // the re-arm below, because the retry IS the next attempt -- arming both would leave two timers
    // with only the later one tracked by `timer`, so stop() would miss one.
    if (params.isBusy()) {
      schedule(params.retryDelayMs);
      return;
    }
    try {
      await params.run();
    } finally {
      // Re-arm on failure as well as success, deliberately. Re-arming only on success means one
      // transient throw silently ends the recurrence forever -- the same class of defect the
      // repeatDelayMs option exists to fix. A persistently failing task logs every interval, which
      // is noisy and visible, rather than quiet and dead.
      if (params.repeatDelayMs !== undefined) {
        schedule(params.repeatDelayMs);
      }
    }
  };
  const schedule = (delayMs: number) => {
    if (isClosing()) {
      return;
    }
    timer = setTimeout(() => {
      timer = null;
      if (isClosing()) {
        return;
      }
      // Optional work retries admission instead of waiting behind a suspend fence
      // that shutdown may never reopen.
      const admission = params.isBusy()
        ? null
        : tryBeginGatewayIndependentRootWorkAdmission("idle-task");
      if (!admission) {
        schedule(params.retryDelayMs);
        return;
      }
      // Publish the join before callbacks can synchronously initiate shutdown.
      running = Promise.resolve()
        .then(() => admission.run(run))
        .catch((error: unknown) => {
          if (!isGatewayRestartDrainError(error)) {
            params.log.warn(`${params.errorMessage}: ${String(error)}`);
          }
        })
        .finally(() => {
          admission.release();
          running = undefined;
        });
    }, delayMs);
    timer.unref?.();
  };
  schedule(params.delayMs);
  return {
    stop: () => {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      return running;
    },
  };
}
