// Operator-requested gateway stop, delivered as an in-process signal.
import { createSubsystemLogger } from "../logging/subsystem.js";

const stopLog = createSubsystemLogger("gateway");

export type GatewayStopEmitResult = {
  status: "emitted" | "unavailable";
};

/**
 * Requests the graceful interactive stop that the run loop already implements
 * for SIGINT: a short delivery grace with admission open, then fence, abort and
 * drain.
 *
 * Why a synthetic signal instead of new lifecycle plumbing: the run loop owns
 * exactly one accepted-request path, and its SIGINT branch is the graduated stop
 * an operator wants. Emitting the signal in-process reuses that path verbatim -
 * including the escalation, so a second request cuts the delivery grace short
 * exactly like a second Ctrl-C. This mirrors how a restart is requested
 * (`process.emit("SIGUSR1")` in restart.ts).
 *
 * This exists because on Windows there is otherwise NO graceful stop:
 * `openclaw gateway stop` is service-manager-only and refuses a non-default
 * OPENCLAW_HOME, SIGBREAK has no handler, and `process.kill(pid, "SIGTERM")`
 * from another process is TerminateProcess - a hard kill with no drain.
 *
 * The emit is deferred to the next macrotask so the caller's response frame
 * flushes before the gateway begins tearing down its own websocket server.
 */
export function requestGatewayStopWithSignalAdmission(reason?: string): GatewayStopEmitResult {
  if (process.listenerCount("SIGINT") === 0) {
    // No run loop owns this process: emitting would be swallowed, and reporting
    // success would tell an operator the gateway is stopping when it is not.
    return { status: "unavailable" };
  }
  stopLog.info(`stop requested${reason ? `: ${reason}` : ""}`);
  setImmediate(() => {
    process.emit("SIGINT");
  });
  return { status: "emitted" };
}
