import { afterEach, describe, expect, it, vi } from "vitest";
import { requestGatewayStopWithSignalAdmission } from "./gateway-stop.js";

const listeners: Array<() => void> = [];

function addSigintListener(handler: () => void): void {
  listeners.push(handler);
  process.on("SIGINT", handler);
}

afterEach(() => {
  for (const handler of listeners.splice(0)) {
    process.removeListener("SIGINT", handler);
  }
  vi.useRealTimers();
});

describe("requestGatewayStopWithSignalAdmission", () => {
  it("reports unavailable when no run loop owns the process", () => {
    // Reporting success here would tell an operator the gateway is stopping
    // when the emit would be swallowed.
    if (process.listenerCount("SIGINT") > 0) {
      // Another suite installed a handler; the branch under test needs none.
      return;
    }
    expect(requestGatewayStopWithSignalAdmission("test")).toEqual({ status: "unavailable" });
  });

  it("emits SIGINT to the run loop, but only after the caller's response can flush", async () => {
    const onSigint = vi.fn();
    addSigintListener(onSigint);

    expect(requestGatewayStopWithSignalAdmission("operator stop")).toEqual({ status: "emitted" });
    // Deferred: the gateway must not begin tearing down its websocket server
    // before the RPC response frame leaves.
    expect(onSigint).not.toHaveBeenCalled();

    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(onSigint).toHaveBeenCalledTimes(1);
  });

  it("emits again on a repeat request so the run loop can escalate", async () => {
    const onSigint = vi.fn();
    addSigintListener(onSigint);

    requestGatewayStopWithSignalAdmission();
    requestGatewayStopWithSignalAdmission();
    await new Promise<void>((resolve) => setImmediate(resolve));

    // A second stop request is a second SIGINT: it cuts the delivery grace
    // short exactly like a second Ctrl-C, rather than being deduplicated.
    expect(onSigint).toHaveBeenCalledTimes(2);
  });
});
