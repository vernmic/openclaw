import type { EventLoopUtilization } from "node:perf_hooks";
import { afterEach, expect, it, vi } from "vitest";
import {
  armWorkerTimeoutStallAfterCoreReady,
  listStalledWorkerRequestKinds,
  noteGatewayCoreReady,
  onWorkerRequestMetrics,
  recordWorkerRequestTimeout,
  trackWorkerRequest,
} from "./worker-request-diagnostics.js";

const { report } = vi.hoisted(() => ({ report: vi.fn() }));
// mock-isolation: Keep the logging sink outside the worker timing fixture.
vi.mock("./worker-request-delivery-report.runtime.js", () => ({
  reportSlowWorkerDelivery: report,
}));

afterEach(() => report.mockClear());

function workerLoop() {
  const totals = { active: 0, idle: 0 };
  return {
    totals,
    performance: {
      eventLoopUtilization: (previous?: EventLoopUtilization): EventLoopUtilization => {
        const active = totals.active - (previous?.active ?? 0);
        const idle = totals.idle - (previous?.idle ?? 0);
        return { active, idle, utilization: active / (active + idle || 1) };
      },
    },
  };
}

it.each([
  { deliveryMs: 4_944, reported: true },
  { deliveryMs: 999, reported: false },
])(
  "names the worker loop state behind a $deliveryMs ms delivery",
  async ({ deliveryMs, reported }) => {
    // Timing stamps exist only while an exporter consumes them.
    const stop = onWorkerRequestMetrics(() => {});
    try {
      const loop = workerLoop();
      loop.totals.active = 50_000;
      const observation = trackWorkerRequest(
        "sqlite_writer",
        "session.actor.appendTranscriptEvent",
      );
      observation.started(14, loop.performance);
      Object.assign(observation.timing!, {
        posted: 1_000,
        received: 1_000 + deliveryMs,
        executeStarted: 1_001 + deliveryMs,
        executeFinished: 1_360 + deliveryMs,
      });
      loop.totals.active += deliveryMs + 359;
      loop.totals.idle += 5;
      observation.completed();
      await vi.dynamicImportSettled();
      if (!reported) {
        expect(report).not.toHaveBeenCalled();
        return;
      }
      expect(report).toHaveBeenCalledExactlyOnceWith({
        requestClass: expect.any(String),
        threadId: 14,
        deliveryMs: 4_944,
        executeMs: 359,
        workerActiveMs: 5_303,
        workerIdleMs: 5,
        workerActiveOutsideExecuteMs: 4_944,
      });
    } finally {
      stop();
    }
  },
);

it("reports a worker kind as stalled only while its last three deadlines fall within five minutes", () => {
  let now = 1_000;
  const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
  try {
    recordWorkerRequestTimeout("diskBudget");
    recordWorkerRequestTimeout("diskBudget");
    expect(listStalledWorkerRequestKinds()).not.toContain("diskBudget");
    recordWorkerRequestTimeout("diskBudget");
    expect(listStalledWorkerRequestKinds()).toContain("diskBudget");
    now += 5 * 60_000;
    expect(listStalledWorkerRequestKinds()).not.toContain("diskBudget");
  } finally {
    clock.mockRestore();
  }
});

it("does not mark a stall for deadlines that expire before the gateway first reaches core-ready", () => {
  let now = 100;
  const clock = vi.spyOn(performance, "now").mockImplementation(() => now);
  try {
    armWorkerTimeoutStallAfterCoreReady();
    recordWorkerRequestTimeout("sessionHistory");
    recordWorkerRequestTimeout("sessionHistory");
    recordWorkerRequestTimeout("sessionHistory");
    expect(listStalledWorkerRequestKinds()).not.toContain("sessionHistory");
    now = 200;
    noteGatewayCoreReady();
    expect(listStalledWorkerRequestKinds()).not.toContain("sessionHistory");
    recordWorkerRequestTimeout("sessionHistory");
    recordWorkerRequestTimeout("sessionHistory");
    recordWorkerRequestTimeout("sessionHistory");
    expect(listStalledWorkerRequestKinds()).toContain("sessionHistory");
  } finally {
    clock.mockRestore();
  }
});
