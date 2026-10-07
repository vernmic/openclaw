import { afterEach, describe, expect, it, vi } from "vitest";
import {
  GatewayDrainingError,
  markGatewayRestartDraining,
  resetGatewayWorkAdmission,
} from "../process/gateway-work-admission.js";
import { scheduleGatewayIdleTask } from "./server-idle-task.js";

afterEach(() => {
  resetGatewayWorkAdmission();
  vi.useRealTimers();
});

describe("scheduleGatewayIdleTask", () => {
  it("still completes ordinary idle work", async () => {
    vi.useFakeTimers();
    const run = vi.fn(async () => {});
    const handle = scheduleGatewayIdleTask({
      delayMs: 10,
      retryDelayMs: 5,
      isClosing: () => false,
      isBusy: () => false,
      run,
      log: { warn: vi.fn() },
      errorMessage: "idle task failed",
    });

    await vi.advanceTimersByTimeAsync(10);
    await Promise.resolve();
    expect(run).toHaveBeenCalledOnce();
    await handle.stop();
  });

  it("quietly cancels idle work rejected by an active restart drain", async () => {
    vi.useFakeTimers();
    const run = vi.fn(async () => {});
    const warn = vi.fn();
    const handle = scheduleGatewayIdleTask({
      delayMs: 10,
      retryDelayMs: 5,
      isClosing: () => false,
      isBusy: () => false,
      run,
      log: { warn },
      errorMessage: "idle task failed",
    });

    markGatewayRestartDraining();
    await vi.advanceTimersByTimeAsync(10);

    expect(run).not.toHaveBeenCalled();
    expect(warn).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    await handle.stop();
  });

  it("warns when idle work throws a draining error without an active restart", async () => {
    vi.useFakeTimers();
    const error = new GatewayDrainingError("unexpected task failure");
    const warn = vi.fn();
    const handle = scheduleGatewayIdleTask({
      delayMs: 10,
      retryDelayMs: 5,
      isClosing: () => false,
      isBusy: () => false,
      run: async () => {
        throw error;
      },
      log: { warn },
      errorMessage: "idle task failed",
    });

    await vi.advanceTimersByTimeAsync(10);

    expect(warn).toHaveBeenCalledWith(`idle task failed: ${String(error)}`);
    await handle.stop();
  });
});

describe("scheduleGatewayIdleTask recurrence", () => {
  it("runs once and stops when repeatDelayMs is omitted", async () => {
    // One-shot is the existing contract and must not change: retained-plugin cleanup relies on it.
    vi.useFakeTimers();
    const run = vi.fn(async () => {});
    const handle = scheduleGatewayIdleTask({
      delayMs: 10,
      retryDelayMs: 5,
      isClosing: () => false,
      isBusy: () => false,
      run,
      log: { warn: vi.fn() },
      errorMessage: "idle task failed",
    });

    await vi.advanceTimersByTimeAsync(10);
    expect(run).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(run).toHaveBeenCalledTimes(1);
    await handle.stop();
  });

  it("re-arms after each completed run when repeatDelayMs is set", async () => {
    // The bug this pins: the agent-db snapshot pass passed only delayMs, so its "hourly" schedule
    // ran once per gateway start (2026-10-06) while the databases it watched changed every hour.
    vi.useFakeTimers();
    const run = vi.fn(async () => {});
    const handle = scheduleGatewayIdleTask({
      delayMs: 10,
      retryDelayMs: 5,
      repeatDelayMs: 20,
      isClosing: () => false,
      isBusy: () => false,
      run,
      log: { warn: vi.fn() },
      errorMessage: "idle task failed",
    });

    await vi.advanceTimersByTimeAsync(10);
    expect(run).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(20);
    expect(run).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(20);
    expect(run).toHaveBeenCalledTimes(3);
    await handle.stop();
  });

  it("keeps recurring after a run throws", async () => {
    // Re-arming only on success would let one transient throw end the recurrence permanently --
    // the same silent-death class this option exists to remove.
    vi.useFakeTimers();
    const run = vi.fn(async () => {
      throw new Error("transient");
    });
    const handle = scheduleGatewayIdleTask({
      delayMs: 10,
      retryDelayMs: 5,
      repeatDelayMs: 20,
      isClosing: () => false,
      isBusy: () => false,
      run,
      log: { warn: vi.fn() },
      errorMessage: "idle task failed",
    });

    await vi.advanceTimersByTimeAsync(10);
    expect(run).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(20);
    expect(run).toHaveBeenCalledTimes(2);
    await handle.stop();
  });

  it("stops recurring once stop() is called", async () => {
    vi.useFakeTimers();
    const run = vi.fn(async () => {});
    const handle = scheduleGatewayIdleTask({
      delayMs: 10,
      retryDelayMs: 5,
      repeatDelayMs: 20,
      isClosing: () => false,
      isBusy: () => false,
      run,
      log: { warn: vi.fn() },
      errorMessage: "idle task failed",
    });

    await vi.advanceTimersByTimeAsync(10);
    expect(run).toHaveBeenCalledTimes(1);
    await handle.stop();
    await vi.advanceTimersByTimeAsync(200);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("defers while busy and arms exactly one timer", async () => {
    // The busy branch must return before the re-arm: arming both would leave two timers with only
    // the later one tracked by `timer`, so stop() would miss one.
    vi.useFakeTimers();
    let busy = true;
    const run = vi.fn(async () => {});
    const handle = scheduleGatewayIdleTask({
      delayMs: 10,
      retryDelayMs: 5,
      repeatDelayMs: 20,
      isClosing: () => false,
      isBusy: () => busy,
      run,
      log: { warn: vi.fn() },
      errorMessage: "idle task failed",
    });

    await vi.advanceTimersByTimeAsync(10);
    expect(run).not.toHaveBeenCalled();
    busy = false;
    await vi.advanceTimersByTimeAsync(5);
    expect(run).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(20);
    expect(run).toHaveBeenCalledTimes(2);
    await handle.stop();
  });
});
