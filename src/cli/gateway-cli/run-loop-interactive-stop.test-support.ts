import { performance } from "node:perf_hooks";
import { expect, it, vi } from "vitest";
import { createDeferredCore } from "../../shared/deferred.js";
import { gatewayWorkAdmissionActual, type runLoopFixture } from "./run-loop-mocks.test-support.js";
import {
  createActiveWorkSnapshot,
  createGatewayServer,
  createRuntimeWithExitSignal,
  withIsolatedSignals,
} from "./run-loop.test-support.js";

export function registerInteractiveGatewayStopTests(fixture: typeof runLoopFixture) {
  const {
    createSignaledLoopHarness,
    createGatewayActiveWorkSnapshot,
    waitForGatewayActiveWork,
    abortEmbeddedAgentRun,
  } = fixture;

  it.each(["delivered", "operator-cut"] as const)(
    "keeps admission open through interactive delivery grace: %s",
    async (outcome) => {
      await withIsolatedSignals(async ({ captureSignal }) => {
        const { runtime, exited, close } = await createSignaledLoopHarness();
        const graceEntered = createDeferredCore();
        const grace = createDeferredCore();
        const active = createActiveWorkSnapshot({ embeddedRuns: 1 });
        createGatewayActiveWorkSnapshot.mockReturnValue(active);
        let graceBudget: number | undefined;
        waitForGatewayActiveWork.mockImplementationOnce(async (timeoutMs) => {
          graceBudget = timeoutMs;
          graceEntered.resolve();
          await grace.promise;
          return { drained: outcome === "delivered", snapshot: active };
        });
        waitForGatewayActiveWork.mockImplementationOnce(async () => ({
          drained: true,
          snapshot: createActiveWorkSnapshot(),
        }));
        let admissionClosedAtAbort: boolean | undefined;
        abortEmbeddedAgentRun.mockImplementation(() => {
          admissionClosedAtAbort = gatewayWorkAdmissionActual.isGatewayWorkAdmissionClosed();
          return true;
        });
        const stop = captureSignal("SIGINT");
        try {
          stop();
          await graceEntered.promise;
          expect(gatewayWorkAdmissionActual.isGatewayWorkAdmissionClosed()).toBe(false);
          expect(graceBudget).toBeGreaterThan(0);
          expect(graceBudget).toBeLessThanOrEqual(10_000);
          expect(abortEmbeddedAgentRun).not.toHaveBeenCalled();
          expect(close).not.toHaveBeenCalled();
          if (outcome === "delivered") {
            createGatewayActiveWorkSnapshot.mockReturnValue(createActiveWorkSnapshot());
            grace.resolve();
          } else {
            stop();
          }
          await expect(exited).resolves.toBe(0);
          expect(gatewayWorkAdmissionActual.isGatewayWorkAdmissionClosed()).toBe(true);
          if (outcome === "delivered") {
            expect(abortEmbeddedAgentRun).not.toHaveBeenCalled();
          } else {
            expect(admissionClosedAtAbort).toBe(true);
            expect(abortEmbeddedAgentRun).toHaveBeenCalledExactlyOnceWith(undefined, {
              mode: "all",
              reason: "restart",
            });
          }
          expect(runtime.exit).toHaveBeenCalledExactlyOnceWith(0);
        } finally {
          grace.resolve();
          await exited;
        }
      });
    },
  );

  it("forces a stuck interactive close through the native stop owner", async () => {
    await withIsolatedSignals(async ({ captureSignal }) => {
      const { close, runtime, exited } = await createSignaledLoopHarness();
      const closeEntered = createDeferredCore();
      const closeHold = createDeferredCore();
      close.mockImplementationOnce(async () => {
        closeEntered.resolve();
        await closeHold.promise;
      });
      const stop = captureSignal("SIGINT");
      vi.useFakeTimers();
      try {
        stop();
        await closeEntered.promise;
        expect(runtime.exit).not.toHaveBeenCalled();
        stop();
        await vi.advanceTimersByTimeAsync(0);
        expect(runtime.exit).toHaveBeenCalledWith(1);
        await expect(exited).resolves.toBe(1);
      } finally {
        closeHold.resolve();
        await vi.advanceTimersByTimeAsync(0);
        vi.useRealTimers();
      }
    });
  });

  it("bounds repeated SIGINT during admitted startup cleanup without extending the deadline", async () => {
    await withIsolatedSignals(async ({ captureSignal }) => {
      const entered = createDeferredCore();
      const cleanup = createDeferredCore();
      const { runtime, exited } = createRuntimeWithExitSignal();
      const { runGatewayLoop } = await import("./run-loop.js");
      const loop = runGatewayLoop({
        runtime,
        start: async (options) => {
          await options!.startupOperation!(async (signal) => {
            entered.resolve();
            await new Promise<void>((resolve) => {
              signal.addEventListener("abort", () => resolve(), { once: true });
            });
            await cleanup.promise;
            throw signal.reason;
          });
          return createGatewayServer(vi.fn(async () => {}));
        },
      });
      const settled = Promise.allSettled([loop]);
      await entered.promise;
      vi.useFakeTimers();
      const clock = vi.spyOn(performance, "now").mockImplementation(() => Date.now());
      try {
        const stop = captureSignal("SIGINT");
        stop();
        await vi.advanceTimersByTimeAsync(0);
        stop();
        await vi.advanceTimersByTimeAsync(30_000);
        expect(runtime.exit).not.toHaveBeenCalled();
        stop();
        await vi.advanceTimersByTimeAsync(14_999);
        expect(runtime.exit).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(1);
        expect(runtime.exit).toHaveBeenCalledWith(1);
        await expect(exited).resolves.toBe(1);
      } finally {
        cleanup.resolve();
        await vi.advanceTimersByTimeAsync(0);
        await settled;
        clock.mockRestore();
        vi.useRealTimers();
      }
    });
  });
}
