import { afterEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { logWarn } from "../logger.js";
import {
  createGatewaySchedulerClock,
  createTestGatewayScheduler,
} from "../test-utils/gateway-scheduler-clock.js";
import {
  createSessionMcpRuntimeManagerLifecycle,
  createSessionMcpRuntimeManagerStore,
} from "./agent-bundle-mcp-manager-lifecycle.js";
import type { CreateSessionMcpRuntime } from "./agent-bundle-mcp-runtime-shared.js";
import type { SessionMcpRuntime } from "./agent-bundle-mcp-types.js";
import { createAgentCleanupScope } from "./run-cleanup-timeout.js";

vi.mock("../logger.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../logger.js")>()),
  logWarn: vi.fn(),
}));

function createRuntimeFixture(input: Parameters<CreateSessionMcpRuntime>[0]): SessionMcpRuntime {
  let lastUsedAt = Date.now();
  let activeLeases = 0;
  return {
    sessionId: input.sessionId,
    sessionKey: input.sessionKey,
    workspaceDir: input.workspaceDir,
    agentDir: input.agentDir,
    requesterScope: input.requesterScope,
    configFingerprint: input.configFingerprint ?? "fixture",
    createdAt: lastUsedAt,
    get lastUsedAt() {
      return lastUsedAt;
    },
    get activeLeases() {
      return activeLeases;
    },
    acquireLease() {
      activeLeases += 1;
      let released = false;
      return () => {
        if (!released) {
          released = true;
          activeLeases -= 1;
        }
      };
    },
    markUsed: () => {
      lastUsedAt = Date.now();
    },
    getCatalog: async () => ({ version: 1, generatedAt: 0, servers: {}, tools: [] }),
    peekCatalog: () => null,
    callTool: async () => ({ content: [] }),
    joinCleanup: async () => {},
    dispose: vi.fn(async () => {}),
  };
}

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.useRealTimers();
  for (const cleanup of cleanups.splice(0)) {
    await cleanup();
  }
  vi.clearAllMocks();
});
function makeHarness() {
  const clock = createGatewaySchedulerClock(Date.now());
  const scheduler = createTestGatewayScheduler(clock.clock);
  const store = createSessionMcpRuntimeManagerStore(
    { scheduler, createRuntime: createRuntimeFixture },
    createRuntimeFixture,
  );
  const lifecycle = createSessionMcpRuntimeManagerLifecycle(store);
  cleanups.push(async () => {
    for (const slot of store.liveRuntimeSlots) {
      for (const [runtime, key] of slot.owners) {
        runtime.joinCleanup = async () => {};
        store.runtimesBySessionId.set(key, runtime);
      }
    }
    const scope = createAgentCleanupScope();
    await scope.run(() => lifecycle.disposeManagedRuntimes());
    await scheduler.stop();
  });
  const add = async (key: string) => {
    const slot = await lifecycle.reserveRuntimeSlot(undefined, true);
    const runtime = createRuntimeFixture({ sessionId: key, workspaceDir: "/test", cfg: {} });
    lifecycle.bindRuntimeSlot(key, runtime, slot);
    store.runtimesBySessionId.set(key, runtime);
    lifecycle.finishRuntimeSlotReservation(slot);
    return { slot, runtime };
  };
  const fill = async () => {
    const victim = await add("victim");
    for (let index = 1; index < 256; index += 1) {
      await add(`session-${index}`);
    }
    return victim;
  };
  return { clock, scheduler, store, lifecycle, add, fill };
}

describe("MCP capacity recovery ownership", () => {
  it("recovers an abandoned owner before rejecting admission and stays quiet", async () => {
    const { store, lifecycle, fill } = makeHarness();
    const { runtime } = await fill();
    store.runtimesBySessionId.delete("victim");
    const replacement = await lifecycle.reserveRuntimeSlot(undefined, true);
    expect(runtime.dispose).toHaveBeenCalledOnce();
    expect(lifecycle.readLiveRuntimeSlotStats()).toMatchObject({
      live: 256,
      registered: 255,
      unregistered: 1,
      creating: 1,
    });
    expect(logWarn).not.toHaveBeenCalled();
    // Model a failed fresh factory: no runtime was created for this reservation.
    lifecycle.finishRuntimeSlotReservation(replacement);
    store.liveRuntimeSlots.delete(replacement);
  });

  it.each(["lease", "work-chain", "pending-disposal"] as const)(
    "protects an owner with %s regardless of idleness and reports the cap only once",
    async (guard) => {
      const { clock, store, lifecycle, fill } = makeHarness();
      const { runtime, slot } = await fill();
      const release = guard === "lease" ? runtime.acquireLease?.() : undefined;
      if (guard === "work-chain") {
        store.runtimeWorkChains.set("victim", Promise.resolve());
      }
      if (guard === "pending-disposal") {
        store.pendingDisposals.set("victim", new Set([Promise.resolve()]));
      }
      slot.idleTtlMs = 1;
      clock.setTime(runtime.lastUsedAt + 100_000_000);
      store.runtimesBySessionId.delete("victim");
      try {
        for (let attempt = 0; attempt < 2; attempt += 1) {
          await expect(lifecycle.reserveRuntimeSlot(undefined, true)).rejects.toThrow(
            "live runtime limit (256)",
          );
        }
        expect(runtime.dispose).not.toHaveBeenCalled();
        expect(logWarn).toHaveBeenCalledOnce();
        expect(lifecycle.readLiveRuntimeSlotStats().live).toBe(256);
      } finally {
        release?.();
        store.runtimeWorkChains.delete("victim");
        store.pendingDisposals.delete("victim");
      }
    },
  );

  it("keeps a creation reservation charged until its factory settles", async () => {
    const { store, lifecycle } = makeHarness();
    const slots = [];
    for (let index = 0; index < 256; index += 1) {
      slots.push(await lifecycle.reserveRuntimeSlot(undefined, true));
    }
    try {
      await expect(lifecycle.reserveRuntimeSlot(undefined, true)).rejects.toThrow("creating=256");
      expect(lifecycle.readLiveRuntimeSlotStats()).toMatchObject({
        live: 256,
        unregistered: 256,
        creating: 256,
      });
    } finally {
      for (const slot of slots) {
        lifecycle.finishRuntimeSlotReservation(slot);
        store.liveRuntimeSlots.delete(slot);
      }
    }
  });

  it("keeps slow native disposal charged, then releases on confirmed closure", async () => {
    const { clock, store, lifecycle, fill } = makeHarness();
    const { runtime } = await fill();
    const held = createDeferred();
    runtime.dispose = vi.fn(() => held.promise);
    store.runtimesBySessionId.delete("victim");
    const disposal = lifecycle.disposeRuntime(runtime, false);
    clock.setTime(runtime.lastUsedAt + 100_000_000);
    try {
      await expect(lifecycle.reserveRuntimeSlot(undefined, true)).rejects.toThrow("draining=1");
      expect(runtime.dispose).toHaveBeenCalledOnce();
      expect(lifecycle.readLiveRuntimeSlotStats().live).toBe(256);
    } finally {
      held.resolve();
      await disposal;
    }
    const replacement = await lifecycle.reserveRuntimeSlot(undefined, true);
    lifecycle.finishRuntimeSlotReservation(replacement);
    store.liveRuntimeSlots.delete(replacement);
    expect(lifecycle.readLiveRuntimeSlotStats().live).toBe(255);
  });

  it("does not turn failed cleanup into spare capacity or a retry loop", async () => {
    const { store, lifecycle, fill } = makeHarness();
    const { runtime } = await fill();
    runtime.joinCleanup = vi.fn(async () => {
      throw new Error("closure unconfirmed");
    });
    store.runtimesBySessionId.delete("victim");
    const scope = createAgentCleanupScope();
    await scope.run(async () => {
      for (let attempt = 0; attempt < 2; attempt += 1) {
        await expect(lifecycle.reserveRuntimeSlot(undefined, true)).rejects.toThrow(
          "cleanup has not confirmed spare capacity",
        );
      }
    });
    expect(runtime.dispose).toHaveBeenCalledOnce();
    expect(logWarn).toHaveBeenCalledOnce();
    expect(lifecycle.readLiveRuntimeSlotStats().live).toBe(256);
  });

  it("coalesces concurrent recovery while preserving the hard capacity bound", async () => {
    const { store, lifecycle, fill } = makeHarness();
    const { runtime } = await fill();
    const entered = createDeferred();
    const held = createDeferred();
    runtime.dispose = vi.fn(() => {
      entered.resolve();
      return held.promise;
    });
    store.runtimesBySessionId.delete("victim");
    const pending = Promise.allSettled([
      lifecycle.reserveRuntimeSlot(undefined, true),
      lifecycle.reserveRuntimeSlot(undefined, true),
    ]);
    await entered.promise;
    expect(runtime.dispose).toHaveBeenCalledOnce();
    held.resolve();
    const results = await pending;
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
    expect(lifecycle.readLiveRuntimeSlotStats().live).toBe(256);
    for (const result of results) {
      if (result.status === "fulfilled") {
        lifecycle.finishRuntimeSlotReservation(result.value);
        store.liveRuntimeSlots.delete(result.value);
      }
    }
  });

  it("bounds recovery waiting without certifying a still-draining transport", async () => {
    vi.useFakeTimers();
    const { store, lifecycle, add } = makeHarness();
    const { runtime } = await add("victim");
    const held = createDeferred();
    runtime.dispose = vi.fn(() => held.promise);
    store.runtimesBySessionId.delete("victim");
    const recovery = lifecycle.recoverUnownedRuntimeSlots();
    await vi.advanceTimersByTimeAsync(10_001);
    await recovery;
    expect(lifecycle.readLiveRuntimeSlotStats()).toMatchObject({ live: 1, draining: 1 });
    held.resolve();
    await lifecycle.recoverUnownedRuntimeSlots();
    expect(lifecycle.readLiveRuntimeSlotStats().live).toBe(0);
  });
});
