/** Session MCP runtime manager lifecycle: maps, idle sweep, dispose, advertised catalog. */
import { AsyncLocalStorage } from "node:async_hooks";
import { logWarn } from "../logger.js";
import { sessionMcpRuntimeOwners } from "./agent-bundle-mcp-runtime-owner.js";
import {
  SESSION_MCP_MAX_LIVE_RUNTIMES,
  SESSION_MCP_ORPHAN_SLOT_GRACE_MS,
  SESSION_MCP_RUNTIME_SWEEP_INTERVAL_MS,
  type CreateSessionMcpRuntime,
} from "./agent-bundle-mcp-runtime-shared.js";
import type {
  McpCatalogTool,
  McpServerCatalog,
  McpToolCatalog,
  RequesterScopedMcpRuntimeHandle,
  SessionMcpConfigReload,
  SessionMcpRuntime,
} from "./agent-bundle-mcp-types.js";
import { recordAgentCleanupFailure } from "./run-cleanup-timeout.js";

// Gateway shutdown preparation and CLI command imports load this before turns.
// The process-owned sweep must not retain its first requesting turn.
const runInMcpManagerContext = AsyncLocalStorage.snapshot();

export type SessionMcpConfigPublication = SessionMcpConfigReload & { pluginGeneration: number };

type AdvertisedScopedCatalogEntry = {
  configFingerprint: string;
  servers: Map<string, McpServerCatalog>;
  toolsByServer: Map<string, McpCatalogTool[]>;
  signaturesByServer: Map<string, string>;
};

/**
 * A live-runtime slot. `orphanSince` is bookkeeping for the sweep's reconciliation pass: the
 * timestamp at which this slot was first observed owned by no runtime, cleared as soon as an owner
 * is seen again. Absent means "currently owned, or never yet observed orphaned".
 */
type SessionMcpRuntimeSlot = { idleTtlMs: number; orphanSince?: number };

type SessionMcpRuntimeManagerStore = {
  configReload?: SessionMcpConfigPublication;
  runtimesBySessionId: Map<string, SessionMcpRuntime>;
  sessionIdBySessionKey: Map<string, string>;
  deferredRetirementSessionIds: Set<string>;
  // Reset/delete retirement survives late creation or reuse by the stopping run.
  requiredRetirementSessionIds: Set<string>;
  connectionMetaByRuntimeKey: Map<string, { connectionHash: string; resolvedAt: number }>;
  advertisedScopedCatalogBySessionId: Map<string, AdvertisedScopedCatalogEntry>;
  runtimeWorkChains: Map<string, Promise<unknown>>;
  disposalInFlight?: Promise<void>;
  pendingDisposals: Map<string, Set<Promise<void>>>;
  createRuntime: CreateSessionMcpRuntime;
  now: () => number;
  idleSweepIntervalMs: number;
  runtimeSlots: WeakMap<SessionMcpRuntime, SessionMcpRuntimeSlot>;
  liveRuntimeSlots: Set<SessionMcpRuntimeSlot>;
  enableIdleSweepTimer: boolean;
  idleSweepTimer: ReturnType<typeof setInterval> | undefined;
  idleSweepInFlight: Promise<void> | undefined;
};

export type SessionMcpRuntimeManagerOpts = {
  createRuntime?: CreateSessionMcpRuntime;
  now?: () => number;
  enableIdleSweepTimer?: boolean;
  idleSweepIntervalMs?: number;
};

function parseRuntimeCacheSessionId(runtimeKey: string): string {
  if (!runtimeKey.startsWith("{")) {
    return runtimeKey;
  }
  try {
    const parsed = JSON.parse(runtimeKey) as { sessionId?: unknown };
    return typeof parsed.sessionId === "string" ? parsed.sessionId : runtimeKey;
  } catch {
    return runtimeKey;
  }
}

export function createSessionMcpRuntimeManagerStore(
  opts: SessionMcpRuntimeManagerOpts,
  createSessionMcpRuntime: CreateSessionMcpRuntime,
): SessionMcpRuntimeManagerStore {
  return {
    // Keys are bare sessionId for static runtimes, or requester composite JSON keys.
    runtimesBySessionId: new Map<string, SessionMcpRuntime>(),
    sessionIdBySessionKey: new Map<string, string>(),
    deferredRetirementSessionIds: new Set<string>(),
    requiredRetirementSessionIds: new Set<string>(),
    // Manager-side only: connection hash + resolve time. Never stores raw url/headers.
    connectionMetaByRuntimeKey: new Map(),
    /**
     * Session-stable advertised catalogs for requester-scoped servers.
     * Keyed by sessionId → serverName. Specs must not vary per sender or shared
     * Codex threads rotate (dynamicToolsFingerprint churn).
     */
    advertisedScopedCatalogBySessionId: new Map(),
    /**
     * Per-runtimeKey serialization for acquisition and dispose.
     * Sections never overlap for one key, so a slow resolve cannot clobber a newer install.
     * Entries are removed when their chain drains.
     */
    runtimeWorkChains: new Map(),
    pendingDisposals: new Map(),
    createRuntime: opts.createRuntime ?? createSessionMcpRuntime,
    now: opts.now ?? Date.now,
    idleSweepIntervalMs: opts.idleSweepIntervalMs ?? SESSION_MCP_RUNTIME_SWEEP_INTERVAL_MS,
    runtimeSlots: new WeakMap(),
    liveRuntimeSlots: new Set(),
    enableIdleSweepTimer: opts.enableIdleSweepTimer !== false,
    idleSweepTimer: undefined,
    idleSweepInFlight: undefined,
  };
}

export type SessionMcpRuntimeManagerLifecycle = ReturnType<
  typeof createSessionMcpRuntimeManagerLifecycle
>;

function scopedCatalogToolsSignature(tools: readonly McpCatalogTool[]): string {
  return JSON.stringify(
    tools.map((tool) => [
      tool.serverName,
      tool.safeServerName,
      tool.toolName,
      tool.title ?? "",
      tool.description ?? "",
      tool.fallbackDescription,
      tool.inputSchema,
      tool.uiResourceUri ?? "",
      tool.uiVisibility ?? null,
    ]),
  );
}

/**
 * Names the sessions holding the most live runtime slots, so a cap hit reads in minutes instead
 * of a two-day investigation.
 *
 * The census leads because it is what separates the two failure modes. A runaway session shows a
 * large `xN` in the holder list; a reclaim shortfall shows a large orphan count behind
 * unremarkable holders. Reporting only the holders makes the mixed case actively misleading - 200
 * stranded slots beside 56 owned ones reads as "nobody is holding much", which points the
 * investigation away from the cause precisely when it is most needed.
 *
 * Slots shared by replacement runtimes count once per session. The orphan count matches what
 * `readLiveRuntimeSlotStats` derives - live slots that no runtime owns - though this pass filters
 * to live slots up front, because only those count against the cap.
 */
function describeLiveRuntimeSlotHolders(store: SessionMcpRuntimeManagerStore): string {
  const liveSlotsBySession = new Map<string, Set<SessionMcpRuntimeSlot>>();
  const ownedLiveSlots = new Set<SessionMcpRuntimeSlot>();
  for (const runtime of store.runtimesBySessionId.values()) {
    const slot = store.runtimeSlots.get(runtime);
    if (!slot || !store.liveRuntimeSlots.has(slot)) {
      continue;
    }
    ownedLiveSlots.add(slot);
    const session = runtime.sessionKey ?? runtime.sessionId;
    const held = liveSlotsBySession.get(session);
    if (held) {
      held.add(slot);
    } else {
      liveSlotsBySession.set(session, new Set([slot]));
    }
  }
  const live = store.liveRuntimeSlots.size;
  const owned = ownedLiveSlots.size;
  const census = `; ${live} live (${live - owned} orphaned, ${owned} owned by ${liveSlotsBySession.size} session(s))`;
  if (liveSlotsBySession.size === 0) {
    return `${census}; no runtime owns a live slot (all orphaned)`;
  }
  const top = [...liveSlotsBySession.entries()]
    .sort((a, b) => b[1].size - a[1].size || a[0].localeCompare(b[0]))
    .slice(0, 3)
    .map(([session, held]) => `${session} x${held.size}`)
    .join(", ");
  return `${census}; top live-slot holders: ${top}`;
}

export function createSessionMcpRuntimeManagerLifecycle(store: SessionMcpRuntimeManagerStore) {
  const reserveRuntimeSlot = (
    existing: SessionMcpRuntime | undefined,
    hasServers: boolean,
  ): { idleTtlMs: number } => {
    const slot = (existing && store.runtimeSlots.get(existing)) ?? { idleTtlMs: 0 };
    if (!hasServers || store.liveRuntimeSlots.has(slot)) {
      return slot;
    }
    if (store.liveRuntimeSlots.size >= SESSION_MCP_MAX_LIVE_RUNTIMES) {
      const message = `bundle-mcp: live runtime limit (${SESSION_MCP_MAX_LIVE_RUNTIMES}) reached; stop or reset unused sessions before connecting another MCP runtime${describeLiveRuntimeSlotHolders(store)}`;
      logWarn(message);
      throw new Error(message);
    }
    store.liveRuntimeSlots.add(slot);
    return slot;
  };
  const releaseEmptyRuntimeSlot = async (runtimeKey: string, runtime: SessionMcpRuntime) => {
    const slot = store.runtimeSlots.get(runtime);
    if (!slot || !store.liveRuntimeSlots.has(slot)) {
      return;
    }
    if (!runtime.joinCleanup) {
      throw new Error("MCP runtime does not expose cleanup ownership");
    }
    await runtime.joinCleanup();
    // Replacement transfers the slot before cleanup yields; only its current owner may release it.
    if (
      store.runtimesBySessionId.get(runtimeKey) === runtime &&
      sessionMcpRuntimeOwners.get(runtime)?.hasServers() !== true
    ) {
      store.liveRuntimeSlots.delete(slot);
    }
  };
  const disposeRuntime = async (runtime: SessionMcpRuntime, releaseSlot = true) => {
    try {
      await runtime.dispose();
      if (!runtime.joinCleanup) {
        throw new Error("MCP runtime does not expose cleanup ownership");
      }
      await runtime.joinCleanup();
      const slot = store.runtimeSlots.get(runtime);
      if (releaseSlot && slot) {
        store.liveRuntimeSlots.delete(slot);
        store.runtimeSlots.delete(runtime);
      }
    } catch (error) {
      recordAgentCleanupFailure();
      throw error;
    }
  };
  const trackDisposal = (runtimeKeys: string[], close: () => Promise<void>): Promise<void> => {
    const disposal = Promise.resolve()
      .then(close)
      .catch((error: unknown) => {
        recordAgentCleanupFailure();
        throw error;
      })
      .finally(() => {
        for (const runtimeKey of runtimeKeys) {
          const pending = store.pendingDisposals.get(runtimeKey);
          pending?.delete(disposal);
          if (pending?.size === 0) {
            store.pendingDisposals.delete(runtimeKey);
          }
        }
      });
    for (const runtimeKey of runtimeKeys) {
      const pending = store.pendingDisposals.get(runtimeKey) ?? new Set<Promise<void>>();
      store.pendingDisposals.set(runtimeKey, pending);
      pending.add(disposal);
    }
    return disposal;
  };
  const forgetSessionKeysForSessionId = (sessionId: string) => {
    for (const [sessionKey, mappedSessionId] of store.sessionIdBySessionKey.entries()) {
      if (mappedSessionId === sessionId) {
        store.sessionIdBySessionKey.delete(sessionKey);
      }
    }
  };

  const runtimeKeysForSessionId = (sessionId: string): string[] => {
    const keys = new Set<string>();
    for (const [runtimeKey, runtime] of store.runtimesBySessionId.entries()) {
      if (runtime.sessionId === sessionId) {
        keys.add(runtimeKey);
      }
    }
    for (const runtimeKey of store.runtimeWorkChains.keys()) {
      if (parseRuntimeCacheSessionId(runtimeKey) === sessionId) {
        keys.add(runtimeKey);
      }
    }
    return [...keys];
  };

  const totalActiveLeasesForSessionId = (sessionId: string): number => {
    let total = 0;
    for (const runtimeKey of runtimeKeysForSessionId(sessionId)) {
      total += store.runtimesBySessionId.get(runtimeKey)?.activeLeases ?? 0;
    }
    return total;
  };

  const runExclusiveOnRuntimeKeys = <T>(
    runtimeKeys: string[],
    work: () => Promise<T>,
  ): Promise<T> => {
    const previous = runtimeKeys
      .map((key) => store.runtimeWorkChains.get(key))
      .filter((pending) => pending !== undefined);
    const run = Promise.allSettled(previous).then(work);
    const settled: Promise<unknown> = run.then(
      () => undefined,
      () => undefined,
    );
    for (const key of runtimeKeys) {
      store.runtimeWorkChains.set(key, settled);
    }
    void settled.finally(() => {
      for (const key of runtimeKeys) {
        if (store.runtimeWorkChains.get(key) === settled) {
          store.runtimeWorkChains.delete(key);
        }
      }
    });
    return run;
  };

  /**
   * Reclaim live slots that no runtime owns any more.
   *
   * `liveRuntimeSlots` is strong and `runtimeSlots` is keyed BY runtime, so a slot whose runtime
   * left `runtimesBySessionId` without releasing it is unreachable: the sweep above cannot iterate
   * it, nothing else holds a reference, and it counts against the cap until the process dies. This
   * pass is the only thing that can find those, by diffing the Set against the runtimes that
   * actually exist.
   *
   * A slot is reclaimed only after SESSION_MCP_ORPHAN_SLOT_GRACE_MS of CONTINUOUS orphanhood,
   * because reserveRuntimeSlot adds a slot before its runtime is registered - a brief unowned
   * window is normal and must not be stolen from a runtime that is still being created.
   */
  const reclaimOrphanedRuntimeSlots = (nowMs: number): number => {
    const owned = new Set<SessionMcpRuntimeSlot>();
    for (const runtime of store.runtimesBySessionId.values()) {
      const slot = store.runtimeSlots.get(runtime);
      if (slot) {
        owned.add(slot);
      }
    }
    let reclaimed = 0;
    for (const slot of store.liveRuntimeSlots) {
      if (owned.has(slot)) {
        // Owned again (or still being created): the orphan clock does not carry over.
        delete slot.orphanSince;
        continue;
      }
      if (slot.orphanSince === undefined) {
        slot.orphanSince = nowMs;
        continue;
      }
      if (nowMs - slot.orphanSince < SESSION_MCP_ORPHAN_SLOT_GRACE_MS) {
        continue;
      }
      store.liveRuntimeSlots.delete(slot);
      reclaimed += 1;
    }
    if (reclaimed > 0) {
      logWarn(
        `bundle-mcp: reclaimed ${reclaimed} orphaned MCP runtime slot(s) held by no runtime; ` +
          `${store.liveRuntimeSlots.size}/${SESSION_MCP_MAX_LIVE_RUNTIMES} live slots remain`,
      );
    }
    return reclaimed;
  };

  const sweepIdleRuntimes = async (): Promise<number> => {
    const nowMs = store.now();
    const expired: Array<{ runtimeKey: string; runtime: SessionMcpRuntime }> = [];
    for (const [runtimeKey, runtime] of store.runtimesBySessionId.entries()) {
      const idleTtlMs = store.runtimeSlots.get(runtime)?.idleTtlMs ?? 0;
      if (idleTtlMs === 0) {
        continue;
      }
      const idleForMs = nowMs - runtime.lastUsedAt;
      if (idleForMs < idleTtlMs) {
        continue;
      }
      // A lease or an outstanding work chain protects a runtime with no staleness ceiling, and
      // must keep doing so. Acquisition runs outside the runtime lease, so the chain keeps its
      // current transport until the chain records the refreshed runtime.
      //
      // DIRECTIVE: do not add an idle ceiling here. 2026-09-27 added one on the theory that
      // `lastUsedAt` evidences holder liveness - "a busy runtime keeps touching it". That is
      // false. `lastUsedAt` measures MCP-runtime idleness, not holder liveness: a session holding
      // a lease through forty minutes of non-MCP work (file edits, shell, provider calls) is
      // indistinguishable from an abandoned lease. On 2026-09-29 the ceiling fired 4 times, every
      // one at activeLeases=1, three of them on one session whose work was still live - removing
      // the runtime and its work-chain entry from under it. That session lost an output, retried,
      // and wrote a duplicate; a later reply was generated three times.
      //
      // The ceiling was also not what fixed the 2026-09-27 exhaustion. That was the orphaned-slot
      // class, which reclaimOrphanedRuntimeSlots below is the only thing that can find: 210
      // reclaims against those 4 pinned evictions over the same window.
      //
      // An abandoned lease can still pin one runtime for the life of the process. Bounding that
      // safely needs positive evidence the holder is gone - an in-flight-run check - never a
      // longer idle timer, which only makes the same misclassification rarer and harder to trace.
      if ((runtime.activeLeases ?? 0) > 0 || store.runtimeWorkChains.has(runtimeKey)) {
        continue;
      }
      store.runtimesBySessionId.delete(runtimeKey);
      store.connectionMetaByRuntimeKey.delete(runtimeKey);
      expired.push({ runtimeKey, runtime });
    }
    reclaimOrphanedRuntimeSlots(nowMs);
    const touchedSessionIds = new Set(expired.map(({ runtime }) => runtime.sessionId));
    for (const sessionId of touchedSessionIds) {
      if (runtimeKeysForSessionId(sessionId).length === 0) {
        store.deferredRetirementSessionIds.delete(sessionId);
        forgetSessionKeysForSessionId(sessionId);
      }
    }
    await Promise.allSettled(
      expired.map(({ runtimeKey, runtime }) =>
        trackDisposal([runtimeKey], () => disposeRuntime(runtime)),
      ),
    );
    ensureIdleSweepTimer();
    return expired.length;
  };

  const queueIdleSweep = () => {
    if (store.idleSweepInFlight) {
      return;
    }
    store.idleSweepInFlight = sweepIdleRuntimes()
      .then(() => undefined)
      .catch((error: unknown) => {
        logWarn(`bundle-mcp: idle runtime sweep failed: ${String(error)}`);
      })
      .finally(() => {
        store.idleSweepInFlight = undefined;
      });
  };

  const ensureIdleSweepTimer = () => {
    const enabled = [...store.runtimesBySessionId.values()].some(
      (runtime) => (store.runtimeSlots.get(runtime)?.idleTtlMs ?? 0) > 0,
    );
    if (!enabled) {
      clearIdleSweepTimer();
      return;
    }
    if (!store.enableIdleSweepTimer || store.idleSweepIntervalMs <= 0 || store.idleSweepTimer) {
      return;
    }
    store.idleSweepTimer = runInMcpManagerContext(() =>
      setInterval(queueIdleSweep, store.idleSweepIntervalMs),
    );
    store.idleSweepTimer.unref?.();
  };

  const clearIdleSweepTimer = () => {
    if (!store.idleSweepTimer) {
      return;
    }
    clearInterval(store.idleSweepTimer);
    store.idleSweepTimer = undefined;
  };

  const disposeRuntimeKeyNow = async (runtimeKey: string): Promise<void> => {
    const runtime = store.runtimesBySessionId.get(runtimeKey);
    store.runtimesBySessionId.delete(runtimeKey);
    store.connectionMetaByRuntimeKey.delete(runtimeKey);
    if (runtime) {
      await disposeRuntime(runtime);
    }
  };

  const disposeManagedRuntimes = (
    sessionId?: string,
    opts?: { preserveRequiredRetirement?: boolean },
  ): Promise<void> => {
    const runtimeKeys = [
      ...new Set(
        sessionId === undefined
          ? [
              ...store.runtimesBySessionId.keys(),
              ...store.runtimeWorkChains.keys(),
              ...store.pendingDisposals.keys(),
            ]
          : [
              sessionId,
              ...runtimeKeysForSessionId(sessionId),
              ...[...store.pendingDisposals.keys()].filter(
                (key) => parseRuntimeCacheSessionId(key) === sessionId,
              ),
            ],
      ),
    ];
    // Capture before queuing: the previous owner may unpublish and settle before
    // this caller enters the runtime-key chain, but its receipt still belongs here.
    const previousDisposals = new Set(
      runtimeKeys.flatMap((key) => [...(store.pendingDisposals.get(key) ?? [])]),
    );
    const priorDisposal = store.disposalInFlight;
    const queued = runExclusiveOnRuntimeKeys(runtimeKeys, async () => {
      await priorDisposal;
      // Clear bookkeeping after admitted acquisitions finish, before successors run.
      if (sessionId === undefined) {
        clearIdleSweepTimer();
        store.configReload = undefined;
        store.sessionIdBySessionKey.clear();
        store.deferredRetirementSessionIds.clear();
        store.requiredRetirementSessionIds.clear();
        store.advertisedScopedCatalogBySessionId.clear();
      } else {
        store.deferredRetirementSessionIds.delete(sessionId);
        if (opts?.preserveRequiredRetirement !== true) {
          store.requiredRetirementSessionIds.delete(sessionId);
        }
        store.advertisedScopedCatalogBySessionId.delete(sessionId);
        forgetSessionKeysForSessionId(sessionId);
      }
      const outcomes = await Promise.allSettled([
        ...previousDisposals,
        ...runtimeKeys.map(disposeRuntimeKeyNow),
      ]);
      const failed = outcomes.find((outcome) => outcome.status === "rejected");
      ensureIdleSweepTimer();
      if (failed) {
        throw failed.reason;
      }
    });
    const disposal = trackDisposal(runtimeKeys, () => queued).catch(() => undefined);
    if (sessionId === undefined) {
      // New session keys also wait for a global teardown already in progress.
      store.disposalInFlight = disposal;
    }
    return disposal.finally(() => {
      if (store.disposalInFlight === disposal) {
        store.disposalInFlight = undefined;
      }
    });
  };

  const rememberAdvertisedScopedCatalog = (
    handle: RequesterScopedMcpRuntimeHandle,
    catalog: McpToolCatalog,
  ): void => {
    const { runtime, advertisedCatalogConfigFingerprint } = handle;
    // An older requester may finish after reconciliation; reject its catalog
    // instead of allowing stale tools to repopulate the session cache.
    const entry = store.advertisedScopedCatalogBySessionId.get(runtime.sessionId);
    if (
      entry?.configFingerprint !== advertisedCatalogConfigFingerprint ||
      sessionMcpRuntimeOwners.get(runtime)?.isCurrent() === false ||
      ![...store.runtimesBySessionId.values()].includes(runtime)
    ) {
      return;
    }
    const toolsByServerName = new Map<string, McpCatalogTool[]>();
    for (const tool of catalog.tools) {
      const list = toolsByServerName.get(tool.serverName) ?? [];
      list.push(tool);
      toolsByServerName.set(tool.serverName, list);
    }
    for (const [serverName, server] of Object.entries(catalog.servers)) {
      const tools = (toolsByServerName.get(serverName) ?? []).toSorted((a, b) =>
        a.toolName.localeCompare(b.toolName),
      );
      const signature = scopedCatalogToolsSignature(tools);
      // Identity compare: overwrite only when the listed tool surface changes.
      if (entry.signaturesByServer.get(serverName) === signature) {
        continue;
      }
      entry.servers.set(serverName, server);
      entry.toolsByServer.set(serverName, tools);
      entry.signaturesByServer.set(serverName, signature);
    }
  };

  const getAdvertisedScopedCatalog = (sessionId: string): McpToolCatalog | null => {
    const entry = store.advertisedScopedCatalogBySessionId.get(sessionId);
    if (!entry || entry.servers.size === 0) {
      return null;
    }
    const servers: Record<string, McpServerCatalog> = {};
    const tools: McpCatalogTool[] = [];
    for (const serverName of [...entry.servers.keys()].toSorted((a, b) => a.localeCompare(b))) {
      servers[serverName] = entry.servers.get(serverName)!;
      tools.push(...(entry.toolsByServer.get(serverName) ?? []));
    }
    tools.sort((a, b) => {
      const serverOrder = a.safeServerName.localeCompare(b.safeServerName);
      if (serverOrder !== 0) {
        return serverOrder;
      }
      return a.toolName.localeCompare(b.toolName);
    });
    return {
      version: 1,
      generatedAt: store.now(),
      servers,
      tools,
    };
  };

  const reconcileAdvertisedScopedCatalogConfig = (
    sessionId: string,
    fingerprint: string,
    preparePublication: boolean,
  ): void => {
    const current = store.advertisedScopedCatalogBySessionId.get(sessionId);
    if (current?.configFingerprint === fingerprint || (!current && !preparePublication)) {
      return;
    }
    store.advertisedScopedCatalogBySessionId.set(sessionId, {
      configFingerprint: fingerprint,
      servers: new Map(),
      toolsByServer: new Map(),
      signaturesByServer: new Map(),
    });
  };

  return {
    store,
    runtimeKeysForSessionId,
    totalActiveLeasesForSessionId,
    runExclusiveOnRuntimeKeys,
    sweepIdleRuntimes,
    /**
     * Live-runtime slot accounting, for diagnostics and tests.
     *
     * Until 2026-09-27 `liveRuntimeSlots.size` was observable only by hitting the cap and reading
     * the throw - i.e. at 100% of capacity, after the damage. `orphaned` is the count the
     * reconciliation pass is watching: slots held by no runtime.
     */
    readLiveRuntimeSlotStats: () => {
      const owned = new Set<SessionMcpRuntimeSlot>();
      for (const runtime of store.runtimesBySessionId.values()) {
        const slot = store.runtimeSlots.get(runtime);
        if (slot) {
          owned.add(slot);
        }
      }
      let orphaned = 0;
      for (const slot of store.liveRuntimeSlots) {
        if (!owned.has(slot)) {
          orphaned += 1;
        }
      }
      return {
        live: store.liveRuntimeSlots.size,
        max: SESSION_MCP_MAX_LIVE_RUNTIMES,
        runtimes: store.runtimesBySessionId.size,
        orphaned,
      };
    },
    reserveRuntimeSlot,
    releaseEmptyRuntimeSlot,
    disposeRuntime,
    ensureIdleSweepTimer,
    disposeRuntimeKeyNow,
    disposeManagedRuntimes,
    rememberAdvertisedScopedCatalog,
    getAdvertisedScopedCatalog,
    reconcileAdvertisedScopedCatalogConfig,
  };
}
