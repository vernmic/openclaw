import type { SessionToolOverrides } from "../config/sessions/types.js";
/** Shared session MCP runtime constants and create-runtime factory type. */
import type { OpenClawConfig } from "../config/types.openclaw.js";
import type { PluginManifestRegistry } from "../plugins/manifest-registry.js";
import type {
  RequesterMcpConnect,
  SessionMcpRequesterScope,
  SessionMcpRuntime,
  SessionMcpRuntimeManager,
} from "./agent-bundle-mcp-types.js";
import type { McpServerConnectionResolved } from "./mcp-connection-resolver.js";

export const SESSION_MCP_RUNTIME_MANAGER_KEY = Symbol.for("openclaw.sessionMcpRuntimeManager");
export const SESSION_MCP_RUNTIME_SWEEP_INTERVAL_MS = 60 * 1000;
// Includes runtimes being created or drained; existing sessions never evict for capacity.
export const SESSION_MCP_MAX_LIVE_RUNTIMES = 256;
/**
 * How long a live slot may be owned by NO runtime before the sweep reclaims it.
 *
 * `liveRuntimeSlots` is a strong Set of slot objects while `runtimeSlots` is a WeakMap keyed by
 * runtime, so a runtime that leaves `runtimesBySessionId` without releasing its slot strands that
 * slot permanently: nothing can reach it, the sweep never iterates it, and it counts against the
 * cap for the life of the process. Observed 2026-09-27: a 48-hour-old gateway held all 256 slots
 * with zero MCP child processes alive, and every agent turn failed at the cap until the process was
 * replaced.
 *
 * A grace is required because `reserveRuntimeSlot` adds the slot BEFORE the runtime is registered
 * (the cap deliberately counts runtimes "being created or drained"), so a momentarily unowned slot
 * is normal. Several sweep intervals of continuous orphanhood is not.
 */
export const SESSION_MCP_ORPHAN_SLOT_GRACE_MS = 5 * 60 * 1000;
/**
 * Extra idleness, beyond a runtime's own TTL, after which the sweep stops honouring
 * `activeLeases > 0` or an outstanding work chain and evicts anyway.
 *
 * Both skips are unbounded as written, so any path that takes a lease (or leaves a work-chain
 * entry) and then dies pins its runtime for the life of the process. `lastUsedAt` is the honest
 * signal: a genuinely busy runtime keeps touching it, so a runtime idle this far past its TTL is
 * held by a lease whose owner is gone.
 */
export const SESSION_MCP_PIN_STALE_MS = 30 * 60 * 1000;

/** Idle eviction is opt-in; zero retains the session lifetime. */
export function resolveSessionMcpRuntimeIdleTtlMs(cfg?: OpenClawConfig): number {
  const raw = cfg?.mcp?.sessionIdleTtlMs;
  return typeof raw === "number" && Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : 0;
}

/** Checks whether harness-scoped MCP can affect a turn without loading its runtime graph. */
export function shouldLoadRequesterScopedMcpHarnessRuntime(params: {
  sessionId: string;
  requesterSenderId?: string | null;
}): boolean {
  if (params.requesterSenderId?.trim()) {
    return true;
  }
  const manager = (globalThis as Record<PropertyKey, unknown>)[SESSION_MCP_RUNTIME_MANAGER_KEY] as
    | SessionMcpRuntimeManager
    | undefined;
  return (manager?.getAdvertisedScopedCatalog(params.sessionId)?.tools.length ?? 0) > 0;
}

export type CreateSessionMcpRuntime = (params: {
  sessionId: string;
  sessionKey?: string;
  workspaceDir: string;
  agentDir?: string;
  cfg?: OpenClawConfig;
  manifestRegistry?: Pick<PluginManifestRegistry, "plugins">;
  includeServerNames?: ReadonlySet<string>;
  excludeServerNames?: ReadonlySet<string>;
  safeServerNamesByServer?: ReadonlyMap<string, string>;
  connectionOverrides?: ReadonlyMap<string, McpServerConnectionResolved>;
  redactConnectionServerNames?: ReadonlySet<string>;
  requesterScope?: SessionMcpRequesterScope;
  requesterConnect?: RequesterMcpConnect;
  configFingerprint?: string;
  toolOverrides?: Pick<SessionToolOverrides, "mcpServers" | "mcpToolsDeny">;
}) => SessionMcpRuntime | Promise<SessionMcpRuntime>;
