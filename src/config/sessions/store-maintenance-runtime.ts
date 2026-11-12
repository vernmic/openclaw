// Runtime maintenance config reads current config and falls back for narrow helpers/tests.
import { getRuntimeConfig } from "../config.js";
import { captureRuntimeConfigAsyncReader } from "../io.runtime.js";
import type { SessionMaintenanceConfig } from "../types.base.js";
import type { OpenClawConfig } from "../types.openclaw.js";
import {
  resolveMaintenanceConfigFromInput,
  type ResolvedSessionMaintenanceConfig,
} from "./store-maintenance.js";

function resolveSubagentArchiveAfterMs(config: OpenClawConfig | undefined): number | null {
  const subagents = config?.agents?.defaults?.subagents;
  if (subagents?.archiveOrphanedSessions !== true) {
    return null;
  }
  const minutes = subagents.archiveAfterMinutes ?? 60;
  return Number.isFinite(minutes) && minutes > 0 ? minutes * 60_000 : null;
}

export function resolveMaintenanceConfig(): ResolvedSessionMaintenanceConfig {
  let maintenance: SessionMaintenanceConfig | undefined;
  let subagentArchiveAfterMs: number | null = null;
  try {
    const config = getRuntimeConfig();
    maintenance = config.session?.maintenance;
    subagentArchiveAfterMs = resolveSubagentArchiveAfterMs(config);
  } catch {
    // Config may not be available in narrow test/runtime helpers.
  }
  return resolveMaintenanceConfigFromInput(maintenance, subagentArchiveAfterMs);
}

/** Use the canonical runtime policy even when a caller supplies only routing config. */
export function captureMaintenanceConfigAsyncReader(assertCallerCurrent: () => void) {
  const read = captureRuntimeConfigAsyncReader({ assertCurrent: assertCallerCurrent });
  return Object.assign(
    async () => {
      let maintenance: SessionMaintenanceConfig | undefined;
      let subagentArchiveAfterMs: number | null = null;
      try {
        const config = await read();
        maintenance = config.session?.maintenance;
        subagentArchiveAfterMs = resolveSubagentArchiveAfterMs(config);
      } catch {
        // Match the native maintenance owner's unavailable-config default, never a retired source.
      }
      read.assertCurrent();
      return resolveMaintenanceConfigFromInput(maintenance, subagentArchiveAfterMs);
    },
    { assertCurrent: read.assertCurrent },
  );
}
