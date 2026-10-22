// Runtime maintenance config reads current config and falls back for narrow helpers/tests.
import { getRuntimeConfig } from "../config.js";
import { captureRuntimeConfigAsyncReader } from "../io.runtime.js";
import type { SessionMaintenanceConfig } from "../types.base.js";
import {
  resolveMaintenanceConfigFromInput,
  type ResolvedSessionMaintenanceConfig,
} from "./store-maintenance.js";

export function resolveMaintenanceConfig(): ResolvedSessionMaintenanceConfig {
  let maintenance: SessionMaintenanceConfig | undefined;
  let subagentArchiveAfterMinutes: number | undefined;
  try {
    const config = getRuntimeConfig();
    maintenance = config.session?.maintenance;
    subagentArchiveAfterMinutes = config.agents?.defaults?.subagents?.archiveAfterMinutes;
  } catch {
    // Config may not be available in narrow test/runtime helpers.
  }
  return resolveMaintenanceConfigFromInput(maintenance, subagentArchiveAfterMinutes);
}

/** Use the canonical runtime policy even when a caller supplies only routing config. */
export function captureMaintenanceConfigAsyncReader(assertCallerCurrent: () => void) {
  const read = captureRuntimeConfigAsyncReader({ assertCurrent: assertCallerCurrent });
  return Object.assign(
    async () => {
      let maintenance: SessionMaintenanceConfig | undefined;
      let subagentArchiveAfterMinutes: number | undefined;
      try {
        const config = await read();
        maintenance = config.session?.maintenance;
        subagentArchiveAfterMinutes = config.agents?.defaults?.subagents?.archiveAfterMinutes;
      } catch {
        // Match the native maintenance owner's unavailable-config default, never a retired source.
        read.assertCurrent();
      }
      read.assertCurrent();
      return resolveMaintenanceConfigFromInput(maintenance, subagentArchiveAfterMinutes);
    },
    { assertCurrent: read.assertCurrent },
  );
}
