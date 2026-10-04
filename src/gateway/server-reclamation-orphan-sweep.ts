import { existsSync } from "node:fs";
import { listAgentIds } from "../agents/agent-scope-config.js";
import { sweepReclamationOrphansForAgentDatabase } from "../config/sessions/session-accessor.sqlite-reclamation-sweep.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { openOpenClawAgentDatabase } from "../state/openclaw-agent-db.js";
import { resolveOpenClawAgentSqlitePath } from "../state/openclaw-agent-db.paths.js";

/**
 * Gateway-boot recovery drive for the reclamation ledger: finishes entries a previous process left
 * mid-workflow. Boot pull IS the crash recovery - in-progress-first queue ordering makes resume the
 * normal path, so there is no separate recovery branch to rot. An empty queue is a no-op.
 *
 * Scope is every CONFIGURED agent, not only those this gateway has already opened: agent databases
 * open lazily, so at boot the set of already-owned ones is effectively empty and scoping to it would
 * make the sweep a no-op against the orphans it exists to finish.
 */
export async function sweepReclamationOrphansAtBoot(params: {
  cfg: OpenClawConfig;
  log: { warn: (message: string) => void };
  env?: NodeJS.ProcessEnv;
  isClosing?: () => boolean;
}): Promise<void> {
  for (const agentId of listAgentIds(params.cfg)) {
    if (params.isClosing?.()) {
      return;
    }
    const path = resolveOpenClawAgentSqlitePath({ agentId, env: params.env });
    // Opening runs ensureOpenClawAgentSchema, so sweeping a never-used agent would CREATE its tables
    // as a side effect of looking for work that cannot be there. An orphan requires a prior
    // reclamation drive, which requires this file to already exist, so a stat is sufficient.
    if (!existsSync(path)) {
      continue;
    }
    try {
      // Sequential by agent: each drive takes exclusive write windows on its own database, and the
      // caller's idle gate reasons about one unit of work at a time.
      const database = openOpenClawAgentDatabase({ agentId, env: params.env, path });
      await sweepReclamationOrphansForAgentDatabase({
        database,
        onError: (error, entry) => {
          params.log.warn(
            `gateway: reclamation orphan sweep entry failed for agent ${agentId}` +
              `${entry ? ` (session ${entry.sessionId})` : ""}: ${String(error)}`,
          );
        },
      });
      // Never close the handle: openOpenClawAgentDatabase returns a CACHED per-agent database that
      // the rest of the gateway shares. Disposal belongs to the database lifecycle, not to us.
    } catch (error) {
      // One locked or corrupt agent database must not starve recovery for every other agent.
      params.log.warn(
        `gateway: reclamation orphan sweep failed for agent ${agentId}: ${String(error)}`,
      );
    }
  }
}
