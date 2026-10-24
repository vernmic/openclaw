import { resolveSessionArtifactDirectory } from "../config/sessions/paths.js";
import { publishSessionStateArchives } from "../config/sessions/session-accessor.sqlite-archive-store.js";
import { recoverPendingSessionReclamations } from "../config/sessions/session-accessor.sqlite-reclamation-phased.js";
import { runSqliteSessionReclamation } from "../config/sessions/session-accessor.sqlite-reclamation-run.js";
import { resolveSessionReclamationDatabaseOptions } from "../config/sessions/session-accessor.sqlite-reclamation.js";
import { readActiveGatewayLockIdentity } from "../infra/gateway-lock.js";
import { readGatewayOwnerLease } from "../infra/gateway-owner-lease.js";
import { hasActiveGatewayStateOwner } from "../infra/gateway-state-owner.js";
import type { OpenClawAgentDatabaseOptions } from "../state/openclaw-agent-db.js";
import { resolveOpenClawStateSqlitePath } from "../state/openclaw-state-db.paths.js";

/** Borrow only the current database handed off by native startup admission. */
export async function recoverGatewaySessionReclamations(params: {
  database: OpenClawAgentDatabaseOptions;
  assertCurrent?: () => void;
  log: { info(message: string): void; warn(message: string): void };
}): Promise<void> {
  const env = params.database.env ?? process.env;
  const statePath = resolveOpenClawStateSqlitePath(env);
  params.assertCurrent?.();
  if (!hasActiveGatewayStateOwner(statePath)) {
    return;
  }
  const lock = await readActiveGatewayLockIdentity({ env, requireInspection: true });
  params.assertCurrent?.();
  if (lock?.pid !== process.pid || !lock.ownerId) {
    return;
  }
  const assertCurrent = () => {
    params.assertCurrent?.();
    const lease = readGatewayOwnerLease({ env, current: true });
    if (
      !hasActiveGatewayStateOwner(statePath) ||
      lease?.state !== "live" ||
      lease.pid !== process.pid ||
      lease.owner !== lock.ownerId
    ) {
      throw new Error("Startup reclamation Gateway ownership changed or could not be verified");
    }
  };
  assertCurrent();
  const databaseOptions = resolveSessionReclamationDatabaseOptions(params.database);
  const recovered = await recoverPendingSessionReclamations(
    {
      databaseOptions,
      archiveDirectory: resolveSessionArtifactDirectory(databaseOptions.path),
      assertCurrent,
      reportDeclined: (message) => params.log.info(`Session reclamation recovery: ${message}`),
    },
    runSqliteSessionReclamation,
  );
  assertCurrent();
  if (recovered > 0) {
    await publishSessionStateArchives(databaseOptions, []);
    assertCurrent();
    params.log.info(
      `Recovered ${recovered} session reclamation job(s) for ${databaseOptions.agentId}`,
    );
  }
}
