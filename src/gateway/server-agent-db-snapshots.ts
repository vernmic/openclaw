import { getRuntimeConfig } from "../config/io.js";
import { resolveStateDir } from "../config/state-dir.js";
import type { GatewayScheduler } from "../infra/gateway-scheduler.js";
import { runtimeProcessEntrypoints } from "../infra/runtime-process-entrypoints.js";
import { resolveRuntimeWorkerUrl } from "../infra/runtime-worker-url.js";
import { WorkerTaskPool } from "../infra/worker-task-pool.js";
import { getGatewayRestartDrainSignal } from "../process/gateway-work-admission.js";
import type { AgentDatabaseSnapshotResult } from "../snapshot/agent-database-snapshots.js";
import type {
  AgentDatabaseSnapshotCommand,
  AgentDatabaseSnapshotReply,
} from "../snapshot/agent-database-snapshots.worker.js";
import { executeExistingOpenClawStateRead } from "../state/openclaw-state-db-readonly.js";
import { scheduleGatewayIdleTask } from "./server-idle-task.js";

/** Gateway scheduling/custody only; SQLite and certification stay with native worker owners. */
export function startGatewayAgentDatabaseSnapshots(
  runtime: { scheduler: GatewayScheduler; lifecycle: { closePreludeStarted: boolean } },
  log: { info: (message: string) => void; warn: (message: string) => void },
) {
  const params = {
    scheduler: runtime.scheduler,
    getConfig: getRuntimeConfig,
    isClosing: () => runtime.lifecycle.closePreludeStarted,
    log,
  };
  const certificates = new Map<string, AgentDatabaseSnapshotResult>();
  let pool: WorkerTaskPool<AgentDatabaseSnapshotCommand, AgentDatabaseSnapshotReply> | undefined;
  let stopped = false;
  let stopping: Promise<void> | undefined;
  const run = (command: AgentDatabaseSnapshotCommand) => {
    pool ??= new WorkerTaskPool({
      workerUrl: resolveRuntimeWorkerUrl(runtimeProcessEntrypoints.agentDatabaseSnapshots),
      maxWorkers: 1,
      maxPendingTasks: 1,
      idleTimeoutMs: 30_000,
    });
    return pool.run(command, { inputBytes: Buffer.byteLength(JSON.stringify(command)) });
  };
  const scheduled = scheduleGatewayIdleTask({
    id: "maintenance:certified-agent-db-snapshots",
    scheduler: params.scheduler,
    delayMs: 30_000,
    retryDelayMs: 30_000,
    repeatDelayMs: 60 * 60_000,
    isClosing: () => stopped || params.isClosing(),
    // A serial dedicated worker bounds pressure. Idle-only admission would starve
    // restore points indefinitely on a continuously active Gateway.
    isBusy: () => false,
    run: async () => {
      const counts = { certified: 0, unchanged: 0, absent: 0, failed: 0 };
      try {
        // Retain the canonical native state-read owner on the Gateway; only its
        // small typed registry result crosses into the snapshot worker.
        const registry = await executeExistingOpenClawStateRead(
          {},
          { type: "agentDatabaseRegistry.read" },
          { live: true },
        );
        if (
          !registry?.ok ||
          registry.type !== "agentDatabaseRegistry.read" ||
          registry.result.status !== "available"
        ) {
          throw new Error("Agent database registry is unavailable for certified snapshots");
        }
        const discovered = await run({
          type: "discover",
          config: params.getConfig(),
          registered: registry.result.entries,
        });
        if (discovered.type !== "targets") {
          throw new Error("Agent snapshot discovery returned an invalid reply");
        }
        const currentKeys = new Set(
          discovered.targets.map((target) => `${target.agentId}\0${target.path}`),
        );
        for (const key of certificates.keys()) {
          if (!currentKeys.has(key)) {
            certificates.delete(key);
          }
        }
        for (const target of discovered.targets) {
          if (
            stopped ||
            params.isClosing() ||
            params.scheduler.signal.aborted ||
            getGatewayRestartDrainSignal().aborted
          ) {
            break;
          }
          const key = `${target.agentId}\0${target.path}`;
          try {
            const reply = await run({
              type: "snapshot",
              target,
              stateDir: resolveStateDir(),
              previous: certificates.get(key),
            });
            if (reply.type !== "snapshot") {
              throw new Error("Agent snapshot returned an invalid reply");
            }
            counts[reply.result.outcome]++;
            if (reply.result.fingerprint) {
              certificates.set(key, reply.result);
            } else {
              certificates.delete(key);
            }
          } catch (error) {
            certificates.delete(key);
            counts.failed++;
            params.log.warn(
              `Certified agent snapshot failed for ${target.agentId}: ${String(error)}`,
            );
          }
        }
      } catch (error) {
        counts.failed++;
        params.log.warn(`Certified agent snapshot discovery failed: ${String(error)}`);
      } finally {
        params.log.info(
          `Agent DB snapshot pass: ${Object.entries(counts)
            .map(([key, count]) => `${key}=${count}`)
            .join(" ")}`,
        );
      }
    },
    log: params.log,
    errorMessage: "Certified agent snapshot pass failed",
  });
  return {
    stop: () =>
      (stopping ??= (async () => {
        stopped = true;
        // The scheduler joins the current artifact before the pool can terminate;
        // shutdown never abandons a native child or publication to a detached task.
        await scheduled.stop();
        await pool?.close();
        certificates.clear();
      })()),
  };
}
