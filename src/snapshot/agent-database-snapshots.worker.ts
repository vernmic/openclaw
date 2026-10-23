import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { resolveConfiguredAgentDatabaseTargets } from "../config/sessions/targets.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { withSqliteReadOnlyWorkerScope } from "../infra/sqlite-readonly-worker.js";
import { inspectDatabasePathIdentitySync } from "../infra/sqlite-worker-identity.js";
import { serveWorkerTasks } from "../infra/worker-task-server.js";
import type { OpenClawRegisteredAgentDatabase } from "../state/openclaw-agent-db-contract.js";
import {
  snapshotAgentDatabase,
  type AgentDatabaseSnapshotResult,
  type AgentDatabaseSnapshotTarget,
} from "./agent-database-snapshots.js";

export type AgentDatabaseSnapshotCommand =
  | { type: "discover"; config: OpenClawConfig; registered: OpenClawRegisteredAgentDatabase[] }
  | {
      type: "snapshot";
      target: AgentDatabaseSnapshotTarget;
      stateDir: string;
      previous?: AgentDatabaseSnapshotResult;
    };
export type AgentDatabaseSnapshotReply =
  | { type: "targets"; targets: AgentDatabaseSnapshotTarget[] }
  | { type: "snapshot"; result: AgentDatabaseSnapshotResult };

serveWorkerTasks<AgentDatabaseSnapshotReply>(async (input, _channel, control) => {
  if (!isRecord(input) || (input.type !== "discover" && input.type !== "snapshot")) {
    throw new Error("Unknown agent database snapshot command");
  }
  // SAFETY: Only this Gateway's private, typed pool sends snapshot commands.
  const command = input as AgentDatabaseSnapshotCommand;
  // Cancellation/retirement must join native acquisition, publication and cleanup,
  // including the isolated read-only child retained by the native snapshot owner.
  return control.runNativeSection(() =>
    withSqliteReadOnlyWorkerScope<AgentDatabaseSnapshotReply>(async () => {
      if (command.type === "snapshot") {
        return { type: "snapshot", result: await snapshotAgentDatabase(command) };
      }
      // The Gateway's canonical state-read owner supplies registry rows. This
      // generic worker must not open/close the live global database itself.
      const registered = command.registered;
      const configured = resolveConfiguredAgentDatabaseTargets(command.config, {
        env: process.env,
        registeredDatabases: registered,
      });
      const targets = new Map<string, AgentDatabaseSnapshotTarget>();
      for (const target of [...configured, ...registered]) {
        let identity = target.path;
        try {
          identity = inspectDatabasePathIdentitySync(target.path)?.key ?? identity;
        } catch {
          // One inaccessible source is reported by its own capture, not discovery.
        }
        targets.set(`${target.agentId}\0${identity}`, {
          agentId: target.agentId,
          path: target.path,
        });
      }
      return { type: "targets", targets: [...targets.values()] };
    }),
  );
});
