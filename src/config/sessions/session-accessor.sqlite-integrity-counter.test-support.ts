import type { WorkerOptions } from "node:worker_threads";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { resolveSqliteFilesystemPath } from "../../infra/node-sqlite.js";

const integrityCounterPreload = `
  import { DatabaseSync } from "node:sqlite";
  import { parentPort, workerData } from "node:worker_threads";
  const databasePath = workerData.integrityDatabasePath ?? (
    workerData.operation === "reclaim"
      ? workerData.databaseOptions.path
      : workerData.plan?.databaseOptions.path
  );
  const prepare = DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.prepare = function(sql) {
    const statement = prepare.call(this, sql);
    // Native Windows opens preserve the SQLite namespace; direct fixture opens need not.
    if ((this.location() === databasePath ||
         this.location() === workerData.integrityDatabaseLocation) &&
        (/^PRAGMA integrity_check;?$/i.test(sql.trim()) ||
         sql.trim() === "PRAGMA integrity_check('sqlite_schema');")) {
      for (const method of ["all", "get", "iterate", "run"]) {
        const execute = statement[method].bind(statement);
        statement[method] = (...args) => {
          Atomics.add(new Int32Array(workerData.integrityChecks), 0, 1);
          const held = workerData.integrityFirstCheck
            ? Atomics.compareExchange(new Int32Array(workerData.integrityFirstCheck), 0, 0, 1) === 0
            : undefined;
          const report = (phase) => parentPort.postMessage({
            type: "test-integrity-check", phase,
            ...(held === undefined ? {} : { held }),
          });
          if (workerData.integrityRelease) {
            report("checking");
            if (held !== false) {
              Atomics.wait(new Int32Array(workerData.integrityRelease), 0, 0);
            }
          }
          const result = execute(...args);
          if (workerData.integrityRelease) {
            report("checked");
          }
          return result;
        };
      }
    }
    return statement;
  };
`;

/** Count native admission gates once, through the full check or the schema table. */
export function withWorkerSqliteIntegrityCounter(
  options: WorkerOptions | undefined,
  counts: SharedArrayBuffer | undefined,
  release?: SharedArrayBuffer,
  databasePath?: string,
  firstCheck?: SharedArrayBuffer,
): WorkerOptions | undefined {
  const data: unknown = options?.workerData;
  const boot = isRecord(data) ? data : undefined;
  const plan = isRecord(boot?.plan) ? boot.plan : undefined;
  const target =
    databasePath ??
    (boot?.operation === "reclaim" && isRecord(boot.databaseOptions)
      ? boot.databaseOptions.path
      : isRecord(plan?.databaseOptions)
        ? plan.databaseOptions.path
        : undefined);
  const integrityDatabaseLocation =
    typeof target === "string" ? resolveSqliteFilesystemPath(target) : undefined;
  return counts
    ? {
        ...options,
        execArgv: [
          ...(options?.execArgv ?? []),
          "--import",
          `data:text/javascript,${encodeURIComponent(integrityCounterPreload)}`,
        ],
        workerData: {
          ...options?.workerData,
          integrityChecks: counts,
          integrityDatabaseLocation,
          ...(databasePath ? { integrityDatabasePath: databasePath } : {}),
          ...(release ? { integrityRelease: release } : {}),
          ...(firstCheck ? { integrityFirstCheck: firstCheck } : {}),
        },
      }
    : options;
}
