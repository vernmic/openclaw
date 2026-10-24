import { randomUUID } from "node:crypto";
import path from "node:path";
import { readDatabasePathIdentitySync } from "../../infra/sqlite-worker-identity.js";
import { KeyedAsyncQueue } from "../../plugin-sdk/keyed-async-queue.js";
import {
  collectActiveSessionWorkAdmissions,
  runExclusiveSessionLifecycleMutation,
} from "../../sessions/session-lifecycle-admission.js";
import { resolveGlobalSingleton } from "../../shared/global-singleton.js";
import type { MaterializedSessionStateDeletePlan } from "./session-accessor.sqlite-archive-types.js";
import { materializeSessionStateDeletePlans } from "./session-accessor.sqlite-archive.js";
import type {
  ReclamationDatabaseOptions,
  ReclamationRecoveryRow,
  SqliteSessionReclamationPlan,
  SqliteSessionReclamationResult,
} from "./session-accessor.sqlite-lifecycle-types.js";
import {
  deserializeReclamationPlan,
  type ResumableReclamationPlan,
} from "./session-accessor.sqlite-reclamation-plan.js";
import type { runSqliteSessionReclamation } from "./session-accessor.sqlite-reclamation-run.js";
import { prepareSessionMaintenancePreservation } from "./store-maintenance-preserve.js";

type RunParams = Parameters<typeof runSqliteSessionReclamation>[0];
type Run = (params: RunParams) => Promise<SqliteSessionReclamationResult>;
const drives = resolveGlobalSingleton(
  Symbol.for("openclaw.nativeResumableReclamation"),
  () => new KeyedAsyncQueue(),
);

function assertNoLiveWork(plan: ResumableReclamationPlan): void {
  const identities = new Set([
    plan.sessionId,
    ...plan.materializedPlans.flatMap((item) =>
      item.snapshot.sessionKey ? [item.snapshot.sessionKey] : [],
    ),
  ]);
  // Process-held admissions are live authority; saved plan protection is only a comparison fact.
  for (const admissions of collectActiveSessionWorkAdmissions().values()) {
    if ([...admissions].some((id) => identities.has(id))) {
      throw new Error("Reclamation session has current live work");
    }
  }
}

/** Drive the same native worker once per durable phase; bytes transfer only on archive append. */
export async function runPhasedSessionReclamation(
  params: RunParams & { plan: ResumableReclamationPlan },
  run: Run,
  recovery?: ReclamationRecoveryRow,
): Promise<SqliteSessionReclamationResult> {
  return drives.enqueue(
    `${path.resolve(params.plan.databaseOptions.path)}\0${params.plan.sessionId}`,
    async () => {
      const ownerId = randomUUID();
      const id = recovery?.id ?? randomUUID();
      let cursor = recovery?.cursor ?? 0;
      let last: SqliteSessionReclamationResult | undefined;
      const livePlans = params.plan.materializedPlans;
      const assertCurrent = () => {
        params.assertCommitAllowed?.();
        if (cursor < 3) {
          assertNoLiveWork(params.plan);
        }
      };
      const phases = ["guard", "archive", "delete", "complete"] as const;
      for (const [index, phase] of phases.entries()) {
        if (cursor > index) {
          continue;
        }
        assertCurrent();
        const preservation = await prepareSessionMaintenancePreservation(
          params.plan.databaseOptions.path,
        );
        try {
          assertCurrent();
          const assertPhaseCurrent = () => {
            assertCurrent();
            const snapshot = preservation.capture();
            const identities = new Set([
              params.plan.sessionId,
              ...params.plan.materializedPlans.flatMap((item) =>
                item.snapshot.sessionKey ? [item.snapshot.sessionKey] : [],
              ),
            ]);
            if (
              cursor < 3 &&
              [...snapshot.providerKeys, ...snapshot.workIdentities].some((key) =>
                identities.has(key),
              )
            ) {
              throw new Error("Reclamation current preservation owner protected this session");
            }
          };
          assertPhaseCurrent();
          let materializedPlans: MaterializedSessionStateDeletePlan[] = livePlans.map((item) => ({
            ...item,
            archive: null,
            archivedTranscript: null,
          }));
          if (phase === "archive") {
            materializedPlans = recovery
              ? await materializeSessionStateDeletePlans(materializedPlans)
              : livePlans;
            assertPhaseCurrent();
          }
          const plan: ResumableReclamationPlan = {
            ...params.plan,
            materializedPlans,
            maintenanceRunBasis: preservation.subagentRunBasis,
            reclamation: {
              id,
              ownerId,
              phase,
              archiveSha256: livePlans[0]?.archive?.sha256,
              ...(recovery && !last ? { expectedOwnerId: recovery.ownerId } : {}),
            },
          };
          last = await run({ ...params, plan, assertCommitAllowed: assertPhaseCurrent });
          // A successful native COMMIT is authoritative even if cancellation arrives afterward.
          // The caller publishes settled receipts before releasing native lifetime custody.
          const receipt = last.reclamationReceipt;
          if (!receipt) {
            return last;
          }
          if (receipt.state === "failed") {
            throw new Error(receipt.error ?? "Resumable reclamation failed");
          }
          if (receipt.state === "declined" || receipt.state === "done") {
            return last;
          }
          if (receipt.cursor <= cursor) {
            throw new Error("Reclamation phase did not advance its receipt");
          }
          cursor = receipt.cursor;
        } finally {
          preservation.dispose();
        }
      }
      if (!last) {
        throw new Error("Reclamation recovery has no next phase");
      }
      return last;
    },
  );
}

/** Bounded startup recovery receives only current configured locators, never saved root authority. */
export async function recoverPendingSessionReclamations(
  params: {
    databaseOptions: ReclamationDatabaseOptions;
    archiveDirectory: string;
    assertCurrent: () => void;
    reportDeclined?: (message: string) => void;
  },
  run: Run,
): Promise<number> {
  const plan: Extract<SqliteSessionReclamationPlan, { kind: "reclamation-pending" }> = {
    kind: "reclamation-pending",
    databaseOptions: params.databaseOptions,
    archiveDirectory: params.archiveDirectory,
    materializedPlans: [],
  };
  let recovered = 0;
  for (;;) {
    const pending = await run({
      plan,
      assertCommitAllowed: params.assertCurrent,
      forceInProcess: false,
    });
    params.assertCurrent();
    if (pending.kind !== "reclamation-pending") {
      throw new Error("Reclamation recovery returned another result");
    }
    if (pending.value.length === 0) {
      break;
    }
    for (const row of pending.value) {
      params.assertCurrent();
      const current = readDatabasePathIdentitySync(params.databaseOptions.path);
      const sourceMatches =
        row.sourcePath === current.canonicalPath &&
        `file:${row.sourceIdentity}` === current.key &&
        row.sourceBirthtime === (current.birthtime ?? null) &&
        path.resolve(row.archiveDirectory) === path.resolve(params.archiveDirectory);
      let restored: ResumableReclamationPlan;
      try {
        if (!sourceMatches) {
          throw new Error(
            "Reclamation copied source identity or archive root changed; reconcile ledger before retrying",
          );
        }
        restored = deserializeReclamationPlan(
          row.planJson,
          params.databaseOptions,
          params.archiveDirectory,
        );
      } catch (error) {
        const message = String(error);
        await run({
          plan: {
            ...plan,
            reject: {
              id: row.id,
              expectedOwnerId: row.ownerId,
              error: message,
              state: sourceMatches ? "failed" : "declined",
            },
          },
          forceInProcess: false,
          assertCommitAllowed: params.assertCurrent,
        });
        params.reportDeclined?.(message);
        continue;
      }
      try {
        await runExclusiveSessionLifecycleMutation("history-evict", {
          scope: params.databaseOptions.path,
          identities: [
            restored.sessionId,
            ...restored.materializedPlans.flatMap((item) =>
              item.snapshot.sessionKey ? [item.snapshot.sessionKey] : [],
            ),
          ],
          run: async () => {
            const result = await runPhasedSessionReclamation(
              { plan: restored, forceInProcess: false, assertCommitAllowed: params.assertCurrent },
              run,
              row,
            );
            if (result.reclamationReceipt?.state === "declined") {
              params.reportDeclined?.(result.reclamationReceipt.error ?? "Reclamation declined");
            }
            if (result.reclamationReceipt?.state === "done") {
              recovered += 1;
            }
          },
        });
      } catch (error) {
        params.assertCurrent();
        params.reportDeclined?.(String(error));
      }
    }
    plan.afterId = pending.value.at(-1)?.id;
    if (pending.value.length < 64) {
      break;
    }
  }
  return recovered;
}
