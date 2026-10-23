import { containsAsciiControlCharacter } from "@openclaw/normalization-core/string-normalization";
import { resolveStateDir } from "../config/paths.js";
import { embedSessionColdArchivesInSnapshot } from "../config/sessions/session-cold-storage-backup.js";
import { withPreparedSqliteSnapshot } from "../infra/sqlite-readonly-location-cleanup.js";
import { prepareSqliteReadOnlyLocation } from "../infra/sqlite-snapshot-source.js";
import {
  createVerifiedSqliteSnapshot,
  type SqliteSnapshotValidator,
} from "../infra/sqlite-snapshot.js";
import { assertNotUpdateCapturePath } from "../infra/update-capture-paths.js";
import { isValidAgentId, normalizeAgentId } from "../routing/session-key.js";
import { assertOpenClawAgentDatabaseForMaintenance } from "../state/openclaw-agent-db-maintenance.js";
import { assertOpenClawStateDatabaseForMaintenance } from "../state/openclaw-state-db.js";
import {
  sanitizeOpenClawGlobalStateSnapshot,
  sanitizeOpenClawStateLeaseRows,
} from "../state/openclaw-state-snapshot-sanitizer.js";
import type { SnapshotDatabaseIdentity, SnapshotDatabaseRef } from "./snapshot-provider.js";

export function normalizeSnapshotIdentity(
  identity: SnapshotDatabaseIdentity,
): SnapshotDatabaseIdentity {
  if (identity.role === "global") {
    return identity;
  }
  if (identity.role === "agent") {
    const agentId = normalizeAgentId(identity.agentId);
    if (!isValidAgentId(identity.agentId) || agentId !== identity.agentId) {
      throw new Error(`SQLite snapshot agent id must be canonical: ${identity.agentId}`);
    }
    return { role: "agent", agentId };
  }
  const id = identity.id.trim();
  if (!id || id !== identity.id || id.length > 256 || containsAsciiControlCharacter(id)) {
    throw new Error("SQLite snapshot generic database id is invalid.");
  }
  return { role: "generic", id };
}

export function buildSnapshotValidator(
  identity: SnapshotDatabaseIdentity,
): SqliteSnapshotValidator {
  if (identity.role === "global") {
    return (database, pathname) =>
      assertOpenClawStateDatabaseForMaintenance(database, { pathname });
  }
  if (identity.role === "agent") {
    return (database, pathname) =>
      assertOpenClawAgentDatabaseForMaintenance(database, {
        agentId: identity.agentId,
        pathname,
      });
  }
  return () => undefined;
}

/** Produce the canonical sanitized, verified copy; compact by default. */
export async function createOpenClawSnapshotCopy(params: {
  database: SnapshotDatabaseRef;
  targetPath: string;
  preserveRowIds?: boolean;
}): Promise<{ identity: SnapshotDatabaseIdentity; path: string; userVersion: number }> {
  assertNotUpdateCapturePath(params.database.path, resolveStateDir());
  const identity = normalizeSnapshotIdentity(params.database.identity);
  const capture = (sourcePath: string) =>
    createVerifiedSqliteSnapshot({
      sourcePath,
      preserveRowIds: params.preserveRowIds,
      targetPath: params.targetPath,
      requireNonEmptySource: identity.role !== "generic",
      transform: async (database) => {
        if (identity.role === "global") {
          sanitizeOpenClawGlobalStateSnapshot(database);
        } else if (identity.role === "agent") {
          sanitizeOpenClawStateLeaseRows(database);
        }
        await embedSessionColdArchivesInSnapshot({
          database,
          sourceStorePath: params.database.path,
        });
      },
      validate: buildSnapshotValidator(identity),
    });
  // Acquire a committed online image in the native isolated child. Direct
  // artifact-preserving copies of a live WAL can retry forever under activity.
  // Preserve IDs on this private image without borrowing the Gateway's writer.
  const result = params.preserveRowIds
    ? await withPreparedSqliteSnapshot(
        await prepareSqliteReadOnlyLocation(params.database.path, { allowLiveOwner: false }),
        capture,
      )
    : await capture(params.database.path);
  return { identity, ...result };
}
