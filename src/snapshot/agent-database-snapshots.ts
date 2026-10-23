import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { hasErrnoCode } from "../infra/errno.js";
import type { FileMutationFingerprint } from "../infra/file-descriptor.js";
import { resolveRequiredOsHomeDir } from "../infra/home-dir.js";
import { isValidAgentId } from "../routing/session-key.js";
import {
  createLocalSqliteSnapshotProvider,
  ensurePrivateSnapshotRepositoryRoot,
} from "./local-repository.js";

export type AgentDatabaseSnapshotTarget = { agentId: string; path: string };
export type AgentDatabaseSnapshotResult = {
  outcome: "certified" | "unchanged" | "absent";
  fingerprint?: string;
  snapshotPath?: string;
  artifactFingerprint?: string;
};

function fingerprint(file: FileMutationFingerprint): string {
  return [file.dev, file.ino, file.birthtimeNs, file.size, file.mtimeNs, file.ctimeNs].join(":");
}

/** Stat only: closing raw source descriptors could release a native writer's POSIX locks. */
export async function readAgentDatabaseSnapshotFingerprint(
  pathname: string,
): Promise<string | undefined> {
  const files = await Promise.all(
    ["", "-wal", "-journal"].map(async (suffix) => {
      try {
        const file = await fs.stat(pathname + suffix, { bigint: true });
        if (!file.isFile()) {
          throw new Error(`SQLite snapshot source is not a regular file: ${pathname + suffix}`);
        }
        // A read-only open can leave an empty WAL. It carries no committed changes.
        return suffix && file.size === 0n ? undefined : file;
      } catch (error) {
        if (hasErrnoCode(error, "ENOENT")) {
          return undefined;
        }
        throw error;
      }
    }),
  );
  if (!files[0]) {
    return undefined;
  }
  return files.map((file) => (file ? fingerprint(file) : "empty")).join("|");
}

async function readArtifactFingerprint(snapshotPath: string): Promise<string | undefined> {
  try {
    try {
      await fs.lstat(path.join(snapshotPath, ".pending"));
      return undefined;
    } catch (error) {
      if (!hasErrnoCode(error, "ENOENT")) {
        throw error;
      }
    }
    const files = await Promise.all(
      ["database.sqlite", "manifest.json"].map((name) =>
        fs.lstat(path.join(snapshotPath, name), { bigint: true }),
      ),
    );
    return files.every((file) => file.isFile() && file.nlink === 1n)
      ? files.map(fingerprint).join("|")
      : undefined;
  } catch {
    // An absent/replaced artifact revokes the optimization, never source admission.
    return undefined;
  }
}

/** One native artifact publication; mutation only invalidates the unchanged optimization. */
export async function snapshotAgentDatabase(params: {
  target: AgentDatabaseSnapshotTarget;
  stateDir: string;
  previous?: AgentDatabaseSnapshotResult;
}): Promise<AgentDatabaseSnapshotResult> {
  if (!isValidAgentId(params.target.agentId)) {
    throw new Error("Agent snapshot requires a canonical agent id");
  }
  let sourcePath: string;
  try {
    sourcePath = await fs.realpath(params.target.path);
  } catch (error) {
    if (hasErrnoCode(error, "ENOENT")) {
      return { outcome: "absent" };
    }
    throw error;
  }
  const before = await readAgentDatabaseSnapshotFingerprint(sourcePath);
  if (!before) {
    return { outcome: "absent" };
  }
  if (
    before === params.previous?.fingerprint &&
    params.previous.snapshotPath &&
    params.previous.artifactFingerprint &&
    params.previous.artifactFingerprint ===
      (await readArtifactFingerprint(params.previous.snapshotPath))
  ) {
    return { ...params.previous, outcome: "unchanged" };
  }
  // State roots can have shared ancestors. Retained artifacts require the native
  // private-user trust boundary and must live outside temporary/cache cleanup.
  const privateRoot = path.join(resolveRequiredOsHomeDir(), ".openclaw-backups");
  await ensurePrivateSnapshotRepositoryRoot(privateRoot);
  const statePath = await fs.realpath(params.stateDir);
  const hashPath = (pathname: string) =>
    createHash("sha256")
      .update(process.platform === "win32" ? pathname.toLowerCase() : pathname)
      .digest("hex");
  const repositoryPath = path.join(
    privateRoot,
    "agent-db-snapshots",
    hashPath(statePath),
    params.target.agentId,
    hashPath(sourcePath),
  );
  await ensurePrivateSnapshotRepositoryRoot(repositoryPath);
  const main = await fs.stat(sourcePath, { bigint: true });
  const wal = await fs.stat(`${sourcePath}-wal`, { bigint: true }).catch((error: unknown) => {
    if (hasErrnoCode(error, "ENOENT")) {
      return undefined;
    }
    throw error;
  });
  const space = await fs.statfs(repositoryPath, { bigint: true });
  // Keep old restore points until publication succeeds; acquisition, validation,
  // and publication can each need a private image on this filesystem.
  const required = (main.size + (wal?.size ?? 0n)) * 4n + 256n * 1024n * 1024n;
  if (space.bavail * space.bsize < required) {
    throw new Error(
      `Insufficient disk space for certified agent snapshot: ${params.target.agentId}`,
    );
  }
  const provider = createLocalSqliteSnapshotProvider({
    repositoryPath,
    preserveRowIds: true,
    allowedDatabaseRoles: ["agent"],
  });
  const snapshot = await provider.createRetained(
    { path: sourcePath, identity: { role: "agent", agentId: params.target.agentId } },
    6,
  );
  const artifactFingerprint = await readArtifactFingerprint(snapshot.ref.path);
  const after = await readAgentDatabaseSnapshotFingerprint(sourcePath).catch(() => undefined);
  // An online backup pins a committed SQLite read transaction. Continuous writes
  // must not prevent that artifact from becoming a restore point.
  return {
    outcome: "certified",
    snapshotPath: snapshot.ref.path,
    artifactFingerprint,
    ...(before === after && artifactFingerprint ? { fingerprint: before } : {}),
  };
}
