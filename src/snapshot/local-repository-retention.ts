import type { BigIntStats, Stats } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { sameFileIdentity, type FileIdentityStat } from "@openclaw/fs-safe/advanced";
import { pinDirectory, syncDirectoryIfSupported } from "../infra/directory-durability.js";
import {
  assertDirectoryIdentity,
  assertTrustedStagingRoot,
} from "./local-repository-directory-policy.js";
import {
  SNAPSHOT_MANIFEST_FILENAME,
  SNAPSHOT_SQLITE_FILENAME,
  type SnapshotDatabaseManifest,
  type SnapshotRef,
} from "./snapshot-provider.js";

// Certified restore-point retention (F15), split from local-repository.ts (line cap) together
// with the file-identity helpers it shares with that module. Nothing here imports it back.

type RetainedSnapshot = { ref: SnapshotRef; manifest: { database: SnapshotDatabaseManifest } };

export function assertRetainedKeep(keep: number): void {
  if (!Number.isSafeInteger(keep) || keep < 1) {
    throw new Error("SQLite snapshot retention must retain at least one restore point");
  }
}

export async function lstatIfExists(pathname: string): Promise<BigIntStats | undefined> {
  try {
    return await fs.lstat(pathname, { bigint: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
}

export async function removePublishedSnapshotDirectoryIfOwned(
  directoryPath: string,
  expectedIdentity: FileIdentityStat,
  publishedEntries: ReadonlyMap<string, FileIdentityStat>,
): Promise<boolean> {
  const currentIdentity = await lstatIfExists(directoryPath);
  if (
    !currentIdentity ||
    currentIdentity.isSymbolicLink() ||
    !currentIdentity.isDirectory() ||
    !sameFileIdentity(currentIdentity, expectedIdentity)
  ) {
    return false;
  }
  const entries = await fs.readdir(directoryPath, { withFileTypes: true });
  for (const entry of entries) {
    const expectedEntryIdentity = publishedEntries.get(entry.name);
    if (!expectedEntryIdentity || entry.isSymbolicLink() || !entry.isFile()) {
      continue;
    }
    const entryPath = path.join(directoryPath, entry.name);
    const currentEntryIdentity = await fs.lstat(entryPath, { bigint: true });
    if (sameFileIdentity(currentEntryIdentity, expectedEntryIdentity)) {
      await fs.unlink(entryPath);
    }
  }
  if ((await fs.readdir(directoryPath)).length > 0) {
    return false;
  }
  await fs.rmdir(directoryPath);
  return true;
}

/** Delete restore points beyond `keep`, never the one just certified, and only exact owned files. */
export async function rotateRetainedSnapshots(params: {
  snapshot: RetainedSnapshot;
  snapshots: readonly RetainedSnapshot[];
  keep: number;
  repositoryPath: string;
  resolveSnapshotDirectory: (ref: SnapshotRef) => Promise<string>;
  assertSnapshotContents: (snapshotDir: string) => Promise<void>;
}): Promise<void> {
  const { snapshot, snapshots, keep } = params;
  // Retention belongs to one named source repository, never a mixed backup repository.
  for (const candidate of snapshots) {
    const current = candidate.manifest.database;
    const expected = snapshot.manifest.database;
    if (
      current.role !== expected.role ||
      current.basename !== expected.basename ||
      (current.role === "agent" &&
        (expected.role !== "agent" || current.agentId !== expected.agentId)) ||
      (current.role === "generic" && (expected.role !== "generic" || current.id !== expected.id))
    ) {
      throw new Error("SQLite retained snapshot repository contains another database owner");
    }
  }
  // Pin the newly certified point even if the wall clock moved backwards.
  const previous = snapshots.filter((candidate) => candidate.ref.path !== snapshot.ref.path);
  const repositoryIdentity = await fs.lstat(params.repositoryPath);
  const trustedRepository = await assertTrustedStagingRoot(
    repositoryIdentity,
    params.repositoryPath,
  );
  for (const candidate of previous.slice(keep - 1)) {
    await assertDirectoryIdentity(trustedRepository, repositoryIdentity);
    const snapshotPath = await params.resolveSnapshotDirectory(candidate.ref);
    const directory = await pinDirectory(snapshotPath, { label: "Retained SQLite snapshot" });
    try {
      await params.assertSnapshotContents(snapshotPath);
      const entries = new Map<string, Stats>();
      for (const entry of [SNAPSHOT_SQLITE_FILENAME, SNAPSHOT_MANIFEST_FILENAME]) {
        entries.set(entry, await fs.lstat(path.join(snapshotPath, entry)));
      }
      await directory.assertCurrent();
      if (
        !(await removePublishedSnapshotDirectoryIfOwned(
          snapshotPath,
          await fs.lstat(snapshotPath),
          entries,
        ))
      ) {
        throw new Error(`Retained SQLite snapshot changed during rotation: ${snapshotPath}`);
      }
    } finally {
      await directory.close();
    }
  }
  await syncDirectoryIfSupported(trustedRepository);
}
