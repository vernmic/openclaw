import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { createPrivateSqliteDirectory } from "../infra/sqlite-private-directory.js";
import { createLocalSqliteSnapshotProvider } from "./local-repository.js";
import { createGenericDatabase, withDatabase } from "./local-repository.test-support.js";
import { hashSnapshotArtifact } from "./manifest.js";

const fault = vi.hoisted(() => ({ enabled: false, injected: false }));
vi.mock("./manifest.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./manifest.js")>();
  return {
    ...actual,
    hashSnapshotArtifact: async (directory: string) => {
      if (
        fault.enabled &&
        (await fs.access(path.join(directory, ".pending")).then(
          () => true,
          () => false,
        ))
      ) {
        fault.enabled = false;
        fault.injected = true;
        const artifactPath = path.join(directory, "database.sqlite");
        const bytes = await fs.readFile(artifactPath);
        bytes.writeUInt8(bytes.readUInt8(0) ^ 1, 0);
        await fs.writeFile(artifactPath, bytes);
      }
      return actual.hashSnapshotArtifact(directory);
    },
  };
});

const tempDirs = useAutoCleanupTempDirTracker(afterEach);
async function createGenericRepositoryFixture(options: { now?: () => Date } = {}) {
  // Shared test setup redirects HOME/cache beneath a sandbox-writable namespace.
  // Use the actual OS account home only for this tracked private test root.
  const root = tempDirs.make(
    "openclaw-snapshot-retention-",
    process.platform === "win32" ? os.userInfo().homedir : undefined,
  );
  const tempDir = path.join(root, "private");
  await createPrivateSqliteDirectory(tempDir);
  const sourcePath = path.join(tempDir, "source.sqlite");
  createGenericDatabase(sourcePath);
  return {
    sourcePath,
    tempDir,
    provider: createLocalSqliteSnapshotProvider({
      repositoryPath: path.join(tempDir, "snapshots"),
      now: options.now,
    }),
  };
}
afterEach(() => {
  fault.enabled = false;
  fault.injected = false;
  vi.restoreAllMocks();
});

it("keeps six committed restore points only after immutable artifact validation succeeds", async () => {
  let time = Date.parse("2026-10-22T00:00:00Z");
  const fixture = await createGenericRepositoryFixture({ now: () => new Date(time++) });
  const source = { path: fixture.sourcePath, identity: { role: "generic" as const, id: "test" } };
  for (let index = 0; index < 6; index++) {
    await fixture.provider.createRetained(source, 6);
  }
  const before = await fixture.provider.list();
  expect(before).toHaveLength(6);
  const { manifest: oldest } = before.at(-1)!;
  // Damage only the output after the source-copy checks. The native publication
  // must reject the artifact, never certify it or rotate a previous restore point.
  fault.enabled = true;
  await expect(fixture.provider.createRetained(source, 6)).rejects.toThrow("hash mismatch");
  expect(fault.injected).toBe(true);
  expect((await fixture.provider.list()).map((item) => item.manifest.snapshotId)).toEqual(
    before.map((item) => item.manifest.snapshotId),
  );
  const newest = await fixture.provider.createRetained(source, 6);
  const after = await fixture.provider.list();
  expect(after).toHaveLength(6);
  expect(after.some((item) => item.manifest.snapshotId === oldest.snapshotId)).toBe(false);
  expect(after[0]!.manifest.snapshotId).toBe(newest.manifest.snapshotId);
  expect(await hashSnapshotArtifact(newest.ref.path)).toMatchObject({
    sha256: newest.manifest.artifact.sha256,
    sizeBytes: newest.manifest.artifact.sizeBytes,
  });
});

it("retains the newly certified point if the wall clock moves backwards", async () => {
  let time = Date.parse("2026-10-22T00:00:00Z");
  const fixture = await createGenericRepositoryFixture({ now: () => new Date(time) });
  const source = { path: fixture.sourcePath, identity: { role: "generic" as const, id: "test" } };
  await fixture.provider.createRetained(source, 1);
  time -= 60_000;
  const newest = await fixture.provider.createRetained(source, 1);
  expect((await fixture.provider.list()).map((item) => item.ref.path)).toEqual([newest.ref.path]);
});

it("preserves implicit row IDs with the opt-in online acquisition while default copies compact", async () => {
  const fixture = await createGenericRepositoryFixture();
  withDatabase(fixture.sourcePath, (database) => {
    database.exec(
      "CREATE TABLE implicit_rows (value TEXT); INSERT INTO implicit_rows(rowid,value) VALUES (17,'retained');",
    );
  });
  const provider = createLocalSqliteSnapshotProvider({
    repositoryPath: path.join(fixture.tempDir, "preserved"),
    preserveRowIds: true,
  });
  const point = await provider.createRetained(
    { path: fixture.sourcePath, identity: { role: "generic", id: "rowids" } },
    6,
  );
  expect(
    withDatabase(
      path.join(point.ref.path, "database.sqlite"),
      (database) => database.prepare("SELECT rowid FROM implicit_rows").get(),
      { readOnly: true },
    ),
  ).toEqual({ rowid: 17 });
  const compacted = await fixture.provider.create({
    path: fixture.sourcePath,
    identity: { role: "generic", id: "rowids" },
  });
  expect(
    withDatabase(
      path.join(compacted.ref.path, "database.sqlite"),
      (database) => database.prepare("SELECT rowid FROM implicit_rows").get(),
      { readOnly: true },
    ),
  ).toEqual({ rowid: 1 });
});
