import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import {
  snapshotAgentDatabase,
  readAgentDatabaseSnapshotFingerprint,
} from "./agent-database-snapshots.js";

const native = vi.hoisted(() => ({ createRetained: vi.fn(), ensureRoot: vi.fn(), home: vi.fn() }));
vi.mock("../infra/home-dir.js", () => ({ resolveRequiredOsHomeDir: native.home }));
vi.mock("./local-repository.js", () => ({
  createLocalSqliteSnapshotProvider: () => ({ createRetained: native.createRetained }),
  ensurePrivateSnapshotRepositoryRoot: native.ensureRoot,
}));
const tempDirs = useAutoCleanupTempDirTracker(afterEach);
let root: string;
let source: string;
let sequence = 0;
beforeEach(async () => {
  root = tempDirs.make("openclaw-certified-snapshot-");
  source = path.join(root, "source.sqlite");
  await fs.writeFile(source, "synthetic database");
  native.createRetained.mockReset();
  native.ensureRoot.mockClear();
  native.home.mockReturnValue(path.join(root, "user"));
  native.ensureRoot.mockImplementation(async (directory: string) =>
    fs.mkdir(directory, { recursive: true }),
  );
  native.createRetained.mockImplementation(async () => {
    const directory = path.join(root, `snapshot-${++sequence}`);
    await fs.mkdir(directory);
    await fs.writeFile(path.join(directory, "database.sqlite"), "verified private artifact");
    await fs.writeFile(path.join(directory, "manifest.json"), "native certificate");
    return { ref: { path: directory } };
  });
});
afterEach(() => vi.restoreAllMocks());
const capture = (previous?: Awaited<ReturnType<typeof snapshotAgentDatabase>>) =>
  snapshotAgentDatabase({ target: { agentId: "main", path: source }, stateDir: root, previous });

describe("recurring agent snapshot certification", () => {
  it("uses a persistent private OS-home root and isolates states sharing one source", async () => {
    await capture();
    const privateRoot = path.join(root, "user", ".openclaw-backups");
    const hashPath = (pathname: string) =>
      createHash("sha256")
        .update(process.platform === "win32" ? pathname.toLowerCase() : pathname)
        .digest("hex");
    const physicalSource = await fs.realpath(source);
    const repository = (state: string) =>
      path.join(
        privateRoot,
        "agent-db-snapshots",
        hashPath(state),
        "main",
        hashPath(physicalSource),
      );
    expect(native.ensureRoot.mock.calls.slice(0, 2)).toEqual([
      [privateRoot],
      [repository(await fs.realpath(root))],
    ]);
    const otherState = path.join(root, "other-state");
    await fs.mkdir(otherState);
    await snapshotAgentDatabase({
      target: { agentId: "main", path: source },
      stateDir: otherState,
    });
    expect(native.ensureRoot.mock.calls.slice(2, 4)).toEqual([
      [privateRoot],
      [repository(await fs.realpath(otherState))],
    ]);
    expect(native.ensureRoot.mock.calls[1]).not.toEqual(native.ensureRoot.mock.calls[3]);
  });

  it("skips a certified unchanged family and treats absent and empty WAL as equivalent", async () => {
    const first = await capture();
    expect(first.outcome).toBe("certified");
    expect(native.createRetained).toHaveBeenCalledWith(
      { path: await fs.realpath(source), identity: { role: "agent", agentId: "main" } },
      6,
    );
    await fs.writeFile(`${source}-wal`, "");
    expect(await capture(first)).toEqual({ ...first, outcome: "unchanged" });
    await fs.unlink(`${source}-wal`);
    expect((await capture(first)).outcome).toBe("unchanged");
    expect(native.createRetained).toHaveBeenCalledOnce();
  });

  it("captures a changed WAL even while the main file is unchanged", async () => {
    const first = await capture();
    const mainBefore = await fs.stat(source, { bigint: true });
    await fs.writeFile(`${source}-wal`, "new committed pages");
    const second = await capture(first);
    expect(second.outcome).toBe("certified");
    expect(second.fingerprint).not.toBe(first.fingerprint);
    expect(await fs.stat(source, { bigint: true })).toEqual(mainBefore);
    expect(native.createRetained).toHaveBeenCalledTimes(2);
  });

  it("does not starve valid restore points when writes continue during capture", async () => {
    const create = native.createRetained.getMockImplementation()!;
    native.createRetained.mockImplementation(async (...args: unknown[]) => {
      const result = await create(...args);
      await fs.writeFile(`${source}-wal`, "concurrent commit");
      return result;
    });
    const result = await capture();
    expect(result).toMatchObject({ outcome: "certified", snapshotPath: expect.any(String) });
    expect(result.fingerprint).toBeUndefined();
  });

  it("recreates a restore point if its certified artifact disappears or changes", async () => {
    const first = await capture();
    await fs.unlink(path.join(first.snapshotPath!, "database.sqlite"));
    const second = await capture(first);
    expect(second.outcome).toBe("certified");
    await fs.writeFile(path.join(second.snapshotPath!, "database.sqlite"), "tampered artifact");
    expect((await capture(second)).outcome).toBe("certified");
    expect(native.createRetained).toHaveBeenCalledTimes(3);
  });

  it("does not publish success when the native artifact owner refuses capture", async () => {
    const first = await capture();
    await fs.writeFile(`${source}-wal`, "changed");
    native.createRetained.mockRejectedValueOnce(new Error("artifact integrity failed"));
    await expect(capture(first)).rejects.toThrow("artifact integrity failed");
    expect(await fs.readFile(path.join(first.snapshotPath!, "database.sqlite"), "utf8")).toBe(
      "verified private artifact",
    );
  });

  it("refuses low disk space before native capture and leaves the previous artifact intact", async () => {
    const first = await capture();
    await fs.writeFile(`${source}-wal`, "changed");
    vi.spyOn(fs, "statfs").mockResolvedValueOnce({
      ...(await fs.statfs(root, { bigint: true })),
      bavail: 0n,
    });
    await expect(capture(first)).rejects.toThrow("Insufficient disk space");
    expect(native.createRetained).toHaveBeenCalledOnce();
    expect(await fs.readFile(path.join(first.snapshotPath!, "database.sqlite"), "utf8")).toBe(
      "verified private artifact",
    );
  });

  it("observes rollback-journal changes and missing sources without opening SQLite", async () => {
    const first = await readAgentDatabaseSnapshotFingerprint(source);
    await fs.writeFile(`${source}-journal`, "rollback data");
    expect(await readAgentDatabaseSnapshotFingerprint(source)).not.toBe(first);
    await fs.unlink(source);
    expect(await capture()).toEqual({ outcome: "absent" });
    expect(native.createRetained).not.toHaveBeenCalled();
  });
});
