import fs from "node:fs";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { requireNodeSqlite } from "./node-sqlite.js";
import { resolveEnvironmentValue } from "./process-env.js";
import {
  captureSqliteReadOnlyWorkerLaunch,
  createScopedSqliteReadOnlyWorker,
} from "./sqlite-readonly-worker.js";

// Fork (2026-10-25): a storage-only env (state routing, no platform facts) must still launch a
// child with this process's platform/account/temp bootstrap; without it os.tmpdir() in the
// child resolved to the relative "<cwd>\undefined\temp".

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

afterEach(() => {
  vi.unstubAllEnvs();
});

function launchValue(env: NodeJS.ProcessEnv, name: string): string | undefined {
  return resolveEnvironmentValue(captureSqliteReadOnlyWorkerLaunch(env).env, name);
}

it("bootstraps platform and temp facts for a partial storage env, keeping its routing", () => {
  const stateRoot = tempDirs.make("openclaw-launch-state-");
  const parentTemp = tempDirs.make("openclaw-launch-parent-temp-");
  vi.stubEnv("TEMP", parentTemp);
  vi.stubEnv("TMP", parentTemp);
  vi.stubEnv("OPENCLAW_TEST_PARENT_SECRET", "must-not-reach-the-child");
  const env = captureSqliteReadOnlyWorkerLaunch({ OPENCLAW_STATE_DIR: stateRoot }).env!;
  expect(resolveEnvironmentValue(env, "TEMP")).toBe(parentTemp);
  expect(resolveEnvironmentValue(env, "TMP")).toBe(parentTemp);
  expect(resolveEnvironmentValue(env, "PATH")).toBeTruthy();
  if (process.platform === "win32") {
    expect(resolveEnvironmentValue(env, "SYSTEMROOT")).toBeTruthy();
  }
  expect(resolveEnvironmentValue(env, "OPENCLAW_STATE_DIR")).toBe(stateRoot);
  expect(resolveEnvironmentValue(env, "OPENCLAW_TEST_PARENT_SECRET")).toBeUndefined();
});

it("lets an explicit temp root in the supplied env win over the parent's", () => {
  const parentTemp = tempDirs.make("openclaw-launch-parent-temp-");
  const ownTemp = tempDirs.make("openclaw-launch-own-temp-");
  vi.stubEnv("TEMP", parentTemp);
  expect(launchValue({ OPENCLAW_STATE_DIR: parentTemp, TEMP: ownTemp }, "TEMP")).toBe(ownTemp);
});

it.runIf(process.platform === "win32")(
  "merges Windows keys case-insensitively, the supplied spelling winning",
  () => {
    const parentTemp = tempDirs.make("openclaw-launch-parent-temp-");
    const ownTemp = tempDirs.make("openclaw-launch-own-temp-");
    vi.stubEnv("TEMP", parentTemp);
    const env = captureSqliteReadOnlyWorkerLaunch({
      OPENCLAW_STATE_DIR: ownTemp,
      temp: ownTemp,
    }).env!;
    const tempKeys = Object.keys(env).filter((key) => key.toUpperCase() === "TEMP");
    expect(tempKeys).toHaveLength(1);
    expect(env[tempKeys[0]!]).toBe(ownTemp);
  },
);

it("keeps the supplied state root over the parent's", () => {
  const parentState = tempDirs.make("openclaw-launch-parent-state-");
  const ownState = tempDirs.make("openclaw-launch-own-state-");
  vi.stubEnv("OPENCLAW_STATE_DIR", parentState);
  expect(launchValue({ OPENCLAW_STATE_DIR: ownState }, "OPENCLAW_STATE_DIR")).toBe(ownState);
});

// The child keeps the process cwd (a source checkout under tsx), so the witness is a delta:
// a partial-env launch must add nothing under "<cwd>/undefined".
function listUndefinedTemp(cwd: string): string[] {
  const dir = path.join(cwd, "undefined");
  if (!fs.existsSync(dir)) return [];
  return (fs.readdirSync(dir, { recursive: true }) as string[]).toSorted();
}

it("runs a partial-env worker without writing temp files under its cwd", async () => {
  const root = tempDirs.make("openclaw-launch-cwd-");
  const source = path.join(root, "source.sqlite");
  const stagingRoot = path.join(root, "staging");
  fs.mkdirSync(stagingRoot);
  const { DatabaseSync } = requireNodeSqlite();
  const writer = new DatabaseSync(source);
  writer.exec("CREATE TABLE witness (value TEXT); INSERT INTO witness VALUES ('preserved');");
  writer.close();
  const launch = captureSqliteReadOnlyWorkerLaunch({
    OPENCLAW_STATE_DIR: path.join(root, "state"),
  });
  const before = listUndefinedTemp(launch.cwd);
  const worker = createScopedSqliteReadOnlyWorker(launch);
  try {
    const location = await worker.run(source, { mode: "sync", stagingRoot });
    expect(typeof location).toBe("string");
  } finally {
    await worker.close();
  }
  expect(listUndefinedTemp(launch.cwd)).toEqual(before);
});
