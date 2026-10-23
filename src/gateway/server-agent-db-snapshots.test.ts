import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import {
  resetGatewayWorkAdmission,
  tryBeginGatewayIndependentRootWorkAdmission,
} from "../process/gateway-work-admission.js";
import type { AgentDatabaseSnapshotCommand } from "../snapshot/agent-database-snapshots.worker.js";
import {
  createGatewaySchedulerClock,
  createTestGatewayScheduler,
} from "../test-utils/gateway-scheduler-clock.js";
import { startGatewayAgentDatabaseSnapshots } from "./server-agent-db-snapshots.js";

vi.mock("../config/io.js", () => ({ getRuntimeConfig: () => ({}) }));

const worker = vi.hoisted(() => ({
  run: vi.fn(),
  close: vi.fn(),
  constructed: vi.fn(),
  registry: vi.fn(),
}));
vi.mock("../infra/worker-task-pool.js", () => ({
  WorkerTaskPool: class {
    constructor(options: unknown) {
      worker.constructed(options);
    }
    run = worker.run;
    close = worker.close;
  },
}));
vi.mock("../state/openclaw-state-db-readonly.js", () => ({
  executeExistingOpenClawStateRead: worker.registry,
}));
let clock: ReturnType<typeof createGatewaySchedulerClock>;
let scheduler: ReturnType<typeof createTestGatewayScheduler>;
let handles: ReturnType<typeof startGatewayAgentDatabaseSnapshots>[];
beforeEach(() => {
  clock = createGatewaySchedulerClock();
  scheduler = createTestGatewayScheduler(clock.clock);
  handles = [];
  worker.run.mockReset();
  worker.registry.mockReset().mockResolvedValue({
    ok: true,
    type: "agentDatabaseRegistry.read",
    result: { status: "available", entries: [] },
  });
  worker.close.mockReset().mockResolvedValue(undefined);
  worker.constructed.mockReset();
});
afterEach(async () => {
  for (const handle of handles) {
    await handle.stop();
  }
  await scheduler.stop();
  resetGatewayWorkAdmission();
});
const target = (agentId: string) => ({ agentId, path: `/synthetic/${agentId}.sqlite` });
function start() {
  const log = { info: vi.fn(), warn: vi.fn() };
  const handle = startGatewayAgentDatabaseSnapshots(
    { scheduler, lifecycle: { closePreludeStarted: false } },
    log,
  );
  handles.push(handle);
  return { handle, log };
}

it("runs despite continuous foreground work, isolates an agent failure, and repeats hourly", async () => {
  const foreground = tryBeginGatewayIndependentRootWorkAdmission("request");
  expect(foreground).not.toBeNull();
  worker.run.mockImplementation(async (command: AgentDatabaseSnapshotCommand) => {
    if (command.type === "discover") {
      return { type: "targets", targets: [target("bad"), target("good")] };
    }
    if (command.target.agentId === "bad") {
      throw new Error("suspect artifact");
    }
    return {
      type: "snapshot",
      result: {
        outcome: "certified",
        fingerprint: "main+wal",
        snapshotPath: "/synthetic/point",
        artifactFingerprint: "immutable",
      },
    };
  });
  try {
    const { log } = start();
    expect(worker.constructed).not.toHaveBeenCalled();
    await clock.advanceBy(30_000);
    expect(worker.run).toHaveBeenCalledTimes(3);
    expect(worker.registry).toHaveBeenCalledWith(
      {},
      { type: "agentDatabaseRegistry.read" },
      { live: true },
    );
    expect(log.info).toHaveBeenLastCalledWith(
      "Agent DB snapshot pass: certified=1 unchanged=0 absent=0 failed=1",
    );
    expect(log.warn).toHaveBeenCalledOnce();
    await clock.advanceBy(60 * 60_000);
    expect(worker.run).toHaveBeenCalledTimes(6);
    expect(worker.run.mock.calls.at(-1)?.[0]).toMatchObject({
      previous: { fingerprint: "main+wal", artifactFingerprint: "immutable" },
    });
  } finally {
    foreground?.release();
  }
});

it("joins current native publication before pool close and starts no further agent on stop", async () => {
  const started = createDeferred();
  const release = createDeferred();
  worker.run.mockImplementation(async (command: AgentDatabaseSnapshotCommand) => {
    if (command.type === "discover") {
      return { type: "targets", targets: [target("first"), target("next")] };
    }
    started.resolve();
    await release.promise;
    return { type: "snapshot", result: { outcome: "certified" } };
  });
  const { handle } = start();
  const wake = clock.advanceBy(30_000);
  await started.promise;
  let stopped = false;
  const stopping = handle.stop().then(() => {
    stopped = true;
  });
  try {
    await Promise.resolve();
    expect(stopped).toBe(false);
    expect(worker.close).not.toHaveBeenCalled();
  } finally {
    release.resolve();
  }
  await stopping;
  await wake;
  expect(worker.run).toHaveBeenCalledTimes(2);
  expect(worker.close).toHaveBeenCalledOnce();
  await clock.advanceBy(60 * 60_000);
  expect(worker.run).toHaveBeenCalledTimes(2);
});

it("retries a failed discovery on the next scheduled pass and logs an all-unchanged pass", async () => {
  worker.run.mockRejectedValueOnce(new Error("registry unavailable"));
  const { log } = start();
  await clock.advanceBy(30_000);
  expect(log.info).toHaveBeenLastCalledWith(
    "Agent DB snapshot pass: certified=0 unchanged=0 absent=0 failed=1",
  );
  worker.run.mockImplementation(async (command: AgentDatabaseSnapshotCommand) =>
    command.type === "discover"
      ? { type: "targets", targets: [target("good")] }
      : { type: "snapshot", result: { outcome: "unchanged" } },
  );
  await clock.advanceBy(60 * 60_000);
  expect(log.info).toHaveBeenLastCalledWith(
    "Agent DB snapshot pass: certified=0 unchanged=1 absent=0 failed=0",
  );
});
