import { afterEach, expect, it, vi } from "vitest";
import { planSessionEntryMaintenance } from "./store-maintenance-plan.js";
import {
  captureMaintenanceConfigAsyncReader,
  resolveMaintenanceConfig,
} from "./store-maintenance-runtime.js";
import { resolveMaintenanceConfigFromInput } from "./store-maintenance.js";

const config = vi.hoisted(() => ({
  value: { agents: { defaults: { subagents: { archiveAfterMinutes: 60 } } } },
}));
vi.mock("../config.js", () => ({ getRuntimeConfig: () => config.value }));
vi.mock("../io.runtime.js", () => ({
  captureRuntimeConfigAsyncReader: ({ assertCurrent }: { assertCurrent: () => void }) =>
    Object.assign(async () => config.value, { assertCurrent }),
}));
afterEach(() => vi.restoreAllMocks());

it("captures the configured child expiry in both native runtime readers and retains caller fencing", async () => {
  expect(resolveMaintenanceConfig().archiveSubagentAfterMs).toBe(3_600_000);
  const assertCurrent = vi.fn();
  const read = captureMaintenanceConfigAsyncReader(assertCurrent);
  expect((await read()).archiveSubagentAfterMs).toBe(3_600_000);
  expect(assertCurrent).toHaveBeenCalled();
  assertCurrent.mockImplementation(() => {
    throw new Error("retired caller");
  });
  await expect(read()).rejects.toThrow("retired caller");
});

it("passes the child threshold into the shared age scan and archives before generic deletion", () => {
  const key = "agent:main:subagent:orphan";
  const entry = { sessionId: "orphan", updatedAt: Date.now() - 31 * 86_400_000 };
  const readAgeCandidates = vi.fn(() => ({ [key]: entry }));
  const result = planSessionEntryMaintenance({
    maintenance: resolveMaintenanceConfigFromInput({ maxDiskBytes: false }, 60),
    initialUnarchivedCount: 1,
    readPreserveKeys: () => undefined,
    readAgeCandidates,
    readCapCandidates: () => undefined,
    log: false,
  });
  expect(readAgeCandidates).toHaveBeenCalledWith(3_600_000);
  expect(result).toMatchObject({ archived: 1, pruned: 0 });
  expect(result.store[key]).toMatchObject({ sessionId: "orphan", archiveReason: "age-retention" });
  for (const value of [undefined, 0, -1, Number.NaN, Infinity]) {
    expect(resolveMaintenanceConfigFromInput(undefined, value).archiveSubagentAfterMs).toBeNull();
  }
});
