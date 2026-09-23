import { describe, expect, it } from "vitest";
import { createSubagentRunRecord } from "../../subagent-test-fixtures.test-helpers.js";
import { resolveSuspendedDeliveryExpiryMs } from "./subagent-registry-suspended-delivery.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

const WEEK_MS = 7 * 24 * 60 * 60_000;
const PERMANENT_FAILURE_MS = 6 * 60 * 60_000;

function suspendedRecord(suspendedReason?: "expiry" | "permanent_failure"): SubagentRunRecord {
  const entry = createSubagentRunRecord({
    runId: "run-1",
    startedAt: 1_000,
    endedAt: 2_000,
    outcome: "ok",
  });
  entry.delivery = {
    ...entry.delivery,
    status: "suspended",
    suspendedAt: 2_000,
    ...(suspendedReason ? { suspendedReason } : {}),
  };
  return entry;
}

describe("resolveSuspendedDeliveryExpiryMs", () => {
  it("retires a permanently failed delivery after six hours, not a week", () => {
    // The sink is gone, not busy: retrying never succeeds, so retention only
    // feeds the registry pressure cap (see the 2026-09-22 backlog of 27). Six
    // hours keeps an overnight cluster readable at the next post-mortem.
    expect(resolveSuspendedDeliveryExpiryMs(suspendedRecord("permanent_failure"))).toBe(
      PERMANENT_FAILURE_MS,
    );
  });

  it("keeps the week-long retention for an expiry suspension", () => {
    expect(resolveSuspendedDeliveryExpiryMs(suspendedRecord("expiry"))).toBe(WEEK_MS);
  });

  it("keeps the week-long retention when no reason is recorded", () => {
    expect(resolveSuspendedDeliveryExpiryMs(suspendedRecord())).toBe(WEEK_MS);
    expect(resolveSuspendedDeliveryExpiryMs()).toBe(WEEK_MS);
  });
});
