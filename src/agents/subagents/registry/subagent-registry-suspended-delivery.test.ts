import { describe, expect, it } from "vitest";
import { createSubagentRunRecord } from "../../subagent-test-fixtures.test-helpers.js";
import { resolveSuspendedDeliveryExpiryMs } from "./subagent-registry-suspended-delivery.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

const WEEK_MS = 7 * 24 * 60 * 60_000;
const PERMANENT_FAILURE_MS = 6 * 60 * 60_000;

function suspendedRecord(delivery: Record<string, unknown>): SubagentRunRecord {
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
    ...delivery,
  } as SubagentRunRecord["delivery"];
  return entry;
}

describe("resolveSuspendedDeliveryExpiryMs", () => {
  it("retires the production shape: disposition permanent_failure, suspendedReason expiry", () => {
    // This is the exact shape of all 29 stuck rows read from state/openclaw.sqlite
    // on 2026-09-23. An earlier version of this resolver keyed only on
    // suspendedReason and therefore matched none of them.
    expect(
      resolveSuspendedDeliveryExpiryMs(
        suspendedRecord({ disposition: "permanent_failure", suspendedReason: "expiry" }),
      ),
    ).toBe(PERMANENT_FAILURE_MS);
  });

  it("retires a delivery suspended explicitly as a permanent failure", () => {
    expect(
      resolveSuspendedDeliveryExpiryMs(suspendedRecord({ suspendedReason: "permanent_failure" })),
    ).toBe(PERMANENT_FAILURE_MS);
  });

  it("keeps the week-long retention for a recoverable expiry suspension", () => {
    // suspendedReason expiry WITHOUT a permanent-failure disposition: attempts
    // ran out, but the delivery path is not known to be gone.
    expect(
      resolveSuspendedDeliveryExpiryMs(
        suspendedRecord({ disposition: "retryable", suspendedReason: "expiry" }),
      ),
    ).toBe(WEEK_MS);
  });

  it("keeps the week-long retention when nothing marks it unrecoverable", () => {
    expect(resolveSuspendedDeliveryExpiryMs(suspendedRecord({}))).toBe(WEEK_MS);
    expect(resolveSuspendedDeliveryExpiryMs()).toBe(WEEK_MS);
  });
});
