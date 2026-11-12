import { describe, expect, it } from "vitest";
import {
  OPENCLAW_SQLITE_DEFAULT_BUSY_TIMEOUT_MS,
  OPENCLAW_SQLITE_MAX_BUSY_TIMEOUT_MS,
  resolveOpenClawSqliteBusyTimeoutMs,
} from "./openclaw-state-db-contract.js";

describe("resolveOpenClawSqliteBusyTimeoutMs", () => {
  it("keeps the default when the override is unset or malformed", () => {
    expect(resolveOpenClawSqliteBusyTimeoutMs({})).toBe(OPENCLAW_SQLITE_DEFAULT_BUSY_TIMEOUT_MS);
    expect(resolveOpenClawSqliteBusyTimeoutMs(undefined)).toBe(
      OPENCLAW_SQLITE_DEFAULT_BUSY_TIMEOUT_MS,
    );
    for (const value of ["", " ", "abc", "-1", "1.5", "15s"]) {
      expect(resolveOpenClawSqliteBusyTimeoutMs({ OPENCLAW_SQLITE_BUSY_TIMEOUT_MS: value })).toBe(
        OPENCLAW_SQLITE_DEFAULT_BUSY_TIMEOUT_MS,
      );
    }
  });

  it("raises the timeout but never lowers it below the default", () => {
    expect(resolveOpenClawSqliteBusyTimeoutMs({ OPENCLAW_SQLITE_BUSY_TIMEOUT_MS: "15000" })).toBe(
      15_000,
    );
    expect(resolveOpenClawSqliteBusyTimeoutMs({ OPENCLAW_SQLITE_BUSY_TIMEOUT_MS: "100" })).toBe(
      OPENCLAW_SQLITE_DEFAULT_BUSY_TIMEOUT_MS,
    );
  });

  it("caps the override at the ceiling", () => {
    expect(resolveOpenClawSqliteBusyTimeoutMs({ OPENCLAW_SQLITE_BUSY_TIMEOUT_MS: "9999999" })).toBe(
      OPENCLAW_SQLITE_MAX_BUSY_TIMEOUT_MS,
    );
  });
});
