import { describe, expect, it } from "vitest";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { classifyProviderUsageLimit, resolveAdvanceOnQuotaExhausted } from "./usage-limit.js";

describe("classifyProviderUsageLimit", () => {
  it("classifies Z.AI plan quota code 1308 as quota", () => {
    expect(classifyProviderUsageLimit({ status: 429, code: "1308", message: "" })).toBe("quota");
    expect(
      classifyProviderUsageLimit({
        status: 429,
        code: "1308",
        message: "429 status code (no body)",
      }),
    ).toBe("quota");
    expect(classifyProviderUsageLimit({ message: '{"error":{"code":1308}}' })).toBe("quota");
    expect(classifyProviderUsageLimit({ message: '{"error":{"code":"1308"}}' })).toBe("quota");
  });

  it("classifies a quota code or insufficient_quota type as quota", () => {
    expect(classifyProviderUsageLimit({ status: 429, code: "insufficient_quota" })).toBe("quota");
    expect(classifyProviderUsageLimit({ status: 429, errorType: "insufficient_quota" })).toBe(
      "quota",
    );
    expect(classifyProviderUsageLimit({ status: 429, code: "quota_exceeded" })).toBe("quota");
  });

  it("keeps an ordinary 429 as a rate limit", () => {
    expect(
      classifyProviderUsageLimit({
        status: 429,
        code: "rate_limit_exceeded",
        message: "Too many requests",
      }),
    ).toBe("rate_limit");
    expect(classifyProviderUsageLimit({ status: 429, message: "429 Too many requests" })).toBe(
      "rate_limit",
    );
  });

  it("classifies an unrelated 4xx as other", () => {
    expect(
      classifyProviderUsageLimit({
        status: 400,
        code: "invalid_request_error",
        message: "Unknown parameter: foo",
      }),
    ).toBe("other");
  });
});

describe("resolveAdvanceOnQuotaExhausted", () => {
  it("stays off unless the setting is true", () => {
    expect(resolveAdvanceOnQuotaExhausted(undefined)).toBe(false);
    expect(resolveAdvanceOnQuotaExhausted({})).toBe(false);
    const disabled: OpenClawConfig = {
      agents: { defaults: { embeddedAgent: { advanceOnQuotaExhausted: false } } },
    };
    const enabled: OpenClawConfig = {
      agents: { defaults: { embeddedAgent: { advanceOnQuotaExhausted: true } } },
    };
    expect(resolveAdvanceOnQuotaExhausted(disabled)).toBe(false);
    expect(resolveAdvanceOnQuotaExhausted(enabled)).toBe(true);
  });
});
