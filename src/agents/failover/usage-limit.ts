import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { parseApiErrorInfo } from "../../shared/assistant-error-format.js";
import { classifyFailoverReasonFromCode } from "./classification-rules.js";
import { isRateLimitErrorMessage } from "./message-patterns.js";
import type { FailoverSignal } from "./signal.js";

/** Plan-quota exhaustion, a short rate limit, or neither. */
export type ProviderUsageLimitKind = "quota" | "rate_limit" | "other";

const ZAI_PLAN_QUOTA_CODE = "1308";
const ZAI_PLAN_QUOTA_CODE_RE = /["']code["']\s*:\s*["']?1308\b/;

function normalizeToken(raw: string | undefined): string | undefined {
  const trimmed = raw?.trim().toLowerCase();
  if (!trimmed || trimmed.length > 128) {
    return undefined;
  }
  return trimmed.replaceAll("-", "_");
}

/** A short provider code or type that names an exhausted plan quota. */
function isQuotaToken(raw: string | undefined): boolean {
  const token = normalizeToken(raw);
  if (!token) {
    return false;
  }
  return token === ZAI_PLAN_QUOTA_CODE || token.includes("quota");
}

function isQuotaSignal(raw: string | undefined): boolean {
  if (!raw) {
    return false;
  }
  return isQuotaToken(raw) || ZAI_PLAN_QUOTA_CODE_RE.test(raw);
}

/**
 * Distinguish an exhausted plan quota from a short rate limit.
 * Free-text usage windows stay with the existing message classifiers.
 */
export function classifyProviderUsageLimit(
  signal: Pick<FailoverSignal, "status" | "code" | "errorType" | "message" | "details">,
): ProviderUsageLimitKind {
  const parsed = parseApiErrorInfo(signal.message);
  const quota =
    isQuotaSignal(signal.code) ||
    isQuotaSignal(signal.errorType) ||
    isQuotaSignal(parsed?.code) ||
    isQuotaSignal(parsed?.type) ||
    signal.details?.some((detail) => isQuotaSignal(detail)) === true ||
    ZAI_PLAN_QUOTA_CODE_RE.test(signal.message ?? "");
  if (quota) {
    return "quota";
  }
  const parsedStatus = parsed?.httpCode !== undefined ? Number(parsed.httpCode) : undefined;
  const status = signal.status ?? (Number.isFinite(parsedStatus) ? parsedStatus : undefined);
  if (
    status === 429 ||
    classifyFailoverReasonFromCode(signal.code) === "rate_limit" ||
    isRateLimitErrorMessage(signal.message ?? "")
  ) {
    return "rate_limit";
  }
  return "other";
}

/** Absent and false keep same-model backoff for plan-quota errors. */
export function resolveAdvanceOnQuotaExhausted(cfg: OpenClawConfig | undefined): boolean {
  return cfg?.agents?.defaults?.embeddedAgent?.advanceOnQuotaExhausted === true;
}
