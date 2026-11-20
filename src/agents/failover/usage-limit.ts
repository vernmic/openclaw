import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { parseApiErrorInfo } from "../../shared/assistant-error-format.js";
import { classifyFailoverReasonFromCode } from "./classification-rules.js";
import { isRateLimitErrorMessage } from "./message-patterns.js";
import type { FailoverSignal } from "./signal.js";

/** Plan-quota exhaustion, a short rate limit, or neither. */
export type ProviderUsageLimitKind = "quota" | "rate_limit" | "other";

const ZAI_PLAN_QUOTA_CODE = "1308";
// Numeric or string `"code": 1308` in a Z.AI body. The body is not itself a token.
const ZAI_PLAN_QUOTA_CODE_RE = /["']code["']\s*:\s*["']?1308\b/;

/**
 * Exact plan-quota tokens after trim, lowercasing, and hyphen-to-underscore.
 * A longer string that only contains one of these is not a match, including details.
 * Z.AI abnormal termination during streaming (SSE) returns no business code (the
 * reason is in `finish_reason`), so a stream that dies on quota is not matched here.
 * https://docs.z.ai/api-reference/api-code
 */
const QUOTA_EXHAUSTION_TOKENS: ReadonlySet<string> = new Set([
  // Z.AI plan windows (HTTP 429). 1302 is the short request rate limit and stays out,
  // as do 1305 (overload) and 1313 (fair-use frequency).
  // https://docs.z.ai/api-reference/api-code
  ZAI_PLAN_QUOTA_CODE,
  "1309",
  "1310",
  "1311",
  "1314",
  "1316",
  "1317",
  "1318",
  "1319",
  "1320",
  "1321",
  // Z.AI insufficient balance or no resource package (HTTP 429). Waiting does not clear it.
  // https://docs.z.ai/api-reference/api-code
  "1113",
  // OpenAI billing type. The four codes are the causes retrying will not clear.
  // https://developers.openai.com/api/docs/guides/error-codes
  "insufficient_quota",
  "credit_balance_exhausted",
  "organization_spend_limit_exceeded",
  "project_spend_limit_exceeded",
  "organization_usage_limit_exceeded",
  // Google Interactions API: daily quota, not a per-minute QuotaFailure.
  // https://aistudio.google.com/docs/api-errors
  "quota_exceeded",
]);

function normalizeToken(raw: string | undefined): string | undefined {
  const trimmed = raw?.trim().toLowerCase();
  if (!trimmed || trimmed.length > 128) {
    return undefined;
  }
  return trimmed.replaceAll("-", "_");
}

/** A whole code, type, or detail that is one plan-quota token. */
function isExactQuotaToken(raw: string | undefined): boolean {
  const token = normalizeToken(raw);
  return token !== undefined && QUOTA_EXHAUSTION_TOKENS.has(token);
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
    isExactQuotaToken(signal.code) ||
    isExactQuotaToken(signal.errorType) ||
    isExactQuotaToken(parsed?.code) ||
    isExactQuotaToken(parsed?.type) ||
    signal.details?.some((detail) => isExactQuotaToken(detail)) === true ||
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
