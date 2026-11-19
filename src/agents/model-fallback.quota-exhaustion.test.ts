import { beforeEach, describe, expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../config/types.openclaw.js";

const mocks = vi.hoisted(() => ({
  sleepWithAbort: vi.fn(async (_ms: number, _abortSignal?: AbortSignal): Promise<void> => {}),
}));

vi.mock("../infra/backoff.js", async () => {
  const actual = await vi.importActual<typeof import("../infra/backoff.js")>("../infra/backoff.js");
  return { ...actual, sleepWithAbort: mocks.sleepWithAbort };
});

vi.mock("./embedded-agent-runner/logger.js", async () => {
  const actual = await vi.importActual<typeof import("./embedded-agent-runner/logger.js")>(
    "./embedded-agent-runner/logger.js",
  );
  return { ...actual, log: { ...actual.log, warn: vi.fn(), error: vi.fn(), debug: vi.fn() } };
});

import { createRetryController } from "./embedded-agent-runner/run/failover-retry-controller.test-support.js";
import { runWithModelFallback } from "./model-fallback-runner.js";

const { sleepWithAbort: sleep } = mocks;

function quotaConfig(advance: boolean): OpenClawConfig {
  return {
    plugins: { enabled: false },
    ...(advance
      ? { agents: { defaults: { embeddedAgent: { advanceOnQuotaExhausted: true } } } }
      : {}),
  };
}

async function runChain(params: {
  advance: boolean;
  code?: string;
  errorType?: string;
  message: string;
  status?: number;
}) {
  const cfg = quotaConfig(params.advance);
  const controller = createRetryController({
    fallbackConfigured: true,
    runParams: { config: cfg, timeoutMs: 600_000 },
  });
  const run = vi.fn(async (provider: string) => {
    if (provider !== "zai") {
      return "fallback-reply";
    }
    const retry = await controller.maybeRetryTransient({
      reason: "rate_limit",
      code: params.code,
      errorType: params.errorType,
      message: params.message,
      status: params.status ?? 429,
    });
    if (retry) {
      return "primary-reply";
    }
    throw Object.assign(new Error(params.message || "429 status code (no body)"), {
      status: params.status ?? 429,
      code: params.code,
    });
  });
  const result = await runWithModelFallback({
    cfg,
    provider: "zai",
    model: "glm-5.3",
    fallbacksOverride: ["deepseek/deepseek-flash"],
    manifestPlugins: [],
    skipAuthProfileRuntime: true,
    run,
  });
  return { result, run };
}

function calledModels(run: { mock: { calls: unknown[][] } }): string[] {
  return run.mock.calls.map(([provider, model]) => `${String(provider)}/${String(model)}`);
}

beforeEach(() => {
  sleep.mockClear();
});

describe("plan-quota model fallback", () => {
  it("advances immediately when a gated 1308 usage limit declines the same-model retry", async () => {
    const { result, run } = await runChain({
      advance: true,
      code: "1308",
      message: "",
    });
    expect(result.result).toBe("fallback-reply");
    expect(calledModels(run)).toEqual(["zai/glm-5.3", "deepseek/deepseek-flash"]);
    expect(sleep).not.toHaveBeenCalled();
  });

  it("advances immediately when a gated insufficient_quota type declines the same-model retry", async () => {
    const { result, run } = await runChain({
      advance: true,
      errorType: "insufficient_quota",
      message: "",
    });
    expect(result.result).toBe("fallback-reply");
    expect(calledModels(run)).toEqual(["zai/glm-5.3", "deepseek/deepseek-flash"]);
    expect(sleep).not.toHaveBeenCalled();
  });

  it("keeps backing off an ordinary 429 on the primary when the gate is on", async () => {
    const { result, run } = await runChain({
      advance: true,
      code: "rate_limit_exceeded",
      message: "Too many requests",
    });
    expect(result.result).toBe("primary-reply");
    expect(calledModels(run)).toEqual(["zai/glm-5.3"]);
    expect(sleep).toHaveBeenCalled();
  });

  it("keeps backing off a 1308 usage limit on the primary when the gate is off", async () => {
    const { result, run } = await runChain({
      advance: false,
      code: "1308",
      message: "",
    });
    expect(result.result).toBe("primary-reply");
    expect(calledModels(run)).toEqual(["zai/glm-5.3"]);
    expect(sleep).toHaveBeenCalled();
  });
});
