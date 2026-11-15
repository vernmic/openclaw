import type { HookContext } from "./agent-tools.before-tool-call.types.js";
import { reconcileCodeModeExecBeforeHookParams } from "./code-mode-control-tools.js";
import type { AnyAgentTool } from "./tools/common.js";

// Moved out of agent-tools.before-tool-call.wrapper.ts (line cap) unchanged; the wrapper re-exports both.

/** Run tool-owned preparation while retaining the exact prepared object. */
export async function prepareBeforeToolCallExecutionParams(params: {
  tool: AnyAgentTool;
  params: unknown;
  toolCallId?: string;
  ctx?: HookContext;
  signal?: AbortSignal;
}): Promise<unknown> {
  const prepare = params.tool.prepareBeforeToolCallParams;
  if (prepare && params.ctx?.awaitSessionDiffBaseline) {
    await params.ctx.awaitSessionDiffBaseline();
    params.signal?.throwIfAborted();
  }
  return prepare
    ? await prepare(params.params, {
        ...(params.toolCallId ? { toolCallId: params.toolCallId } : {}),
        ...(params.ctx ? { hookContext: params.ctx } : {}),
        ...(params.signal ? { signal: params.signal } : {}),
      })
    : params.params;
}

/** Reconcile hook rewrites and restore tool-owned state before execution. */
export function finalizeBeforeToolCallExecutionParams(params: {
  tool: AnyAgentTool;
  preparedParams: unknown;
  hookParams: unknown;
  adjustedParams: unknown;
  finalizerMode: "adapter" | "wrapped";
}): unknown {
  const reconciledParams = reconcileCodeModeExecBeforeHookParams({
    owner: { tool: params.tool },
    originalParams: params.preparedParams,
    hookParams: params.hookParams,
    adjustedParams: params.adjustedParams,
  });
  // Tool preparation may key private state in a WeakMap by this exact object.
  // Keep the original identity until finalization transfers valid state to rewrites.
  const finalize = params.tool.finalizeBeforeToolCallParams;
  if (!finalize) {
    return reconciledParams;
  }
  if (params.finalizerMode === "adapter") {
    return finalize(reconciledParams, params.preparedParams);
  }
  return finalize.call(params.tool, reconciledParams, params.preparedParams) ?? reconciledParams;
}
