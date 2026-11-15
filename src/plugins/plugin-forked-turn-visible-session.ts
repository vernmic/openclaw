import { randomUUID } from "node:crypto";
import { resolveAgentRunSessionTarget } from "../agents/run-session-target.js";
import { SessionManager } from "../agents/sessions/index.js";
import { createSessionEntryWithTranscript } from "../config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { normalizeAgentId } from "../routing/session-key.js";

const log = createSubsystemLogger("plugins");

export type ForkTurnMessage = Parameters<SessionManager["appendMessageAsync"]>[0];

/**
 * One session-key segment for a plugin's category: lowercase letters, digits and dashes. The
 * session key is the operator-facing handle, so a category that cannot be a segment is refused
 * rather than silently rewritten into a different one.
 */
export function visibleSessionCategory(category: string): string {
  const normalized = category
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "");
  if (!normalized) {
    throw new Error("A visible fork session needs a category.");
  }
  return normalized;
}

/**
 * Copies a settled plugin fork's own turns into an ordinary, listable session
 * `agent:<agentId>:<category>:<uuid>` so an operator can open the work from the session list.
 * The fork itself stays hidden; this record is written only after it settles and never touches
 * the source session.
 */
export async function recordPluginForkedTurnSession(params: {
  config: OpenClawConfig;
  agentId: string;
  workspaceDir: string;
  category: string;
  label?: string;
  messages: readonly ForkTurnMessage[];
}): Promise<{ sessionKey: string; sessionId: string } | undefined> {
  // A fork refused before its model ran has no turns; its refusal stays with its caller.
  if (params.messages.length === 0) {
    return undefined;
  }
  const agentId = normalizeAgentId(params.agentId);
  const category = visibleSessionCategory(params.category);
  const opaqueId = randomUUID();
  const sessionId = randomUUID();
  const sessionKey = `agent:${agentId}:${category}:${opaqueId}`;
  const target = await resolveAgentRunSessionTarget({
    agentId,
    config: params.config,
    sessionId,
    sessionKey,
    missingSessionKey: "create",
  });
  const created = await createSessionEntryWithTranscript(
    target,
    () => ({
      ok: true,
      entry: {
        sessionId,
        updatedAt: Date.now(),
        // Unique per record; some label writers reject duplicates.
        label: `${params.label?.trim() || category} ${opaqueId.slice(0, 8)}`,
        category: params.category.trim() || category,
      },
    }),
    { cwd: params.workspaceDir },
  );
  if (!created.ok) {
    throw new Error(`Could not create the visible fork session: ${String(created.error)}`);
  }
  const session = await SessionManager.openAsync(target, params.workspaceDir);
  for (const message of params.messages) {
    await session.appendMessageAsync(structuredClone(message), { config: params.config });
  }
  return { sessionKey, sessionId };
}

/** Records the session when the caller asked for one; a failure only warns (the fork succeeded). */
export async function recordPluginForkedTurnSessionIfRequested(params: {
  record: { category: string; label?: string } | undefined;
  config: OpenClawConfig;
  agentId: string;
  workspaceDir: string;
  messages: readonly ForkTurnMessage[] | undefined;
}): Promise<{ sessionKey: string } | undefined> {
  if (!params.record || !params.messages?.length) {
    return undefined;
  }
  try {
    return await recordPluginForkedTurnSession({
      config: params.config,
      agentId: params.agentId,
      workspaceDir: params.workspaceDir,
      category: params.record.category,
      ...(params.record.label ? { label: params.record.label } : {}),
      messages: params.messages,
    });
  } catch (error) {
    log.warn(`could not record a visible fork session: ${String(error)}`);
    return undefined;
  }
}
