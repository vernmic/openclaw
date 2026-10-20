import { randomUUID } from "node:crypto";
import { resolveAgentRunSessionTarget } from "../../agents/run-session-target.js";
import { SessionManager } from "../../agents/sessions/index.js";
import type { CustomMessage } from "../../agents/sessions/messages.js";
import type { SessionMessageEntry } from "../../agents/sessions/session-manager-types.js";
import { createSessionEntryWithTranscript } from "../../config/sessions/session-accessor.js";
import type { TranscriptEntryAnchor } from "../../config/sessions/transcript-entry-anchor.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { normalizeAgentId } from "../../routing/session-key.js";

/** Session-list bucket that groups the visible records of Skill Workshop reviews. */
export const SKILL_EXPERIENCE_REVIEW_SESSION_CATEGORY = "Skill Workshop reviews";

type ReviewMessage = Parameters<SessionManager["appendMessageAsync"]>[0];

/** Entry ids present before the review ran; everything after them is the review's own turns. */
export function captureSkillExperienceReviewBaseline(
  sessionManager: Pick<SessionManager, "getEntries">,
): ReadonlySet<string> {
  return new Set(sessionManager.getEntries().map((entry) => entry.id));
}

export function collectSkillExperienceReviewMessages(
  sessionManager: Pick<SessionManager, "getEntries">,
  baseline: ReadonlySet<string>,
): ReviewMessage[] {
  return sessionManager
    .getEntries()
    .filter(
      (entry): entry is SessionMessageEntry => entry.type === "message" && !baseline.has(entry.id),
    )
    .map((entry) => structuredClone(entry.message) as ReviewMessage);
}

/**
 * Copies one settled review's own turns into an ordinary, listable session so the
 * operator can open it in the Control UI. The review itself still runs detached under
 * its internal identity; this record is written only after it settles and never
 * touches the source session.
 */
export async function recordVisibleSkillExperienceReview(params: {
  config: OpenClawConfig;
  source: TranscriptEntryAnchor;
  workspaceDir: string;
  outcome: string;
  messages: readonly ReviewMessage[];
  error?: string;
}): Promise<{ sessionKey: string } | undefined> {
  // A review refused before its model ran has no turns; its error stays in the review status.
  if (params.messages.length === 0) {
    return undefined;
  }
  const messages: ReviewMessage[] = [...params.messages];
  if (params.error) {
    const failure: CustomMessage = {
      role: "custom",
      customType: "skill-workshop-review-error",
      content: `Review failed: ${params.error}`,
      display: true,
      excludeFromContext: true,
      timestamp: Date.now(),
    };
    messages.push(failure);
  }
  const agentId = normalizeAgentId(params.source.agentId);
  const opaqueId = randomUUID();
  const sessionId = randomUUID();
  const sessionKey = `agent:${agentId}:workshop-review:${opaqueId}`;
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
        // Unique per review; some label writers reject duplicates.
        label: `Skill review (${params.outcome}) ${opaqueId.slice(0, 8)}`,
        category: SKILL_EXPERIENCE_REVIEW_SESSION_CATEGORY,
      },
    }),
    { cwd: params.workspaceDir },
  );
  if (!created.ok) {
    throw new Error(`Could not create the visible review session: ${String(created.error)}`);
  }
  const session = await SessionManager.openAsync(target, params.workspaceDir);
  for (const message of messages) {
    await session.appendMessageAsync(structuredClone(message), { config: params.config });
  }
  return { sessionKey };
}
