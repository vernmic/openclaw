import type { QueueMode } from "../../../../packages/gateway-protocol/src/schema/logs-chat.js";
import type { SessionEntry } from "../../../config/sessions/types.js";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import type { QueueDropPolicy } from "../../../config/types.queue.js";
import type { WrapperGateSettings, WrapperGateWindow } from "./wrapper-gate.js";

export type { QueueDropPolicy } from "../../../config/types.queue.js";

export type QueueSettings = {
  mode: QueueMode;
  debounceMs?: number;
  cap?: number;
  dropPolicy?: QueueDropPolicy;
  /** Wrapper-gate feature settings; absent or disabled keeps legacy scheduling. */
  wrapperGate?: WrapperGateSettings;
};

/** Wrapper-gate state a followup mailbox carries (F17); absent while the gate is off. */
export type GateState = {
  wrapperGateSettings?: WrapperGateSettings;
  wrapperGateWindow?: WrapperGateWindow;
};

export type ResolveQueueSettingsParams = {
  cfg: OpenClawConfig;
  channel?: string;
  sessionEntry?: SessionEntry;
  inlineMode?: QueueMode;
  inlineOptions?: Partial<QueueSettings>;
  pluginDebounceMs?: number;
};

export type QueueDedupeMode = "message-id" | "none";

export type EnqueueFollowupRunOptions = {
  /** Physical owner of a new source, when it differs from the configured store. */
  storePath?: string;
  position?: "tail" | "front";
  steerCandidate?: boolean;
};
