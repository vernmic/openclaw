import { stableJsonValue } from "@openclaw/normalization-core";
import { hashTextSha256 } from "./hash.js";
import type { SandboxBrowserConfig, SandboxDockerConfig, SandboxWorkspaceAccess } from "./types.js";

/**
 * Stable sandbox config hashing for container reuse decisions.
 *
 * Undefined values and object key order are normalized so semantically equal
 * configs keep the same hash while security epoch changes force recreation.
 */
export const SANDBOX_DOCKER_EXPLICIT_ENV_POLICY_EPOCH = "explicit-config-env-v1";

type SandboxHashInput = {
  docker: SandboxDockerConfig;
  dockerEnvPolicyEpoch?: string;
  workspaceAccess: SandboxWorkspaceAccess;
  workspaceDir: string;
  agentWorkspaceDir: string;
  mountFormatVersion: number;
  createArgsEpoch: string;
  managedMounts?: readonly string[];
};

type SandboxBrowserHashInput = SandboxHashInput & {
  browser: Pick<
    SandboxBrowserConfig,
    | "cdpPort"
    | "cdpSourceRange"
    | "vncPort"
    | "noVncPort"
    | "headless"
    | "noVncEnabled"
    | "autoStartTimeoutMs"
  >;
  securityEpoch: string;
};

/** Caller-side tweak over the shared stable clone: the sandbox hash drops undefined array entries. */
function dropUndefinedArrayEntries(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value
      .filter((entry): entry is unknown => entry !== undefined)
      .map(dropUndefinedArrayEntries);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        dropUndefinedArrayEntries(entry),
      ]),
    );
  }
  return value;
}

/** Computes the sandbox container config hash. */
export function computeSandboxConfigHash(input: SandboxHashInput): string {
  return hashTextSha256(
    JSON.stringify(dropUndefinedArrayEntries(stableJsonValue(input, { keyOrder: "locale" }))),
  );
}

/** Computes the browser-enabled sandbox container config hash. */
export function computeSandboxBrowserConfigHash(input: SandboxBrowserHashInput): string {
  return computeSandboxConfigHash(input);
}
