import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { computeSandboxBrowserConfigHash, computeSandboxConfigHash } from "./config-hash.js";

// The deleted local normalizer, reproduced verbatim: the shared stableJsonValue plus the
// caller-side undefined-array-entry drop must stay byte-equal with it so existing
// container hashes (persisted reuse decisions) never change.
function legacyNormalizeForHash(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(legacyNormalizeForHash).filter((item): item is unknown => item !== undefined);
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value).toSorted(([a], [b]) => a.localeCompare(b));
    const normalized: Record<string, unknown> = {};
    for (const [key, entryValue] of entries) {
      const next = legacyNormalizeForHash(entryValue);
      if (next !== undefined) {
        normalized[key] = next;
      }
    }
    return normalized;
  }
  return value;
}

it.each(["plain", "browser"])(
  "computeSandboxConfigHash stays byte-equal with the deleted normalizer (%s)",
  (kind) => {
    const input = kind === "browser" ? createBrowserHashInput() : createHashInput();
    // Fold in undefined members and undefined array entries alongside mixed-case keys.
    const withUndefined = {
      ...input,
      Zebra: undefined,
      apple: 1,
      managedMounts: ["/a", undefined, "/b"],
    };
    const legacy = JSON.stringify(legacyNormalizeForHash(withUndefined));
    // The hash is sha256 over exactly those legacy bytes.
    expect(computeSandboxConfigHash(withUndefined)).toBe(
      createHash("sha256").update(legacy).digest("hex"),
    );
  },
);
import { SANDBOX_DOCKER_CREATE_ARGS_EPOCH } from "./constants.js";
import type { SandboxDockerConfig } from "./types.js";
import { SANDBOX_MOUNT_FORMAT_VERSION } from "./workspace-mounts.js";

function createHashInput(docker: Partial<SandboxDockerConfig> = {}) {
  return {
    docker: {
      image: "openclaw-sandbox:test",
      containerPrefix: "openclaw-sbx-",
      workdir: "/workspace",
      readOnlyRoot: true,
      tmpfs: ["/tmp", "/var/tmp", "/run"],
      network: "none",
      capDrop: ["ALL"],
      env: { LANG: "C.UTF-8" },
      ...docker,
    },
    workspaceAccess: "rw" as const,
    workspaceDir: "/tmp/workspace",
    agentWorkspaceDir: "/tmp/workspace",
    mountFormatVersion: SANDBOX_MOUNT_FORMAT_VERSION,
    createArgsEpoch: SANDBOX_DOCKER_CREATE_ARGS_EPOCH,
  };
}

function createBrowserHashInput() {
  return {
    ...createHashInput(),
    securityEpoch: "epoch-v1",
    browser: {
      cdpPort: 9222,
      cdpSourceRange: undefined,
      vncPort: 5900,
      noVncPort: 6080,
      headless: false,
      noVncEnabled: true,
      autoStartTimeoutMs: 12000,
    },
  };
}

describe("sandbox config hashes", () => {
  it("ignores object key order", () => {
    const left = computeSandboxConfigHash(
      createHashInput({ env: { LANG: "C.UTF-8", B: "2", A: "1" } }),
    );
    const right = computeSandboxConfigHash(
      createHashInput({ env: { A: "1", B: "2", LANG: "C.UTF-8" } }),
    );
    expect(left).toBe(right);
  });

  it.each([false, true])("preserves bind order (browser=%s)", (browser) => {
    const binds = ["/tmp/workspace:/workspace:rw", "/tmp/cache:/cache:ro"];
    const hash = (orderedBinds: string[]) => {
      const input = createHashInput({ binds: orderedBinds });
      return browser
        ? computeSandboxBrowserConfigHash({ ...createBrowserHashInput(), docker: input.docker })
        : computeSandboxConfigHash(input);
    };
    expect(hash(binds)).not.toBe(hash(binds.toReversed()));
  });

  it.each([
    ["skills", "/tmp/workspace/skills:/workspace/skills:ro", []],
    ["resources", "/host/attachments:/openclaw/attachments:ro", undefined],
  ] as const)("changes when read-only %s mounts change", (_, mount, before) => {
    const shared = createHashInput();
    expect(computeSandboxConfigHash({ ...shared, managedMounts: before })).not.toBe(
      computeSandboxConfigHash({ ...shared, managedMounts: [mount] }),
    );
  });

  it.each([
    ["mount format", { mountFormatVersion: SANDBOX_MOUNT_FORMAT_VERSION - 1 }],
    ["security epoch", { securityEpoch: "epoch-v2" }],
  ] as const)("changes when browser %s changes", (_, change) => {
    const shared = createBrowserHashInput();
    expect(computeSandboxBrowserConfigHash(shared)).not.toBe(
      computeSandboxBrowserConfigHash({ ...shared, ...change }),
    );
  });

  it("changes when cdp source range changes", () => {
    const shared = createBrowserHashInput();
    const left = computeSandboxBrowserConfigHash({
      ...shared,
      browser: { ...shared.browser, cdpSourceRange: "172.21.0.1/32" },
    });
    const right = computeSandboxBrowserConfigHash({
      ...shared,
      browser: { ...shared.browser, cdpSourceRange: "172.22.0.1/32" },
    });
    expect(left).not.toBe(right);
  });
});
