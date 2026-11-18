import { stableJsonValue } from "@openclaw/normalization-core";
import type { GatewayReadSharing } from "./descriptor.js";

type ShareKey = GatewayReadSharing["shareKey"];

export function operatorReadShareKey(...[caller, params]: Parameters<ShareKey>): string | null {
  const { client } = caller;
  if (
    !client?.authenticatedUserProfile ||
    client.connect.role !== "operator" ||
    client.internal?.syntheticClient ||
    client.internal?.agentToolCaller ||
    client.internal?.agentRuntimeIdentity ||
    client.internal?.operatorRunAuthority ||
    client.internal?.pluginSubagentRequester ||
    client.internal?.nodeInvokeStream
  ) {
    return null;
  }
  // In-memory read-share key only: key order just needs to be deterministic.
  return JSON.stringify(
    stableJsonValue([
      client.authenticatedUserProfile.profileId,
      client.authenticatedUserId,
      client.internal?.operatorRoleActor,
      (client.connect.scopes ?? []).toSorted(),
      (client.connect.caps ?? []).toSorted(),
      client.connect.client.id,
      client.connect.client.mode,
      params,
    ]),
  );
}

export const cronListShareKey: ShareKey = (caller, params) =>
  caller.read?.shareable === true ? operatorReadShareKey(caller, params) : null;

export const modelsListShareKey: ShareKey = (caller, params) =>
  params.refresh === true ? null : operatorReadShareKey(caller, params);
