import { describe, expect, it, vi } from "vitest";
import { Agent } from "./agent.js";

describe("Agent stale listener events", () => {
  it("ignores an event outside an active run", async () => {
    // The constructor resolves the stream owner even though this test never starts a run.
    const agent = new Agent({
      streamFn: (() => {
        throw new Error("stream must not be used by a stale listener test");
      }) as never,
    });
    const listener = vi.fn();
    agent.subscribe(listener);
    const processEvents = (
      agent as unknown as {
        processEvents(event: unknown): Promise<void>;
      }
    ).processEvents.bind(agent);
    await expect(processEvents({ type: "agent_start" })).resolves.toBeUndefined();
    expect(listener).not.toHaveBeenCalled();
  });
});
