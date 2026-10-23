import { describe, expect, it } from "vitest";
import { createTelegramDispatchHttpFixture } from "./bot-message-dispatch.telegram-http.test-support.js";

describe("temporary Telegram reasoning during work", () => {
  const http = createTelegramDispatchHttpFixture();
  it("shows current reasoning between tools and leaves only the final answer", async () => {
    await http.dispatchProgressTurn(
      async (options) => {
        await http.emitToolStart(options, {
          name: "exec",
          phase: "start",
          toolCallId: "reasoning-progress-probe",
          args: { command: "fixture-only" },
        });
        await options?.onReasoningStream?.({
          text: "Checking the stored evidence before choosing the next step.",
          replace: true,
        });
        await http.waitForBotApiCall((call) =>
          String(call.fields.text).includes("Checking the stored evidence"),
        );
        expect([...http.visibleMessages.values()].join("\n")).toContain(
          "Checking the stored evidence",
        );
        await options?.onReasoningStream?.({
          text: "The evidence is consistent; preparing the result now.",
          replace: true,
        });
        await http.waitForBotApiCall((call) =>
          String(call.fields.text).includes("The evidence is consistent"),
        );
        expect([...http.visibleMessages.values()].join("\n")).toContain(
          "The evidence is consistent",
        );
      },
      {
        mode: "progress",
        toolProgress: true,
        cfg: { agents: { defaults: { reasoningDefault: "stream" } } },
        finalReply: { text: "Verified result delivered." },
      },
    );
    expect([...http.visibleMessages.values()]).toEqual(["Verified result delivered."]);
    const writes = http.calls.filter(
      (call) => call.method === "sendMessage" || call.method === "editMessageText",
    );
    expect(
      writes.some((call) => String(call.fields.text).includes("Checking the stored evidence")),
    ).toBe(true);
    expect(writes.at(-1)?.fields.text).toBe("Verified result delivered.");
  });
});
