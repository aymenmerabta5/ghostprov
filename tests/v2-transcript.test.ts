import { describe, expect, it } from "bun:test";
import type { SseFrame } from "../src/relay/sse";
import {
  TranscriptProof,
  V2TerminalProofError,
  validateTerminalProof,
} from "../src/v2/protocol/transcript";

const REQUEST_ID = "req-terminal-1";
const REQUEST_DIGEST = "a".repeat(64);
const FINAL = {
  id: "resp_1",
  object: "response",
  status: "completed",
  output: [{ type: "message", content: [{ type: "output_text", text: "hello 🌍" }] }],
};

function event(event: string, data: unknown): SseFrame {
  return { event, data: JSON.stringify(data) };
}

function completedFrames(): SseFrame[] {
  return [
    event("response.created", { type: "response.created", response: { id: "resp_1" } }),
    event("response.output_text.delta", { type: "response.output_text.delta", delta: "hello 🌍" }),
    event("response.completed", { type: "response.completed", response: FINAL }),
  ];
}

describe("V2 transcript proof", () => {
  it("accepts only a terminal matching request, response, counts, and transcript digest", async () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    for (const frame of completedFrames()) transcript.append(frame);
    const terminal = await transcript.createTerminal(FINAL);

    await expect(validateTerminalProof(terminal, transcript.snapshot())).resolves.toEqual(FINAL);
    await expect(validateTerminalProof({
      ...terminal,
      transcriptSha256: "0".repeat(64),
    }, transcript.snapshot())).rejects.toThrow("transcript digest");
    await expect(validateTerminalProof({
      ...terminal,
      responseId: "resp_wrong",
    }, transcript.snapshot())).rejects.toThrow("response ID");
    await expect(validateTerminalProof({
      ...terminal,
      eventCount: terminal.eventCount + 1,
    }, transcript.snapshot())).rejects.toThrow("event count");
  });

  it("uses UTF-8 byte counts rather than JavaScript string length", async () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    for (const frame of completedFrames()) transcript.append(frame);
    const terminal = await transcript.createTerminal();
    const canonical = completedFrames()
      .map(frame => `event: ${frame.event}\ndata: ${frame.data}\n\n`)
      .join("");

    expect(terminal.byteCount).toBe(new TextEncoder().encode(canonical).byteLength);
    expect(terminal.byteCount).toBeGreaterThan(canonical.length);
  });

  it("rejects duplicate or missing terminal events", async () => {
    const duplicate = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    for (const frame of completedFrames()) duplicate.append(frame);
    expect(() => duplicate.append(completedFrames().at(-1)!)).toThrow("duplicate terminal");

    const missing = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    missing.append(event("response.created", { type: "response.created", response: { id: "resp_1" } }));
    await expect(missing.createTerminal()).rejects.toBeInstanceOf(V2TerminalProofError);
  });

  it("rejects mismatched final object and final-event digest", async () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    for (const frame of completedFrames()) transcript.append(frame);
    await expect(transcript.createTerminal({ ...FINAL, id: "resp_other" })).rejects.toThrow("final object");
    const terminal = await transcript.createTerminal();
    await expect(validateTerminalProof({
      ...terminal,
      finalSha256: "f".repeat(64),
    }, transcript.snapshot())).rejects.toThrow("final digest");
  });

  it("enforces a reservation-backed transcript byte ceiling", () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, { maxBytes: 32 });
    expect(() => transcript.append(event("response.output_text.delta", {
      type: "response.output_text.delta",
      delta: "x".repeat(64),
    }))).toThrow("byte limit");
  });
});
