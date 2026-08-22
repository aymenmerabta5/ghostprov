import { describe, expect, it } from "bun:test";
import { abortableSleep } from "../src/v2/bootstrap/sleep";

class TrackingSignal {
  aborted = false;
  added = 0;
  removed = 0;

  addEventListener(_event: string, _listener: EventListenerOrEventListenerObject): void {
    this.added++;
  }

  removeEventListener(_event: string, _listener: EventListenerOrEventListenerObject): void {
    this.removed++;
  }
}

describe("Deno sidecar bootstrap helpers", () => {
  it("removes the abort listener after a normal polling sleep", async () => {
    const signal = new TrackingSignal();

    await abortableSleep(0, signal as unknown as AbortSignal);

    expect(signal.added).toBe(1);
    expect(signal.removed).toBe(1);
  });
});
