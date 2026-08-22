import { describe, expect, it } from "bun:test";
import {
  GP_V2_REQUEST_MAX_BYTES,
  sha256Hex,
} from "../src/v2/protocol/shared";
import { RequestAssembler } from "../src/v2/sidecar/assembler";
import { createFakeV2System } from "./support/fake-v2-system";

describe("V2 large payload boundaries", () => {
  it("preserves a 4 MiB request and response byte-for-byte through segmentation", async () => {
    const system = await createFakeV2System({
      runtimes: 1,
      physicalPerRuntime: 1,
    });
    try {
      const input = "ghostprov-012345".repeat(Math.ceil((4 * 1024 * 1024) / 16))
        .slice(0, 4 * 1024 * 1024);
      const expectedDigest = await sha256Hex(input);
      const started = performance.now();
      const handle = await Promise.race([
        system.router.startRelay({
          model: "gpt-5.6-sol",
          input,
          stream: true,
        }),
        Bun.sleep(45_000).then(() => {
          throw new Error(`4 MiB commit stalled: ${JSON.stringify({
            elapsedMs: performance.now() - started,
            router: system.router.snapshot(),
            upstreamCalls: system.totalUpstreamCalls(),
          })}`);
        }),
      ]);
      const events = Array.fromAsync(handle.events());

      const final = await Promise.race([
        handle.final,
        Bun.sleep(45_000).then(() => {
          throw new Error(`4 MiB terminal stalled: ${JSON.stringify({
            elapsedMs: performance.now() - started,
            router: system.router.snapshot(),
            upstreamCalls: system.totalUpstreamCalls(),
            stats: handle.stats,
          })}`);
        }),
      ]);
      const streamed = await events;
      const output = (final.output as any[])[0].content[0].text as string;

      expect(await sha256Hex(output)).toBe(expectedDigest);
      expect(streamed.some(frame => frame.event === "response.completed")).toBe(true);
      expect(system.totalUpstreamCalls()).toBe(1);
    } finally {
      await system.close();
    }
  }, 100_000);

  it("rejects 64 MiB plus one before reserving assembly or starting upstream", () => {
    const assembler = new RequestAssembler();

    expect(() => assembler.accept({
      kind: "request_open",
      requestId: "req-too-large",
      requestDigest: "a".repeat(64),
      decodedBytes: GP_V2_REQUEST_MAX_BYTES + 1,
      encodedBytes: 1,
      encoding: "identity",
      partCount: 1,
      epoch: 1,
      sequence: 0,
    })).toThrow("bounds");
    expect(assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
  });
});
