import { describe, expect, it } from "bun:test";
import {
  createFakeV2System,
} from "./support/fake-v2-system";

describe("V2 deterministic concurrency", () => {
  it("settles 512 unique requests across four runtimes with 128 active each", async () => {
    const system = await createFakeV2System({
      runtimes: 4,
      physicalPerRuntime: 128,
      holdUpstream: true,
    });
    try {
      const pending = Array.from({ length: 512 }, (_, index) => system.router.startRelay(
        {
          model: "gpt-5.6-sol",
          input: `v2-${index}`,
          stream: true,
        },
        { clientId: `client-${index % 32}`, source: "http" },
      ));

      await system.waitForActive(512);
      const handles = await Promise.all(pending);
      expect(system.runtimes.map(runtime => runtime.upstream.maxActive)).toEqual([
        128, 128, 128, 128,
      ]);

      system.releaseUpstream();
      const finals = await Promise.all(handles.map(handle => handle.final));

      expect(finals).toHaveLength(512);
      expect(new Set(finals.map(value => value.id)).size).toBe(512);
      expect(system.totalUpstreamCalls()).toBe(512);
      expect(system.router.snapshot()).toMatchObject({ queued: 0, active: 0 });
    } finally {
      await system.close();
    }
  }, 20_000);

  it("holds one physical wave and admits every client fairly", async () => {
    const system = await createFakeV2System({
      runtimes: 1,
      physicalPerRuntime: 128,
      holdUpstream: true,
    });
    try {
      const pending = Array.from({ length: 256 }, (_, index) => {
        const client = `client-${Math.floor(index / 16)}`;
        return system.router.startRelay({
          model: "gpt-5.6-sol",
          input: `${client}:job-${index}`,
          stream: true,
        }, { clientId: client, source: "http" });
      });

      await system.waitForActive(128);

      expect(system.runtimes[0]!.upstream.maxActive).toBe(128);
      const firstClients = new Set(
        system.runtimes[0]!.upstream.startedInputs
          .slice(0, 32)
          .map(input => input.split(":", 1)[0]),
      );
      expect(firstClients.size).toBe(16);

      system.releaseUpstream();
      const handles = await Promise.race([
        Promise.all(pending),
        Bun.sleep(5_000).then(() => {
          throw new Error(`second wave stalled: ${JSON.stringify({
            router: system.router.snapshot(),
            upstreamCalls: system.runtimes[0]!.upstream.calls,
            upstreamActive: system.runtimes[0]!.upstream.active,
            uniqueInputs: new Set(system.runtimes[0]!.upstream.startedInputs).size,
            duplicateInputs: system.runtimes[0]!.upstream.startedInputs.length -
              new Set(system.runtimes[0]!.upstream.startedInputs).size,
          })}`);
        }),
      ]);
      await Promise.all(handles.map(handle => handle.final));
      expect(system.runtimes[0]!.upstream.calls).toBe(256);
      expect(system.runtimes[0]!.upstream.maxActive).toBe(128);
    } finally {
      await system.close();
    }
  }, 20_000);
});
