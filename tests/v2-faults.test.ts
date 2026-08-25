import { describe, expect, it } from "bun:test";
import type { JournalRecord } from "../src/v2/sidecar/journal";
import type {
  DirectResponsesExecution,
  UpstreamTerminal,
} from "../src/v2/sidecar/upstream";
import { UpstreamExecutionError } from "../src/v2/sidecar/upstream";
import { parseAwarenessEnvelope } from "../src/v2/protocol/validate";
import type { AwarenessEnvelope, HostFrame } from "../src/v2/protocol/shared";
import {
  SidecarHarness,
  TEST_BINDING,
} from "./support/fake-v2-sidecar";
import { createFakeV2System } from "./support/fake-v2-system";

const BODY = { model: "gpt-5.6-sol", input: "fault-proof", stream: true };

describe("V2 deterministic fault recovery", () => {
  it("survives dropped, duplicated, and reordered Awareness publications exactly once", async () => {
    const system = await createFakeV2System({
      runtimes: 1,
      physicalPerRuntime: 4,
      faults: {
        drop: [3, 7],
        duplicate: [4, 9],
        reverseEvery: 3,
      },
    });
    try {
      const handle = await system.router.startRelay(BODY, { clientId: "fault-client" });
      await expect(handle.final).resolves.toMatchObject({ status: "completed" });
      expect(system.totalUpstreamCalls()).toBe(1);
      expect(system.runtimes[0]!.snapshot().quarantined).toBe(0);
    } finally {
      await system.close();
    }
  }, 10_000);

  it("never retries an execution that reached FETCH_STARTING", async () => {
    const behavior = async (input: DirectResponsesExecution): Promise<UpstreamTerminal> => {
      await input.beforeFetch();
      throw new UpstreamExecutionError("proxy reset", "transport", true);
    };
    const first = new SidecarHarness({ behavior });
    await first.sendRequest(BODY);
    await first.settle();
    const restarted = new SidecarHarness({ records: first.storage.records });

    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(first.upstream.calls).toBe(1);
    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      ambiguous: true,
    }));
  });

  it("replays a proven terminal spool after restart without another upstream call", async () => {
    const first = new SidecarHarness();
    await first.sendRequest(BODY);
    await first.settle();
    const restarted = new SidecarHarness({
      records: first.storage.records,
      terminals: first.storage.terminals,
    });

    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.frames.some(frame => frame.kind === "response_terminal")).toBe(true);
  });

  it("rejects stale leader generations at the protocol boundary", () => {
    const envelope: AwarenessEnvelope<HostFrame> = {
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: TEST_BINDING.schemaHash,
      buildHash: TEST_BINDING.buildHash,
      projectId: TEST_BINDING.projectId,
      sessionId: TEST_BINDING.sessionId,
      peerId: "host",
      instanceId: "host-instance",
      leaderGeneration: TEST_BINDING.leaderGeneration - 1,
      epoch: 1,
      revision: 1,
      ack: { epoch: 1, sequence: -1 },
      credits: 1,
      active: 0,
      queued: 0,
      draining: false,
      frames: [],
    };

    expect(() => parseAwarenessEnvelope(envelope, TEST_BINDING, "host"))
      .toThrow("leader binding mismatch");
  });
});
