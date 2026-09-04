import { describe, expect, it } from "bun:test";
import { parseYSweetEnvelope, type YSweetEnvelope } from "../src/prism/protocol";
import { projectId, type ProjectId } from "../src/sandbox/types";
import type {
  AwarenessChannel,
  AwarenessPeerUpdate,
} from "../src/v2/awareness/channel";
import {
  SplitAwarenessChannel,
  SplitAwarenessChannelError,
} from "../src/v2/awareness/split-channel";
import type {
  AwarenessEnvelope,
  HostFrame,
  SidecarFrame,
} from "../src/v2/protocol/shared";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");

function envelope<T>(peerId: string, revision: number): AwarenessEnvelope<T> {
  return {
    protocol: "gp-awareness-v2",
    schemaVersion: 1,
    schemaHash: "a".repeat(64),
    buildHash: "b".repeat(64),
    projectId: PROJECT,
    sessionId: "sandbox-session-1",
    peerId,
    instanceId: `${peerId}-instance`,
    leaderGeneration: 1,
    epoch: 1,
    revision,
    ack: { epoch: 1, sequence: -1 },
    credits: 8,
    active: 0,
    queued: 0,
    draining: false,
    frames: [],
  };
}

class LaneChannel<TLocal, TRemote> implements AwarenessChannel<TLocal, TRemote> {
  readonly published: AwarenessEnvelope<TLocal>[] = [];
  readonly listeners = new Set<(update: AwarenessPeerUpdate<TRemote>) => void>();
  readonly failureListeners = new Set<(error: Error) => void>();
  rotations: YSweetEnvelope[] = [];
  clears = 0;
  connects = 0;
  closes = 0;
  failConnect = false;

  constructor(
    readonly projectId: ProjectId,
    readonly localPeerId: string,
    readonly remotePeerId: string,
  ) {}

  async connect(): Promise<void> {
    this.connects++;
    if (this.failConnect) throw new Error("lane connect failed");
  }

  publish(value: AwarenessEnvelope<TLocal>): void {
    this.published.push(value);
  }

  clear(): void {
    this.clears++;
  }

  subscribe(listener: (update: AwarenessPeerUpdate<TRemote>) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  peerIds(): readonly string[] {
    return [this.remotePeerId];
  }

  rotateCredential(value: YSweetEnvelope): void {
    this.rotations.push(value);
  }

  onConnectionFailure(listener: (error: Error) => void): () => void {
    this.failureListeners.add(listener);
    return () => this.failureListeners.delete(listener);
  }

  async close(): Promise<void> {
    this.closes++;
  }

  emit(value: AwarenessEnvelope<TRemote>): void {
    for (const listener of this.listeners) {
      listener({ peerClientId: this.remotePeerId, envelope: value });
    }
  }

  fail(error = new Error("lane disconnected")): void {
    for (const listener of this.failureListeners) listener(error);
  }
}

describe("SplitAwarenessChannel", () => {
  it("keeps control and data publications in independent ephemeral states", async () => {
    const data = new LaneChannel<HostFrame, SidecarFrame>(PROJECT, "host:data", "sidecar:data");
    const control = new LaneChannel<HostFrame, SidecarFrame>(PROJECT, "host:control", "sidecar:control");
    const split = new SplitAwarenessChannel({ data, control });
    const updates: AwarenessEnvelope<SidecarFrame>[] = [];
    split.subscribe(update => updates.push(update.envelope));

    await split.connect();
    split.publish(envelope<HostFrame>("host", 1), "data");
    split.publish(envelope<HostFrame>("host", 1), "control");

    expect(data.published).toHaveLength(1);
    expect(data.published[0]?.lane).toBe("data");
    expect(control.published).toHaveLength(1);
    expect(control.published[0]?.lane).toBe("control");

    data.emit(envelope<SidecarFrame>("sidecar", 7));
    control.emit(envelope<SidecarFrame>("sidecar", 19));
    expect(updates.map(value => [value.lane, value.revision])).toEqual([
      ["data", 7],
      ["control", 19],
    ]);
    expect(split.peerIds()).toEqual(["sidecar:control", "sidecar:data"]);

    split.clear("control");
    expect(control.clears).toBe(1);
    expect(data.clears).toBe(0);
    split.clear();
    expect(control.clears).toBe(2);
    expect(data.clears).toBe(1);

    const credential = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "rotated-secret",
      authorization: "full",
    });
    await split.rotateCredential?.(credential);
    expect(data.rotations).toEqual([credential]);
    expect(control.rotations).toEqual([credential]);
    await split.close();
    expect(data.closes).toBe(1);
    expect(control.closes).toBe(1);
  });

  it("closes both lanes when either initial connection fails", async () => {
    const data = new LaneChannel<HostFrame, SidecarFrame>(PROJECT, "host:data", "sidecar:data");
    const control = new LaneChannel<HostFrame, SidecarFrame>(PROJECT, "host:control", "sidecar:control");
    control.failConnect = true;
    const split = new SplitAwarenessChannel({ data, control });

    await expect(split.connect()).rejects.toThrow("lane connect failed");
    expect(data.closes).toBe(1);
    expect(control.closes).toBe(1);
  });

  it("propagates a post-sync lane failure and reconnects through credential rotation", async () => {
    const data = new LaneChannel<HostFrame, SidecarFrame>(PROJECT, "host:data", "sidecar:data");
    const control = new LaneChannel<HostFrame, SidecarFrame>(PROJECT, "host:control", "sidecar:control");
    const split = new SplitAwarenessChannel({ data, control });
    const failures: Error[] = [];
    split.onConnectionFailure?.(error => failures.push(error));
    await split.connect();

    data.fail(new Error("data lane token expired"));
    expect(failures).toHaveLength(1);
    expect(failures[0]).toBeInstanceOf(SplitAwarenessChannelError);
    expect(failures[0]).toMatchObject({
      name: "SplitAwarenessChannelError",
      lane: "data",
      kind: "other",
      connectionCode: null,
    });
    const credential = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "reminted-secret",
      authorization: "full",
    });
    await split.rotateCredential?.(credential);
    expect(data.rotations).toEqual([credential]);
    expect(control.rotations).toEqual([credential]);
    expect(data.closes).toBe(0);
    expect(control.closes).toBe(0);
    await split.close();
  });
});
