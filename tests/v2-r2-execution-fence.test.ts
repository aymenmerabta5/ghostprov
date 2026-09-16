import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";
import {
  DenoAwarenessPeer,
  type DenoAwarenessProviderLike,
} from "../sandbox/v2/awareness-peer";
import { DenoAwarenessPeer as R10DenoAwarenessPeer } from "../sandbox/v2/r10-awareness-peer";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import {
  GP_V2_R2_SCHEMA_VERSION,
  base64UrlEncode,
  sha256Hex,
  type AwarenessLane,
  type HostFrame,
  type ProtocolBinding,
  type R2RequestCommitFrame,
  type R2AwarenessEnvelope,
  type ReliableFrame,
  type SidecarFrame,
} from "../src/v2/protocol/shared";
import {
  V2ProtocolError,
  parseAwarenessEnvelope,
} from "../src/v2/protocol/validate";
import { AdmissionController } from "../src/v2/sidecar/admission";
import { RequestAssembler } from "../src/v2/sidecar/assembler";
import { SidecarEngine } from "../src/v2/sidecar/engine";
import { ClaimJournal } from "../src/v2/sidecar/journal";
import {
  FakeSidecarStorage,
  completedUpstream,
} from "./support/fake-v2-sidecar";

const PROJECT = "e631d12c-d917-4434-b360-41b4715d6679";
const HOST_INSTANCE = "host-r2-elected";
const SELECTED_SIDECAR = "sidecar-r2-selected";
const FOREIGN_SIDECAR = "sidecar-r2-foreign";

const BINDING: ProtocolBinding = {
  protocolRevision: "r2",
  projectId: PROJECT,
  sessionId: "sandbox-session-r2",
  leaderGeneration: 17,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  launchNonce: "launch-r2-exact-fence",
};

class FakeProvider implements DenoAwarenessProviderLike {
  readonly awareness: Awareness;
  readonly #listeners = new Map<string, Set<(...args: any[]) => void>>();

  constructor(doc: Y.Doc) {
    this.awareness = new Awareness(doc);
  }

  on(event: string, listener: (...args: any[]) => void): void {
    let listeners = this.#listeners.get(event);
    if (!listeners) this.#listeners.set(event, listeners = new Set());
    listeners.add(listener);
  }

  off(event: string, listener: (...args: any[]) => void): void {
    this.#listeners.get(event)?.delete(listener);
  }

  connect(): void {
    queueMicrotask(() => this.#emit("sync", true));
  }

  disconnect(): void {}

  destroy(): void {
    this.awareness.destroy();
  }

  #emit(event: string, ...args: any[]): void {
    for (const listener of this.#listeners.get(event) ?? []) listener(...args);
  }
}

interface TestRuntime {
  readonly instanceId: string;
  readonly providers: Map<AwarenessLane, FakeProvider>;
  readonly peer: DenoAwarenessPeer | R10DenoAwarenessPeer;
  readonly engine: SidecarEngine;
  readonly assembler: RequestAssembler;
  readonly admission: AdmissionController;
  readonly journal: ClaimJournal;
  readonly storage: FakeSidecarStorage;
  readonly emitted: SidecarFrame[];
  readonly stats: { fetches: number; fatals: number };
  close(): Promise<void>;
}

async function createRuntime(
  instanceId: string,
  options: {
    beforeFence?: (check: number) => Promise<void>;
    r10?: boolean;
  } = {},
): Promise<TestRuntime> {
  const providers = new Map<AwarenessLane, FakeProvider>();
  const timeline: string[] = [];
  const emitted: SidecarFrame[] = [];
  const stats = { fetches: 0, fatals: 0 };
  const storage = new FakeSidecarStorage(timeline);
  const journal = new ClaimJournal({
    storage,
    flushIntervalMs: 0,
    maxBatch: 32,
  });
  const assembler = new RequestAssembler({
    maxReservedBytes: 16 * 1024 * 1024,
  });
  const admission = new AdmissionController({
    physicalMax: 1,
    acceptedMax: 2,
    hostCeiling: 1,
    memoryBudgetBytes: 16 * 1024 * 1024,
  });
  let peer!: DenoAwarenessPeer | R10DenoAwarenessPeer;
  let fenceChecks = 0;
  const engine = new SidecarEngine({
    binding: BINDING,
    assembler,
    journal,
    admission,
    upstream: {
      execute: input => completedUpstream({
        ...input,
        beforeFetch: async () => {
          await input.beforeFetch();
          stats.fetches++;
        },
      }),
    },
    assertExecutionFence: async frame => {
      fenceChecks++;
      await options.beforeFence?.(fenceChecks);
      peer.assertExecutionFence(frame);
    },
    onFatalError: () => { stats.fatals++; },
    emit: (frame, priority, streamId) => {
      emitted.push(frame);
      peer.enqueue(frame, priority, streamId);
    },
  });
  const Peer = options.r10 ? R10DenoAwarenessPeer : DenoAwarenessPeer;
  peer = new Peer({
    credential: {
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
      rotationKey: createCredentialRotationKey(),
    },
    binding: BINDING,
    instanceId,
    engine,
    providerFactory: (doc, _credential, lane) => {
      const provider = new FakeProvider(doc);
      providers.set(lane, provider);
      return provider;
    },
    publicationIntervalMs: 1,
    retransmitMs: 60_000,
    connectTimeoutMs: 250,
    closeTimeoutMs: 500,
    initialActivePerSandbox: 1,
    maxActivePerSandbox: 1,
  });
  await engine.start();
  await peer.connect();

  return {
    instanceId,
    providers,
    peer,
    engine,
    assembler,
    admission,
    journal,
    storage,
    emitted,
    stats,
    async close() {
      await peer.close();
      admission.close();
      await journal.close();
    },
  };
}

function emptyHostEnvelope(options: {
  targetInstanceId: string;
  hostInstanceId?: string;
  revision?: number;
  lane?: AwarenessLane;
}): R2AwarenessEnvelope<HostFrame> {
  return {
    protocol: "gp-awareness-v2",
    protocolRevision: "r2",
    schemaVersion: GP_V2_R2_SCHEMA_VERSION,
    schemaHash: BINDING.schemaHash,
    buildHash: BINDING.buildHash,
    projectId: BINDING.projectId,
    sessionId: BINDING.sessionId,
    peerId: "host",
    instanceId: options.hostInstanceId ?? HOST_INSTANCE,
    targetInstanceId: options.targetInstanceId,
    launchNonce: BINDING.launchNonce,
    leaderGeneration: BINDING.leaderGeneration,
    epoch: 1,
    revision: options.revision ?? 0,
    ack: { epoch: 1, sequence: -1 },
    credits: 1,
    active: 0,
    queued: 0,
    draining: false,
    frames: [],
    lane: options.lane ?? "data",
  };
}

function controlHostEnvelope(options: {
  targetInstanceId: string;
  hostInstanceId?: string;
  revision?: number;
}): R2AwarenessEnvelope<HostFrame> {
  return emptyHostEnvelope({ ...options, lane: "control" });
}

async function requestHostEnvelope(options: {
  targetInstanceId: string;
  hostInstanceId?: string;
  requestId?: string;
  revision?: number;
}): Promise<R2AwarenessEnvelope<HostFrame>> {
  const hostInstanceId = options.hostInstanceId ?? HOST_INSTANCE;
  const requestId = options.requestId ?? "req-r2-exact-fence";
  const bytes = new TextEncoder().encode(JSON.stringify({
    model: "gpt-5.6-sol",
    input: "prove exact R2 execution fencing",
  }));
  const requestDigest = await sha256Hex(bytes);
  const manifest = {
    requestDigest,
    decodedBytes: bytes.byteLength,
    encodedBytes: bytes.byteLength,
    encoding: "identity" as const,
    partCount: 1,
  };
  const frames: ReliableFrame<HostFrame>[] = [
    {
      kind: "request_open",
      requestId,
      ...manifest,
      epoch: 1,
      sequence: 0,
    },
    {
      kind: "request_data",
      requestId,
      partIndex: 0,
      offset: 0,
      dataBase64Url: base64UrlEncode(bytes),
      epoch: 1,
      sequence: 1,
    },
    {
      kind: "request_commit",
      requestId,
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      projectId: BINDING.projectId,
      sessionId: BINDING.sessionId,
      leaderGeneration: BINDING.leaderGeneration,
      buildHash: BINDING.buildHash,
      schemaHash: BINDING.schemaHash,
      launchNonce: BINDING.launchNonce,
      hostInstanceId,
      targetInstanceId: options.targetInstanceId,
      ...manifest,
      epoch: 1,
      sequence: 2,
    },
  ];
  return {
    ...emptyHostEnvelope({
      targetInstanceId: options.targetInstanceId,
      hostInstanceId,
      revision: options.revision ?? 1,
    }),
    frames,
  };
}

function startHostEnvelope(
  request: R2AwarenessEnvelope<HostFrame>,
  revision = request.revision + 1,
): R2AwarenessEnvelope<HostFrame> {
  const commit = request.frames.find(frame => frame.kind === "request_commit");
  if (!commit || !("protocolRevision" in commit)) {
    throw new Error("R2 request envelope has no fenced commit");
  }
  return {
    ...request,
    revision,
    frames: [{
      ...commit,
      kind: "request_start",
      sequence: commit.sequence + 1,
    }],
  };
}

function publishHostEnvelope(
  remote: Awareness,
  runtimes: readonly TestRuntime[],
  envelope: R2AwarenessEnvelope<HostFrame>,
): void {
  const lane = envelope.lane ?? "data";
  const field = lane === "data" ? "ghostprovV2Data" : "ghostprovV2Control";
  remote.setLocalStateField(field, envelope);
  const update = encodeAwarenessUpdate(remote, [remote.clientID]);
  for (const runtime of runtimes) {
    applyAwarenessUpdate(
      runtime.providers.get(lane)!.awareness,
      update,
      "test-host",
    );
  }
}

function removeHostControl(remote: Awareness, runtimes: readonly TestRuntime[]): void {
  remote.setLocalStateField("ghostprovV2Control", null);
  const update = encodeAwarenessUpdate(remote, [remote.clientID]);
  for (const runtime of runtimes) {
    applyAwarenessUpdate(
      runtime.providers.get("control")!.awareness,
      update,
      "test-host-control-removed",
    );
  }
}

async function eventually(
  predicate: () => boolean,
  message: string,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (predicate()) return;
    await Bun.sleep(5);
  }
  throw new Error(message);
}

function expectProtocolFailure(value: unknown, code: string): void {
  try {
    parseAwarenessEnvelope(value, BINDING, "host");
    throw new Error("Expected the R2 envelope to fail closed");
  } catch (error) {
    expect(error).toBeInstanceOf(V2ProtocolError);
    expect((error as V2ProtocolError).code).toBe(code);
  }
}

describe("R2 exact execution fencing", () => {
  it.each([false, true])("revokes election on control revision rollback and while the exact host is draining (R10=%s)", async r10 => {
    const runtime = await createRuntime(SELECTED_SIDECAR, { r10 });
    const hostDoc = new Y.Doc();
    const host = new Awareness(hostDoc);
    try {
      publishHostEnvelope(
        host,
        [runtime],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR, revision: 5 }),
      );
      const request = await requestHostEnvelope({ targetInstanceId: SELECTED_SIDECAR });
      const commit = request.frames.find(frame => frame.kind === "request_commit");
      if (!commit || commit.kind !== "request_commit" || !("protocolRevision" in commit)) {
        throw new Error("missing R2 commit");
      }
      const fencedCommit = commit as ReliableFrame<R2RequestCommitFrame>;
      expect(() => runtime.peer.assertExecutionFence(fencedCommit)).not.toThrow();

      publishHostEnvelope(
        host,
        [runtime],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR, revision: 4 }),
      );
      expect(() => runtime.peer.assertExecutionFence(fencedCommit)).toThrow(
        "R2 execution fence is no longer elected",
      );

      publishHostEnvelope(host, [runtime], {
        ...controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR, revision: 6 }),
        draining: true,
      });
      expect(() => runtime.peer.assertExecutionFence(fencedCommit)).toThrow(
        "R2 execution fence is no longer elected",
      );

      publishHostEnvelope(
        host,
        [runtime],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR, revision: 7 }),
      );
      expect(() => runtime.peer.assertExecutionFence(fencedCommit)).not.toThrow();
    } finally {
      host.destroy();
      hostDoc.destroy();
      await runtime.close();
    }
  });

  it("allows exactly the targeted sidecar to ACK, durably claim, and fetch", async () => {
    const selected = await createRuntime(SELECTED_SIDECAR);
    const foreign = await createRuntime(FOREIGN_SIDECAR);
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      // Pin one exact steady host first, allowing startup replay to complete
      // before the same immutable Awareness state carries executable work.
      publishHostEnvelope(
        remote,
        [selected, foreign],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR }),
      );
      const request = await requestHostEnvelope({
        targetInstanceId: SELECTED_SIDECAR,
        revision: 1,
      });
      publishHostEnvelope(remote, [selected, foreign], request);
      await eventually(
        () => selected.emitted.some(frame => frame.kind === "request_prepared"),
        "targeted R2 sidecar did not prepare the request",
      );
      publishHostEnvelope(remote, [selected, foreign], startHostEnvelope(request));

      await eventually(
        () => selected.engine.snapshot().active === 0 && selected.stats.fetches === 1,
        "targeted R2 execution did not settle",
      );

      expect(selected.emitted.filter(frame => frame.kind === "request_ack")).toHaveLength(1);
      expect(selected.storage.records.filter(record => record.state === "claimed")).toHaveLength(1);
      expect(selected.stats.fetches).toBe(1);

      expect(foreign.emitted).toEqual([]);
      expect(foreign.storage.records).toEqual([]);
      expect(foreign.stats.fetches).toBe(0);
      expect(foreign.assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await Promise.all([selected.close(), foreign.close()]);
    }
  });

  it("ignores executable traffic from a different host after host pinning", async () => {
    const runtime = await createRuntime(SELECTED_SIDECAR);
    const electedDoc = new Y.Doc();
    const electedHost = new Awareness(electedDoc);
    const staleDoc = new Y.Doc();
    const staleHost = new Awareness(staleDoc);

    try {
      publishHostEnvelope(
        electedHost,
        [runtime],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR }),
      );
      await Bun.sleep(20);
      publishHostEnvelope(
        staleHost,
        [runtime],
        await requestHostEnvelope({
          targetInstanceId: SELECTED_SIDECAR,
          hostInstanceId: "host-r2-stale",
          requestId: "req-from-stale-host",
          revision: 1,
        }),
      );
      await Bun.sleep(30);

      expect(runtime.emitted).toEqual([]);
      expect(runtime.storage.records).toEqual([]);
      expect(runtime.stats.fetches).toBe(0);
      expect(runtime.assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
    } finally {
      electedHost.destroy();
      electedDoc.destroy();
      staleHost.destroy();
      staleDoc.destroy();
      await runtime.close();
    }
  });

  it("drops a queued commit silently when the elected control host disappears", async () => {
    const runtime = await createRuntime(SELECTED_SIDECAR);
    const hostDoc = new Y.Doc();
    const host = new Awareness(hostDoc);
    const held = runtime.admission.tryAcquire(4 * 1024 * 1024);
    expect(held).not.toBeNull();

    try {
      publishHostEnvelope(
        host,
        [runtime],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR }),
      );
      publishHostEnvelope(
        host,
        [runtime],
        await requestHostEnvelope({ targetInstanceId: SELECTED_SIDECAR }),
      );
      await eventually(
        () => runtime.admission.snapshot().queued === 1,
        "R2 commit never queued behind admission",
      );

      removeHostControl(host, [runtime]);
      held!.release();
      await eventually(
        () => runtime.stats.fatals >= 1 &&
          runtime.engine.snapshot().active === 0 &&
          runtime.admission.snapshot().queued === 0,
        "R2 queued commit did not settle after election loss",
      );

      expect(runtime.stats.fetches).toBe(0);
      expect(runtime.stats.fatals).toBeGreaterThanOrEqual(1);
      expect(runtime.emitted).toEqual([]);
      expect(runtime.storage.records.filter(record => record.state === "fetch_starting"))
        .toHaveLength(0);
    } finally {
      held?.release();
      host.destroy();
      hostDoc.destroy();
      await runtime.close();
    }
  });

  it("does not ACK or fetch when the host disappears at beforeFetch and a replacement appears", async () => {
    let reachedBeforeFetch!: () => void;
    const beforeFetch = new Promise<void>(resolve => { reachedBeforeFetch = resolve; });
    let releaseFence!: () => void;
    const fenceBlocked = new Promise<void>(resolve => { releaseFence = resolve; });
    const runtime = await createRuntime(SELECTED_SIDECAR, {
      beforeFence: async check => {
        if (check !== 5) return;
        reachedBeforeFetch();
        await fenceBlocked;
      },
    });
    const hostDoc = new Y.Doc();
    const host = new Awareness(hostDoc);

    try {
      publishHostEnvelope(
        host,
        [runtime],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR }),
      );
      const request = await requestHostEnvelope({ targetInstanceId: SELECTED_SIDECAR });
      publishHostEnvelope(host, [runtime], request);
      await eventually(
        () => runtime.emitted.some(frame => frame.kind === "request_prepared"),
        "R2 sidecar did not prepare before beforeFetch test",
      );
      publishHostEnvelope(host, [runtime], startHostEnvelope(request));
      await beforeFetch;

      removeHostControl(host, [runtime]);
      publishHostEnvelope(
        host,
        [runtime],
        controlHostEnvelope({
          targetInstanceId: SELECTED_SIDECAR,
          hostInstanceId: "host-r2-replacement",
          revision: 1,
        }),
      );
      releaseFence();
      await eventually(
        () => runtime.engine.snapshot().active === 0,
        "R2 beforeFetch did not settle after election replacement",
      );

      expect(runtime.stats.fetches).toBe(0);
      expect(runtime.stats.fatals).toBeGreaterThanOrEqual(1);
      expect(runtime.emitted.map(frame => frame.kind)).toEqual(["request_prepared"]);
      expect(runtime.emitted.some(frame => frame.kind === "request_ack")).toBe(false);
      expect(runtime.storage.records.filter(record => record.state === "fetch_starting"))
        .toHaveLength(0);
    } finally {
      releaseFence?.();
      host.destroy();
      hostDoc.destroy();
      await runtime.close();
    }
  });

  it("reactivates only when the same pinned host restores its control state", async () => {
    const runtime = await createRuntime(SELECTED_SIDECAR);
    const hostDoc = new Y.Doc();
    const host = new Awareness(hostDoc);

    try {
      publishHostEnvelope(
        host,
        [runtime],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR }),
      );
      removeHostControl(host, [runtime]);
      publishHostEnvelope(
        host,
        [runtime],
        controlHostEnvelope({ targetInstanceId: SELECTED_SIDECAR, revision: 1 }),
      );
      const request = await requestHostEnvelope({ targetInstanceId: SELECTED_SIDECAR });
      publishHostEnvelope(host, [runtime], request);
      await eventually(
        () => runtime.emitted.some(frame => frame.kind === "request_prepared"),
        "same host did not prepare after regaining election",
      );
      publishHostEnvelope(host, [runtime], startHostEnvelope(request));
      await eventually(() => runtime.stats.fetches === 1, "same host did not regain election");

      expect(runtime.emitted.filter(frame => frame.kind === "request_ack")).toHaveLength(1);
      expect(runtime.stats.fatals).toBe(0);
    } finally {
      host.destroy();
      hostDoc.destroy();
      await runtime.close();
    }
  });

  it("fails closed for mismatched launch, build, schema, revision, host, and commit target", async () => {
    const envelope = await requestHostEnvelope({ targetInstanceId: SELECTED_SIDECAR });

    expectProtocolFailure({ ...envelope, launchNonce: "another-launch" }, "binding");
    expectProtocolFailure({ ...envelope, buildHash: "c".repeat(64) }, "binding");
    expectProtocolFailure({ ...envelope, schemaHash: "d".repeat(64) }, "binding");
    expectProtocolFailure({ ...envelope, schemaVersion: 1 }, "revision");
    expectProtocolFailure({ ...envelope, protocolRevision: "r1" }, "revision");

    const commit = envelope.frames[2]!;
    expect(commit.kind).toBe("request_commit");
    expectProtocolFailure({
      ...envelope,
      frames: [
        envelope.frames[0],
        envelope.frames[1],
        { ...commit, hostInstanceId: "host-r2-stale" },
      ],
    }, "commitment");
    expectProtocolFailure({
      ...envelope,
      frames: [
        envelope.frames[0],
        envelope.frames[1],
        { ...commit, targetInstanceId: FOREIGN_SIDECAR },
      ],
    }, "commitment");
  });
});
