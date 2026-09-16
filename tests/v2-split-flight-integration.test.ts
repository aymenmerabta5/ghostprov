import { describe, expect, it, spyOn } from "bun:test";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
  removeAwarenessStates,
} from "y-protocols/awareness";
import { DenoAwarenessPeer, type DenoAwarenessProviderLike } from "../sandbox/v2/awareness-peer";
import { DenoAwarenessPeer as R10DenoAwarenessPeer } from "../sandbox/v2/r10-awareness-peer";
import { SidecarEngine as R10SidecarEngine } from "../src/v2/sidecar/r10-engine";
import { OutputBackpressure } from "../src/v2/sidecar/output-sink";
import { createExactSizedV2Request } from "./support/v2-performance-harness";
import { parseProvisionedSandbox, parseResourceCredential, parseYSweetEnvelope } from "../src/prism/protocol";
import { V2SandboxRuntime, type V2RuntimeLivenessDiagnostic } from "../src/v2/runtime";
import type { RunHandle } from "../src/relay/run-handle";
import { projectId } from "../src/sandbox/types";
import { SplitAwarenessChannel } from "../src/v2/awareness/split-channel";
import {
  YSweetAwarenessChannel,
  type AwarenessProviderLike,
} from "../src/v2/awareness/y-sweet-channel";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import type {
  HostFrame,
  ProtocolBinding,
  ReliableFrame,
  SidecarFrame,
} from "../src/v2/protocol/shared";
import { canonicalJson, utf8Bytes, utf8Text } from "../src/v2/protocol/shared";
import { TranscriptProof } from "../src/v2/protocol/transcript";
import { parseAwarenessEnvelope } from "../src/v2/protocol/validate";
import { V2TransportSession } from "../src/v2/transport/session";
import { AdmissionController } from "../src/v2/sidecar/admission";
import { RequestAssembler } from "../src/v2/sidecar/assembler";
import { SidecarEngine } from "../src/v2/sidecar/engine";
import { ClaimJournal } from "../src/v2/sidecar/journal";
import {
  FakeSidecarStorage,
  completedUpstream,
} from "./support/fake-v2-sidecar";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const BINDING: ProtocolBinding = {
  protocolRevision: "r1",
  launchNonce: "launch-nonce-r1",
  projectId: PROJECT,
  sessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};

class HubProvider implements AwarenessProviderLike, DenoAwarenessProviderLike {
  readonly awareness: Awareness;
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  connected = false;
  connects = 0;
  disconnects = 0;
  syncGate: Promise<void> | null = null;

  constructor(
    readonly hub: AwarenessHub,
    readonly doc: Y.Doc,
    readonly label: string,
  ) {
    this.awareness = new Awareness(doc);
    this.awareness.on("update", this.#onUpdate);
  }

  on(event: string, listener: (...args: any[]) => void): void {
    let values = this.listeners.get(event);
    if (!values) this.listeners.set(event, values = new Set());
    values.add(listener);
  }

  off(event: string, listener: (...args: any[]) => void): void {
    this.listeners.get(event)?.delete(listener);
  }

  connect(): void {
    if (this.connected) return;
    this.connects++;
    this.connected = true;
    this.hub.connect(this);
    queueMicrotask(() => {
      if (this.syncGate) void this.syncGate.then(() => this.emit("sync", true));
      else this.emit("sync", true);
    });
  }

  disconnect(): void {
    if (!this.connected) return;
    this.disconnects++;
    this.connected = false;
    this.hub.disconnect(this);
  }

  destroy(): void {
    this.disconnect();
    this.awareness.off("update", this.#onUpdate);
    this.awareness.destroy();
  }

  emit(event: string, ...args: any[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }

  readonly #onUpdate = (
    change: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown,
  ): void => {
    if (origin === this.hub) return;
    this.hub.broadcast(this, [...change.added, ...change.updated, ...change.removed]);
  };
}

class AwarenessHub {
  readonly providers = new Set<HubProvider>();
  readonly bytesByLabel = new Map<string, number>();
  dropBroadcast?: (source: HubProvider) => boolean;
  onConnect?: (source: HubProvider) => void;

  create(doc: Y.Doc, label: string): HubProvider {
    return new HubProvider(this, doc, label);
  }

  connect(provider: HubProvider): void {
    this.onConnect?.(provider);
    for (const existing of this.providers) {
      this.copyLocal(existing, provider);
      this.copyLocal(provider, existing);
    }
    this.providers.add(provider);
  }

  disconnect(provider: HubProvider): void {
    this.providers.delete(provider);
  }

  broadcast(source: HubProvider, clientIds: readonly number[]): void {
    if (clientIds.length === 0) return;
    if (this.dropBroadcast?.(source)) return;
    const update = encodeAwarenessUpdate(source.awareness, [...clientIds]);
    this.bytesByLabel.set(
      source.label,
      (this.bytesByLabel.get(source.label) ?? 0) + update.byteLength,
    );
    for (const target of this.providers) {
      if (target !== source) applyAwarenessUpdate(target.awareness, update, this);
    }
  }

  copyLocal(source: HubProvider, target: HubProvider): void {
    if (!source.awareness.getLocalState()) return;
    const update = encodeAwarenessUpdate(source.awareness, [source.doc.clientID]);
    applyAwarenessUpdate(target.awareness, update, this);
  }
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!predicate()) {
    if (performance.now() >= deadline) throw new Error("split-flight condition timed out");
    await Bun.sleep(2);
  }
}

describe("V2 split-flight production components", () => {
  it.each([
    ["manual control", false, "control"],
    ["automatic control", true, "control"],
    ["automatic data", true, "data"],
  ] as const)("recovers %s while retaining a 901KB tail and committed R10 output", async (_name, automatic, faultLane) => {
    const hub = new AwarenessHub();
    const nonce = "11111111-1111-4111-8111-111111111111";
    const binding: ProtocolBinding = { ...BINDING, protocolRevision: "r2", launchNonce: nonce };
    const credential = parseYSweetEnvelope({ url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`, docId: PROJECT, token: "offline-current", authorization: "full" });
    const providers = new Map<string, HubProvider>();
    const createProvider = (doc: Y.Doc, label: string) => {
      const provider = hub.create(doc, label); providers.set(label, provider); return provider;
    };
    const hostLane = (lane: "data" | "control") => new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: credential, localPeerId: `host:${lane}`,
      field: lane === "data" ? "ghostprovV2Data" : "ghostprovV2Control",
      parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
      providerFactory: doc => createProvider(doc, `host-${lane}`), syncTimeoutMs: 500,
    });
    const channel = new SplitAwarenessChannel({ data: hostLane("data"), control: hostLane("control") });
    const session = new V2TransportSession({ binding, channel,
      expectedSidecarInstanceId: "silent-control-sidecar", hostInstanceId: "silent-control-host",
      maxResponseBytes: 2 * 1024 * 1024, publicationIntervalMs: 1, retransmitMs: 25, livenessTimeoutMs: 60,
      // The incident's lost-progress fallback uses this exact32-frame R2 flight.
      transportProfile: "standard", gzip: async bytes => bytes,
    });
    const journal = new ClaimJournal({ storage: new FakeSidecarStorage([]), flushIntervalMs: 0, maxBatch: 16 });
    const admission = new AdmissionController({ physicalMax: 4, acceptedMax: 8, hostCeiling: 4,
      memoryBudgetBytes: 64 * 1024 * 1024 });
    const pressure = new OutputBackpressure({ highFrames: 256, highBytes: 256 * 1024,
      lowFrames: 64, lowBytes: 64 * 1024, hardFrames: 1024, hardBytes: 1024 * 1024 });
    const outputGate = Promise.withResolvers<void>();
    const bodies: Record<string, unknown>[] = [];
    let peer!: R10DenoAwarenessPeer;
    let responseFrames = 0;
    let peakWindowBytes = 0;
    const engine = new R10SidecarEngine({ binding,
      assembler: new RequestAssembler({ maxReservedBytes: 64 * 1024 * 1024 }), journal, admission,
      batching: true, outputBackpressure: pressure, assertExecutionFence: frame => peer.assertExecutionFence(frame),
      upstream: { execute: async input => {
        const ordinal = bodies.length;
        bodies.push(JSON.parse(utf8Text(input.body)));
        await input.beforeFetch();
        if (ordinal === 0) await outputGate.promise;
        await input.onPrelude({ status: 200, contentType: "text/event-stream", upstreamRequestId: null });
        const responseId = `resp_control_${ordinal}`;
        const delta = "x".repeat(1024);
        const final = { id: responseId, object: "response", status: "completed",
          output: [{ type: "message", content: [{ type: "output_text", text: delta.repeat(254) }] }] };
        const events = [
          { event: "response.created", data: JSON.stringify({ type: "response.created", response: { id: responseId } }) },
          ...Array.from({ length: 254 }, () => ({ event: "response.output_text.delta",
            data: JSON.stringify({ type: "response.output_text.delta", delta }) })),
          { event: "response.completed", data: JSON.stringify({ type: "response.completed", response: final }) },
        ];
        const proof = new TranscriptProof("upstream", "0".repeat(64), { retainBytes: false });
        for (let index = 0; index < events.length; index++) {
          await proof.appendCanonicalStream(events[index]!, { onChunk: input.onTranscriptBytes });
          if (index !== events.length - 1) await input.onEvent(events[index]!);
        }
        const terminal = await proof.createTerminal();
        return { responseId, status: terminal.terminalStatus, eventCount: terminal.eventCount,
          byteCount: terminal.byteCount, transcriptSha256: terminal.transcriptSha256,
          finalEventIndex: terminal.finalEventIndex, finalSha256: terminal.finalSha256,
          final, event: events.at(-1)! };
      } },
      emit: (frame, priority, stream) => {
        if (frame.kind === "response_event" || frame.kind === "response_batch") responseFrames++;
        peer.enqueue(frame, priority, stream);
        const current = providers.get("sidecar-data")?.awareness.getLocalState()?.ghostprovV2Data;
        if (current) peakWindowBytes = Math.max(peakWindowBytes,
          current.frames.reduce((total: number, entry: unknown) => total + utf8Bytes(JSON.stringify(entry)).byteLength, 0));
      },
    });
    peer = new R10DenoAwarenessPeer({ binding, instanceId: "silent-control-sidecar", engine,
      credential: { url: credential.url, baseUrl: credential.baseUrl, docId: PROJECT,
        token: "offline-current", authorization: "full", rotationKey: createCredentialRotationKey() },
      providerFactory: (doc, _credential, lane) => createProvider(doc, `sidecar-${lane}`),
      outputBackpressure: pressure, publicationIntervalMs: 1, retransmitMs: 25,
      connectTimeoutMs: 100, initialActivePerSandbox: 4, maxActivePerSandbox: 4,
    });
    let provisions = 0;
    let bootstrapCreates = 0;
    let credentialMints = 0;
    let forcedStops = 0;
    const liveness: V2RuntimeLivenessDiagnostic[] = [];
    const sandbox = parseProvisionedSandbox({ url: "https://prism.test/offline", token: "offline-sandbox",
      sandbox_session_id: binding.sessionId });
    const resource = parseResourceCredential({ access_token: "offline-resource", expires_at: (Date.now() + 600_000) / 1000,
      scopes: ["project_files:read"], resources_base_url: "https://prism.test/resources" }, Date.now());
    const runtime = automatic ? new V2SandboxRuntime({
      projectId: PROJECT, workerIndex: 1, leaderGeneration: binding.leaderGeneration,
      assets: { revision: "r10", protocolRevision: "r2", transportProfile: "standard",
        buildHash: binding.buildHash, schemaHash: binding.schemaHash, entrypointPath: "offline/sidecar.bundle.js",
        manifest: { path: "offline/manifest.json", logicalPath: "manifest.json", content: "{}", sha256: "c".repeat(64) },
        obsoletePaths: [], files: [] },
      transportReservationBytes: 16 * 1024 * 1024,
      governor: { snapshot: () => ({ reservationCapacityBytes: 256 * 1024 * 1024, reservedBytes: 0, paused: false }) },
      provisioning: {
        provision: async (_project, _signal, hooks) => {
          provisions++; await hooks?.onSandboxAcquired?.(sandbox);
          return { projectId: PROJECT, sandbox, resource, ySweet: credential, provisionedAtMs: Date.now() };
        },
        refreshResources: async () => resource,
        mintYSweet: async () => { credentialMints++; return credential; },
      },
      sidecarProbePeriodMs: 50, sidecarSuspectRecheckMs: 10, stopTimeoutMs: 1_000,
      sidecarCommitStallMs: faultLane === "data" ? 200 : undefined,
      createGateway: () => ({ heartbeat: async () => ({ ok: true, status: 200, durationMs: 1, sessionId: binding.sessionId }) }),
      createHeartbeat: () => ({ start() {}, stop: async () => undefined, snapshot: () => ({ lastSuccessAtMs: Date.now() }) }),
      createYDoc: () => ({ connect: async () => undefined, destroy: async () => undefined }),
      createAwareness: actual => { expect(actual).toEqual(binding); return channel; },
      createBootstrap: () => {
        bootstrapCreates++;
        return { ensureReady: async () => ({ instanceId: "silent-control-sidecar", releaseRevision: "r10", protocolRevision: "r2",
          launchNonce: binding.launchNonce, buildHash: binding.buildHash, schemaHash: binding.schemaHash,
          leaderGeneration: binding.leaderGeneration, capacity: 4 }),
        drainAndStop: async () => undefined, forceStop: async () => { forcedStops++; },
        sealCredentialRotation: async () => "{}" };
      },
      createTransport: () => session,
      onLivenessDiagnostic: event => liveness.push(event),
    }) : null;
    const run = (body: Record<string, unknown>) => runtime ? runtime.run(body) : session.run(body);
    let blocked = false;
    let firstRootParts = 0;
    let blockedTail: readonly unknown[] | null = null;
    let rootRequestId: string | null = null;
    hub.onConnect = provider => {
      if (provider.label === `host-${faultLane}` && provider.connects > 1) blocked = false;
    };
    hub.dropBroadcast = source => {
      if (source.label === "host-data") {
        const frames = source.awareness.getLocalState()?.ghostprovV2Data?.frames as ReliableFrame<HostFrame>[] | undefined;
        const opened = frames?.find(frame => frame.kind === "request_open" && frame.decodedBytes === 901_823);
        if (opened && "requestId" in opened) rootRequestId = opened.requestId;
        const data = frames?.filter(frame => frame.kind === "request_data" && frame.requestId === rootRequestId) ?? [];
        if (data.length && firstRootParts === 0) firstRootParts = data.length;
        if (!blockedTail && data.some(frame => frame.kind === "request_data" && frame.partIndex >= 31)) {
          blocked = true;
          blockedTail = structuredClone(frames!);
          // Model remote Awareness expiry on a silent control socket. The data
          // socket remains connected; no close/error event reaches the host.
          if (faultLane === "control") {
            removeAwarenessStates(providers.get("sidecar-control")!.awareness,
              [providers.get("host-control")!.doc.clientID], hub);
          }
        }
      }
      return blocked && (faultLane === "control" ? source.label.endsWith("control") : source.label === "host-data");
    };
    const complete = async (handle: RunHandle) => {
      const events = Array.fromAsync(handle.events());
      const final = await handle.final;
      expect(await events).toHaveLength(256);
      expect(final.status).toBe("completed");
    };
    let pending: Promise<unknown> | undefined;
    try {
      await engine.start(); await peer.connect();
      if (runtime) {
        const random = spyOn(crypto, "randomUUID").mockReturnValueOnce(nonce);
        try { await runtime.start(); } finally { random.mockRestore(); }
      } else await session.connect();
      await waitFor(() => session.snapshot().credits > 0);
      const background = await run(createExactSizedV2Request(4096, 0));
      const backgroundDone = complete(background);
      void backgroundDone.catch(() => undefined);
      const root = createExactSizedV2Request(901_823, 1, "incompressible");
      let rootCommitted = false;
      const rootRun = run(root).then(handle => { rootCommitted = true; return handle; });
      void rootRun.catch(() => undefined);
      pending = rootRun;
      await waitFor(() => blockedTail !== null);
      expect(firstRootParts).toBe(31);
      expect(rootCommitted).toBe(false);
      const healthyLane = faultLane === "control" ? "data" : "control";
      const healthyConnects = providers.get(`host-${healthyLane}`)!.connects;
      outputGate.resolve();
      await waitFor(() => responseFrames > 0);
      const synced = Promise.withResolvers<void>();
      providers.get(`host-${faultLane}`)!.syncGate = synced.promise;
      let reconnecting: Promise<void> | undefined;
      if (runtime) {
        await waitFor(() => providers.get(`host-${faultLane}`)!.connects === 2);
        expect(liveness).toEqual(expect.arrayContaining([
          expect.objectContaining({ signal: "sidecar_probe", outcome: "failed", errorKind: faultLane === "control" ? "timeout" : "reliable_stall" }),
          expect.objectContaining({ signal: "awareness", outcome: "started", awarenessLane: faultLane }),
        ]));
      } else {
        await expect(session.probeLiveness()).rejects.toMatchObject({ code: "timeout" });
        await session.resumeAwareness();
        expect(providers.get("host-control")!.connects).toBe(1);
        expect(rootCommitted).toBe(false);
        expect(providers.get("host-data")!.awareness.getLocalState()!.ghostprovV2Data.frames).toEqual(blockedTail);
        reconnecting = session.reconnectAwareness?.("control", AbortSignal.timeout(2_000)) ??
          session.resumeAwareness(AbortSignal.timeout(2_000));
        void reconnecting.catch(() => undefined);
      }
      // Outgoing ACK/RTO work continues during this delayed handshake. It must
      // retain reliable ownership instead of turning reconnect into a protocol failure.
      await Bun.sleep(100);
      expect(session.snapshot().connected).toBe(true);
      expect(session.snapshot().ambiguousQuarantined).toBe(0);
      synced.resolve();
      await reconnecting;
      if (runtime) await waitFor(() => liveness.some(event => event.signal === "awareness" && event.outcome === "succeeded"));
      else await expect(session.probeLiveness()).resolves.toMatchObject({ sentAtMs: expect.any(Number) });
      const rootHandle = await rootRun;
      await complete(rootHandle);
      await backgroundDone;
      const deltaBody = createExactSizedV2Request(21_052, 2, "incompressible");
      deltaBody.previous_response_id = "resp_control_1";
      deltaBody.input = (deltaBody.input as string).slice(0,
        (deltaBody.input as string).length - (utf8Bytes(canonicalJson(deltaBody)).byteLength - 21_052));
      await complete(await run(deltaBody));
      expect(bodies).toHaveLength(3);
      expect(bodies[1]).toEqual(root);
      expect(bodies[2]).toEqual(deltaBody);
      expect(utf8Bytes(canonicalJson(bodies[1])).byteLength).toBe(901_823);
      expect(utf8Bytes(canonicalJson(bodies[2])).byteLength).toBe(21_052);
      expect(providers.get(`host-${faultLane}`)!.connects).toBe(2);
      expect(providers.get(`host-${healthyLane}`)!.connects).toBe(healthyConnects);
      expect(peakWindowBytes).toBeLessThanOrEqual(128 * 1024);
      expect(pressure.snapshot().hardExceeded).toBe(false);
      if (runtime) {
        expect(provisions).toBe(1); expect(bootstrapCreates).toBe(1);
        expect(credentialMints).toBe(0); expect(forcedStops).toBe(0);
        expect(runtime.snapshot().state).toBe("ready");
      }
    } finally {
      outputGate.resolve(); await runtime?.stop(); await session.close(); await pending?.catch(() => undefined);
      await peer.close(); admission.close(); await journal.close();
    }
  }, 10_000);

  it("commits a request through independent host and sidecar lane providers", async () => {
    const hub = new AwarenessHub();
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const hostLane = (
      lane: "data" | "control",
      field: "ghostprovV2Data" | "ghostprovV2Control",
    ) => new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: `host:${lane}`,
      field,
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => hub.create(doc, `host-${lane}`),
      syncTimeoutMs: 100,
    });
    const channel = new SplitAwarenessChannel({
      data: hostLane("data", "ghostprovV2Data"),
      control: hostLane("control", "ghostprovV2Control"),
    });
    const session = new V2TransportSession({
      binding: BINDING,
      channel,
      maxResponseBytes: 2 * 1024 * 1024,
      publicationIntervalMs: 1,
      retransmitMs: 50,
    });

    let peer!: DenoAwarenessPeer;
    let opened: Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> | null = null;
    let commits = 0;
    const engine = {
      snapshot: () => ({ credits: 128, active: 0, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async (frame: ReliableFrame<HostFrame>) => {
        if (frame.kind === "request_open") opened = frame;
        if (frame.kind !== "request_commit" || !opened) return;
        commits++;
        peer.enqueue({
          kind: "request_ack",
          requestId: opened.requestId,
          requestDigest: opened.requestDigest,
          committed: true,
        }, "terminal", opened.requestId);
      },
    };
    peer = new DenoAwarenessPeer({
      credential: {
        url: ySweet.url,
        baseUrl: ySweet.baseUrl,
        docId: ySweet.docId,
        token: ySweet.token.reveal(),
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => hub.create(doc, `sidecar-${lane}`),
      publicationIntervalMs: 1,
      retransmitMs: 50,
      connectTimeoutMs: 100,
      initialActivePerSandbox: 8,
      maxActivePerSandbox: 16,
    });

    try {
      await peer.connect();
      await session.connect();
      await waitFor(() => session.snapshot().credits === 8);
      const handle = await session.run({
        model: "gpt-5.6-sol",
        input: "split-flight-integration",
        store: false,
      });
      void handle.final.catch(() => undefined);

      expect(commits).toBe(1);
      expect(session.snapshot().committed).toBe(1);
      const hostControl = [...hub.providers]
        .find(provider => provider.label === "host-control")!
        .awareness.getLocalState()?.ghostprovV2Control;
      const sidecarControl = [...hub.providers]
        .find(provider => provider.label === "sidecar-control")!
        .awareness.getLocalState()?.ghostprovV2Control;
      expect(hostControl).toMatchObject({ lane: "control", frames: [] });
      expect(sidecarControl).toMatchObject({ lane: "control", frames: [] });
      expect(hub.bytesByLabel.get("host-control")).toBeLessThan(
        hub.bytesByLabel.get("host-data")!,
      );
    } finally {
      await session.close();
      await peer.close();
    }
  });

  it("keeps an already-reserved commit admissible while its host flight is congested", async () => {
    const hub = new AwarenessHub();
    const binding: ProtocolBinding = {
      ...BINDING,
      protocolRevision: "r2",
      launchNonce: "launch-congested-commit",
    };
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    let holdHostData = true;
    hub.dropBroadcast = source => holdHostData && source.label === "host-stalled-data";
    const hostLane = (
      lane: "data" | "control",
      field: "ghostprovV2Data" | "ghostprovV2Control",
    ) => new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: `host-stalled:${lane}`,
      field,
      parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
      providerFactory: doc => hub.create(doc, `host-stalled-${lane}`),
      syncTimeoutMs: 100,
    });
    const session = new V2TransportSession({
      binding,
      channel: new SplitAwarenessChannel({
        data: hostLane("data", "ghostprovV2Data"),
        control: hostLane("control", "ghostprovV2Control"),
      }),
      expectedSidecarInstanceId: "sidecar-congested-commit",
      hostInstanceId: "host-congested-commit",
      maxResponseBytes: 2 * 1024 * 1024,
      publicationIntervalMs: 1,
      retransmitMs: 25,
      transportStallMs: 30,
    });
    let sidecarCeiling = 1;
    const sidecarCeilings: number[] = [];
    let commits = 0;
    let cancellations = 0;
    let preemptions = 0;
    const engine = {
      snapshot: () => ({
        ready: true,
        credits: sidecarCeiling > 0 ? 1 : 0,
        active: 0,
        queued: 0,
        draining: false,
      }),
      setHostCeiling: (value: number) => {
        sidecarCeiling = value;
        sidecarCeilings.push(value);
      },
      accept: async (frame: ReliableFrame<HostFrame>) => {
        if (frame.kind === "request_commit") {
          commits++;
          while (sidecarCeiling === 0) await Bun.sleep(2);
        } else if (frame.kind === "request_cancel") {
          cancellations++;
        }
      },
      preemptRequest: () => { preemptions++; },
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: ySweet.url,
        baseUrl: ySweet.baseUrl,
        docId: ySweet.docId,
        token: ySweet.token.reveal(),
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding,
      instanceId: "sidecar-congested-commit",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => hub.create(doc, `sidecar-stalled-${lane}`),
      publicationIntervalMs: 1,
      retransmitMs: 25,
      transportStallMs: 30,
      connectTimeoutMs: 100,
      initialActivePerSandbox: 1,
      maxActivePerSandbox: 1,
    });
    const controller = new AbortController();
    let pending: Promise<unknown> | null = null;

    try {
      await peer.connect();
      await session.connect();
      await waitFor(() => session.snapshot().credits > 0);
      pending = session.run({
        model: "gpt-5.6-sol",
        input: "congested commit",
      }, controller.signal);
      void pending.catch(() => undefined);
      await waitFor(() => {
        const provider = [...hub.providers].find(value => value.label === "host-stalled-data");
        return (provider?.awareness.getLocalState()?.ghostprovV2Data?.frames ?? [])
          .some((frame: HostFrame) => frame.kind === "request_commit");
      });
      await waitFor(() => (
        session.snapshot().diagnostics?.host.stallEventsTotal === 1
      ));

      expect(session.snapshot().admissionBlockReason).toBe("transport_congested");
      const ceilingsBeforeReduction = sidecarCeilings.length;
      session.setHostCeiling(0);
      await waitFor(() => sidecarCeilings.length > ceilingsBeforeReduction);
      // Local admission is closed, but the already-owned sidecar credit remains
      // available even when the requested future-work ceiling drops to zero.
      expect(sidecarCeilings.at(-1)).toBe(1);

      controller.abort();
      await expect(pending).rejects.toMatchObject({
        name: "V2PreCommitError",
        code: "cancelled_precommit",
        committed: false,
      });
      holdHostData = false;
      await waitFor(() => cancellations === 1);
      expect(commits).toBe(1);
      expect(preemptions).toBe(1);
    } finally {
      holdHostData = false;
      controller.abort();
      await Promise.allSettled(pending ? [pending] : []);
      await session.close().catch(() => undefined);
      await peer.close().catch(() => undefined);
    }
  }, 10_000);

  it("lets a fresh host retire an orphan recovery terminal and restore credits", async () => {
    const hub = new AwarenessHub();
    const binding: ProtocolBinding = {
      ...BINDING,
      protocolRevision: "r2",
      launchNonce: "launch-fresh-orphan-recovery",
      sessionId: "fresh-orphan-recovery-session",
      leaderGeneration: 12,
    };
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const requestId = "req-fresh-host-orphan";
    const requestDigest = "e".repeat(64);
    const event = {
      event: "response.completed",
      data: JSON.stringify({
        type: "response.completed",
        response: { id: "resp_fresh_host_orphan", status: "completed", output: [] },
      }),
    };
    const transcript = new TranscriptProof(requestId, requestDigest);
    transcript.append(event);
    const terminal = await transcript.createTerminal();
    const terminalBytes = utf8Bytes(`event: ${event.event}\ndata: ${event.data}\n\n`);
    const storage = new FakeSidecarStorage([]);
    const seed = new ClaimJournal({ storage, flushIntervalMs: 0, maxBatch: 8 });
    await seed.claim({ requestId, requestDigest, leaderGeneration: binding.leaderGeneration });
    await seed.markFetchStarting(requestId);
    await seed.markTerminal(
      requestId,
      terminal.responseId,
      terminal.transcriptSha256,
      terminalBytes,
      { status: 200, contentType: "text/event-stream", upstreamRequestId: "upstream-orphan" },
    );
    await seed.close();

    const hostLane = (
      lane: "data" | "control",
      field: "ghostprovV2Data" | "ghostprovV2Control",
    ) => new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: `fresh-host:${lane}`,
      field,
      parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
      providerFactory: doc => hub.create(doc, `fresh-host-${lane}`),
      syncTimeoutMs: 100,
    });
    const session = new V2TransportSession({
      binding,
      channel: new SplitAwarenessChannel({
        data: hostLane("data", "ghostprovV2Data"),
        control: hostLane("control", "ghostprovV2Control"),
      }),
      expectedSidecarInstanceId: "sidecar-fresh-orphan-recovery",
      hostInstanceId: "host-fresh-orphan-recovery",
      maxResponseBytes: 2 * 1024 * 1024,
      publicationIntervalMs: 1,
      retransmitMs: 25,
    });
    const journal = new ClaimJournal({ storage, flushIntervalMs: 0, maxBatch: 8 });
    const admission = new AdmissionController({
      physicalMax: 1,
      acceptedMax: 2,
      hostCeiling: 1,
      memoryBudgetBytes: 16 * 1024 * 1024,
    });
    let upstreamCalls = 0;
    let peer!: DenoAwarenessPeer;
    const engine = new SidecarEngine({
      binding,
      assembler: new RequestAssembler({ maxReservedBytes: 16 * 1024 * 1024 }),
      journal,
      admission,
      upstream: {
        execute: async () => {
          upstreamCalls++;
          throw new Error("recovery must not execute upstream");
        },
      },
      assertExecutionFence: frame => peer.assertExecutionFence(frame),
      emit: (frame, priority, streamId) => peer.enqueue(frame, priority, streamId),
    });
    peer = new DenoAwarenessPeer({
      credential: {
        url: ySweet.url,
        baseUrl: ySweet.baseUrl,
        docId: ySweet.docId,
        token: ySweet.token.reveal(),
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding,
      instanceId: "sidecar-fresh-orphan-recovery",
      engine,
      providerFactory: (doc, _credential, lane) => hub.create(doc, `fresh-sidecar-${lane}`),
      publicationIntervalMs: 1,
      retransmitMs: 25,
      connectTimeoutMs: 100,
      initialActivePerSandbox: 1,
      maxActivePerSandbox: 1,
    });

    try {
      await engine.start();
      await peer.connect();
      await session.connect();
      await waitFor(() => journal.status(requestId) === "acked");
      await waitFor(() => session.snapshot().credits > 0);
      expect(storage.terminals.has(requestId)).toBe(false);
      expect(upstreamCalls).toBe(0);
      expect(session.snapshot()).toMatchObject({ connected: true, credits: 1 });
    } finally {
      await session.close().catch(() => undefined);
      await peer.close().catch(() => undefined);
      admission.close();
      await journal.close().catch(() => undefined);
    }
  }, 10_000);

  it("recovers a persisted terminal across two real R2 launches without another fetch", async () => {
    const hub = new AwarenessHub();
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const oldBinding: ProtocolBinding = {
      protocolRevision: "r2",
      launchNonce: "launch-recovery-source",
      projectId: PROJECT,
      sessionId: "same-persisted-sandbox-session",
      leaderGeneration: 9,
      buildHash: "c".repeat(64),
      schemaHash: "d".repeat(64),
    };
    const newBinding: ProtocolBinding = {
      ...oldBinding,
      launchNonce: "launch-recovery-target",
    };
    const storage = new FakeSidecarStorage([]);
    let upstreamCalls = 0;
    let droppedTerminalFlights = 0;
    const newSidecarFlights: string[][] = [];
    let releaseAfterPrefix!: () => void;
    const afterPrefix = new Promise<void>(resolve => { releaseAfterPrefix = resolve; });
    let prefixProduced!: () => void;
    const prefixProduction = new Promise<void>(resolve => { prefixProduced = resolve; });
    hub.dropBroadcast = source => {
      const frames = source.awareness.getLocalState()?.ghostprovV2Data?.frames ?? [];
      if (source.label === "sidecar-new-data" && frames.length > 0) {
        newSidecarFlights.push(frames.map((frame: SidecarFrame) => frame.kind));
      }
      if (source.label !== "sidecar-old-data") return false;
      const drop = frames.some((frame: SidecarFrame) => frame.kind === "response_terminal");
      if (drop) droppedTerminalFlights++;
      return drop;
    };

    const createHost = (
      binding: ProtocolBinding,
      label: string,
      sidecarInstanceId: string,
    ) => {
      const lane = (
        lane: "data" | "control",
        field: "ghostprovV2Data" | "ghostprovV2Control",
      ) => new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
        envelope: ySweet,
        localPeerId: `${label}:${lane}`,
        field,
        parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
        providerFactory: doc => hub.create(doc, `${label}-${lane}`),
        syncTimeoutMs: 100,
      });
      return new V2TransportSession({
        binding,
        channel: new SplitAwarenessChannel({
          data: lane("data", "ghostprovV2Data"),
          control: lane("control", "ghostprovV2Control"),
        }),
        expectedSidecarInstanceId: sidecarInstanceId,
        maxResponseBytes: 2 * 1024 * 1024,
        publicationIntervalMs: 1,
        retransmitMs: 50,
      });
    };
    const createSidecar = (
      binding: ProtocolBinding,
      label: string,
      instanceId: string,
      journal: ClaimJournal,
      admission: AdmissionController,
    ) => {
      let peer!: DenoAwarenessPeer;
      const engine = new SidecarEngine({
        binding,
        assembler: new RequestAssembler({ maxReservedBytes: 16 * 1024 * 1024 }),
        journal,
        admission,
        upstream: {
          execute: async input => {
            const call = ++upstreamCalls;
            await input.beforeFetch();
            await input.onPrelude?.({
              status: 200,
              contentType: "text/event-stream",
              upstreamRequestId: `upstream-${upstreamCalls}`,
            });
            let emitted = 0;
            return completedUpstream({
              ...input,
              beforeFetch: async () => undefined,
              onEvent: async event => {
                await input.onEvent(event);
                emitted++;
                if (call === 1 && emitted === 1) {
                  prefixProduced();
                  await afterPrefix;
                }
              },
            }, "persisted recovery");
          },
        },
        assertExecutionFence: frame => peer.assertExecutionFence(frame),
        emit: (frame, priority, streamId) => peer.enqueue(frame, priority, streamId),
      });
      peer = new DenoAwarenessPeer({
        credential: {
          url: ySweet.url,
          baseUrl: ySweet.baseUrl,
          docId: ySweet.docId,
          token: ySweet.token.reveal(),
          authorization: "full",
          rotationKey: createCredentialRotationKey(),
        },
        binding,
        instanceId,
        engine,
        providerFactory: (doc, _credential, lane) => hub.create(doc, `${label}-${lane}`),
        publicationIntervalMs: 1,
        retransmitMs: 50,
        connectTimeoutMs: 100,
        initialActivePerSandbox: 1,
        maxActivePerSandbox: 1,
      });
      return { engine, peer };
    };

    const journal1 = new ClaimJournal({ storage, flushIntervalMs: 0, maxBatch: 32 });
    const admission1 = new AdmissionController({
      physicalMax: 1,
      acceptedMax: 2,
      hostCeiling: 1,
      memoryBudgetBytes: 16 * 1024 * 1024,
    });
    const sidecar1 = createSidecar(
      oldBinding,
      "sidecar-old",
      "sidecar-persisted-old",
      journal1,
      admission1,
    );
    const host1 = createHost(oldBinding, "host-old", "sidecar-persisted-old");
    let host2: V2TransportSession | null = null;
    let sidecar2: ReturnType<typeof createSidecar> | null = null;
    let journal2: ClaimJournal | null = null;
    let admission2: AdmissionController | null = null;

    try {
      await sidecar1.engine.start();
      await sidecar1.peer.connect();
      await host1.connect();
      await waitFor(() => host1.snapshot().credits > 0);
      const handle = await host1.run({
        model: "gpt-5.6-sol",
        input: "recover persisted terminal",
        stream: true,
      });
      const iterator = handle.events();
      await prefixProduction;
      const prefix = await Promise.race([
        iterator.next(),
        Bun.sleep(2_000).then(() => { throw new Error("source prefix timed out"); }),
      ]);
      expect(prefix).toMatchObject({
        done: false,
        value: { event: "response.created" },
      });
      await waitFor(() => {
        const provider = [...hub.providers].find(value => value.label === "sidecar-old-data");
        return (provider?.awareness.getLocalState()?.ghostprovV2Data?.frames?.length ?? 0) === 0;
      });
      releaseAfterPrefix();
      await waitFor(() => droppedTerminalFlights > 0 && storage.terminals.size === 1);
      expect(upstreamCalls).toBe(1);
      expect(storage.terminals.size).toBe(1);
      expect(await Promise.race([
        handle.final.then(() => "settled" as const, () => "settled" as const),
        Promise.resolve("pending" as const),
      ])).toBe("pending");

      const capsule = await host1.suspendSidecarRecovery("real-two-launch");
      await sidecar1.peer.close();
      admission1.close();
      await journal1.close();

      journal2 = new ClaimJournal({ storage, flushIntervalMs: 0, maxBatch: 32 });
      admission2 = new AdmissionController({
        physicalMax: 1,
        acceptedMax: 2,
        hostCeiling: 1,
        memoryBudgetBytes: 16 * 1024 * 1024,
      });
      sidecar2 = createSidecar(
        newBinding,
        "sidecar-new",
        "sidecar-persisted-new",
        journal2,
        admission2,
      );
      host2 = createHost(newBinding, "host-new", "sidecar-persisted-new");
      await sidecar2.engine.start();
      await sidecar2.peer.connect();
      const barrier = host2.adoptRecoveredSidecar(capsule);
      await host2.connect();
      const recoveredDelta = await Promise.race([
        iterator.next(),
        Bun.sleep(5_000).then(() => { throw new Error("recovered delta timed out"); }),
      ]);
      const recoveredCompleted = await Promise.race([
        iterator.next(),
        Bun.sleep(5_000).then(() => { throw new Error("recovered completion timed out"); }),
      ]);
      expect(recoveredDelta).toMatchObject({
        done: false,
        value: { event: "response.output_text.delta" },
      });
      expect(recoveredCompleted).toMatchObject({
        done: false,
        value: { event: "response.completed" },
      });
      expect(await iterator.next()).toMatchObject({ done: true });
      await expect(handle.final).resolves.toMatchObject({ status: "completed" });
      await Promise.race([
        barrier,
        Bun.sleep(5_000).then(() => { throw new Error("real recovery barrier timed out"); }),
      ]);

      expect(upstreamCalls).toBe(1);
      expect(storage.terminals.size).toBe(0);
      expect(journal2.status(storage.records[0]!.requestId)).toBe("acked");
      const terminalFlight = newSidecarFlights.findIndex(kinds => kinds.includes("response_terminal"));
      const markerFlight = newSidecarFlights.findIndex(kinds => kinds.includes("recovery_complete"));
      expect(terminalFlight).toBeGreaterThanOrEqual(0);
      expect(markerFlight).toBeGreaterThan(terminalFlight);
      expect(newSidecarFlights[terminalFlight]).not.toContain("recovery_complete");
      expect(newSidecarFlights[markerFlight]).not.toContain("response_terminal");
    } finally {
      releaseAfterPrefix?.();
      await host2?.close().catch(() => undefined);
      await sidecar2?.peer.close().catch(() => undefined);
      admission2?.close();
      await journal2?.close().catch(() => undefined);
      await host1.close().catch(() => undefined);
      await sidecar1.peer.close().catch(() => undefined);
      admission1.close();
      await journal1.close().catch(() => undefined);
    }
  }, 15_000);
});
