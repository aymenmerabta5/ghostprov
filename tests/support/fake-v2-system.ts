import type { SseFrame } from "../../src/relay/sse";
import type { RunHandle } from "../../src/relay/run-handle";
import { ResourceGovernor } from "../../src/router/resource-governor";
import { MultiSandboxRouter } from "../../src/router/router";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../../src/sandbox/registry";
import { projectId, type ProjectId } from "../../src/sandbox/types";
import type {
  AwarenessChannel,
  AwarenessPeerUpdate,
} from "../../src/v2/awareness/channel";
import {
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  type AwarenessEnvelope,
  type AwarenessLane,
  type HostFrame,
  type ProtocolBinding,
  type SidecarFrame,
} from "../../src/v2/protocol/shared";
import { WeightedFrameScheduler, type FramePriority } from "../../src/v2/protocol/scheduler";
import { TranscriptProof } from "../../src/v2/protocol/transcript";
import { InboundFrameCursor, OutboundFrameWindow } from "../../src/v2/protocol/window";
import { AdmissionController } from "../../src/v2/sidecar/admission";
import { RequestAssembler } from "../../src/v2/sidecar/assembler";
import { SidecarEngine } from "../../src/v2/sidecar/engine";
import { ClaimJournal } from "../../src/v2/sidecar/journal";
import type {
  DirectResponsesExecution,
  UpstreamTerminal,
} from "../../src/v2/sidecar/upstream";
import { V2TransportSession } from "../../src/v2/transport/session";
import {
  FakeAwarenessNetwork,
  type FaultOptions,
} from "./fake-awareness";
import { FakeSidecarStorage } from "./fake-v2-sidecar";

const MiB = 1024 * 1024;

class Gate {
  #released = false;
  #resolve!: () => void;
  readonly #promise = new Promise<void>(resolve => { this.#resolve = resolve; });

  wait(): Promise<void> {
    return this.#released ? Promise.resolve() : this.#promise;
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.#resolve();
  }
}

export class FakeScaleUpstream {
  calls = 0;
  active = 0;
  maxActive = 0;
  readonly startedInputs: string[] = [];
  readonly receivedBodies: Record<string, unknown>[] = [];

  constructor(
    readonly runtimeIndex: number,
    readonly gate: Gate,
  ) {}

  async execute(input: DirectResponsesExecution): Promise<UpstreamTerminal> {
    await input.beforeFetch();
    const body = JSON.parse(new TextDecoder().decode(input.body)) as Record<string, unknown>;
    const call = ++this.calls;
    this.active++;
    this.maxActive = Math.max(this.maxActive, this.active);
    this.startedInputs.push(String(body.input ?? ""));
    this.receivedBodies.push(body);
    try {
      await this.gate.wait();
      const responseId = `resp_${this.runtimeIndex}_${call}`;
      const tools = Array.isArray(body.tools) ? body.tools as Array<Record<string, unknown>> : [];
      const imageTool = tools.find(tool => tool.type === "image_generation");
      const functionTool = tools.find(tool => tool.type === "function");
      const output = imageTool
        ? [{
          id: `ig_${this.runtimeIndex}_${call}`,
          type: "image_generation_call",
          status: "completed",
          result: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
          revised_prompt: "deterministic image",
          background: imageTool.background ?? "opaque",
          output_format: imageTool.output_format ?? "png",
          quality: imageTool.quality ?? "low",
          size: imageTool.size ?? "1024x1024",
        }]
        : functionTool
          ? [{
            id: `fc_${this.runtimeIndex}_${call}`,
            type: "function_call",
            status: "completed",
            call_id: "call_fake_1",
            name: functionTool.name,
            arguments: JSON.stringify({ value: "V2_TOOL_OK" }),
          }]
          : [{
            type: "message",
            role: "assistant",
            status: "completed",
            content: [{ type: "output_text", text: String(body.input ?? "") }],
          }];
      const final = {
        id: responseId,
        object: "response",
        status: "completed",
        output,
      };
      const events: SseFrame[] = [
        {
          event: "response.created",
          data: JSON.stringify({ type: "response.created", response: { id: responseId } }),
        },
        ...(!imageTool && !functionTool ? [{
          event: "response.output_text.delta",
          data: JSON.stringify({
            type: "response.output_text.delta",
            delta: String(body.input ?? ""),
          }),
        }] : []),
        {
          event: "response.completed",
          data: JSON.stringify({ type: "response.completed", response: final }),
        },
      ];
      const transcript = new TranscriptProof("upstream", "0".repeat(64));
      for (const event of events) {
        transcript.append(event);
        await input.onEvent(event);
      }
      const terminal = await transcript.createTerminal();
      const snapshot = transcript.snapshot();
      return {
        responseId,
        status: "completed",
        eventCount: terminal.eventCount,
        byteCount: terminal.byteCount,
        transcriptSha256: terminal.transcriptSha256,
        finalEventIndex: terminal.finalEventIndex,
        finalSha256: terminal.finalSha256,
        final,
        transcriptBytes: snapshot.canonicalBytes,
      };
    } finally {
      this.active--;
    }
  }
}

class FakeSidecarPeer {
  readonly #binding: ProtocolBinding;
  readonly #channel: AwarenessChannel<SidecarFrame, HostFrame>;
  readonly #engine: SidecarEngine;
  readonly #outbound = new OutboundFrameWindow<SidecarFrame>({ epoch: 1 });
  readonly #inbound = new InboundFrameCursor<HostFrame>({ epoch: 1 });
  readonly #scheduler = new WeightedFrameScheduler<SidecarFrame>();
  #unsubscribe: (() => void) | null = null;
  #processing: Promise<void> = Promise.resolve();
  #revision = 0;
  readonly #hostRevision: Record<AwarenessLane, number> = {
    data: -1,
    control: -1,
  };
  #closed = false;

  constructor(options: {
    binding: ProtocolBinding;
    channel: AwarenessChannel<SidecarFrame, HostFrame>;
    engine: SidecarEngine;
  }) {
    this.#binding = options.binding;
    this.#channel = options.channel;
    this.#engine = options.engine;
  }

  async connect(): Promise<void> {
    this.#unsubscribe = this.#channel.subscribe(this.#onHost);
    await this.#channel.connect();
    await this.#engine.start();
    this.#publish();
  }

  enqueue(
    frame: SidecarFrame,
    priority: FramePriority,
    streamId: string,
  ): void {
    if (this.#closed) return;
    this.#scheduler.enqueue(priority, streamId, frame);
    this.#pump();
    this.#publish();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#processing.catch(() => undefined);
    this.#unsubscribe?.();
    await this.#channel.close();
  }

  readonly #onHost = (update: AwarenessPeerUpdate<HostFrame>): void => {
    const lane = update.envelope.lane ?? "data";
    if (update.envelope.revision <= this.#hostRevision[lane]) return;
    this.#hostRevision[lane] = update.envelope.revision;
    this.#processing = this.#processing
      .then(() => this.#acceptEnvelope(update.envelope))
      .catch(() => undefined);
  };

  async #acceptEnvelope(envelope: AwarenessEnvelope<HostFrame>): Promise<void> {
    const lane = envelope.lane ?? "data";
    const before = this.#outbound.snapshot().frames.length;
    if (lane === "control" || envelope.lane === undefined) {
      this.#engine.setHostCeiling(envelope.credits);
    }
    this.#outbound.acknowledge(envelope.ack);
    const ackProgress = this.#outbound.snapshot().frames.length < before;
    if (lane === "control" && envelope.frames.length > 0) {
      throw new Error("fake sidecar received framed control state");
    }
    const frames = lane === "data" ? this.#inbound.accept(envelope.frames) : [];
    for (const frame of frames) await this.#engine.accept(frame);
    const pumped = this.#pump();
    if (frames.length > 0 || ackProgress || pumped) this.#publish();
  }

  #pump(): boolean {
    let changed = false;
    for (;;) {
      const snapshot = this.#outbound.snapshot();
      const frameBudget = GP_V2_WINDOW_MAX_FRAMES - snapshot.frames.length;
      const byteBudget = GP_V2_WINDOW_MAX_BYTES - snapshot.encodedBytes;
      if (frameBudget < 1 || byteBudget < 512) break;
      const next = this.#scheduler.take(1, Math.max(1, byteBudget - 256));
      if (!next.length) break;
      this.#outbound.push(next[0]!);
      changed = true;
    }
    return changed;
  }

  #publish(): void {
    if (this.#closed) return;
    const transport = this.#engine.snapshot();
    const outbound = this.#outbound.snapshot();
    this.#channel.publish({
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: this.#binding.schemaHash,
      buildHash: this.#binding.buildHash,
      projectId: this.#binding.projectId,
      sessionId: this.#binding.sessionId,
      peerId: "sidecar",
      instanceId: `sidecar-${this.#binding.projectId}`,
      leaderGeneration: this.#binding.leaderGeneration,
      epoch: outbound.epoch,
      revision: this.#revision++,
      ack: this.#inbound.ack(),
      credits: transport.credits,
      active: transport.active,
      queued: transport.queued,
      draining: transport.draining,
      frames: outbound.frames,
    });
  }
}

export class FakeV2ScaleRuntime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  readonly upstream: FakeScaleUpstream;
  readonly #physical: number;
  readonly #network: FakeAwarenessNetwork<HostFrame, SidecarFrame>;
  readonly #host: AwarenessChannel<HostFrame, SidecarFrame>;
  readonly #sidecar: AwarenessChannel<SidecarFrame, HostFrame>;
  readonly #session: V2TransportSession;
  readonly #peer: FakeSidecarPeer;
  #pumpTimer: ReturnType<typeof setInterval> | null = null;
  #flushing = false;
  #state = "configured";

  constructor(options: {
    index: number;
    physical: number;
    gate: Gate;
    faults?: FaultOptions;
  }) {
    this.projectId = projectId(
      `00000000-0000-4000-8000-${String(options.index).padStart(12, "0")}`,
    );
    this.#physical = options.physical;
    this.#network = new FakeAwarenessNetwork(options.faults);
    const binding: ProtocolBinding = {
      projectId: this.projectId,
      sessionId: `sandbox-session-${options.index}`,
      leaderGeneration: 1,
      buildHash: "a".repeat(64),
      schemaHash: "b".repeat(64),
    };
    this.#host = this.#network.connectHost(`host-${options.index}`, this.projectId);
    this.#sidecar = this.#network.connectSidecar(`sidecar-${options.index}`, this.projectId);
    this.#session = new V2TransportSession({
      binding,
      channel: this.#host,
      maxResponseBytes: 16 * MiB,
      hostCeiling: options.physical,
      retransmitMs: 25,
      publicationIntervalMs: 1,
    });
    this.upstream = new FakeScaleUpstream(options.index, options.gate);
    const timeline: string[] = [];
    const storage = new FakeSidecarStorage(timeline);
    const admission = new AdmissionController({
      physicalMax: options.physical,
      acceptedMax: options.physical * 2,
      hostCeiling: options.physical,
      memoryBudgetBytes: Math.max(64 * MiB, options.physical * 16 * MiB),
    });
    let peer!: FakeSidecarPeer;
    const engine = new SidecarEngine({
      binding,
      assembler: new RequestAssembler({ maxReservedBytes: 128 * MiB }),
      journal: new ClaimJournal({ storage, flushIntervalMs: 0, maxBatch: 128 }),
      admission,
      upstream: this.upstream,
      emit: (frame, priority, streamId) => peer.enqueue(frame, priority, streamId),
    });
    peer = new FakeSidecarPeer({ binding, channel: this.#sidecar, engine });
    this.#peer = peer;
  }

  async start(): Promise<void> {
    if (this.#state === "ready") return;
    this.#pumpTimer = setInterval(() => { void this.#flush(); }, 1);
    await this.#session.connect();
    await this.#peer.connect();
    await this.#waitFor(() => this.#session.snapshot().credits > 0);
    this.#state = "ready";
  }

  async drain(reason: string): Promise<void> {
    this.#state = "draining";
    await this.#session.drain(reason);
    await this.#peer.close();
    this.#stopPump();
    this.#state = "stopped";
  }

  async stop(): Promise<void> {
    await this.#session.close();
    await this.#peer.close();
    this.#stopPump();
    this.#state = "stopped";
  }

  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle> {
    return this.#session.run(body, signal);
  }

  snapshot(): RoutableSandboxSnapshot {
    const transport = this.#session.snapshot();
    const capacity = this.#state === "ready" ? this.#physical : 0;
    return {
      projectId: this.projectId,
      state: this.#state,
      capacity,
      active: transport.active,
      available: this.#state === "ready"
        ? Math.max(0, Math.min(transport.credits, capacity - transport.active - transport.quarantined))
        : 0,
      quarantined: transport.quarantined,
      heartbeatAgeMs: 0,
      credentialExpiresInMs: 60_000,
      ackEwmaMs: transport.awarenessRttMs ?? 0,
      completionEwmaMs: 0,
      recentFailureRate: 0,
      queueDepth: transport.queued,
      memoryPressure: 0,
      failure: null,
    };
  }

  async #flush(): Promise<void> {
    if (this.#flushing) return;
    this.#flushing = true;
    try {
      await this.#network.flushAll();
    } finally {
      this.#flushing = false;
    }
  }

  async #waitFor(predicate: () => boolean, timeoutMs = 10_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!predicate()) {
      if (Date.now() >= deadline) throw new Error("Fake V2 runtime condition timed out");
      await this.#flush();
      await Bun.sleep(1);
    }
  }

  #stopPump(): void {
    if (this.#pumpTimer) clearInterval(this.#pumpTimer);
    this.#pumpTimer = null;
  }
}

export interface FakeV2System {
  router: MultiSandboxRouter;
  runtimes: readonly FakeV2ScaleRuntime[];
  waitForActive(count: number, timeoutMs?: number): Promise<void>;
  releaseUpstream(): void;
  totalUpstreamCalls(): number;
  close(): Promise<void>;
}

export async function createFakeV2System(options: {
  runtimes: number;
  physicalPerRuntime: number;
  holdUpstream?: boolean;
  faults?: FaultOptions;
}): Promise<FakeV2System> {
  const gates = Array.from({ length: options.runtimes }, () => new Gate());
  if (!options.holdUpstream) gates.forEach(gate => gate.release());
  const runtimes = gates.map((gate, index) => new FakeV2ScaleRuntime({
    index: index + 1,
    physical: options.physicalPerRuntime,
    gate,
    faults: options.faults,
  }));
  await Promise.all(runtimes.map(runtime => runtime.start()));
  const registry = new SandboxRegistry(runtimes);
  const probe = {
    sample: () => ({
      totalBytes: 64 * 1024 ** 3,
      freeBytes: 52 * 1024 ** 3,
      rssBytes: 256 * MiB,
      heapBytes: 64 * MiB,
      externalBytes: 16 * MiB,
      logicalProcessors: 16,
    }),
  };
  const router = new MultiSandboxRouter({
    registry,
    governor: new ResourceGovernor({ probe }),
  });
  return {
    router,
    runtimes,
    async waitForActive(count: number, timeoutMs = 15_000): Promise<void> {
      const deadline = Date.now() + timeoutMs;
      while (runtimes.reduce((sum, runtime) => sum + runtime.upstream.active, 0) < count) {
        if (Date.now() >= deadline) {
          throw new Error(`Timed out waiting for ${count} active V2 upstream calls`);
        }
        await Bun.sleep(2);
      }
    },
    releaseUpstream() { gates.forEach(gate => gate.release()); },
    totalUpstreamCalls() {
      return runtimes.reduce((sum, runtime) => sum + runtime.upstream.calls, 0);
    },
    async close() {
      router.beginShutdown();
      await Promise.allSettled(runtimes.map(runtime => runtime.stop()));
      await router.close();
    },
  };
}
