import * as Y from "npm:yjs@13.6.27";
import { YSweetProvider } from "npm:@y-sweet/client@0.9.1";
import type { BootstrapCredentialPayload } from "../../src/v2/bootstrap/crypto.ts";
import {
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type SidecarFrame,
} from "../../src/v2/protocol/shared.ts";
import { parseAwarenessEnvelope } from "../../src/v2/protocol/validate.ts";
import {
  InboundFrameCursor,
  OutboundFrameWindow,
} from "../../src/v2/protocol/window.ts";
import {
  WeightedFrameScheduler,
  type FramePriority,
} from "../../src/v2/protocol/scheduler.ts";
import type { SidecarEngine } from "../../src/v2/sidecar/engine.ts";

export interface DenoAwarenessPeerOptions {
  credential: BootstrapCredentialPayload;
  binding: ProtocolBinding;
  instanceId: string;
  engine: SidecarEngine;
  retransmitMs?: number;
}

export class DenoAwarenessPeer {
  readonly #binding: ProtocolBinding;
  readonly #instanceId: string;
  readonly #engine: SidecarEngine;
  readonly #doc: Y.Doc;
  readonly #provider: YSweetProvider;
  readonly #outbound = new OutboundFrameWindow<SidecarFrame>({ epoch: 1 });
  readonly #inbound = new InboundFrameCursor<HostFrame>({ epoch: 1 });
  readonly #scheduler = new WeightedFrameScheduler<SidecarFrame>();
  readonly #retransmitMs: number;
  #revision = 0;
  #hostPeerId: string | null = null;
  #hostClientId: number | null = null;
  #hostRevision = -1;
  #timer: number | null = null;
  #closed = false;

  constructor(options: DenoAwarenessPeerOptions) {
    this.#binding = options.binding;
    this.#instanceId = options.instanceId;
    this.#engine = options.engine;
    this.#retransmitMs = options.retransmitMs ?? 250;
    this.#doc = new Y.Doc({ guid: options.credential.docId });
    this.#provider = new YSweetProvider(
      async () => options.credential as any,
      options.credential.docId,
      this.#doc,
      {
        connect: false,
        initialClientToken: options.credential as any,
        offlineSupport: false,
        showDebuggerLink: false,
      },
    );
    this.#provider.awareness.on("change", this.#onAwarenessChange);
  }

  async connect(): Promise<void> {
    if (this.#closed) throw new Error("Awareness peer is closed");
    await this.#provider.connect();
    this.#publish();
    this.#timer = setInterval(() => {
      if (this.#outbound.snapshot().frames.length) this.#publish();
    }, this.#retransmitMs) as unknown as number;
  }

  enqueue(
    frame: SidecarFrame,
    priority: FramePriority,
    streamId: string,
    weight = 1,
  ): void {
    if (this.#closed) return;
    this.#scheduler.enqueue(priority, streamId, frame, weight);
    this.#pump();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer !== null) clearInterval(this.#timer);
    this.#provider.awareness.off("change", this.#onAwarenessChange);
    this.#provider.awareness.setLocalState(null);
    this.#provider.disconnect();
    this.#provider.destroy();
    this.#doc.destroy();
  }

  readonly #onAwarenessChange = (
    change: { added: number[]; updated: number[]; removed: number[] },
  ): void => {
    for (const clientId of change.removed) {
      if (clientId === this.#hostClientId) {
        this.#hostPeerId = null;
        this.#hostClientId = null;
        this.#hostRevision = -1;
      }
    }
    for (const clientId of [...change.added, ...change.updated]) {
      if (clientId === this.#doc.clientID) continue;
      const raw = this.#provider.awareness.getStates().get(clientId)?.ghostprovV2;
      if (raw == null) continue;
      let envelope: AwarenessEnvelope<HostFrame>;
      try {
        envelope = parseAwarenessEnvelope(raw, this.#binding, "host");
      } catch {
        continue;
      }
      if (this.#hostPeerId && envelope.peerId !== this.#hostPeerId) continue;
      if (envelope.revision <= this.#hostRevision) continue;
      this.#hostPeerId = envelope.peerId;
      this.#hostClientId = clientId;
      this.#hostRevision = envelope.revision;
      try {
        this.#engine.setHostCeiling(envelope.credits);
        this.#outbound.acknowledge(envelope.ack);
        const currentEpoch = this.#inbound.ack().epoch;
        if (envelope.epoch > currentEpoch) this.#inbound.reset(envelope.epoch);
        const frames = this.#inbound.accept(envelope.frames);
        for (const frame of frames) {
          void this.#engine.accept(frame).catch(() => undefined);
        }
        this.#pump();
      } catch {
        // Invalid ACK/window transitions are ignored; a valid current host
        // envelope must re-establish contiguous state.
      }
    }
  };

  #pump(): void {
    for (;;) {
      const snapshot = this.#outbound.snapshot();
      const remainingFrames = GP_V2_WINDOW_MAX_FRAMES - snapshot.frames.length;
      const remainingBytes = GP_V2_WINDOW_MAX_BYTES - snapshot.encodedBytes;
      if (remainingFrames < 1 || remainingBytes < 512) break;
      const next = this.#scheduler.take(1, Math.min(15 * 1024, remainingBytes - 256));
      if (!next.length) break;
      this.#outbound.push(next[0]);
    }
    this.#publish();
  }

  #publish(): void {
    if (this.#closed) return;
    const transport = this.#engine.snapshot();
    const outbound = this.#outbound.snapshot();
    const envelope: AwarenessEnvelope<SidecarFrame> = {
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: this.#binding.schemaHash,
      buildHash: this.#binding.buildHash,
      projectId: this.#binding.projectId,
      sessionId: this.#binding.sessionId,
      peerId: "sidecar",
      instanceId: this.#instanceId,
      leaderGeneration: this.#binding.leaderGeneration,
      epoch: outbound.epoch,
      revision: this.#revision++,
      ack: this.#inbound.ack(),
      credits: transport.credits,
      active: transport.active,
      queued: transport.queued,
      draining: transport.draining,
      frames: outbound.frames,
    };
    this.#provider.awareness.setLocalStateField("ghostprovV2", envelope);
  }
}
