import type { YSweetEnvelope } from "../../prism/protocol";
import type { ProjectId } from "../../sandbox/types";
import type { AwarenessEnvelope, AwarenessLane } from "../protocol/shared";
import type { AwarenessChannel, AwarenessPeerUpdate } from "./channel";

export interface SplitAwarenessChannelOptions<TLocal, TRemote> {
  data: AwarenessChannel<TLocal, TRemote>;
  control: AwarenessChannel<TLocal, TRemote>;
}

/**
 * Keeps high-volume reliable frames and low-volume ACK/control state on
 * independent Awareness client states while exposing one transport channel.
 */
export class SplitAwarenessChannel<TLocal, TRemote>
implements AwarenessChannel<TLocal, TRemote> {
  readonly projectId: ProjectId;
  readonly localPeerId: string;
  readonly #data: AwarenessChannel<TLocal, TRemote>;
  readonly #control: AwarenessChannel<TLocal, TRemote>;
  #closed = false;
  #connected = false;
  #connecting: Promise<void> | null = null;

  constructor(options: SplitAwarenessChannelOptions<TLocal, TRemote>) {
    if (options.data.projectId !== options.control.projectId) {
      throw new TypeError("Split Awareness lanes belong to different projects");
    }
    this.projectId = options.data.projectId;
    this.localPeerId = options.data.localPeerId;
    this.#data = options.data;
    this.#control = options.control;
  }

  connect(signal?: AbortSignal): Promise<void> {
    if (this.#closed) return Promise.reject(new Error("Split Awareness channel is closed"));
    if (this.#connected) return Promise.resolve();
    if (this.#connecting) return this.#connecting;
    const connecting = Promise.all([
      this.#data.connect(signal),
      this.#control.connect(signal),
    ]).then(() => {
      this.#connected = true;
    }).catch(async error => {
      await this.close();
      throw error;
    }).finally(() => {
      if (this.#connecting === connecting) this.#connecting = null;
    });
    this.#connecting = connecting;
    return connecting;
  }

  publish(envelope: AwarenessEnvelope<TLocal>, lane: AwarenessLane = "data"): void {
    if (this.#closed) throw new Error("Split Awareness channel is closed");
    const selected = lane === "data" ? this.#data : this.#control;
    selected.publish({ ...envelope, lane }, lane);
  }

  clear(lane?: AwarenessLane): void {
    if (this.#closed) return;
    if (lane === undefined || lane === "data") this.#data.clear("data");
    if (lane === undefined || lane === "control") this.#control.clear("control");
  }

  subscribe(listener: (update: AwarenessPeerUpdate<TRemote>) => void): () => void {
    if (this.#closed) throw new Error("Split Awareness channel is closed");
    const subscribeLane = (
      lane: AwarenessLane,
      channel: AwarenessChannel<TLocal, TRemote>,
    ) => channel.subscribe(update => listener({
      peerClientId: `${lane}:${String(update.peerClientId)}`,
      envelope: { ...update.envelope, lane },
    }));
    const unsubscribeData = subscribeLane("data", this.#data);
    const unsubscribeControl = subscribeLane("control", this.#control);
    return () => {
      unsubscribeData();
      unsubscribeControl();
    };
  }

  peerIds(): readonly string[] {
    return [...new Set([
      ...this.#data.peerIds(),
      ...this.#control.peerIds(),
    ])].sort();
  }

  rotateCredential(envelope: YSweetEnvelope): void {
    if (this.#closed) throw new Error("Split Awareness channel is closed");
    this.#data.rotateCredential?.(envelope);
    this.#control.rotateCredential?.(envelope);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#connected = false;
    await Promise.allSettled([
      this.#data.close(),
      this.#control.close(),
    ]);
  }
}
