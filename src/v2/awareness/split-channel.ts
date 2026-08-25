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
  readonly #failureListeners = new Set<(error: Error) => void>();
  readonly #unsubscribeFailures: Array<() => void> = [];
  #closed = false;
  #connected = false;
  #everConnected = false;
  #connecting: Promise<void> | null = null;

  constructor(options: SplitAwarenessChannelOptions<TLocal, TRemote>) {
    if (options.data.projectId !== options.control.projectId) {
      throw new TypeError("Split Awareness lanes belong to different projects");
    }
    this.projectId = options.data.projectId;
    this.localPeerId = options.data.localPeerId;
    this.#data = options.data;
    this.#control = options.control;
    for (const channel of [this.#data, this.#control]) {
      const unsubscribe = channel.onConnectionFailure?.(error => {
        if (this.#closed) return;
        this.#connected = false;
        for (const listener of this.#failureListeners) {
          try { listener(error); } catch {}
        }
      });
      if (unsubscribe) this.#unsubscribeFailures.push(unsubscribe);
    }
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
      this.#everConnected = true;
    }).catch(async error => {
      this.#connected = false;
      if (!this.#everConnected) await this.close();
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

  onConnectionFailure(listener: (error: Error) => void): () => void {
    if (this.#closed) throw new Error("Split Awareness channel is closed");
    this.#failureListeners.add(listener);
    return () => this.#failureListeners.delete(listener);
  }

  async rotateCredential(envelope: YSweetEnvelope): Promise<void> {
    if (this.#closed) throw new Error("Split Awareness channel is closed");
    await Promise.all([
      Promise.resolve(this.#data.rotateCredential?.(envelope)),
      Promise.resolve(this.#control.rotateCredential?.(envelope)),
    ]);
    if (this.#closed) throw new Error("Split Awareness channel is closed");
    this.#connected = true;
    this.#everConnected = true;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#connected = false;
    for (const unsubscribe of this.#unsubscribeFailures.splice(0)) unsubscribe();
    this.#failureListeners.clear();
    await Promise.allSettled([
      this.#data.close(),
      this.#control.close(),
    ]);
  }
}
