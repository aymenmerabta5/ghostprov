import type { ProjectId } from "../../sandbox/types";
import type { YSweetEnvelope } from "../../prism/protocol";
import type { AwarenessEnvelope, AwarenessLane } from "../protocol/shared";

export interface AwarenessPeerUpdate<T> {
  peerClientId: number | string;
  envelope: AwarenessEnvelope<T>;
}

export interface AwarenessChannel<TLocal, TRemote> {
  readonly projectId: ProjectId;
  readonly localPeerId: string;
  connect(signal?: AbortSignal): Promise<void>;
  /** Explicitly renew one existing provider connection without replacing its credential or state. */
  reconnect?(lane: AwarenessLane, signal?: AbortSignal): Promise<void>;
  publish(envelope: AwarenessEnvelope<TLocal>, lane?: AwarenessLane): void;
  clear(lane?: AwarenessLane): void;
  subscribe(listener: (update: AwarenessPeerUpdate<TRemote>) => void): () => void;
  peerIds(): readonly string[];
  rotateCredential?(envelope: YSweetEnvelope): void | Promise<void>;
  onConnectionFailure?(listener: (error: Error) => void): () => void;
  close(): Promise<void>;
}
