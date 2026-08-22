import type { ProjectId } from "../../sandbox/types";
import type { YSweetEnvelope } from "../../prism/protocol";
import type { AwarenessEnvelope } from "../protocol/shared";

export interface AwarenessPeerUpdate<T> {
  peerClientId: number | string;
  envelope: AwarenessEnvelope<T>;
}

export interface AwarenessChannel<TLocal, TRemote> {
  readonly projectId: ProjectId;
  readonly localPeerId: string;
  connect(signal?: AbortSignal): Promise<void>;
  publish(envelope: AwarenessEnvelope<TLocal>): void;
  clear(): void;
  subscribe(listener: (update: AwarenessPeerUpdate<TRemote>) => void): () => void;
  peerIds(): readonly string[];
  rotateCredential?(envelope: YSweetEnvelope): void;
  close(): Promise<void>;
}
