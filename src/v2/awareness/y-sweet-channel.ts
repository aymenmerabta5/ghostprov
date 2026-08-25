import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import WebSocket from "ws";
import type { Awareness } from "y-protocols/awareness";
import type { YSweetEnvelope } from "../../prism/protocol";
import type { ProjectId } from "../../sandbox/types";
import type {
  AwarenessChannel,
  AwarenessPeerUpdate,
} from "./channel";
import type { AwarenessEnvelope } from "../protocol/shared";

export interface AwarenessProviderLike {
  readonly awareness: Pick<Awareness,
    | "getStates"
    | "getLocalState"
    | "setLocalState"
    | "setLocalStateField"
    | "on"
    | "off"
  >;
  on(event: string, listener: (...args: any[]) => void): void;
  off(event: string, listener: (...args: any[]) => void): void;
  connect(): void;
  disconnect(): void;
  destroy(): void;
}

export class AwarenessChannelError extends Error {
  readonly name = "AwarenessChannelError";

  constructor(
    message: string,
    readonly kind: "closed" | "connect" | "timeout" | "state" | "disconnected" | "auth",
  ) {
    super(message);
  }
}

export interface YSweetAwarenessChannelOptions<TLocal, TRemote> {
  envelope: YSweetEnvelope;
  localPeerId: string;
  field: "ghostprovV2" | "ghostprovV2Data" | "ghostprovV2Control";
  parseRemote(value: unknown): AwarenessEnvelope<TRemote>;
  providerFactory?: (
    doc: Y.Doc,
    credential: () => YSweetEnvelope,
  ) => AwarenessProviderLike;
  syncTimeoutMs?: number;
}

function defaultProviderFactory(
  credential: () => YSweetEnvelope,
): (doc: Y.Doc) => AwarenessProviderLike {
  class AuthenticatedWebSocket extends WebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      const target = new URL(String(url));
      target.searchParams.set("token", credential().token.reveal());
      super(target, protocols as any);
    }
  }
  return doc => new WebsocketProvider(
    credential().url,
    credential().docId,
    doc,
    {
      connect: false,
      disableBc: true,
      WebSocketPolyfill: AuthenticatedWebSocket as any,
    },
  ) as AwarenessProviderLike;
}

export class YSweetAwarenessChannel<TLocal, TRemote>
implements AwarenessChannel<TLocal, TRemote> {
  readonly projectId: ProjectId;
  readonly localPeerId: string;
  readonly #field: "ghostprovV2" | "ghostprovV2Data" | "ghostprovV2Control";
  readonly #parseRemote: (value: unknown) => AwarenessEnvelope<TRemote>;
  readonly #syncTimeoutMs: number;
  readonly #doc: Y.Doc;
  readonly #provider: AwarenessProviderLike;
  readonly #listeners = new Set<(update: AwarenessPeerUpdate<TRemote>) => void>();
  readonly #failureListeners = new Set<(error: Error) => void>();
  readonly #peers = new Map<number, string>();
  #connected = false;
  #closed = false;
  #connecting: Promise<void> | null = null;
  #cancelConnecting: ((error: Error) => void) | null = null;
  #providerGeneration = 0;
  #providerLifecycle: {
    generation: number;
    onSync: (synced: boolean) => void;
    onError: (...args: any[]) => void;
    onClose: (...args: any[]) => void;
  } | null = null;
  #lastConnectionError: AwarenessChannelError | null = null;
  #credential: YSweetEnvelope;

  constructor(options: YSweetAwarenessChannelOptions<TLocal, TRemote>) {
    if (!options.localPeerId || options.localPeerId.length > 128) {
      throw new AwarenessChannelError("localPeerId is invalid", "state");
    }
    const timeout = options.syncTimeoutMs ?? 15_000;
    if (!Number.isFinite(timeout) || timeout < 1) {
      throw new AwarenessChannelError("syncTimeoutMs must be positive", "state");
    }
    this.projectId = options.envelope.docId;
    this.#credential = options.envelope;
    this.localPeerId = options.localPeerId;
    this.#field = options.field;
    this.#parseRemote = options.parseRemote;
    this.#syncTimeoutMs = timeout;
    this.#doc = new Y.Doc({ guid: options.envelope.docId });
    const credential = () => this.#credential;
    this.#provider = options.providerFactory
      ? options.providerFactory(this.#doc, credential)
      : defaultProviderFactory(credential)(this.#doc);
    this.#provider.awareness.on("change", this.#onAwarenessChange);
  }

  connect(signal?: AbortSignal): Promise<void> {
    if (this.#closed) {
      return Promise.reject(new AwarenessChannelError("Awareness channel is closed", "closed"));
    }
    if (this.#connected) return Promise.resolve();
    if (this.#connecting) return this.#connecting;
    const generation = ++this.#providerGeneration;
    this.#detachProviderLifecycle();
    const connecting = new Promise<void>((resolve, reject) => {
      let settled = false;
      const cleanupWait = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        this.#provider.off("sync", onSync);
        this.#cancelConnecting = null;
      };
      const fail = (error: Error) => {
        if (
          generation !== this.#providerGeneration ||
          this.#providerLifecycle?.generation !== generation
        ) return;
        const wasConnected = this.#connected;
        this.#connected = false;
        if (error instanceof AwarenessChannelError) this.#lastConnectionError = error;
        this.#peers.clear();
        cleanupWait();
        this.#detachProviderLifecycle();
        try { this.#provider.disconnect(); } catch {}
        if (settled) {
          if (wasConnected) this.#notifyConnectionFailure(error);
          return;
        }
        settled = true;
        reject(error);
      };
      const onSync = (synced: boolean) => {
        if (
          !synced || settled ||
          generation !== this.#providerGeneration ||
          this.#providerLifecycle?.generation !== generation
        ) return;
        settled = true;
        this.#connected = true;
        this.#lastConnectionError = null;
        cleanupWait();
        resolve();
      };
      const onError = (...args: any[]) => fail(this.#connectionFailure(
        "Y-Sweet Awareness connection failed",
        args,
        settled,
      ));
      const onClose = (...args: any[]) => fail(this.#connectionFailure(
        "Y-Sweet Awareness connection closed",
        args,
        settled,
      ));
      const onAbort = () => fail(new DOMException(
        "Awareness connection aborted",
        "AbortError",
      ));
      const timer = setTimeout(() => fail(new AwarenessChannelError(
        "Y-Sweet Awareness synchronization timed out",
        "timeout",
      )), this.#syncTimeoutMs);
      this.#cancelConnecting = error => fail(error);
      this.#providerLifecycle = { generation, onSync, onError, onClose };
      this.#provider.on("sync", onSync);
      this.#provider.on("connection-error", onError);
      this.#provider.on("connection-close", onClose);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      else this.#provider.connect();
    }).finally(() => {
      if (this.#connecting === connecting) this.#connecting = null;
    });
    this.#connecting = connecting;
    return connecting;
  }

  publish(envelope: AwarenessEnvelope<TLocal>): void {
    if (this.#closed) throw new AwarenessChannelError("Awareness channel is closed", "closed");
    if (!this.#connected) {
      throw this.#lastConnectionError ?? new AwarenessChannelError(
        "Awareness channel is not connected",
        "state",
      );
    }
    this.#provider.awareness.setLocalStateField(this.#field, envelope);
  }

  clear(): void {
    if (this.#closed) return;
    this.#provider.awareness.setLocalStateField(this.#field, null);
  }

  subscribe(
    listener: (update: AwarenessPeerUpdate<TRemote>) => void,
  ): () => void {
    if (this.#closed) throw new AwarenessChannelError("Awareness channel is closed", "closed");
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  onConnectionFailure(listener: (error: Error) => void): () => void {
    if (this.#closed) throw new AwarenessChannelError("Awareness channel is closed", "closed");
    this.#failureListeners.add(listener);
    return () => this.#failureListeners.delete(listener);
  }

  peerIds(): readonly string[] {
    return [...new Set(this.#peers.values())].sort();
  }

  rotateCredential(envelope: YSweetEnvelope): Promise<void> {
    if (this.#closed) {
      throw new AwarenessChannelError("Awareness channel is closed", "closed");
    }
    if (envelope.docId !== this.projectId) {
      throw new AwarenessChannelError(
        "Rotated Y-Sweet credential belongs to another project",
        "state",
      );
    }
    this.#credential = envelope;
    return this.#connected ? Promise.resolve() : this.connect();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#connected = false;
    this.#cancelConnecting?.(new AwarenessChannelError(
      "Awareness channel closed during connection",
      "closed",
    ));
    this.#providerGeneration++;
    this.#detachProviderLifecycle();
    this.#provider.awareness.off("change", this.#onAwarenessChange);
    this.#provider.awareness.setLocalState(null);
    this.#provider.disconnect();
    this.#provider.destroy();
    this.#doc.destroy();
    this.#peers.clear();
    this.#listeners.clear();
    this.#failureListeners.clear();
  }

  #detachProviderLifecycle(): void {
    const lifecycle = this.#providerLifecycle;
    if (!lifecycle) return;
    this.#provider.off("sync", lifecycle.onSync);
    this.#provider.off("connection-error", lifecycle.onError);
    this.#provider.off("connection-close", lifecycle.onClose);
    this.#providerLifecycle = null;
  }

  #connectionFailure(
    message: string,
    values: readonly unknown[],
    postSync: boolean,
  ): AwarenessChannelError {
    const candidate = values.find(value => value && typeof value === "object") as {
      status?: unknown;
      code?: unknown;
    } | undefined;
    const status = typeof candidate?.status === "number"
      ? candidate.status
      : typeof candidate?.code === "number"
        ? candidate.code
        : null;
    return new AwarenessChannelError(
      message,
      status === 401 || status === 403 ? "auth" : postSync ? "disconnected" : "connect",
    );
  }

  #notifyConnectionFailure(error: Error): void {
    for (const listener of this.#failureListeners) {
      try { listener(error); } catch {}
    }
  }

  readonly #onAwarenessChange = (
    change: { added: number[]; updated: number[]; removed: number[] },
  ): void => {
    if (this.#closed || !this.#connected) return;
    for (const clientId of change.removed) this.#peers.delete(clientId);
    for (const clientId of [...change.added, ...change.updated]) {
      if (clientId === this.#doc.clientID) continue;
      const state = this.#provider.awareness.getStates().get(clientId);
      const raw = state?.[this.#field];
      if (raw == null) {
        this.#peers.delete(clientId);
        continue;
      }
      let envelope: AwarenessEnvelope<TRemote>;
      try {
        envelope = this.#parseRemote(raw);
      } catch {
        this.#peers.delete(clientId);
        continue;
      }
      this.#peers.set(clientId, envelope.peerId);
      const update: AwarenessPeerUpdate<TRemote> = {
        peerClientId: clientId,
        envelope,
      };
      for (const listener of this.#listeners) listener(update);
    }
  };
}
