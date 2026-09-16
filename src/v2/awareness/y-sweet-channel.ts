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
import type { AwarenessEnvelope, AwarenessLane } from "../protocol/shared";
import { withLocalAwarenessPublisher } from "./local-publisher";
import { awarenessSocketTarget } from "./socket-target";

const MAX_AWARENESS_SYNC_PEERS = 128;

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
    connectionCode: number | null = null,
  ) {
    super(message);
    this.connectionCode = Number.isSafeInteger(connectionCode) &&
        connectionCode !== null && connectionCode >= 0 && connectionCode <= 65_535
      ? connectionCode
      : null;
  }

  readonly connectionCode: number | null;
}

export interface YSweetAwarenessChannelOptions<TRemote> {
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
): (doc: Y.Doc) => WebsocketProvider {
  const { url: initialUrl, docId: expectedDocId } = credential();
  class AuthenticatedWebSocket extends WebSocket {
    constructor(_url: string | URL, protocols?: string | string[]) {
      const { url, docId, token } = credential();
      const target = awarenessSocketTarget({ url, docId, token: token.reveal() }, expectedDocId);
      super(target, protocols as any);
    }
  }
  return doc => withLocalAwarenessPublisher(new WebsocketProvider(
    initialUrl,
    expectedDocId,
    doc,
    {
      connect: false,
      disableBc: true,
      WebSocketPolyfill: AuthenticatedWebSocket as any,
    },
  ));
}

/** Native production provider shared with guarded, passive transport observers. */
export function createYSweetAwarenessProvider(
  doc: Y.Doc,
  credential: () => YSweetEnvelope,
): WebsocketProvider {
  return defaultProviderFactory(credential)(doc);
}

/**
 * The pinned y-websocket provider guards old close callbacks but not old
 * open/message/error callbacks. Fence them before they mutate provider state.
 * Custom providers without a ws slot keep their own lifecycle contract.
 */
function fenceProviderSocketCallbacks(provider: AwarenessProviderLike): boolean {
  if (!("ws" in provider)) return true;
  const slot = Object.getOwnPropertyDescriptor(provider, "ws");
  if (!slot?.configurable || !("value" in slot) || !slot.writable) return false;
  let current: unknown = slot.value;
  const install = (socket: object): void => {
    for (const name of ["onopen", "onmessage", "onerror", "onclose"] as const) {
      const own = Object.getOwnPropertyDescriptor(socket, name);
      if (own?.configurable === false) throw new AwarenessChannelError("Provider socket callbacks cannot be fenced", "state");
      let descriptor = own;
      let prototype = Object.getPrototypeOf(socket);
      for (let depth = 0; !descriptor && prototype && depth < 8; depth++) {
        descriptor = Object.getOwnPropertyDescriptor(prototype, name);
        prototype = Object.getPrototypeOf(prototype);
      }
      const accessor = descriptor?.set ? descriptor : null;
      let handler = descriptor?.get ? descriptor.get.call(socket) : descriptor?.value;
      const assign = (value: unknown): void => {
        handler = typeof value === "function" ? function (...args: unknown[]) {
          if (current === socket) return value.apply(socket, args);
        } : value;
        accessor?.set?.call(socket, handler);
      };
      Object.defineProperty(socket, name, {
        configurable: true,
        enumerable: descriptor?.enumerable ?? true,
        get: () => accessor?.get ? accessor.get.call(socket) : handler,
        set: assign,
      });
      assign(handler);
    }
  };
  Object.defineProperty(provider, "ws", {
    configurable: true, enumerable: slot.enumerable,
    get: () => current,
    set: value => {
      current = value;
      if (value && typeof value === "object") install(value);
    },
  });
  if (current && typeof current === "object") install(current);
  return true;
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
  readonly #socketCallbacksFenced: boolean;
  readonly #listeners = new Set<(update: AwarenessPeerUpdate<TRemote>) => void>();
  readonly #failureListeners = new Set<(error: Error) => void>();
  readonly #peers = new Map<number, string>();
  #connected = false;
  #closed = false;
  #connecting: Promise<void> | null = null;
  #reconnecting: Promise<void> | null = null;
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

  constructor(options: YSweetAwarenessChannelOptions<TRemote>) {
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
      : createYSweetAwarenessProvider(this.#doc, credential);
    this.#socketCallbacksFenced = fenceProviderSocketCallbacks(this.#provider);
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
        this.#replayCurrentPeers(generation);
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

  reconnect(lane: AwarenessLane, signal?: AbortSignal): Promise<void> {
    if (lane !== "data" && lane !== "control") return Promise.reject(new TypeError("Unknown Awareness lane"));
    if (this.#closed) return Promise.reject(new AwarenessChannelError("Awareness channel is closed", "closed"));
    if (signal?.aborted) return Promise.reject(signal.reason);
    if (!this.#socketCallbacksFenced) return Promise.reject(new AwarenessChannelError("Provider cannot safely reconnect its current socket", "state"));
    if ((this.#field === "ghostprovV2Data" && lane !== "data") ||
        (this.#field === "ghostprovV2Control" && lane !== "control")) {
      return Promise.reject(new AwarenessChannelError("Awareness reconnect lane does not match this provider", "state"));
    }
    if (this.#reconnecting) return this.#reconnecting;
    const previous = this.#connecting;
    this.#cancelConnecting?.(new AwarenessChannelError("Awareness connection superseded by reconnect", "connect"));
    void previous?.catch(() => undefined);
    this.#connecting = null;
    // Fence callbacks before closing the socket. Keep the provider, Y.Doc,
    // local Awareness state, exact client identity and current credential.
    this.#providerGeneration++;
    this.#detachProviderLifecycle();
    this.#connected = false;
    this.#peers.clear();
    this.#lastConnectionError = new AwarenessChannelError("Awareness channel is reconnecting", "disconnected");
    try { this.#provider.disconnect(); }
    catch {
      return Promise.reject(new AwarenessChannelError("Awareness provider could not disconnect for reconnect", "connect"));
    }
    const reconnecting = this.connect(signal).finally(() => {
      if (this.#reconnecting === reconnecting) this.#reconnecting = null;
    });
    this.#reconnecting = reconnecting;
    return reconnecting;
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
    const connectionCode = typeof candidate?.status === "number"
      ? candidate.status
      : typeof candidate?.code === "number"
        ? candidate.code
        : null;
    return new AwarenessChannelError(
      message,
      connectionCode === 401 || connectionCode === 403
        ? "auth"
        : postSync ? "disconnected" : "connect",
      connectionCode,
    );
  }

  #notifyConnectionFailure(error: Error): void {
    for (const listener of this.#failureListeners) {
      try { listener(error); } catch {}
    }
  }

  #isCurrentConnection(generation: number): boolean {
    return !this.#closed && this.#connected && generation === this.#providerGeneration;
  }

  #replayCurrentPeers(generation: number): void {
    // Awareness can retain an update before document sync. Its identical-state
    // clock renewals emit no "change", so revisit the retained state once for
    // this synchronized connection without waiting for another publication.
    let scanned = 0;
    for (const clientId of this.#provider.awareness.getStates().keys()) {
      if (!this.#isCurrentConnection(generation)) return;
      if (clientId === this.#doc.clientID) continue;
      if (scanned >= MAX_AWARENESS_SYNC_PEERS) return;
      scanned++;
      this.#readPeer(clientId, generation);
    }
  }

  #readPeer(clientId: number, generation: number): void {
    if (!this.#isCurrentConnection(generation) || clientId === this.#doc.clientID) return;
    const state = this.#provider.awareness.getStates().get(clientId);
    const raw = state?.[this.#field];
    if (raw == null) {
      this.#peers.delete(clientId);
      return;
    }
    let envelope: AwarenessEnvelope<TRemote>;
    try {
      envelope = this.#parseRemote(raw);
    } catch {
      if (this.#isCurrentConnection(generation)) this.#peers.delete(clientId);
      return;
    }
    if (!this.#isCurrentConnection(generation)) return;
    this.#peers.set(clientId, envelope.peerId);
    const update: AwarenessPeerUpdate<TRemote> = {
      peerClientId: clientId,
      envelope,
    };
    for (const listener of this.#listeners) {
      if (!this.#isCurrentConnection(generation)) return;
      listener(update);
    }
  }

  readonly #onAwarenessChange = (
    change: { added: number[]; updated: number[]; removed: number[] },
  ): void => {
    if (this.#closed || !this.#connected) return;
    const generation = this.#providerGeneration;
    for (const clientId of change.removed) this.#peers.delete(clientId);
    for (const clientId of change.added) this.#readPeer(clientId, generation);
    for (const clientId of change.updated) this.#readPeer(clientId, generation);
  };
}
