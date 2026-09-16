import * as decoding from "lib0/decoding";
import * as encoding from "lib0/encoding";
import type WebSocket from "ws";
import { MAX_REMOTE_PATH_UPDATE_BYTES } from "./remote-path-readback";

const MAX_UPDATE_BYTES = MAX_REMOTE_PATH_UPDATE_BYTES;
const MAX_CANDIDATES = 2;

export class RemoteReadbackChannelError extends Error {
  readonly name = "RemoteReadbackChannelError";
  constructor(readonly reason: "busy" | "disconnected" | "connection_changed" | "aborted" | "wire_overflow" | "malformed_sync" | "send_failed") {
    super(`Remote document readback ${reason}`);
  }
}

export interface ServerReadbackRequest {
  readonly signal: AbortSignal;
  readonly acceptUpdate: (update: Uint8Array) => boolean;
}

/** Reads only socket-origin SyncStep2 frames. It never creates a provider or publishes document state. */
export class ServerReadbackChannel {
  #busy = false;
  constructor(
    readonly socket: () => WebSocket | null,
    readonly fence: (socket: WebSocket) => void = socket => socket.terminate(),
  ) {}

  async request(request: ServerReadbackRequest): Promise<void> {
    if (this.#busy) throw new RemoteReadbackChannelError("busy");
    if (request.signal.aborted) throw new RemoteReadbackChannelError("aborted");
    const socket = this.socket();
    if (!socket || socket.readyState !== 1) throw new RemoteReadbackChannelError("disconnected");
    if (socket.bufferedAmount > MAX_UPDATE_BYTES) throw new RemoteReadbackChannelError("wire_overflow");
    this.#busy = true;
    try {
      await new Promise<void>((resolve, reject) => {
        let settled = false, candidates = 0, totalBytes = 0;
        const finish = (error?: unknown) => {
          if (settled) return;
          settled = true;
          request.signal.removeEventListener("abort", abort);
          socket.off("message", message);
          socket.off("close", close);
          socket.off("error", failed);
          if (error) {
            // The query may still have a full reply in flight. Disable its
            // ordinary parser and close this exact source before releasing ownership.
            try { this.fence(socket); } catch { /* Caller also destroys its exact connection. */ }
          }
          if (error) reject(error); else resolve();
        };
        const abort = () => finish(new RemoteReadbackChannelError("aborted"));
        const close = () => finish(new RemoteReadbackChannelError("disconnected"));
        const failed = () => finish(new RemoteReadbackChannelError("disconnected"));
        const message = (raw: unknown, binary: boolean) => {
          if (settled) return;
          if (this.socket() !== socket) { finish(new RemoteReadbackChannelError("connection_changed")); return; }
          if (!binary) return;
          const bytes = raw instanceof Uint8Array ? raw : raw instanceof ArrayBuffer ? new Uint8Array(raw) : null;
          if (!bytes) { finish(new RemoteReadbackChannelError("malformed_sync")); return; }
          if (bytes.byteLength > MAX_UPDATE_BYTES + 16) { finish(new RemoteReadbackChannelError("wire_overflow")); return; }
          try {
            const decoder = decoding.createDecoder(bytes);
            if (decoding.readVarUint(decoder) !== 0) return;
            if (decoding.readVarUint(decoder) !== 1) return;
            if (++candidates > MAX_CANDIDATES || bytes.byteLength > MAX_UPDATE_BYTES + 16) throw new RemoteReadbackChannelError("wire_overflow");
            const update = decoding.readVarUint8Array(decoder);
            totalBytes += update.byteLength;
            if (update.byteLength > MAX_UPDATE_BYTES || totalBytes > MAX_UPDATE_BYTES * MAX_CANDIDATES) throw new RemoteReadbackChannelError("wire_overflow");
            if (decoder.pos !== bytes.byteLength) throw new RemoteReadbackChannelError("malformed_sync");
            if (request.signal.aborted) { abort(); return; }
            if (request.acceptUpdate(update)) finish();
          } catch (error) {
            if (error instanceof RemoteReadbackChannelError) finish(error);
            else if (error instanceof Error && error.name === "RemotePathReadbackError") finish(error);
            else finish(new RemoteReadbackChannelError("malformed_sync"));
          }
        };
        request.signal.addEventListener("abort", abort, { once: true });
        socket.prependListener("message", message);
        socket.once("close", close);
        socket.once("error", failed);
        if (request.signal.aborted) { abort(); return; }
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, 0); // y-websocket Sync
        encoding.writeVarUint(encoder, 0); // Yjs SyncStep1
        encoding.writeVarUint8Array(encoder, new Uint8Array([0])); // empty state vector
        try {
          socket.send(encoding.toUint8Array(encoder), { binary: true }, error => {
            if (error) finish(new RemoteReadbackChannelError("send_failed"));
          });
        } catch { finish(new RemoteReadbackChannelError("send_failed")); }
      });
    } finally { this.#busy = false; }
  }
}
