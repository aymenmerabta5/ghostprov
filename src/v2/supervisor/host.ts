import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import WebSocket from "ws";
import {
  sealCredentialRotation,
  type BootstrapBinding,
  type SealedBootstrapCredential,
} from "../bootstrap/crypto";
import type { YSweetEnvelope } from "../../prism/protocol";
import {
  GP_V2_SUPERVISOR_FIELD,
  GP_V2_SUPERVISOR_MAX_SEQUENCE,
  GP_V2_SUPERVISOR_PROTOCOL,
  GP_V2_SUPERVISOR_VERSION,
  parseSupervisorSandboxMessage,
  parseSupervisorCommand,
  sameSupervisorBinding,
  signSupervisorHostMessage,
  verifySupervisorSandboxMessage,
  type SignedSupervisorSandboxMessage,
  type SupervisorCommand,
  type SupervisorProcessIdentity,
  type SupervisorStatus,
} from "./protocol";
import type { SupervisorAwarenessProviderLike } from "./provider";
import { SupervisorMailbox } from "./mailbox";

export const SUPERVISOR_MAX_PENDING_OPERATIONS = 4;

export class V2SupervisorError extends Error {
  readonly name = "V2SupervisorError";

  constructor(
    message: string,
    readonly code:
      | "closed"
      | "connect"
      | "protocol"
      | "remote"
      | "timeout",
    readonly remoteCode?: string,
  ) {
    super(message);
  }
}

export interface V2SupervisorLaunchResult {
  readonly encodedHello: string;
  readonly process: SupervisorProcessIdentity;
}

export interface V2BootstrapSupervisor {
  readonly binding: BootstrapBinding;
  readonly instanceId: string;
  ensureReady(signal?: AbortSignal): Promise<void>;
  connect(signal?: AbortSignal): Promise<void>;
  launch(input: {
    readonly sidecarBinding: BootstrapBinding;
    readonly entrypointPath: string;
    readonly staticConfig: string;
  }, signal?: AbortSignal): Promise<V2SupervisorLaunchResult>;
  deliverCredential(input: {
    readonly sidecarBinding: BootstrapBinding;
    readonly sealed: SealedBootstrapCredential;
  }, signal?: AbortSignal): Promise<void>;
  stop(input: {
    readonly launchNonces: readonly string[];
    readonly reason: string;
  }, signal?: AbortSignal): Promise<string | null>;
  rotateCredential(ySweet: YSweetEnvelope, rotationId: string, signal?: AbortSignal): Promise<void>;
  shutdown(reason: string, signal?: AbortSignal): Promise<void>;
  close(): Promise<void>;
}

export interface V2SupervisorHostOptions {
  readonly binding: BootstrapBinding;
  readonly instanceId: string;
  readonly rotationKey: string;
  readonly credential: YSweetEnvelope;
  readonly connectTimeoutMs?: number;
  readonly commandTimeoutMs?: number;
  readonly providerFactory?: (
    doc: Y.Doc,
    credential: () => YSweetEnvelope,
  ) => SupervisorAwarenessProviderLike;
}

interface PendingStatus {
  readonly sequence: number;
  readonly resolve: (status: SupervisorStatus) => void;
  readonly reject: (error: unknown) => void;
  readonly cleanup: () => void;
}

function defaultProviderFactory(
  credential: () => YSweetEnvelope,
): (doc: Y.Doc) => SupervisorAwarenessProviderLike {
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
  ) as unknown as SupervisorAwarenessProviderLike;
}

function boundedPositive(value: number | undefined, fallback: number, label: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved < 1 || resolved > 300_000) {
    throw new RangeError(`${label} must be an integer from 1 through 300000`);
  }
  return resolved;
}

function abortError(signal?: AbortSignal): unknown {
  return signal?.reason instanceof Error
    ? signal.reason
    : new DOMException("V2 supervisor operation aborted", "AbortError");
}

export class V2SupervisorHost implements V2BootstrapSupervisor {
  readonly binding: BootstrapBinding;
  readonly instanceId: string;
  readonly #rotationKey: string;
  readonly #connectTimeoutMs: number;
  readonly #commandTimeoutMs: number;
  readonly #doc: Y.Doc;
  readonly #provider: SupervisorAwarenessProviderLike;
  #credential: YSweetEnvelope;
  #connected = false;
  #closed = false;
  #connecting: Promise<void> | null = null;
  #connectResolve: (() => void) | null = null;
  #connectReject: ((error: unknown) => void) | null = null;
  #pending: PendingStatus | null = null;
  #sequence = 0;
  readonly #mailbox = new SupervisorMailbox<SignedSupervisorSandboxMessage>(value => this.#verifyStatus(value));
  #operationTail: Promise<void> = Promise.resolve();
  #pendingOperations = 0;

  constructor(options: V2SupervisorHostOptions) {
    this.binding = Object.freeze({ ...options.binding });
    this.instanceId = options.instanceId;
    this.#rotationKey = options.rotationKey;
    this.#credential = options.credential;
    this.#connectTimeoutMs = boundedPositive(options.connectTimeoutMs, 20_000, "connectTimeoutMs");
    this.#commandTimeoutMs = boundedPositive(options.commandTimeoutMs, 45_000, "commandTimeoutMs");
    this.#doc = new Y.Doc({ guid: `${options.credential.docId}:ghostprov:supervisor:host` });
    const credential = () => this.#credential;
    this.#provider = options.providerFactory
      ? options.providerFactory(this.#doc, credential)
      : defaultProviderFactory(credential)(this.#doc);
    this.#provider.awareness.on("change", this.#onAwarenessChange);
    this.#provider.on("connection-error", this.#onConnectionError);
    this.#provider.on("connection-close", this.#onConnectionError);
  }

  ensureReady(signal?: AbortSignal): Promise<void> {
    return this.connect(signal);
  }

  connect(signal?: AbortSignal): Promise<void> {
    if (this.#closed) return Promise.reject(new V2SupervisorError("V2 supervisor host is closed", "closed"));
    if (this.#connected) return Promise.resolve();
    if (this.#connecting) return this.#connecting;
    const connecting = new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;
      const finish = (operation: () => void) => {
        if (settled) return;
        settled = true;
        if (timer !== undefined) clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        this.#connectResolve = null;
        this.#connectReject = null;
        operation();
      };
      const onAbort = () => finish(() => reject(abortError(signal)));
      this.#connectResolve = () => finish(() => {
        this.#connected = true;
        resolve();
      });
      this.#connectReject = error => finish(() => reject(error));
      timer = setTimeout(() => this.#connectReject?.(
        new V2SupervisorError("Timed out waiting for V2 supervisor readiness", "timeout"),
      ), this.#connectTimeoutMs);
      (timer as any).unref?.();
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      else this.#provider.connect();
    }).finally(() => {
      if (this.#connecting === connecting) this.#connecting = null;
    });
    this.#connecting = connecting;
    return connecting;
  }

  launch(input: {
    readonly sidecarBinding: BootstrapBinding;
    readonly entrypointPath: string;
    readonly staticConfig: string;
  }, signal?: AbortSignal): Promise<V2SupervisorLaunchResult> {
    const command = parseSupervisorCommand({ kind: "launch", ...input });
    if (command.kind !== "launch") throw new TypeError("Expected a supervisor launch command");
    return this.#enqueue(async () => {
      const status = await this.#send(command, signal);
      if (status.kind !== "launch_hello" || !sameSupervisorBinding(status.sidecarBinding, command.sidecarBinding)) {
        throw new V2SupervisorError("Supervisor returned mismatched launch proof", "protocol");
      }
      return { encodedHello: status.encodedHello, process: status.process };
    });
  }

  deliverCredential(input: {
    readonly sidecarBinding: BootstrapBinding;
    readonly sealed: SealedBootstrapCredential;
  }, signal?: AbortSignal): Promise<void> {
    const command = parseSupervisorCommand({ kind: "credential", sidecarBinding: input.sidecarBinding, sealedJson: JSON.stringify(input.sealed) });
    if (command.kind !== "credential") throw new TypeError("Expected a supervisor credential command");
    return this.#enqueue(async () => {
      const status = await this.#send(command, signal);
      if (status.kind !== "credential_written" || status.launchNonce !== command.sidecarBinding.nonce) {
        throw new V2SupervisorError("Supervisor returned mismatched credential proof", "protocol");
      }
    });
  }

  stop(input: {
    readonly launchNonces: readonly string[];
    readonly reason: string;
  }, signal?: AbortSignal): Promise<string | null> {
    const command = parseSupervisorCommand({ kind: "stop", ...input });
    return this.#enqueue(async () => {
      const status = await this.#send(command, signal);
      if (status.kind !== "stopped") {
        throw new V2SupervisorError("Supervisor returned mismatched stop proof", "protocol");
      }
      return status.launchNonce;
    });
  }

  rotateCredential(
    ySweet: YSweetEnvelope,
    rotationId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    return this.#enqueue(async () => {
      const sealed = await sealCredentialRotation({
        rotationKey: this.#rotationKey,
        binding: {
          projectId: this.binding.projectId,
          sandboxSessionId: this.binding.sandboxSessionId,
          leaderGeneration: this.binding.leaderGeneration,
          buildHash: this.binding.buildHash,
          schemaHash: this.binding.schemaHash,
          instanceId: this.instanceId,
          rotationId,
          protocolRevision: "r2",
          schemaVersion: 2,
          launchNonce: this.binding.nonce,
        },
        credential: {
          url: ySweet.url,
          baseUrl: ySweet.baseUrl,
          docId: ySweet.docId,
          token: ySweet.token.reveal(),
          authorization: ySweet.authorization,
        },
      });
      const status = await this.#send({
        kind: "rotate",
        rotationId,
        sealedJson: JSON.stringify(sealed),
      }, signal);
      if (status.kind !== "credential_rotated" || status.rotationId !== rotationId) {
        throw new V2SupervisorError("Supervisor returned mismatched rotation proof", "protocol");
      }
      this.#credential = ySweet;
    });
  }

  shutdown(reason: string, signal?: AbortSignal): Promise<void> {
    const command = parseSupervisorCommand({ kind: "shutdown", reason });
    return this.#enqueue(async () => {
      const status = await this.#send(command, signal);
      if (status.kind !== "shutdown") {
        throw new V2SupervisorError("Supervisor returned mismatched shutdown proof", "protocol");
      }
    });
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#connected = false;
    this.#connectReject?.(new V2SupervisorError("V2 supervisor host closed during connection", "closed"));
    this.#rejectPending(new V2SupervisorError("V2 supervisor host closed during command", "closed"));
    await this.#mailbox.close();
    await this.#operationTail.catch(() => undefined);
    this.#provider.awareness.off("change", this.#onAwarenessChange);
    this.#provider.off("connection-error", this.#onConnectionError);
    this.#provider.off("connection-close", this.#onConnectionError);
    this.#provider.awareness.setLocalState(null);
    this.#provider.disconnect();
    this.#provider.destroy();
    this.#doc.destroy();
  }

  #enqueue<T>(operation: () => Promise<T>): Promise<T> {
    if (this.#closed) return Promise.reject(new V2SupervisorError("V2 supervisor host is closed", "closed"));
    if (this.#pendingOperations >= SUPERVISOR_MAX_PENDING_OPERATIONS) {
      return Promise.reject(new V2SupervisorError("V2 supervisor command queue is full", "protocol"));
    }
    this.#pendingOperations++;
    const run = this.#operationTail.then(() => {
      if (this.#closed) throw new V2SupervisorError("V2 supervisor host is closed", "closed");
      return operation();
    }).finally(() => { this.#pendingOperations--; });
    this.#operationTail = run.then(() => undefined, () => undefined);
    return run;
  }

  async #send(command: SupervisorCommand, signal?: AbortSignal): Promise<SupervisorStatus> {
    if (this.#closed) throw new V2SupervisorError("V2 supervisor host is closed", "closed");
    signal?.throwIfAborted();
    await this.connect(signal);
    signal?.throwIfAborted();
    if (this.#pending) throw new V2SupervisorError("V2 supervisor command overlap is invalid", "protocol");
    if (this.#sequence >= GP_V2_SUPERVISOR_MAX_SEQUENCE) {
      throw new V2SupervisorError("V2 supervisor sequence capacity is exhausted", "protocol");
    }
    const sequence = this.#sequence + 1;
    const signed = await signSupervisorHostMessage(this.#rotationKey, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL,
      version: GP_V2_SUPERVISOR_VERSION,
      direction: "host",
      binding: this.binding,
      supervisorInstanceId: this.instanceId,
      sequence,
      message: command,
    });
    if (this.#closed) throw new V2SupervisorError("V2 supervisor host is closed", "closed");
    if (!this.#connected) throw new V2SupervisorError("V2 supervisor connection changed before command", "connect");
    signal?.throwIfAborted();
    this.#sequence = sequence;
    return new Promise<SupervisorStatus>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const onAbort = () => this.#rejectPending(abortError(signal));
      const cleanup = () => {
        if (timer !== undefined) clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        this.#provider.awareness.setLocalStateField(GP_V2_SUPERVISOR_FIELD, null);
      };
      this.#pending = { sequence, resolve, reject, cleanup };
      timer = setTimeout(() => this.#rejectPending(
        new V2SupervisorError("V2 supervisor command timed out", "timeout"),
      ), this.#commandTimeoutMs);
      (timer as any).unref?.();
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      else this.#provider.awareness.setLocalStateField(GP_V2_SUPERVISOR_FIELD, signed);
    });
  }

  #rejectPending(error: unknown): void {
    const pending = this.#pending;
    if (!pending) return;
    this.#pending = null;
    pending.cleanup();
    pending.reject(error);
  }

  #acceptStatus(value: unknown): void {
    let parsed: SignedSupervisorSandboxMessage;
    try {
      parsed = parseSupervisorSandboxMessage(value);
    } catch {
      return;
    }
    if (
      this.#closed ||
      !sameSupervisorBinding(parsed.binding, this.binding) ||
      parsed.supervisorInstanceId !== this.instanceId ||
      (parsed.sequence !== this.#pending?.sequence &&
        !(parsed.sequence === 0 && parsed.message.kind === "ready" && this.#connectResolve))
    ) return;
    this.#mailbox.offer(parsed);
  }

  async #verifyStatus(parsed: SignedSupervisorSandboxMessage): Promise<void> {
    if (!await verifySupervisorSandboxMessage(this.#rotationKey, parsed)) return;
    if (this.#closed) return;
    if (parsed.sequence === 0 && parsed.message.kind === "ready") {
      this.#connectResolve?.();
      return;
    }
    const pending = this.#pending;
    if (!pending || pending.sequence !== parsed.sequence) return;
    this.#pending = null;
    pending.cleanup();
    if (parsed.message.kind === "error") {
      pending.reject(new V2SupervisorError("V2 supervisor rejected a command", "remote", parsed.message.code));
    } else {
      pending.resolve(parsed.message);
    }
  }

  readonly #onAwarenessChange = (change: {
    added: number[];
    updated: number[];
    removed: number[];
  }): void => {
    if (this.#closed) return;
    for (const clientId of [...change.added, ...change.updated]) {
      if (clientId === this.#doc.clientID) continue;
      const state = this.#provider.awareness.getStates().get(clientId);
      this.#acceptStatus(state?.[GP_V2_SUPERVISOR_FIELD]);
    }
  };

  readonly #onConnectionError = (): void => {
    if (this.#closed) return;
    this.#connected = false;
    const error = new V2SupervisorError("V2 supervisor Awareness connection failed", "connect");
    this.#connectReject?.(error);
    this.#rejectPending(error);
  };
}
