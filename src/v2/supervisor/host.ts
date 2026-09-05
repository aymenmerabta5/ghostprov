import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import WebSocket from "ws";
import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
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
const SUPERVISOR_MAX_SCANNED_PEERS = 128;
const SUPERVISOR_CHALLENGE_MAX_PUBLICATIONS = 4;
const SUPERVISOR_PUBLICATION_FIELD = "ghostprovV2SupervisorPublication";
export type V2SupervisorPhase = "transport" | "bootstrap" | "sequence" | "challenge" | "command";

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
    readonly phase?: V2SupervisorPhase,
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
  readDiagnosticLog?(file: import("../diagnostics/sandbox-log-schema").SandboxLogFile, signal?: AbortSignal): Promise<string | null>;
  close(): Promise<void>;
}

export interface V2SupervisorHostOptions {
  readonly binding: BootstrapBinding;
  readonly instanceId: string;
  readonly rotationKey: string;
  readonly credential: YSweetEnvelope;
  readonly connectTimeoutMs?: number;
  readonly commandTimeoutMs?: number;
  readonly clock?: Clock;
  readonly providerFactory?: (
    doc: Y.Doc,
    credential: () => YSweetEnvelope,
  ) => SupervisorAwarenessProviderLike;
}

interface PendingStatus {
  readonly sequence: number;
  readonly deadlineMs: number;
  readonly phase: "challenge" | "command";
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

/** Shared production provider factory; guarded live verifiers observe the same transport. */
export function createV2SupervisorProvider(
  doc: Y.Doc,
  credential: () => YSweetEnvelope,
): SupervisorAwarenessProviderLike {
  return defaultProviderFactory(credential)(doc);
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
  readonly #clock: Clock;
  readonly #doc: Y.Doc;
  readonly #provider: SupervisorAwarenessProviderLike;
  #credential: YSweetEnvelope;
  #connected = false;
  #transportSynced = false;
  #bootstrapProven = false;
  #connectionVersion = 0;
  #closed = false;
  #connecting: Promise<void> | null = null;
  #connectResolve: (() => void) | null = null;
  #connectReject: ((error: unknown) => void) | null = null;
  #connectContinue: (() => void) | null = null;
  #connectPhase: V2SupervisorPhase | null = null;
  #pending: PendingStatus | null = null;
  #sequence = 0;
  #confirmedSequence = 0;
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
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#doc = new Y.Doc({ guid: `${options.credential.docId}:ghostprov:supervisor:host` });
    const credential = () => this.#credential;
    this.#provider = options.providerFactory
      ? options.providerFactory(this.#doc, credential)
      : createV2SupervisorProvider(this.#doc, credential);
    this.#provider.awareness.on("change", this.#onAwarenessChange);
    this.#provider.on("connection-error", this.#onConnectionError);
    this.#provider.on("connection-close", this.#onConnectionError);
    this.#provider.on("sync", this.#onSync);
  }

  ensureReady(signal?: AbortSignal): Promise<void> {
    return this.connect(signal);
  }

  connect(signal?: AbortSignal): Promise<void> {
    if (this.#closed) return Promise.reject(new V2SupervisorError("V2 supervisor host is closed", "closed"));
    if (signal?.aborted) return Promise.reject(abortError(signal));
    if (this.#connected) return Promise.resolve();
    if (this.#connecting) return this.#connecting;
    const deadlineMs = this.#clock.now() + this.#connectTimeoutMs;
    const connecting = new Promise<void>((resolve, reject) => {
      const timeout = new AbortController();
      const operation = new AbortController();
      let settled = false;
      let probing = false;
      const finish = (complete: () => void) => {
        if (settled) return;
        settled = true;
        timeout.abort();
        operation.abort();
        signal?.removeEventListener("abort", onAbort);
        this.#connectResolve = null;
        this.#connectReject = null;
        this.#connectContinue = null;
        this.#connectPhase = null;
        complete();
      };
      const onAbort = () => finish(() => reject(abortError(signal)));
      const fail = (error: unknown) => finish(() => reject(error));
      const succeed = () => {
        if (this.#clock.now() >= deadlineMs) {
          fail(new V2SupervisorError("Supervisor readiness arrived after its deadline", "timeout", undefined, this.#connectPhase ?? "transport"));
          return;
        }
        finish(() => {
          this.#connected = true;
          this.#bootstrapProven = true;
          resolve();
        });
      };
      this.#connectResolve = succeed;
      this.#connectReject = fail;
      this.#connectPhase = "transport";
      this.#connectContinue = () => {
        if (settled || probing || !this.#transportSynced) return;
        if (!this.#bootstrapProven) { this.#connectPhase = "bootstrap"; return; }
        // A signed retained reply can establish which command was consumed,
        // but only a new challenge reply can establish current peer liveness.
        if (this.#confirmedSequence !== this.#sequence) { this.#connectPhase = "sequence"; return; }
        probing = true;
        this.#connectPhase = "challenge";
        void (async () => {
          const rotationId = `reconnect-${crypto.randomUUID()}`;
          const command = await this.#rotationCommand(this.#credential, rotationId);
          const status = await this.#publishCommand(command, operation.signal, deadlineMs);
          if (status.kind !== "credential_rotated" || status.rotationId !== rotationId) {
            throw new V2SupervisorError("Supervisor reconnect proof did not match its challenge", "protocol", undefined, "challenge");
          }
          succeed();
        })().catch(fail);
      };
      void this.#clock.sleep(this.#connectTimeoutMs, timeout.signal).then(() => fail(
        new V2SupervisorError("Timed out waiting for V2 supervisor readiness", "timeout", undefined, this.#connectPhase ?? "transport"),
      ), error => { if (!timeout.signal.aborted) fail(error); });
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      else {
        try {
          // Never reannounce an ambiguously sent lifecycle command on reconnect.
          this.#clearLocalCommand();
          this.#provider.connect();
          this.#scanPeerStates();
          this.#connectContinue?.();
        } catch (error) { fail(error); }
      }
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
    const credential = Object.freeze({ ...ySweet });
    return this.#enqueue(async () => {
      signal?.throwIfAborted();
      const command = await this.#rotationCommand(credential, rotationId);
      signal?.throwIfAborted();
      // Reauthentication must use the newly minted credential even if the
      // previous transport credential is already expired. Fresh proof still
      // gates readiness and every following lifecycle command.
      if (!this.#connected) this.#credential = credential;
      const status = await this.#send(command, signal);
      if (status.kind !== "credential_rotated" || status.rotationId !== rotationId) {
        throw new V2SupervisorError("Supervisor returned mismatched rotation proof", "protocol");
      }
      this.#credential = credential;
    });
  }

  async #rotationCommand(ySweet: YSweetEnvelope, rotationId: string): Promise<Extract<SupervisorCommand, { kind: "rotate" }>> {
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
    const command = parseSupervisorCommand({ kind: "rotate", rotationId, sealedJson: JSON.stringify(sealed) });
    if (command.kind !== "rotate") throw new TypeError("Expected a supervisor rotation command");
    return command;
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
    this.#transportSynced = false;
    this.#connectReject?.(new V2SupervisorError("V2 supervisor host closed during connection", "closed"));
    this.#rejectPending(new V2SupervisorError("V2 supervisor host closed during command", "closed"));
    await this.#mailbox.close();
    await this.#operationTail.catch(() => undefined);
    this.#provider.awareness.off("change", this.#onAwarenessChange);
    this.#provider.off("connection-error", this.#onConnectionError);
    this.#provider.off("connection-close", this.#onConnectionError);
    this.#provider.off("sync", this.#onSync);
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
    return this.#publishCommand(command, signal);
  }

  async #publishCommand(command: SupervisorCommand, signal?: AbortSignal, reconnectDeadlineMs?: number): Promise<SupervisorStatus> {
    signal?.throwIfAborted();
    const reconnecting = reconnectDeadlineMs !== undefined;
    if (reconnecting && (command.kind !== "rotate" || !command.rotationId.startsWith("reconnect-"))) {
      throw new V2SupervisorError("Only a fresh reconnect challenge may be reannounced", "protocol", undefined, "challenge");
    }
    const connectionVersion = this.#connectionVersion;
    if (this.#pending) throw new V2SupervisorError("V2 supervisor command overlap is invalid", "protocol");
    if (this.#sequence !== this.#confirmedSequence) {
      throw new V2SupervisorError("Previous supervisor command consumption is unproven", "protocol", undefined, "sequence");
    }
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
    if (connectionVersion !== this.#connectionVersion ||
      (!this.#connected && !(reconnecting && this.#transportSynced && this.#connectResolve))) {
      throw new V2SupervisorError("V2 supervisor connection changed before command", "connect", undefined, reconnecting ? "challenge" : "command");
    }
    signal?.throwIfAborted();
    this.#sequence = sequence;
    return new Promise<SupervisorStatus>((resolve, reject) => {
      const timeout = new AbortController();
      const reannounce = new AbortController();
      const rejectCurrent = (error: unknown) => { if (this.#pending === pending) this.#rejectPending(error); };
      const onAbort = () => rejectCurrent(abortError(signal));
      const cleanup = () => {
        timeout.abort();
        reannounce.abort();
        signal?.removeEventListener("abort", onAbort);
        this.#clearLocalCommand();
      };
      const pending: PendingStatus = { sequence, resolve, reject, cleanup,
        deadlineMs: Math.min(this.#clock.now() + this.#commandTimeoutMs, reconnectDeadlineMs ?? Infinity), phase: reconnecting ? "challenge" : "command" };
      this.#pending = pending;
      void this.#clock.sleep(this.#commandTimeoutMs, timeout.signal).then(() => {
        if (!timeout.signal.aborted) rejectCurrent(new V2SupervisorError(
          "V2 supervisor command timed out", "timeout", undefined, reconnecting ? "challenge" : "command",
        ));
      }, error => { if (!timeout.signal.aborted) rejectCurrent(error); });
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      else {
        try {
          this.#provider.awareness.setLocalStateField(GP_V2_SUPERVISOR_FIELD, signed);
          if (reconnecting) {
            const intervalMs = Math.max(1, Math.min(1000, Math.floor(this.#connectTimeoutMs / 4), Math.floor(this.#commandTimeoutMs / 4)));
            void (async () => {
              for (let attempt = 1; attempt < SUPERVISOR_CHALLENGE_MAX_PUBLICATIONS; attempt++) {
                await this.#clock.sleep(intervalMs, reannounce.signal);
                if (this.#pending !== pending || this.#closed || !this.#transportSynced || connectionVersion !== this.#connectionVersion) return;
                if (this.#clock.now() >= pending.deadlineMs) return;
                // The authenticated command remains byte-for-byte unchanged.
                // Yjs 'change' observers ignore identical state renewals, so a
                // bounded outer marker makes the peer revisit its exact-command
                // reply cache. Ordinary lifecycle commands are never reannounced.
                this.#provider.awareness.setLocalStateField(SUPERVISOR_PUBLICATION_FIELD, { sequence, attempt });
              }
            })().catch(error => { if (!reannounce.signal.aborted) rejectCurrent(error); });
          }
        }
        catch (error) { this.#rejectPending(error); }
      }
    });
  }

  #rejectPending(error: unknown): void {
    const pending = this.#pending;
    if (!pending) return;
    this.#pending = null;
    if (pending.sequence > this.#confirmedSequence) this.#connected = false;
    try { pending.cleanup(); } catch { /* The cleanup helper disconnects before failing closed. */ }
    finally { pending.reject(error); }
  }

  #clearLocalCommand(): void {
    try { this.#provider.awareness.setLocalStateField(GP_V2_SUPERVISOR_FIELD, null); }
    catch {
      this.#connected = false;
      this.#transportSynced = false;
      try { this.#provider.disconnect(); } catch { /* A later connect must clear the field before reconnecting. */ }
      throw new V2SupervisorError("Supervisor command state could not be cleared", "protocol", undefined, "command");
    }
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
        !(parsed.sequence === 0 && parsed.message.kind === "ready" && !this.#bootstrapProven && this.#connectResolve) &&
        !(this.#connectResolve && this.#sequence > this.#confirmedSequence && parsed.sequence === this.#sequence))
    ) return;
    this.#mailbox.offer(parsed);
  }

  async #verifyStatus(parsed: SignedSupervisorSandboxMessage): Promise<void> {
    const connectionVersion = this.#connectionVersion;
    if (!await verifySupervisorSandboxMessage(this.#rotationKey, parsed)) return;
    if (this.#closed) return;
    if (parsed.sequence === 0 && parsed.message.kind === "ready") {
      if (!this.#bootstrapProven && this.#transportSynced && connectionVersion === this.#connectionVersion) {
        this.#connectResolve?.();
      }
      return;
    }
    if (parsed.sequence === this.#sequence) this.#confirmedSequence = parsed.sequence;
    const pending = this.#pending;
    if (!pending || pending.sequence !== parsed.sequence) { this.#connectContinue?.(); return; }
    if (this.#clock.now() >= pending.deadlineMs) {
      this.#rejectPending(new V2SupervisorError("Supervisor reply arrived after its deadline", "timeout", undefined, pending.phase));
      return;
    }
    this.#pending = null;
    try { pending.cleanup(); }
    catch (error) { pending.reject(error); return; }
    if (parsed.message.kind === "error") {
      pending.reject(new V2SupervisorError("V2 supervisor rejected a command", "remote", parsed.message.code));
    } else {
      pending.resolve(parsed.message);
    }
    this.#connectContinue?.();
  }

  #scanPeerStates(): void {
    let scanned = 0;
    for (const [clientId, state] of this.#provider.awareness.getStates()) {
      if (++scanned > SUPERVISOR_MAX_SCANNED_PEERS) break;
      if (clientId !== this.#doc.clientID) this.#acceptStatus(state[GP_V2_SUPERVISOR_FIELD]);
    }
  }

  readonly #onSync = (synced: boolean): void => {
    if (this.#closed) return;
    if (!synced) {
      if (this.#transportSynced) this.#onConnectionError();
      return;
    }
    this.#transportSynced = true;
    this.#scanPeerStates();
    this.#connectContinue?.();
  };

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
    this.#transportSynced = false;
    this.#connectionVersion++;
    if (!this.#bootstrapProven && this.#connectResolve && !this.#pending) {
      // The provider can reconnect during the one initial handshake. Keep its
      // original deadline and require resync plus signed readiness; no bootstrap
      // job or lifecycle command is resubmitted after a transient dial failure.
      this.#connectPhase = "transport";
      return;
    }
    const error = new V2SupervisorError("V2 supervisor Awareness connection failed", "connect", undefined, this.#connectPhase ?? "transport");
    this.#connectReject?.(error);
    this.#rejectPending(error);
  };
}
