import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import { withLocalAwarenessPublisher } from "../../src/v2/awareness/local-publisher.ts";
import { awarenessSocketTarget } from "../../src/v2/awareness/socket-target.ts";
import type { BootstrapBinding, BootstrapCredentialPayload } from "../../src/v2/bootstrap/crypto.ts";
import { protocolBindingFromBootstrap } from "../../src/v2/bootstrap/binding.ts";
import { SidecarCredentialRotator } from "../../src/v2/bootstrap/rotation.ts";
import {
  GP_V2_SUPERVISOR_FIELD,
  GP_V2_SUPERVISOR_PROTOCOL,
  GP_V2_SUPERVISOR_VERSION,
  parseSupervisorHostMessage,
  sameSupervisorBinding,
  signSupervisorSandboxMessage,
  verifySupervisorHostMessage,
  type SignedSupervisorHostMessage,
  type SignedSupervisorSandboxMessage,
  type SupervisorCommand,
  type SupervisorErrorCode,
  type SupervisorStatus,
} from "../../src/v2/supervisor/protocol.ts";
import { canonicalJson } from "../../src/v2/protocol/shared.ts";
import type { SupervisorAwarenessProviderLike } from "../../src/v2/supervisor/provider.ts";
import { SupervisorMailbox } from "../../src/v2/supervisor/mailbox.ts";
import { SupervisorCommandError } from "./supervisor-peer.ts";
export { SupervisorCommandError } from "./supervisor-peer.ts";

function permissionErrorCode(error: Error): SupervisorErrorCode {
  const permission = /Requires (read|write|env|net|sys|run|ffi) access/i
    .exec(error.message)?.[1]?.toLowerCase();
  if (permission === "env") return "sidecar_permission_env";
  if (permission === "ffi") return "sidecar_permission_ffi";
  if (permission === "net") return "sidecar_permission_net";
  if (permission === "read") return "sidecar_permission_read";
  if (permission === "run") return "sidecar_permission_run";
  if (permission === "sys") return "sidecar_permission_sys";
  if (permission === "write") return "sidecar_permission_write";
  return "sidecar_permission";
}

export interface DenoSupervisorPeerOptions {
  readonly credential: BootstrapCredentialPayload;
  readonly binding: BootstrapBinding;
  readonly instanceId: string;
  readonly handle: (command: SupervisorCommand) => Promise<SupervisorStatus>;
  readonly onShutdown?: () => void;
  readonly connectTimeoutMs?: number;
  readonly providerFactory?: (
    doc: Y.Doc,
    credential: () => BootstrapCredentialPayload,
  ) => SupervisorAwarenessProviderLike;
}

function defaultProviderFactory(
  credential: () => BootstrapCredentialPayload,
): (doc: Y.Doc) => SupervisorAwarenessProviderLike {
  const { url: initialUrl, docId: expectedDocId } = credential();
  class AuthenticatedWebSocket extends WebSocket {
    constructor(_url: string | URL, protocols?: string | string[]) {
      const { url, docId, token } = credential();
      const target = awarenessSocketTarget({ url, docId, token }, expectedDocId);
      super(target, protocols);
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
    } as any,
  )) as unknown as SupervisorAwarenessProviderLike;
}

export function createR10SupervisorProvider(
  doc: Y.Doc,
  credential: () => BootstrapCredentialPayload,
): SupervisorAwarenessProviderLike {
  return defaultProviderFactory(credential)(doc);
}

function safeConnectTimeout(value: number | undefined): number {
  const resolved = value ?? 20_000;
  if (!Number.isSafeInteger(resolved) || resolved < 1 || resolved > 300_000) {
    throw new RangeError("Supervisor connectTimeoutMs must be an integer from 1 through 300000");
  }
  return resolved;
}

export class DenoSupervisorPeer {
  readonly #rotator: SidecarCredentialRotator;
  readonly #binding: BootstrapBinding;
  readonly #instanceId: string;
  readonly #handle: (command: SupervisorCommand) => Promise<SupervisorStatus>;
  readonly #onShutdown?: () => void;
  readonly #connectTimeoutMs: number;
  readonly #doc: Y.Doc;
  readonly #provider: SupervisorAwarenessProviderLike;
  #closed = false;
  #connected = false;
  readonly #mailbox = new SupervisorMailbox<SignedSupervisorHostMessage>(value => this.#process(value));
  #lastSequence = 0;
  #lastCommand: string | null = null;
  #lastStatus: SignedSupervisorSandboxMessage | null = null;
  #shutdownStatus: SignedSupervisorSandboxMessage | null = null;
  #shutdownTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: DenoSupervisorPeerOptions) {
    this.#binding = options.binding;
    this.#instanceId = options.instanceId;
    this.#handle = options.handle;
    this.#onShutdown = options.onShutdown;
    this.#connectTimeoutMs = safeConnectTimeout(options.connectTimeoutMs);
    this.#doc = new Y.Doc({ guid: `${options.credential.docId}:ghostprov:supervisor:sandbox` });
    this.#rotator = new SidecarCredentialRotator({
      credential: options.credential,
      binding: protocolBindingFromBootstrap(options.binding, "r2"),
      instanceId: options.instanceId,
    });
    const credential = () => this.#rotator.current();
    this.#provider = options.providerFactory
      ? options.providerFactory(this.#doc, credential)
      : defaultProviderFactory(credential)(this.#doc);
    this.#provider.awareness.on("change", this.#onAwarenessChange);
  }

  async connect(signal?: AbortSignal): Promise<void> {
    if (this.#closed) throw new SupervisorCommandError("not_ready");
    if (this.#connected) return;
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (operation: () => void) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        this.#provider.off("sync", onSync);
        this.#provider.off("connection-error", onError);
        this.#provider.off("connection-close", onError);
        operation();
      };
      const onSync = (synced: boolean) => {
        if (!synced) return;
        finish(resolve);
      };
      const onError = () => finish(() => reject(new SupervisorCommandError("not_ready")));
      const onAbort = () => finish(() => reject(
        signal?.reason ?? new DOMException("Supervisor connect aborted", "AbortError"),
      ));
      const timer = setTimeout(onError, this.#connectTimeoutMs);
      (timer as any).unref?.();
      this.#provider.on("sync", onSync);
      this.#provider.on("connection-error", onError);
      this.#provider.on("connection-close", onError);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      else this.#provider.connect();
    });
    this.#connected = true;
    await this.#publish(0, { kind: "ready" });
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#connected = false;
    if (this.#shutdownTimer !== null) clearTimeout(this.#shutdownTimer);
    this.#shutdownTimer = null;
    await this.#mailbox.close();
    this.#provider.awareness.off("change", this.#onAwarenessChange);
    this.#provider.awareness.setLocalState(null);
    this.#provider.disconnect();
    this.#provider.destroy();
    this.#doc.destroy();
  }

  currentCredential(): BootstrapCredentialPayload { return this.#rotator.current(); }

  /** Called only by final cleanup after child, peer and ownership retirement. */
  async retirementReceipt(): Promise<SignedSupervisorSandboxMessage> {
    if (!this.#closed) throw new Error("Supervisor retirement receipt requires a closed peer");
    return this.#shutdownStatus ??= await signSupervisorSandboxMessage(this.#rotator.current().rotationKey, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL,
      version: GP_V2_SUPERVISOR_VERSION,
      direction: "sandbox",
      binding: this.#binding,
      supervisorInstanceId: this.#instanceId,
      sequence: this.#lastSequence,
      message: { kind: "shutdown" },
    });
  }

  async #publish(sequence: number, status: SupervisorStatus): Promise<void> {
    if (this.#closed) return;
    const signed = await signSupervisorSandboxMessage(this.#rotator.current().rotationKey, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL,
      version: GP_V2_SUPERVISOR_VERSION,
      direction: "sandbox",
      binding: this.#binding,
      supervisorInstanceId: this.#instanceId,
      sequence,
      message: status,
    });
    if (this.#closed) return;
    if (status.kind === "shutdown") this.#shutdownStatus ??= signed;
    this.#lastStatus = signed;
    this.#provider.awareness.setLocalStateField(GP_V2_SUPERVISOR_FIELD, signed);
  }

  #queue(value: unknown): void {
    let parsed: SignedSupervisorHostMessage;
    try {
      parsed = parseSupervisorHostMessage(value);
    } catch {
      return;
    }
    if (
      this.#closed ||
      !sameSupervisorBinding(parsed.binding, this.#binding) ||
      parsed.supervisorInstanceId !== this.#instanceId ||
      parsed.sequence < this.#lastSequence || parsed.sequence > this.#lastSequence + 1
    ) return;
    this.#mailbox.offer(parsed);
  }

  async #process(parsed: SignedSupervisorHostMessage): Promise<void> {
    if (this.#closed || !await verifySupervisorHostMessage(this.#rotator.current().rotationKey, parsed)) return;
    if (this.#closed) return;
    const canonical = canonicalJson(parsed);
    if (parsed.sequence < this.#lastSequence) return;
    if (parsed.sequence === this.#lastSequence) {
      if (canonical === this.#lastCommand && this.#lastStatus) {
        this.#provider.awareness.setLocalStateField(GP_V2_SUPERVISOR_FIELD, this.#lastStatus);
      } else {
        await this.#publish(parsed.sequence, { kind: "error", code: "command_conflict" });
      }
      return;
    }
    if (parsed.sequence !== this.#lastSequence + 1) {
      await this.#publish(parsed.sequence, { kind: "error", code: "command_conflict" });
      return;
    }
    this.#lastSequence = parsed.sequence;
    this.#lastCommand = canonical;
    let status: SupervisorStatus;
    try {
      if (parsed.message.kind === "rotate") {
        await this.#rotator.accept(parsed.message.rotationId, parsed.message.sealedJson);
        status = { kind: "credential_rotated", rotationId: parsed.message.rotationId };
      } else {
        status = await this.#handle(parsed.message);
      }
    } catch (error) {
      const fallback = error instanceof Error && (error.name === "NotCapable" || error.name === "PermissionDenied")
        ? permissionErrorCode(error)
        : error instanceof Error && error.name === "NotFound"
          ? "handler_not_found"
          : error instanceof TypeError
            ? "handler_type"
            : "handler_exception";
      status = { kind: "error", code: error instanceof SupervisorCommandError ? error.code : fallback };
    }
    await this.#publish(parsed.sequence, status);
    if (status.kind === "shutdown" && !this.#closed && this.#shutdownTimer === null) {
      this.#shutdownTimer = setTimeout(() => {
        this.#shutdownTimer = null;
        this.#onShutdown?.();
      }, 250);
    }
  }

  readonly #onAwarenessChange = (change: {
    added: number[];
    updated: number[];
    removed: number[];
  }): void => {
    if (this.#closed || !this.#connected) return;
    for (const clientId of [...change.added, ...change.updated]) {
      if (clientId === this.#doc.clientID) continue;
      const state = this.#provider.awareness.getStates().get(clientId);
      this.#queue(state?.[GP_V2_SUPERVISOR_FIELD]);
    }
  };
}
