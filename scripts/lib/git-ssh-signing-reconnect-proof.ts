import { createV2SupervisorProvider, V2SupervisorHost, type V2SupervisorHostOptions } from "../../src/v2/supervisor/host";
import { GP_V2_SUPERVISOR_FIELD, parseSupervisorSandboxMessage, sameSupervisorBinding } from "../../src/v2/supervisor/protocol";
import * as decoding from "lib0/decoding";
import type { SupervisorAwarenessProviderLike } from "../../src/v2/supervisor/provider";

interface CandidateSocket {
  readonly readyState: number;
  terminate(): void;
}

function containsExpectedReply(data: Uint8Array, options: V2SupervisorHostOptions, rotationId: string): boolean {
  if (data.length > 64 * 1024) return false;
  try {
    const message = decoding.createDecoder(data);
    if (decoding.readVarUint(message) !== 1) return false;
    const awareness = decoding.createDecoder(decoding.readVarUint8Array(message));
    const count = decoding.readVarUint(awareness);
    if (count > 128) return false;
    for (let index = 0; index < count; index++) {
      decoding.readVarUint(awareness); decoding.readVarUint(awareness);
      const encoded = decoding.readVarString(awareness);
      if (encoded.length > 16 * 1024) return false;
      const state: unknown = JSON.parse(encoded);
      if (!state || typeof state !== "object" || Array.isArray(state)) continue;
      try {
        const reply = parseSupervisorSandboxMessage((state as Record<string, unknown>)[GP_V2_SUPERVISOR_FIELD]);
        if (sameSupervisorBinding(reply.binding, options.binding) && reply.supervisorInstanceId === options.instanceId &&
          reply.message.kind === "credential_rotated" && reply.message.rotationId === rotationId) return true;
      } catch { /* An unrelated peer is not a fault-injection target. */ }
    }
  } catch { /* Malformed data must never be classified as the target reply. */ }
  return false;
}

interface ObservedSupervisor {
  readonly worker: number;
  readonly generation: number;
  host: V2SupervisorHost | null;
  provider: SupervisorAwarenessProviderLike | null;
  closeVersion: number;
  lastCloseAt: number;
  provenVersion: number;
  failedVersion: number;
  expectForcedClose: boolean;
  initialProof: boolean;
  initialDisconnects: number;
  forcedDisconnects: number;
  naturalDisconnects: number;
  challenges: number;
  challengePublications: number;
  injectedChallengeDrops: number;
  injectedReplyDrops: number;
  readonly restoreHooks: Array<() => void>;
  readonly restores: number[];
  failures: number;
  readonly startedAt: number;
  readonly trace: Array<{ ms: number; event: string; sequence?: number; kind?: string; synced?: boolean }>;
  traceDropped: number;
  observedSequence: number;
  observedKind: string;
  synchronized: boolean;
}

function trace(state: ObservedSupervisor, value: Omit<ObservedSupervisor["trace"][number], "ms">) {
  if (state.trace.length >= 128) { state.traceDropped++; return; }
  state.trace.push({ ms: Math.round(performance.now() - state.startedAt), ...value });
}

/** Observes only explicitly supplied supervisors in the isolated live verifier. */
export class R9ReconnectProof {
  readonly #target: 1 | 8;
  readonly #dropChallenge: boolean;
  readonly #dropReply: boolean;
  readonly #entries: ObservedSupervisor[] = [];
  #naturalWindow = false;

  constructor(target: 1 | 8, options: { dropChallenge?: boolean; dropReply?: boolean } = {}) {
    this.#target = target;
    this.#dropChallenge = options.dropChallenge === true;
    this.#dropReply = options.dropReply === true;
  }

  observeNatural(enabled: boolean): void { this.#naturalWindow = enabled; }

  createHost(options: V2SupervisorHostOptions, worker: number, generation: number): V2SupervisorHost {
    if (!Number.isSafeInteger(worker) || worker < 1 || worker > this.#target ||
      generation < 1 || generation > 2 || this.#entries.length >= this.#target * 2) {
      throw new Error("Reconnect observer identity or count bound exceeded");
    }
    const state: ObservedSupervisor = { worker, generation, host: null, provider: null, closeVersion: 0,
      lastCloseAt: 0, provenVersion: 0, failedVersion: 0, expectForcedClose: false,
      initialProof: false, initialDisconnects: 0,
      forcedDisconnects: 0, naturalDisconnects: 0, challenges: 0, challengePublications: 0,
      injectedChallengeDrops: 0, injectedReplyDrops: 0, restoreHooks: [], restores: [], failures: 0, startedAt: performance.now(), trace: [], traceDropped: 0,
      observedSequence: -1, observedKind: "none", synchronized: false };
    this.#entries.push(state);
    const host = new V2SupervisorHost({ ...options, providerFactory: (doc, credential) => {
      const provider = createV2SupervisorProvider(doc, credential);
      state.provider = provider;
      provider.on("sync", (synced: boolean) => { state.synchronized = synced === true; trace(state, { event: "sync", synced: synced === true }); });
      provider.awareness.on("change", () => {
        let scanned = 0;
        for (const value of provider.awareness.getStates().values()) {
          if (++scanned > 128) break;
          try {
            const status = parseSupervisorSandboxMessage(value[GP_V2_SUPERVISOR_FIELD]);
            if (!sameSupervisorBinding(status.binding, options.binding) || status.supervisorInstanceId !== options.instanceId) continue;
            if (state.observedSequence !== status.sequence || state.observedKind !== status.message.kind) {
              state.observedSequence = status.sequence; state.observedKind = status.message.kind;
              trace(state, { event: "peer_status_observed", sequence: status.sequence, kind: status.message.kind });
            }
          } catch { /* Unrelated or malformed peer state is not diagnostic evidence. */ }
        }
      });
      provider.on("connection-close", () => {
        state.closeVersion++; state.lastCloseAt = performance.now(); trace(state, { event: "connection_close" });
        if (!state.initialProof) state.initialDisconnects++;
        else if (state.expectForcedClose) { state.forcedDisconnects++; state.expectForcedClose = false; }
        else if (this.#naturalWindow) state.naturalDisconnects++;
      });
      const publish = provider.awareness.setLocalStateField.bind(provider.awareness);
      provider.awareness.setLocalStateField = (field, value) => {
        if (field === GP_V2_SUPERVISOR_FIELD && value && typeof value === "object" && "sequence" in value &&
            typeof value.sequence === "number" && Number.isSafeInteger(value.sequence)) {
          trace(state, { event: "host_command", sequence: value.sequence });
        }
        if (field === GP_V2_SUPERVISOR_FIELD && value && typeof value === "object" &&
          "message" in value && value.message && typeof value.message === "object" &&
          "kind" in value.message && value.message.kind === "rotate" &&
          "rotationId" in value.message && typeof value.message.rotationId === "string" &&
          value.message.rotationId.startsWith("reconnect-")) {
          state.challenges++;
          state.challengePublications++;
          if (this.#dropReply && worker === (this.#target === 1 ? 1 : 2) && state.injectedReplyDrops === 0 &&
            state.forcedDisconnects >= (this.#target === 1 ? 3 : 1)) {
            const socket = (provider as unknown as { ws?: { onmessage: ((event: { data: unknown }) => unknown) | null } }).ws;
            if (!socket || typeof socket.onmessage !== "function") throw new Error("Candidate reply-loss seam unavailable");
            const receive = socket.onmessage;
            const rotationId = value.message.rotationId;
            const wrapped = (event: { data: unknown }) => {
              const data = typeof event.data === "string" ? Buffer.from(event.data)
                : event.data instanceof ArrayBuffer ? Buffer.from(event.data)
                : ArrayBuffer.isView(event.data) ? Buffer.from(event.data.buffer, event.data.byteOffset, event.data.byteLength) : null;
              if (data && containsExpectedReply(data, options, rotationId)) {
                state.injectedReplyDrops++;
                if (socket.onmessage === wrapped) socket.onmessage = receive;
                return;
              }
              return receive.call(socket, event);
            };
            socket.onmessage = wrapped;
            if (state.restoreHooks.length >= 2) throw new Error("Reply-loss hook bound exceeded");
            state.restoreHooks.push(() => { if (socket.onmessage === wrapped) socket.onmessage = receive; });
          }
          if (this.#dropChallenge && worker === 1 && state.injectedChallengeDrops === 0 && state.forcedDisconnects >= 2) {
            // Retain the exact local command but suppress its first transport
            // update. A later publication marker must carry the same command.
            const awareness = provider.awareness as typeof provider.awareness & { emit(event: string, args: unknown[]): void };
            const emit = awareness.emit;
            if (typeof emit !== "function") throw new Error("Candidate challenge-loss seam unavailable");
            awareness.emit = function(event, args) {
              if (event === "update") { state.injectedChallengeDrops++; return; }
              return emit.call(this, event, args);
            };
            try { publish(field, value); } finally { awareness.emit = emit; }
            return;
          }
        }
        if (field === "ghostprovV2SupervisorPublication" && value) state.challengePublications++;
        publish(field, value);
      };
      return provider;
    } });
    state.host = host;
    const connect = host.connect.bind(host);
    host.connect = async signal => {
      const version = state.closeVersion;
      try {
        await connect(signal);
        if (!state.initialProof) {
          state.initialProof = true;
          state.provenVersion = state.closeVersion;
          return;
        }
        if (version > state.provenVersion) {
          state.provenVersion = version;
          if (state.restores.length >= 32) throw new Error("Reconnect timing bound exceeded");
          state.restores.push(Math.round((performance.now() - state.lastCloseAt) * 1000) / 1000);
        }
      } catch (error) {
        trace(state, { event: "connect_failed" });
        if (version > state.failedVersion) { state.failedVersion = version; state.failures++; }
        throw error;
      }
    };
    return host;
  }

  async forceReconnect(): Promise<void> {
    if (this.#entries.length !== this.#target || this.#entries.some(value => value.generation !== 1)) {
      throw new Error("Forced reconnect requires the original complete supervisor fleet");
    }
    const operations = this.#entries.map(async state => {
      if (!state.host || !state.provider) throw new Error("Missing live supervisor transport");
      // The provider is the exact production instance created above for this
      // dedicated candidate. No process, external listener, or foreign socket
      // is enumerated or signalled.
      const socket = (state.provider as SupervisorAwarenessProviderLike & { ws?: CandidateSocket | null }).ws;
      if (!socket || socket.readyState !== 1 || typeof socket.terminate !== "function") {
        throw new Error("Candidate supervisor socket is not connected");
      }
      const version = state.closeVersion;
      state.expectForcedClose = true;
      trace(state, { event: "force_reconnect" });
      socket.terminate();
      const deadline = performance.now() + 5_000;
      while (state.closeVersion === version) {
        if (performance.now() >= deadline) throw new Error("Candidate socket close was not observed");
        await Bun.sleep(5);
      }
      await state.host.connect(AbortSignal.timeout(15_000));
      if (state.provenVersion !== state.closeVersion) throw new Error("Reconnect lacked authenticated supervisor proof");
    });
    const outcomes = await Promise.allSettled(operations);
    for (const state of this.#entries) for (const restore of state.restoreHooks.splice(0)) restore();
    const failed = outcomes.find(value => value.status === "rejected");
    if (failed?.status === "rejected") throw failed.reason;
  }

  snapshot() {
    return this.#entries.map(state => ({ worker: state.worker, generation: state.generation,
      initialDisconnects: state.initialDisconnects,
      forcedDisconnects: state.forcedDisconnects, naturalDisconnects: state.naturalDisconnects,
      challenges: state.challenges, authenticatedRestores: state.restores.length, failures: state.failures,
      challengePublications: state.challengePublications, injectedChallengeDrops: state.injectedChallengeDrops, injectedReplyDrops: state.injectedReplyDrops,
      restoreMs: [...state.restores], synchronized: state.synchronized, observedSequence: state.observedSequence,
      observedKind: state.observedKind, trace: [...state.trace], traceDropped: state.traceDropped }));
  }
}
