import { parseFrames, type SseFrame } from "../../relay/sse";
import {
  GP_V2_FRAME_MAX_BYTES,
  base64UrlEncode,
  sha256Hex,
  utf8Bytes,
  utf8Text,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type SidecarFrame,
} from "../protocol/shared";
import { TranscriptProof } from "../protocol/transcript";
import type { FramePriority } from "../protocol/scheduler";
import type { SidecarDiagnosticsObserver } from "../diagnostics/sidecar-observer";
import { ResponseBatcher } from "./response-batcher";
import type { OutputBackpressure } from "./output-sink";
import { RequestAssembler } from "./assembler";
import {
  ClaimJournal,
  ClaimJournalError,
  type RecoveredForcedAmbiguous,
  type RecoveredProviderError,
} from "./journal";
import {
  UpstreamExecutionError,
  type DirectResponsesExecution,
  type UpstreamTerminal,
} from "./upstream";

const MiB = 1024 * 1024;
const RESPONSE_PART_BYTES = 8 * 1024;

export interface SidecarAdmission {
  acquire(reservedBytes: number, signal?: AbortSignal): Promise<{ release(): void }>;
  setHostCeiling(value: number): void;
  close(): void;
  snapshot(): {
    active: number;
    queued: number;
    effectiveLimit: number;
    physicalMax: number;
    reservedBytes: number;
    memoryBudgetBytes: number;
  };
}

export interface SidecarUpstream {
  execute(input: DirectResponsesExecution): Promise<UpstreamTerminal>;
}

export interface SidecarEngineSnapshot {
  ready: boolean;
  active: number;
  queued: number;
  credits: number;
  draining: boolean;
  ambiguous: number;
  replaying: number;
  forcedDrain: boolean;
}

interface ActiveRequest {
  requestId: string;
  requestDigest: string;
  body: Uint8Array;
  controller: AbortController;
  lease: { release(): void };
  eventIndex: number;
  cancellation: "none" | "client" | "drain";
  forcedDrainReported: boolean;
  deferredCancellationOutcome:
    | { kind: "client" }
    | { kind: "drain"; message: string }
    | null;
  detachParentAbort?: () => void;
}

const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;
const DEFAULT_DRAIN_ABORT_SETTLEMENT_MS = 5_000;

function positiveDuration(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 1) {
    throw new RangeError(`${name} must be positive`);
  }
  return value;
}

function abortError(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("aborted", "AbortError");
}

async function waitForStartup<T>(
  startup: Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  if (!signal) return startup;
  if (signal.aborted) throw abortError(signal);
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", onAbort);
    };
    const succeed = (value: T) => {
      cleanup();
      resolve(value);
    };
    const fail = (error: unknown) => {
      cleanup();
      reject(error);
    };
    const onAbort = () => fail(abortError(signal));
    signal.addEventListener("abort", onAbort, { once: true });
    startup.then(succeed, fail);
  });
}

export class SidecarEngine {
  readonly #binding: ProtocolBinding;
  readonly #assembler: RequestAssembler;
  readonly #journal: ClaimJournal;
  readonly #admission: SidecarAdmission;
  readonly #upstream: SidecarUpstream;
  readonly #emitFrame: (
    frame: SidecarFrame,
    priority: FramePriority,
    streamId: string,
  ) => void;
  readonly #onDrainRequested?: (reason: string) => void;
  readonly #onFatalError?: (error: Error) => void;
  readonly #assertOwnership?: () => Promise<void>;
  readonly #diagnostics?: SidecarDiagnosticsObserver;
  readonly #batcher: ResponseBatcher | null;
  readonly #outputBackpressure?: OutputBackpressure;
  readonly #drainTimeoutMs: number;
  readonly #drainAbortSettlementMs: number;
  readonly #deferAcceptUntilStarted: boolean;
  readonly #active = new Map<string, ActiveRequest>();
  readonly #commitControllers = new Map<string, AbortController>();
  readonly #ambiguous = new Set<string>();
  readonly #terminalDigests = new Map<string, string>();
  readonly #terminalReplays = new Map<string, Promise<void>>();
  readonly #outcomeReplays = new Map<string, Promise<void>>();
  readonly #drainWaiters: Array<() => void> = [];
  #recoveredTerminals: readonly string[] = [];
  #recoveredProviderErrors: readonly RecoveredProviderError[] = [];
  #recoveredCancelled: readonly string[] = [];
  #recoveredForcedAmbiguous: readonly RecoveredForcedAmbiguous[] = [];
  #recoveredAmbiguous: readonly string[] = [];
  #recoveryReplayPromise: Promise<void> | null = null;
  #recoveryReplayComplete = false;
  #replaying = 0;
  #ready = false;
  #started = false;
  #draining = false;
  #committing = 0;
  #drainedEmitted = false;
  #drainNotified = false;
  #forcedDrain = false;
  #startPromise: Promise<void> | null = null;
  #drainPromise: Promise<void> | null = null;
  readonly #startupReady: Promise<void>;
  #resolveStartup!: () => void;
  #rejectStartup!: (error: unknown) => void;

  constructor(options: {
    binding: ProtocolBinding;
    assembler: RequestAssembler;
    journal: ClaimJournal;
    admission: SidecarAdmission;
    upstream: SidecarUpstream;
    onDrainRequested?: (reason: string) => void;
    onFatalError?: (error: Error) => void;
    assertOwnership?: () => Promise<void>;
    diagnostics?: SidecarDiagnosticsObserver;
    batching?: boolean;
    outputBackpressure?: OutputBackpressure;
    drainTimeoutMs?: number;
    drainAbortSettlementMs?: number;
    deferAcceptUntilStarted?: boolean;
    emit(
      frame: SidecarFrame,
      priority: FramePriority,
      streamId: string,
    ): void;
  }) {
    this.#binding = options.binding;
    this.#assembler = options.assembler;
    this.#journal = options.journal;
    this.#admission = options.admission;
    this.#upstream = options.upstream;
    this.#onDrainRequested = options.onDrainRequested;
    this.#onFatalError = options.onFatalError;
    this.#assertOwnership = options.assertOwnership;
    this.#diagnostics = options.diagnostics;
    this.#emitFrame = options.emit;
    this.#batcher = options.batching ? new ResponseBatcher({ emit: (frame, priority) => this.#emitFrame(frame, priority, frame.requestId) }) : null;
    this.#outputBackpressure = options.outputBackpressure;
    this.#drainTimeoutMs = positiveDuration(
      options.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS,
      "drainTimeoutMs",
    );
    this.#drainAbortSettlementMs = positiveDuration(
      options.drainAbortSettlementMs ?? DEFAULT_DRAIN_ABORT_SETTLEMENT_MS,
      "drainAbortSettlementMs",
    );
    this.#deferAcceptUntilStarted = options.deferAcceptUntilStarted ?? false;
    this.#startupReady = new Promise<void>((resolve, reject) => {
      this.#resolveStartup = resolve;
      this.#rejectStartup = reject;
    });
    // Startup failures are still observed by start() and accept(). This handler
    // only prevents an unhandled-rejection report when no frame was waiting.
    void this.#startupReady.catch(() => undefined);
  }

  start(signal?: AbortSignal): Promise<void> {
    if (this.#startPromise) return this.#startPromise;
    this.#started = true;
    this.#startPromise = this.#start(signal);
    return this.#startPromise;
  }

  async #start(signal?: AbortSignal): Promise<void> {
    try {
      if (signal?.aborted) throw abortError(signal);
      const recovered = await waitForStartup(this.#journal.recover(), signal);
      if (signal?.aborted) throw abortError(signal);
      // Startup is deliberately scan-only. Replaying a large terminal before
      // the host transport exists can fill output backpressure while bootstrap
      // is still waiting for positive credits, creating a circular deadlock.
      this.#recoveredTerminals = recovered.terminals;
      this.#recoveredProviderErrors = recovered.providerErrors;
      this.#recoveredCancelled = recovered.cancelled;
      this.#recoveredForcedAmbiguous = recovered.forcedAmbiguous;
      this.#recoveredAmbiguous = recovered.ambiguous;
      this.#ready = true;
      this.#resolveStartup();
    } catch (error) {
      this.#rejectStartup(error);
      throw error;
    }
  }

  /**
   * Starts recovery delivery only after the peer has observed a bound host
   * transport that can ACK output. Calls are idempotent and never block normal
   * frame acceptance; one recovered request is replayed at a time.
   */
  replayRecovered(signal?: AbortSignal): Promise<void> {
    if (this.#recoveryReplayComplete) return Promise.resolve();
    if (this.#recoveryReplayPromise) return this.#recoveryReplayPromise;
    const replay = this.#replayRecovered(signal).then(() => {
      this.#recoveryReplayComplete = true;
    }).finally(() => {
      if (this.#recoveryReplayPromise === replay) this.#recoveryReplayPromise = null;
    });
    this.#recoveryReplayPromise = replay;
    return replay;
  }

  async #replayRecovered(signal?: AbortSignal): Promise<void> {
    if (!this.#ready) await waitForStartup(this.#startupReady, signal);
    for (const requestId of this.#recoveredAmbiguous) {
      if (signal?.aborted) throw abortError(signal);
      const identity = this.#journal.identity(requestId);
      if (identity?.leaderGeneration !== this.#binding.leaderGeneration) continue;
      await this.#outputBackpressure?.wait(signal);
      this.#ambiguous.add(requestId);
      this.#emitCommittedAck(requestId, identity.requestDigest);
      this.#emitError(
        requestId,
        "recovered_ambiguous",
        "Upstream execution has no proven terminal after recovery",
        true,
      );
    }
    for (const recovered of this.#recoveredProviderErrors) {
      if (signal?.aborted) throw abortError(signal);
      if (recovered.leaderGeneration !== this.#binding.leaderGeneration) continue;
      await this.#outputBackpressure?.wait(signal);
      this.#emitCommittedAck(recovered.requestId, recovered.requestDigest);
      this.#emitError(
        recovered.requestId,
        recovered.error.code,
        recovered.error.message,
        false,
        recovered.error.rateLimit,
      );
    }
    for (const requestId of this.#recoveredCancelled) {
      if (signal?.aborted) throw abortError(signal);
      await this.#outputBackpressure?.wait(signal);
      const identity = this.#journal.identity(requestId);
      if (identity?.leaderGeneration !== this.#binding.leaderGeneration) continue;
      this.#emitCommittedAck(requestId, identity.requestDigest);
      this.#emit({ kind: "request_cancelled", requestId }, "control", requestId);
    }
    for (const recovered of this.#recoveredForcedAmbiguous) {
      if (signal?.aborted) throw abortError(signal);
      if (recovered.leaderGeneration !== this.#binding.leaderGeneration) continue;
      await this.#outputBackpressure?.wait(signal);
      this.#ambiguous.add(recovered.requestId);
      this.#emitCommittedAck(recovered.requestId, recovered.requestDigest);
      this.#emitError(
        recovered.requestId,
        recovered.error.code,
        recovered.error.message,
        true,
      );
    }
    for (const requestId of this.#recoveredTerminals) {
      if (signal?.aborted) throw abortError(signal);
      await this.#scheduleTerminalReplay(requestId, signal);
    }
  }

  async accept(
    frame: ReliableFrame<HostFrame>,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!this.#started && !this.#deferAcceptUntilStarted) {
      throw new Error("Sidecar engine is not started");
    }
    if (!this.#ready) await waitForStartup(this.#startupReady, signal);
    switch (frame.kind) {
      case "request_open":
        this.#diagnostics?.requestOpened(frame.requestId);
      case "request_data":
        this.#assembler.accept(frame);
        return;
      case "request_commit":
        this.#diagnostics?.commitReceived(frame.requestId);
        this.#committing++;
        const commitController = new AbortController();
        let detachCommitAbort: (() => void) | undefined;
        if (signal?.aborted) commitController.abort(abortError(signal));
        else if (signal) {
          const forwardAbort = () => commitController.abort(abortError(signal));
          signal.addEventListener("abort", forwardAbort, { once: true });
          detachCommitAbort = () => signal.removeEventListener("abort", forwardAbort);
        }
        this.#commitControllers.set(frame.requestId, commitController);
        try {
          await this.#commit(frame.requestId, commitController.signal);
        } finally {
          if (this.#commitControllers.get(frame.requestId) === commitController) {
            this.#commitControllers.delete(frame.requestId);
          }
          detachCommitAbort?.();
          this.#committing--;
          this.#finishDrainIfIdle();
        }
        return;
      case "request_cancel": {
        this.#batcher?.flush(frame.requestId,"control");
        this.#assembler.cancel(frame.requestId);
        const active = this.#active.get(frame.requestId);
        if (active) {
          if (active.cancellation === "none") active.cancellation = "client";
          active.controller.abort();
        } else {
          this.#emit({ kind: "request_cancelled", requestId: frame.requestId }, "control", frame.requestId);
        }
        return;
      }
      case "response_ack":
        await this.#ackTerminal(frame.requestId, frame.terminalDigest);
        return;
      case "drain":
        if (!this.#drainNotified) {
          this.#drainNotified = true;
          this.#onDrainRequested?.(frame.reason);
        }
        await this.drain(frame.reason);
        return;
      case "ping":
        // Unlike frame-free Awareness liveness, this reliable ping traverses
        // the inbound cursor and accept chain. It therefore remains the R1
        // engine-progress probe (and is also used during bootstrap) without a
        // wire-schema change.
        this.#emit(
          { kind: "pong", sentAtMs: frame.sentAtMs },
          "control",
          "control",
        );
        return;
      case "credential_rotate":
        this.#emitError(
          "credential",
          "credential_handler_unavailable",
          "Credential rotation must be handled by the sidecar bootstrap peer",
          false,
        );
    }
  }

  drain(_reason: string): Promise<void> {
    if (!this.#drainPromise) this.#drainPromise = this.#drain();
    return this.#drainPromise;
  }

  async #drain(): Promise<void> {
    this.#batcher?.flushAll();
    if (!this.#draining) {
      this.#draining = true;
      this.#admission.close();
      for (const controller of this.#commitControllers.values()) {
        controller.abort(new DOMException("Sidecar is draining", "AbortError"));
      }
    }
    if (this.#active.size === 0 && this.#committing === 0) {
      this.#finishDrainIfIdle();
      return;
    }
    if (await this.#waitForDrainIdle(this.#drainTimeoutMs)) return;

    for (const request of this.#active.values()) {
      if (request.cancellation === "none") request.cancellation = "drain";
      request.controller.abort(new DOMException("Sidecar drain timed out", "TimeoutError"));
    }
    this.#forcedDrain = true;
    if (await this.#waitForDrainIdle(this.#drainAbortSettlementMs)) return;

    // Never claim a clean drain while an abort-ignoring upstream call remains
    // alive. Emit one explicit ambiguous outcome per stuck request and a
    // non-zero drained snapshot so the host cannot mistake forced teardown for
    // graceful completion.
    for (const request of this.#active.values()) {
      if (request.forcedDrainReported) continue;
      await this.#emitForcedDrainOutcome(
        request,
        "Committed execution did not settle after forced drain cancellation",
        true,
      );
    }
    this.#emit({
      kind: "drained",
      active: this.#active.size + this.#committing,
      queued: this.#admission.snapshot().queued,
    }, "control", "control");
  }

  async #waitForDrainIdle(timeoutMs: number): Promise<boolean> {
    if (this.#active.size === 0 && this.#committing === 0) return true;
    let waiter!: () => void;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const idle = new Promise<true>(resolve => {
      waiter = () => resolve(true);
      this.#drainWaiters.push(waiter);
    });
    const timeout = new Promise<false>(resolve => {
      timer = setTimeout(() => resolve(false), timeoutMs);
      (timer as any).unref?.();
    });
    try {
      return await Promise.race([idle, timeout]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      const index = this.#drainWaiters.indexOf(waiter);
      if (index >= 0) this.#drainWaiters.splice(index, 1);
    }
  }

  setHostCeiling(value: number): void {
    this.#admission.setHostCeiling(value);
  }

  maintenance(): { expiredAssemblies: number } {
    return { expiredAssemblies: this.#assembler.sweepExpired().length };
  }

  async acknowledgeDeliveredOutcomes(requestIds: readonly string[]): Promise<void> {
    for (const requestId of new Set(requestIds)) {
      const state = this.#journal.status(requestId);
      if (
        state !== "provider_error" &&
        state !== "cancelled" &&
        state !== "forced_ambiguous"
      ) continue;
      try {
        await this.#journal.acknowledgeOutcome(requestId);
      } catch (error) {
        this.#reportFatalJournalError(error);
        throw error;
      }
      this.#ambiguous.delete(requestId);
      this.#diagnostics?.removeRequest(requestId);
    }
  }

  snapshot(): SidecarEngineSnapshot {
    const admission = this.#admission.snapshot();
    return {
      ready: this.#ready,
      active: this.#active.size,
      queued: admission.queued,
      credits: Math.max(0, admission.effectiveLimit - admission.active),
      draining: this.#draining,
      ambiguous: this.#ambiguous.size,
      replaying: this.#replaying,
      forcedDrain: this.#forcedDrain,
    };
  }

  async #commit(requestId: string, signal?: AbortSignal): Promise<void> {
    if (this.#draining) {
      this.#assembler.cancel(requestId);
      this.#reject(requestId, "draining", "Sidecar is draining");
      return;
    }
    const active = this.#active.get(requestId);
    if (active) {
      this.#assembler.cancel(requestId);
      this.#emit({
        kind: "request_ack",
        requestId,
        requestDigest: active.requestDigest,
        committed: true,
        ...(this.#diagnostics?.requestAck(requestId) ? { diagnostics: this.#diagnostics.requestAck(requestId) } : {}),
      }, "terminal", requestId);
      return;
    }

    let body: Uint8Array;
    try {
      body = await this.#assembler.commit(requestId);
      this.#diagnostics?.assemblyCompleted(requestId);
    } catch (error) {
      this.#reject(
        requestId,
        "assembly",
        error instanceof Error ? error.message : "Request assembly failed",
      );
      return;
    }
    if (this.#draining || signal?.aborted) {
      body.fill(0);
      this.#reject(requestId, "draining", "Sidecar is draining");
      return;
    }
    try {
      const digest = await sha256Hex(body);
      if (this.#draining || signal?.aborted) {
        body.fill(0);
        this.#reject(requestId, "draining", "Sidecar is draining");
        return;
      }
      await this.#proveOwnership();
      const claimStarted = performance.now();
      await this.#journal.claim({
        requestId,
        requestDigest: digest,
        leaderGeneration: this.#binding.leaderGeneration,
      });
      if (this.#draining || signal?.aborted) {
        body.fill(0);
        this.#reject(requestId, "draining", "Sidecar is draining");
        return;
      }
      this.#diagnostics?.claimCompleted(requestId, performance.now() - claimStarted);
      const state = this.#journal.status(requestId);
      if (state === "fetch_starting") {
        body.fill(0);
        this.#ambiguous.add(requestId);
        this.#emitCommittedAck(requestId, digest);
        this.#emitError(
          requestId,
          "recovered_ambiguous",
          "Request was already started upstream without terminal proof",
          true,
        );
        return;
      }
      if (state === "terminal") {
        body.fill(0);
        // Per-request dispatch isolates this replay from unrelated requests;
        // never swallow a failed duplicate replay after commitment.
        await this.#scheduleTerminalReplay(requestId, signal);
        return;
      }
      if (
        state === "provider_error" ||
        state === "cancelled" ||
        state === "forced_ambiguous"
      ) {
        body.fill(0);
        await this.#scheduleOutcomeReplay(requestId, signal);
        return;
      }
      if (state === "acked") {
        body.fill(0);
        this.#reject(requestId, "already_acked", "Request terminal was already acknowledged");
        return;
      }

      let lease: { release(): void };
      try {
        const admissionStarted = performance.now();
        lease = await this.#admission.acquire(
          Math.max(4 * MiB, body.byteLength * 2),
          signal,
        );
        this.#diagnostics?.admissionCompleted(requestId, performance.now() - admissionStarted);
      } catch {
        body.fill(0);
        this.#reject(
          requestId,
          this.#draining || signal?.aborted ? "draining" : "capacity",
          this.#draining || signal?.aborted
            ? "Sidecar is draining"
            : "Sidecar admission capacity is unavailable",
        );
        return;
      }
      if (this.#draining || signal?.aborted) {
        lease.release();
        body.fill(0);
        this.#reject(requestId, "draining", "Sidecar is draining");
        return;
      }
      const controller = new AbortController();
      let detachParentAbort: (() => void) | undefined;
      if (signal?.aborted) controller.abort(abortError(signal));
      else if (signal) {
        const forwardAbort = () => controller.abort(abortError(signal));
        signal.addEventListener("abort", forwardAbort, { once: true });
        detachParentAbort = () => signal.removeEventListener("abort", forwardAbort);
      }
      const request: ActiveRequest = {
        requestId,
        requestDigest: digest,
        body,
        controller,
        lease,
        eventIndex: 0,
        cancellation: "none",
        forcedDrainReported: false,
        deferredCancellationOutcome: null,
        detachParentAbort,
      };
      this.#active.set(requestId, request);
      try {
        const diagnostics = this.#diagnostics?.requestAck(requestId);
        this.#emit({
          kind: "request_ack",
          requestId,
          requestDigest: digest,
          committed: true,
          ...(diagnostics ? { diagnostics } : {}),
        }, "terminal", requestId);
      } catch (error) {
        this.#active.delete(requestId);
        detachParentAbort?.();
        lease.release();
        body.fill(0);
        throw error;
      }
      void this.#execute(request);
    } catch (error) {
      body.fill(0);
      this.#reject(
        requestId,
        "claim",
        error instanceof Error ? error.message : "Durable claim failed",
      );
      if (this.#reportFatalJournalError(error) || this.#isOwnershipError(error)) {
        throw error;
      }
    }
  }

  async #execute(request: ActiveRequest): Promise<void> {
    try {
      const terminal = await this.#upstream.execute({
        body: request.body,
        signal: request.controller.signal,
        beforeFetch: async () => {
          await this.#proveOwnership();
          try {
            await this.#journal.markFetchStarting(request.requestId);
          } catch (error) {
            this.#reportFatalJournalError(error);
            throw error;
          }
          await this.#proveOwnership();
          this.#diagnostics?.fetchStarted(request.requestId);
        },
        onEvent: async event => {
          const eventIndex = request.eventIndex++;
          await this.#emitEvent(request.requestId, eventIndex, event, this.#diagnostics?.upstreamEvent(request.requestId, eventIndex), request.controller.signal);
        },
      });
      if (request.cancellation !== "none" && request.controller.signal.aborted) {
        if (request.cancellation === "drain") {
          await this.#emitForcedDrainOutcome(
            request,
            "Committed execution was cancelled by forced sidecar drain",
            false,
          );
        } else if (!request.forcedDrainReported) {
          await this.#emitClientCancellation(request);
        }
        return;
      }
      await this.#journal.markTerminal(
        request.requestId,
        terminal.responseId,
        terminal.transcriptSha256,
        terminal.transcriptBytes,
      );
      this.#terminalDigests.set(request.requestId, terminal.transcriptSha256);
      const diagnostics = this.#diagnostics?.requestTerminal(request.requestId);
      this.#batcher?.flush(request.requestId,"terminal");
      this.#emit({
        kind: "response_terminal",
        requestId: request.requestId,
        requestDigest: request.requestDigest,
        responseId: terminal.responseId,
        eventCount: terminal.eventCount,
        byteCount: terminal.byteCount,
        transcriptSha256: terminal.transcriptSha256,
        terminalStatus: terminal.status,
        finalEventIndex: terminal.finalEventIndex,
        finalSha256: terminal.finalSha256,
        ...(diagnostics ? { diagnostics } : {}),
        ...(terminal.rateLimit ? { rateLimit: terminal.rateLimit } : {}),
      }, "terminal", request.requestId);
    } catch (error) {
      if (request.cancellation !== "none" && request.controller.signal.aborted) {
        if (request.cancellation === "drain") {
          await this.#emitForcedDrainOutcome(
            request,
            "Committed execution was cancelled by forced sidecar drain",
            false,
          );
        } else if (!request.forcedDrainReported) {
          await this.#emitClientCancellation(request);
        }
        return;
      }
      this.#reportFatalJournalError(error);
      const upstream = error instanceof UpstreamExecutionError ? error : null;
      const ambiguous = upstream?.ambiguous ?? true;
      if (upstream && !ambiguous) {
        try {
          await this.#journal.markProviderError(request.requestId, {
            ...(upstream.status === undefined ? {} : { status: upstream.status }),
            code: upstream.code,
            message: upstream.message,
            ...(upstream.rateLimit ? { rateLimit: upstream.rateLimit } : {}),
          });
        } catch (journalError) {
          this.#reportFatalJournalError(journalError);
        }
      }
      if (ambiguous) this.#ambiguous.add(request.requestId);
      this.#emitError(
        request.requestId,
        upstream?.code ?? "internal",
        upstream?.message ?? "Sidecar execution failed after commitment",
        ambiguous,
        upstream?.rateLimit,
      );
    } finally {
      request.detachParentAbort?.();
      request.body.fill(0);
      request.lease.release();
      this.#active.delete(request.requestId);
      const deferred = request.deferredCancellationOutcome;
      if (deferred?.kind === "client") {
        this.#emit({
          kind: "request_cancelled",
          requestId: request.requestId,
        }, "control", request.requestId);
      } else if (deferred?.kind === "drain") {
        this.#emitError(
          request.requestId,
          "forced_drain_cancelled",
          deferred.message,
          true,
        );
      }
      this.#finishDrainIfIdle();
    }
  }

  async #emitClientCancellation(request: ActiveRequest): Promise<void> {
    try {
      await this.#journal.markCancelled(request.requestId);
    } catch (error) {
      this.#reportFatalJournalError(error);
    }
    request.deferredCancellationOutcome = { kind: "client" };
  }

  async #emitForcedDrainOutcome(
    request: ActiveRequest,
    message: string,
    emitImmediately: boolean,
  ): Promise<void> {
    if (request.forcedDrainReported) return;
    request.forcedDrainReported = true;
    try {
      await this.#journal.markForcedAmbiguous(request.requestId, {
        code: "forced_drain_cancelled",
        message,
      });
    } catch (error) {
      this.#reportFatalJournalError(error);
    }
    this.#ambiguous.add(request.requestId);
    if (emitImmediately) {
      this.#emitError(
        request.requestId,
        "forced_drain_cancelled",
        message,
        true,
      );
    } else {
      request.deferredCancellationOutcome = { kind: "drain", message };
    }
  }

  async #emitEvent(requestId: string, eventIndex: number, event: SseFrame, diagnostics?: import("../diagnostics/types").V2FirstEventDiagnostics, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    if (!diagnostics && this.#batcher) {
      await this.#outputBackpressure?.wait(signal);
      if (this.#batcher.push(requestId, eventIndex, event)) return;
    }
    const bytes = utf8Bytes(event.data);
    const partCount = Math.max(1, Math.ceil(bytes.byteLength / RESPONSE_PART_BYTES));
    for (let partIndex = 0, offset = 0; partIndex < partCount; partIndex++) {
      if (signal?.aborted) throw abortError(signal);
      await this.#outputBackpressure?.wait(signal);
      const part = bytes.subarray(offset, offset + RESPONSE_PART_BYTES);
      const frame: SidecarFrame = {
        kind: "response_event",
        requestId,
        eventIndex,
        event: event.event,
        partIndex,
        partCount,
        offset,
        dataBase64Url: base64UrlEncode(part),
        ...(partIndex === 0 && diagnostics ? { diagnostics } : {}),
      };
      if (utf8Bytes(JSON.stringify(frame)).byteLength > GP_V2_FRAME_MAX_BYTES) {
        throw new Error("Segmented response event exceeds physical frame limit");
      }
      this.#emit(frame, "data", requestId);
      offset += part.byteLength;
    }
  }

  #scheduleTerminalReplay(requestId: string, signal?: AbortSignal): Promise<void> {
    const existing = this.#terminalReplays.get(requestId);
    if (existing) return existing;
    this.#replaying++;
    const replay = this.#replayTerminal(requestId, signal).finally(() => {
      if (this.#terminalReplays.get(requestId) === replay) {
        this.#terminalReplays.delete(requestId);
      }
      this.#replaying = Math.max(0, this.#replaying - 1);
    });
    this.#terminalReplays.set(requestId, replay);
    return replay;
  }

  #scheduleOutcomeReplay(requestId: string, signal?: AbortSignal): Promise<void> {
    const existing = this.#outcomeReplays.get(requestId);
    if (existing) return existing;
    this.#replaying++;
    const replay = this.#replayOutcome(requestId, signal).finally(() => {
      if (this.#outcomeReplays.get(requestId) === replay) {
        this.#outcomeReplays.delete(requestId);
      }
      this.#replaying = Math.max(0, this.#replaying - 1);
    });
    this.#outcomeReplays.set(requestId, replay);
    return replay;
  }

  async #replayOutcome(requestId: string, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    const recovered = await this.#journal.readRecoveredOutcome(requestId);
    if (!recovered || recovered.leaderGeneration !== this.#binding.leaderGeneration) {
      throw new Error("Recovered definitive outcome is unavailable");
    }
    this.#emitCommittedAck(requestId, recovered.requestDigest);
    if (recovered.outcome.kind === "provider_error") {
      this.#emitError(
        requestId,
        recovered.outcome.error.code,
        recovered.outcome.error.message,
        false,
        recovered.outcome.error.rateLimit,
      );
      return;
    }
    if (recovered.outcome.kind === "cancelled") {
      this.#emit({ kind: "request_cancelled", requestId }, "control", requestId);
      return;
    }
    this.#ambiguous.add(requestId);
    this.#emitError(
      requestId,
      recovered.outcome.error.code,
      recovered.outcome.error.message,
      true,
    );
  }

  async #replayTerminal(requestId: string, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    const identity = this.#journal.identity(requestId);
    if (
      identity?.state !== "terminal" ||
      identity.leaderGeneration !== this.#binding.leaderGeneration
    ) return;
    let readFailed = false;
    let recovered: Awaited<ReturnType<ClaimJournal["readRecoveredTerminal"]>>;
    let parsed: ReturnType<typeof parseFrames>;
    let terminal: Awaited<ReturnType<TranscriptProof["createTerminal"]>>;
    try {
      try {
        recovered = await this.#journal.readRecoveredTerminal(requestId);
      } catch (error) {
        readFailed = true;
        throw error;
      }
      if (!recovered || recovered.leaderGeneration !== this.#binding.leaderGeneration) {
        throw new Error("terminal spool is unavailable");
      }
      const raw = utf8Text(recovered.bytes);
      parsed = parseFrames(raw, 0);
      if (signal?.aborted) throw abortError(signal);
      if (parsed.nextOffset !== raw.length) throw new Error("terminal spool is incomplete");
      const transcript = new TranscriptProof(requestId, recovered.requestDigest);
      for (const event of parsed.frames) transcript.append(event);
      terminal = await transcript.createTerminal();
      if (
        terminal.responseId !== recovered.responseId ||
        terminal.transcriptSha256 !== recovered.terminalDigest
      ) throw new Error("terminal spool identity mismatch");
    } catch (error) {
      if (
        signal?.aborted ||
        (error instanceof DOMException && error.name === "AbortError")
      ) throw error;
      this.#ambiguous.add(requestId);
      this.#emitCommittedAck(requestId, identity.requestDigest);
      this.#emitError(
        requestId,
        "terminal_recovery_failed",
        readFailed
          ? "Recovered terminal spool could not be read"
          : "Recovered terminal spool did not match durable proof",
        true,
      );
      if (readFailed) {
        const fatal = error instanceof Error
          ? error
          : new Error("Recovered terminal storage read failed");
        this.#onFatalError?.(fatal);
      }
      return;
    }

    if (signal?.aborted) throw abortError(signal);
    this.#emitCommittedAck(requestId, identity.requestDigest);
    try {
      let eventIndex = 0;
      for (const event of parsed.frames) {
        await this.#emitEvent(requestId, eventIndex++, event, undefined, signal);
      }
      this.#terminalDigests.set(requestId, terminal.transcriptSha256);
      this.#batcher?.flush(requestId,"terminal");
      this.#emit({ ...terminal, requestId, requestDigest: recovered!.requestDigest }, "terminal", requestId);
    } catch (error) {
      if (
        signal?.aborted ||
        (error instanceof DOMException && error.name === "AbortError")
      ) throw error;
      this.#ambiguous.add(requestId);
      this.#emitError(
        requestId,
        "terminal_replay_delivery_failed",
        "Validated terminal replay could not be delivered",
        true,
      );
      throw error;
    }
  }

  async #ackTerminal(requestId: string, digest?: string): Promise<void> {
    const expected = this.#terminalDigests.get(requestId);
    if (!expected || !digest || digest !== expected) return;
    await this.#journal.acknowledgeTerminal(requestId);
    this.#terminalDigests.delete(requestId);
    this.#ambiguous.delete(requestId);
    this.#diagnostics?.removeRequest(requestId);
  }

  #reject(requestId: string, code: string, message: string): void {
    this.#emit({
      kind: "request_reject",
      requestId,
      code: this.#safeCode(code),
      message: this.#safeMessage(message),
    }, "control", requestId);
  }

  #emitCommittedAck(requestId: string, requestDigest: string): void {
    this.#emit({
      kind: "request_ack",
      requestId,
      requestDigest,
      committed: true,
    }, "terminal", requestId);
  }

  #emitError(
    requestId: string,
    code: string,
    message: string,
    ambiguous: boolean,
    rateLimit?: import("../rate-limit").RateLimitHint,
  ): void {
    this.#batcher?.flush(requestId,"terminal");
    this.#emit({
      kind: "response_error",
      requestId,
      code: this.#safeCode(code),
      message: this.#safeMessage(message),
      ambiguous,
      ...(rateLimit ? { rateLimit } : {}),
    }, "terminal", requestId);
  }

  #safeCode(value: string): string {
    return /^[A-Za-z][A-Za-z0-9._-]{0,63}$/.test(value) ? value : "internal";
  }

  #reportFatalJournalError(value: unknown): boolean {
    if (!(value instanceof ClaimJournalError)) return false;
    if (value.code !== "storage" && value.code !== "corrupt") return false;
    this.#onFatalError?.(value);
    return true;
  }

  #isOwnershipError(value: unknown): boolean {
    return value instanceof Error && value.name === "SidecarOwnershipError";
  }

  async #proveOwnership(): Promise<void> {
    if (!this.#assertOwnership) return;
    try {
      await this.#assertOwnership();
    } catch (error) {
      const fatal = error instanceof Error
        ? error
        : new Error("Sidecar ownership validation failed");
      this.#onFatalError?.(fatal);
      throw fatal;
    }
  }

  #safeMessage(value: string): string {
    return value
      .replace(/\b(token|api[_-]?key|secret|password)\s*[:=]\s*[^;\s,]+/gi, "$1=[REDACTED]")
      .replace(/[A-Za-z0-9+/_=-]{48,}/g, "[REDACTED]")
      .slice(0, 512) || "Sidecar request failed";
  }

  #emit(frame: SidecarFrame, priority: FramePriority, streamId: string): void {
    this.#emitFrame(frame, priority, streamId);
  }

  #finishDrainIfIdle(): void {
    if (!this.#draining || this.#active.size > 0 || this.#committing > 0) return;
    if (!this.#drainedEmitted) {
      this.#drainedEmitted = true;
      this.#emit({
        kind: "drained",
        active: 0,
        queued: this.#admission.snapshot().queued,
      }, "control", "control");
    }
    for (const resolve of this.#drainWaiters.splice(0)) resolve();
  }
}
