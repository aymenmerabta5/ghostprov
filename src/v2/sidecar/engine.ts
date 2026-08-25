import type { SseFrame } from "../../relay/sse";
import {
  GP_V2_FRAME_MAX_BYTES,
  utf8Bytes,
  type HostFrame,
  type ProtocolBinding,
  type R1RequestCommitFrame,
  type R2ExecutionFence,
  type R2RequestCommitFrame,
  type R2RequestPreparedFrame,
  type R2RequestStartFrame,
  type ReliableFrame,
  type SidecarFrame,
} from "../protocol/shared";
import { TranscriptProof } from "../protocol/transcript";
import type { FramePriority } from "../protocol/scheduler";
import type { SidecarDiagnosticsObserver } from "../diagnostics/sidecar-observer";
import {
  ResponseBatcher,
  streamResponseEventParts,
} from "./response-batcher";
import type { OutputBackpressure } from "./output-sink";
import {
  atomicBytePermitOf,
  releaseAtomicBytePermit,
  tryResizeAtomicBytePermit,
} from "../memory/permit-carrier";
import { RequestAssembler, verifiedRequestDigestOf } from "./assembler";
import { v2RequestBodyWorkingSetBytes } from "../memory/request-memory-sizing";
import {
  ClaimJournal,
  ClaimJournalError,
  type RecoveredForcedAmbiguous,
  type RecoveredProviderError,
  type JournalTerminalReader,
  type JournalTerminalSpool,
} from "./journal";
import {
  IncrementalSseEventParser,
  UpstreamExecutionError,
  type DirectResponsesExecution,
  type UpstreamTerminal,
} from "./upstream";
import type { AtomicBytePermitPool } from "../memory/atomic-byte-permits";

const MiB = 1024 * 1024;
const RESPONSE_PART_BYTES = 8 * 1024;

async function streamTerminalFrames(
  reader: JournalTerminalReader,
  signal: AbortSignal | undefined,
  permits: AtomicBytePermitPool | undefined,
  onFrame: (frame: SseFrame) => void | Promise<void>,
): Promise<void> {
  const parser = new IncrementalSseEventParser({
    maxEventBytes: 64 * 1024 * 1024,
    permits,
    signal,
  });
  const consume = async (parsed: import("./upstream").IncrementalSseEvent) => {
    try {
      if (signal?.aborted) throw abortError(signal);
      await onFrame(parsed.frame);
    } finally {
      parsed.release();
    }
  };
  try {
    for await (const chunk of reader.chunks(signal)) {
      await parser.push(chunk, consume);
    }
    const finished = await parser.finish(consume);
    if (finished.trailingNonWhitespace) {
      throw new Error("terminal spool is incomplete");
    }
  } finally {
    parser.abort();
  }
}

export interface SidecarAdmission {
  acquire(
    reservedBytes: number,
    signal?: AbortSignal,
    memoryAlreadyReserved?: boolean,
  ): Promise<{ release(): void }>;
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
  forcedDrainTask?: Promise<"forced" | "definitive_won" | "unproven">;
  deferredCancellationOutcome:
    | { kind: "client" }
    | { kind: "drain"; message: string }
    | { kind: "ambiguous"; code: string; message: string }
    | null;
  executionFence: R2RequestStartFrame | null;
  fetchStarted: boolean;
  terminalSpool: JournalTerminalSpool | null;
  detachParentAbort?: () => void;
}

interface PreparedRequest {
  requestId: string;
  requestDigest: string;
  body: Uint8Array;
  lease: { release(): void };
  commitment: R2RequestCommitFrame;
  terminalSpool: JournalTerminalSpool;
  expires?: ReturnType<typeof setTimeout>;
}

export class V2ExecutionFenceError extends Error {
  readonly name = "V2ExecutionFenceError";
}

const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;
const DEFAULT_DRAIN_ABORT_SETTLEMENT_MS = 5_000;
const DEFAULT_PREPARED_TIMEOUT_MS = 60_000;

function positiveDuration(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 1) {
    throw new RangeError(`${name} must be positive`);
  }
  return value;
}

function abortError(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("aborted", "AbortError");
}

function releaseRequestBody(body: Uint8Array): void {
  releaseAtomicBytePermit(body);
  body.fill(0);
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
  readonly #assertExecutionFence?: (
    frame: R2RequestCommitFrame | R2RequestStartFrame,
  ) => void | Promise<void>;
  readonly #diagnostics?: SidecarDiagnosticsObserver;
  readonly #batcher: ResponseBatcher | null;
  readonly #outputBackpressure?: OutputBackpressure;
  readonly #atomicBytePermits?: AtomicBytePermitPool;
  readonly #drainTimeoutMs: number;
  readonly #drainAbortSettlementMs: number;
  readonly #preparedTimeoutMs: number;
  readonly #deferAcceptUntilStarted: boolean;
  readonly #active = new Map<string, ActiveRequest>();
  readonly #prepared = new Map<string, PreparedRequest>();
  readonly #preparedCleanupIds = new Set<string>();
  readonly #unprovenPreparedCleanupIds = new Set<string>();
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
  #preparedCleanups = 0;
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
    assertExecutionFence?: (
      frame: R2RequestCommitFrame | R2RequestStartFrame,
    ) => void | Promise<void>;
    diagnostics?: SidecarDiagnosticsObserver;
    batching?: boolean;
    outputBackpressure?: OutputBackpressure;
    atomicBytePermits?: AtomicBytePermitPool;
    drainTimeoutMs?: number;
    drainAbortSettlementMs?: number;
    preparedTimeoutMs?: number;
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
    this.#assertExecutionFence = options.assertExecutionFence;
    if (options.binding.protocolRevision === "r2" && !options.assertExecutionFence) {
      throw new TypeError("R2 sidecar engine requires assertExecutionFence");
    }
    this.#diagnostics = options.diagnostics;
    this.#emitFrame = options.emit;
    this.#batcher = options.batching ? new ResponseBatcher({ emit: (frame, priority) => this.#emitFrame(frame, priority, frame.requestId) }) : null;
    this.#outputBackpressure = options.outputBackpressure;
    this.#atomicBytePermits = options.atomicBytePermits;
    this.#drainTimeoutMs = positiveDuration(
      options.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS,
      "drainTimeoutMs",
    );
    this.#drainAbortSettlementMs = positiveDuration(
      options.drainAbortSettlementMs ?? DEFAULT_DRAIN_ABORT_SETTLEMENT_MS,
      "drainAbortSettlementMs",
    );
    this.#preparedTimeoutMs = positiveDuration(
      options.preparedTimeoutMs ?? DEFAULT_PREPARED_TIMEOUT_MS,
      "preparedTimeoutMs",
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
      if (this.#binding.protocolRevision === "r2") {
        await waitForStartup(
          this.#journal.pruneOtherGenerations(this.#binding.leaderGeneration),
          signal,
        );
      }
      const recovered = await waitForStartup(this.#journal.recover(), signal);
      if (signal?.aborted) throw abortError(signal);
      const retiredRetryable: string[] = [];
      if (this.#binding.protocolRevision === "r2") {
        // CLAIMED contains no execution risk and a restarted process no longer
        // has the decoded body or old target-instance fence. Such claims are
        // unreachable, so retire them durably instead of leaking journal quota.
        for (const requestId of recovered.retryable) {
          const identity = this.#journal.identity(requestId);
          if (identity?.leaderGeneration !== this.#binding.leaderGeneration) continue;
          await waitForStartup(this.#abandonClaim(requestId, true), signal);
          retiredRetryable.push(requestId);
        }
      }
      // Startup is deliberately scan-only. Replaying a large terminal before
      // the host transport exists can fill output backpressure while bootstrap
      // is still waiting for positive credits, creating a circular deadlock.
      this.#recoveredTerminals = recovered.terminals;
      this.#recoveredProviderErrors = recovered.providerErrors;
      this.#recoveredCancelled = Object.freeze([
        ...recovered.cancelled,
        ...retiredRetryable,
      ]);
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

  hasRecoveredWork(): boolean {
    return !this.#recoveryReplayComplete && (
      this.#recoveredTerminals.length > 0 ||
      this.#recoveredProviderErrors.length > 0 ||
      this.#recoveredCancelled.length > 0 ||
      this.#recoveredForcedAmbiguous.length > 0 ||
      this.#recoveredAmbiguous.length > 0
    );
  }

  hasPendingTerminalAcknowledgement(requestId: string): boolean {
    return this.#terminalDigests.has(requestId);
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
      if (recovered.error.providerError?.origin === "sse") {
        this.#emitRecoveredPrelude(
          recovered.requestId,
          recovered.prelude,
          recovered.error.providerError.upstreamRequestId,
        );
      }
      this.#emitError(
        recovered.requestId,
        recovered.error.code,
        recovered.error.message,
        false,
        recovered.error.rateLimit,
        recovered.error.providerError,
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
        await this.#proveExecutionBoundary(frame);
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
          await this.#commit(frame, commitController.signal);
        } finally {
          if (this.#commitControllers.get(frame.requestId) === commitController) {
            this.#commitControllers.delete(frame.requestId);
          }
          detachCommitAbort?.();
          this.#committing--;
          this.#finishDrainIfIdle();
        }
        return;
      case "request_start":
        await this.#proveExecutionBoundary(frame);
        this.#committing++;
        try {
          await this.#startPrepared(frame, signal);
        } finally {
          this.#committing--;
          this.#finishDrainIfIdle();
        }
        return;
      case "request_cancel": {
        this.#batcher?.flush(frame.requestId,"control");
        this.#assembler.cancel(frame.requestId);
        const prepared = this.#prepared.get(frame.requestId);
        if (prepared) {
          try {
            await this.#discardPrepared(prepared, "client");
          } catch (error) {
            this.#reportFatalCancellationStorage(error);
            this.#ambiguous.add(frame.requestId);
            this.#emitError(
              frame.requestId,
              "cancellation_unproven",
              "Prepared cancellation could not be durably recorded",
              true,
            );
          }
          return;
        }
        const active = this.#active.get(frame.requestId);
        if (active) {
          if (active.cancellation === "none") active.cancellation = "client";
          active.controller.abort();
        } else {
          const state = this.#journal.status(frame.requestId);
          if (
            state === "provider_error" ||
            state === "cancelled" ||
            state === "forced_ambiguous"
          ) {
            await this.#scheduleOutcomeReplay(frame.requestId, signal);
          } else if (state === "terminal") {
            await this.#scheduleTerminalReplay(frame.requestId, signal);
          } else if (state === "claimed") {
            try {
              await this.#journal.markCancelled(frame.requestId);
              this.#emit(
                { kind: "request_cancelled", requestId: frame.requestId },
                "control",
                frame.requestId,
              );
            } catch (error) {
              this.#reportFatalCancellationStorage(error);
              this.#ambiguous.add(frame.requestId);
              this.#emitError(
                frame.requestId,
                "cancellation_unproven",
                "Cancellation could not be durably recorded",
                true,
              );
            }
          } else if (state === "fetch_starting") {
            this.#ambiguous.add(frame.requestId);
            this.#emitError(
              frame.requestId,
              "cancellation_unproven",
              "Cancellation has no durable definitive outcome",
              true,
            );
          } else {
            // Unknown, never-claimed prefetch, and already-retired requests are
            // idempotent request-local cancellation outcomes. There is no
            // execution that can be misclassified, so never retire the whole
            // sandbox for a duplicate/stale client cancel.
            this.#emit(
              { kind: "request_cancelled", requestId: frame.requestId },
              "control",
              frame.requestId,
            );
          }
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

  /**
   * Synchronous cancellation preemption used before a per-request accept chain.
   * It prevents a cancel frame from waiting behind a blocked admission acquire.
   */
  preemptRequest(requestId: string): void {
    this.#assembler.cancel(requestId);
    this.#commitControllers.get(requestId)?.abort(
      new DOMException("Request cancelled before execution", "AbortError"),
    );
    const active = this.#active.get(requestId);
    if (active) {
      if (active.cancellation === "none") active.cancellation = "client";
      active.controller.abort(new DOMException("Request cancelled", "AbortError"));
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
      for (const prepared of [...this.#prepared.values()]) {
        void this.#discardPrepared(prepared, "drain").catch(error => {
          this.#reportUnprovenDurableOutcome(
            error,
            "Prepared drain cancellation could not be durably recorded",
          );
        });
      }
    }
    if (
      this.#active.size === 0 &&
      this.#prepared.size === 0 &&
      this.#preparedCleanups === 0 &&
      this.#unprovenPreparedCleanupIds.size === 0 &&
      this.#committing === 0
    ) {
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
    await Promise.all([...this.#active.values()].map(request => (
      this.#emitForcedDrainOutcome(
        request,
        "Committed execution did not settle after forced drain cancellation",
        true,
      )
    )));
    this.#emit({
      kind: "drained",
      active: this.#active.size + this.#prepared.size + this.#preparedCleanups +
        this.#unprovenPreparedCleanupIds.size + this.#committing,
      queued: this.#admission.snapshot().queued,
    }, "control", "control");
  }

  async #waitForDrainIdle(timeoutMs: number): Promise<boolean> {
    if (
      this.#active.size === 0 &&
      this.#prepared.size === 0 &&
      this.#preparedCleanups === 0 &&
      this.#unprovenPreparedCleanupIds.size === 0 &&
      this.#committing === 0
    ) return true;
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
      active: this.#active.size + this.#prepared.size + this.#preparedCleanups +
        this.#unprovenPreparedCleanupIds.size,
      queued: admission.queued,
      credits: Math.max(0, admission.effectiveLimit - admission.active),
      draining: this.#draining,
      ambiguous: this.#ambiguous.size,
      replaying: this.#replaying,
      forcedDrain: this.#forcedDrain,
    };
  }

  async #commit(
    commitment: Extract<ReliableFrame<HostFrame>, { kind: "request_commit" }>,
    signal?: AbortSignal,
  ): Promise<void> {
    const requestId = commitment.requestId;
    if (this.#draining) {
      this.#assembler.cancel(requestId);
      this.#reject(requestId, "draining", "Sidecar is draining");
      return;
    }
    const prepared = this.#prepared.get(requestId);
    if (prepared) {
      this.#assembler.cancel(requestId);
      if (
        !("protocolRevision" in commitment) ||
        commitment.protocolRevision !== "r2" ||
        !this.#sameR2Fence(prepared.commitment, commitment)
      ) {
        this.#reject(requestId, "commit_conflict", "Duplicate request commitment conflicts");
        return;
      }
      // The first proof is already in the reliable outbound window. Re-emitting
      // on a logical duplicate would grow scheduler state without adding safety.
      return;
    }
    const active = this.#active.get(requestId);
    if (active) {
      this.#assembler.cancel(requestId);
      if (
        "protocolRevision" in commitment &&
        commitment.protocolRevision === "r2" &&
        active.executionFence &&
        !this.#sameR2Fence(active.executionFence, commitment)
      ) {
        this.#reject(requestId, "commit_conflict", "Duplicate request commitment conflicts");
      } else if (active.executionFence === null || active.fetchStarted) {
        this.#emitRequestAck(active);
      }
      return;
    }

    let body: Uint8Array;
    let lease: { release(): void } | null = null;
    let claimed = false;
    try {
      body = await this.#assembler.commit(commitment);
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
      releaseRequestBody(body);
      this.#reject(requestId, "draining", "Sidecar is draining");
      return;
    }
    const digest = verifiedRequestDigestOf(body);
    if (!digest) {
      releaseRequestBody(body);
      this.#reject(requestId, "assembly", "Request body has no verified digest");
      return;
    }
    const bodyPermit = atomicBytePermitOf(body);
    if (bodyPermit) {
      let covered = false;
      try {
        covered = tryResizeAtomicBytePermit(
          body,
          Math.max(bodyPermit.bytes, v2RequestBodyWorkingSetBytes(body.byteLength)),
          { allowJumbo: false },
        );
      } catch {
        covered = false;
      }
      if (!covered) {
        releaseRequestBody(body);
        this.#reject(requestId, "capacity", "Request body working-set memory is unavailable");
        return;
      }
    }
    try {
      if (this.#draining || signal?.aborted) {
        releaseRequestBody(body);
        this.#reject(requestId, "draining", "Sidecar is draining");
        return;
      }
      await this.#proveExecutionBoundary(commitment);
      const claimStarted = performance.now();
      await this.#journal.claim({
        requestId,
        requestDigest: digest,
        leaderGeneration: this.#binding.leaderGeneration,
      });
      claimed = true;
      if (this.#draining || signal?.aborted) {
        releaseRequestBody(body);
        await this.#abandonClaim(requestId);
        this.#reject(requestId, "draining", "Sidecar is draining");
        return;
      }
      this.#diagnostics?.claimCompleted(requestId, performance.now() - claimStarted);
      const state = this.#journal.status(requestId);
      if (state === "fetch_starting") {
        releaseRequestBody(body);
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
        releaseRequestBody(body);
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
        releaseRequestBody(body);
        await this.#scheduleOutcomeReplay(requestId, signal);
        return;
      }
      if (state === "acked") {
        releaseRequestBody(body);
        this.#reject(requestId, "already_acked", "Request terminal was already acknowledged");
        return;
      }

      try {
        const admissionStarted = performance.now();
        lease = await this.#admission.acquire(
          Math.max(4 * MiB, body.byteLength * 2),
          signal,
          bodyPermit !== null,
        );
        this.#diagnostics?.admissionCompleted(requestId, performance.now() - admissionStarted);
      } catch {
        releaseRequestBody(body);
        await this.#abandonClaim(requestId);
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
        lease = null;
        releaseRequestBody(body);
        await this.#abandonClaim(requestId);
        this.#reject(requestId, "draining", "Sidecar is draining");
        return;
      }
      await this.#proveExecutionBoundary(commitment);
      if (this.#draining || signal?.aborted) {
        lease.release();
        lease = null;
        releaseRequestBody(body);
        await this.#abandonClaim(requestId);
        this.#reject(requestId, "draining", "Sidecar is draining");
        return;
      }

      if ("protocolRevision" in commitment && commitment.protocolRevision === "r2") {
        let terminalSpool: JournalTerminalSpool;
        try {
          terminalSpool = await this.#journal.beginTerminalSpool(requestId);
        } catch (error) {
          lease.release();
          lease = null;
          releaseRequestBody(body);
          await this.#abandonClaim(requestId);
          if (error instanceof ClaimJournalError && error.code === "capacity") {
            this.#reject(
              requestId,
              "capacity",
              "Sidecar terminal spool capacity is unavailable",
            );
            return;
          }
          throw error;
        }
        if (this.#draining || signal?.aborted) {
          await terminalSpool.abort();
          lease.release();
          lease = null;
          releaseRequestBody(body);
          await this.#abandonClaim(requestId);
          this.#reject(requestId, "draining", "Sidecar is draining");
          return;
        }
        const waiting: PreparedRequest = {
          requestId,
          requestDigest: digest,
          body,
          lease,
          commitment,
          terminalSpool,
        };
        this.#prepared.set(requestId, waiting);
        lease = null;
        waiting.expires = setTimeout(() => {
          void this.#expirePrepared(waiting);
        }, this.#preparedTimeoutMs);
        waiting.expires.unref?.();
        try {
          this.#emitPrepared(waiting);
        } catch (error) {
          await this.#discardPrepared(waiting, "failed");
          throw error;
        }
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
        executionFence: null,
        fetchStarted: false,
        terminalSpool: null,
        detachParentAbort,
      };
      this.#active.set(requestId, request);
      lease = null;
      try {
        this.#emitRequestAck(request);
      } catch (error) {
        this.#active.delete(requestId);
        detachParentAbort?.();
        request.lease.release();
        releaseRequestBody(body);
        throw error;
      }
      void this.#execute(request);
    } catch (error) {
      lease?.release();
      releaseRequestBody(body);
      if (this.#isFatalExecutionBoundaryError(error)) throw error;
      if (
        claimed &&
        this.#journal.status(requestId) === "claimed" &&
        !this.#prepared.has(requestId) &&
        !this.#active.has(requestId)
      ) {
        try { await this.#abandonClaim(requestId); }
        catch (cleanupError) {
          if (this.#reportFatalJournalError(cleanupError)) throw cleanupError;
        }
      }
      this.#reject(
        requestId,
        "claim",
        error instanceof Error ? error.message : "Durable claim failed",
      );
      if (this.#reportFatalJournalError(error)) {
        throw error;
      }
    }
  }

  async #startPrepared(
    start: ReliableFrame<R2RequestStartFrame>,
    signal?: AbortSignal,
  ): Promise<void> {
    const requestId = start.requestId;
    if (this.#binding.protocolRevision !== "r2") {
      this.#reject(requestId, "revision", "request_start is not part of R1");
      return;
    }
    if (this.#draining || signal?.aborted) {
      this.#reject(requestId, "draining", "Sidecar is draining");
      return;
    }

    const active = this.#active.get(requestId);
    if (active) {
      if (!active.executionFence || !this.#sameR2Fence(active.executionFence, start)) {
        this.#reject(requestId, "start_conflict", "Duplicate request start conflicts");
      } else if (active.fetchStarted) {
        this.#emitRequestAck(active);
      }
      return;
    }

    const prepared = this.#prepared.get(requestId);
    if (!prepared) {
      if (this.#preparedCleanupIds.has(requestId)) return;
      const state = this.#journal.status(requestId);
      const identity = this.#journal.identity(requestId);
      if (
        identity &&
        (identity.requestDigest !== start.requestDigest ||
          identity.leaderGeneration !== this.#binding.leaderGeneration)
      ) {
        this.#reject(requestId, "start_conflict", "Request start conflicts with journal identity");
        return;
      }
      if (state === "fetch_starting" && identity) {
        this.#ambiguous.add(requestId);
        this.#emitCommittedAck(requestId, identity.requestDigest);
        this.#emitError(
          requestId,
          "recovered_ambiguous",
          "Request was already started upstream without terminal proof",
          true,
        );
      } else if (state === "terminal") {
        await this.#scheduleTerminalReplay(requestId, signal);
      } else if (
        state === "provider_error" ||
        state === "cancelled" ||
        state === "forced_ambiguous"
      ) {
        await this.#scheduleOutcomeReplay(requestId, signal);
      } else {
        this.#reject(requestId, "not_prepared", "Request body is not prepared for execution");
      }
      return;
    }
    if (!this.#sameR2Fence(prepared.commitment, start)) {
      this.#reject(requestId, "start_conflict", "Request start does not match preparation");
      return;
    }

    // Atomically transfer ownership out of the prepared set before any await.
    // Concurrent duplicate starts can therefore observe only one active owner.
    this.#prepared.delete(requestId);
    if (prepared.expires) clearTimeout(prepared.expires);
    prepared.expires = undefined;
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
      requestDigest: prepared.requestDigest,
      body: prepared.body,
      controller,
      lease: prepared.lease,
      eventIndex: 0,
      cancellation: "none",
      forcedDrainReported: false,
      deferredCancellationOutcome: null,
      executionFence: start,
      fetchStarted: false,
      terminalSpool: prepared.terminalSpool,
      detachParentAbort,
    };
    this.#active.set(requestId, request);
    void this.#execute(request);
  }

  #sameR2Fence(left: R2ExecutionFence, right: R2ExecutionFence): boolean {
    return left.requestId === right.requestId &&
      left.protocolRevision === "r2" && right.protocolRevision === "r2" &&
      left.schemaVersion === right.schemaVersion &&
      left.projectId === right.projectId &&
      left.sessionId === right.sessionId &&
      left.leaderGeneration === right.leaderGeneration &&
      left.buildHash === right.buildHash &&
      left.schemaHash === right.schemaHash &&
      left.launchNonce === right.launchNonce &&
      left.hostInstanceId === right.hostInstanceId &&
      left.targetInstanceId === right.targetInstanceId &&
      left.requestDigest === right.requestDigest &&
      left.decodedBytes === right.decodedBytes &&
      left.encodedBytes === right.encodedBytes &&
      left.encoding === right.encoding &&
      left.partCount === right.partCount;
  }

  #emitPrepared(prepared: PreparedRequest): void {
    this.#diagnostics?.prepared(prepared.requestId);
    const fence = prepared.commitment;
    const frame: R2RequestPreparedFrame = {
      kind: "request_prepared",
      requestId: fence.requestId,
      protocolRevision: "r2",
      schemaVersion: fence.schemaVersion,
      projectId: fence.projectId,
      sessionId: fence.sessionId,
      leaderGeneration: fence.leaderGeneration,
      buildHash: fence.buildHash,
      schemaHash: fence.schemaHash,
      launchNonce: fence.launchNonce,
      hostInstanceId: fence.hostInstanceId,
      targetInstanceId: fence.targetInstanceId,
      requestDigest: fence.requestDigest,
      decodedBytes: fence.decodedBytes,
      encodedBytes: fence.encodedBytes,
      encoding: fence.encoding,
      partCount: fence.partCount,
    };
    this.#emit(frame, "control", prepared.requestId);
  }

  async #expirePrepared(prepared: PreparedRequest): Promise<void> {
    try {
      await this.#discardPrepared(prepared, "expired");
    } catch (error) {
      this.#reportFatalJournalError(error);
      const fatal = error instanceof Error ? error : new Error("Prepared request cleanup failed");
      this.#onFatalError?.(fatal);
    }
  }

  async #discardPrepared(
    prepared: PreparedRequest,
    reason: "client" | "drain" | "expired" | "failed",
  ): Promise<boolean> {
    if (this.#prepared.get(prepared.requestId) !== prepared) return false;
    this.#preparedCleanups++;
    this.#preparedCleanupIds.add(prepared.requestId);
    let durableCancellation = false;
    try {
      this.#prepared.delete(prepared.requestId);
      if (prepared.expires) clearTimeout(prepared.expires);
      prepared.expires = undefined;
      releaseRequestBody(prepared.body);
      prepared.lease.release();
      await prepared.terminalSpool.abort();
      await this.#journal.markCancelled(prepared.requestId);
      durableCancellation = true;
      if (reason === "client") {
        this.#emit({
          kind: "request_cancelled",
          requestId: prepared.requestId,
        }, "control", prepared.requestId);
      } else {
        this.#emitError(
          prepared.requestId,
          reason === "expired"
            ? "prepared_expired"
            : reason === "failed" ? "prepared_failed" : "draining",
          reason === "expired"
            ? "Prepared request expired before request_start"
            : reason === "failed"
              ? "Prepared request could not be published"
              : "Sidecar drained before request_start",
          false,
        );
      }
      return true;
    } finally {
      this.#preparedCleanups = Math.max(0, this.#preparedCleanups - 1);
      if (durableCancellation) {
        this.#preparedCleanupIds.delete(prepared.requestId);
      } else {
        this.#unprovenPreparedCleanupIds.add(prepared.requestId);
        if (this.#draining) this.#forcedDrain = true;
      }
      this.#finishDrainIfIdle();
    }
  }

  async #abandonClaim(requestId: string, retainOutcome = false): Promise<void> {
    if (this.#journal.status(requestId) !== "claimed") return;
    await this.#journal.markCancelled(requestId);
    if (!retainOutcome) {
      await this.#journal.acknowledgeOutcome(requestId);
      this.#diagnostics?.removeRequest(requestId);
    }
  }

  async #execute(request: ActiveRequest): Promise<void> {
    let fatalBoundaryFailure = false;
    let withheldTerminalEvent: SseFrame | null = null;
    let upstreamPrelude: import("../protocol/shared").UpstreamPrelude | null = null;
    let terminalSpool: Awaited<ReturnType<ClaimJournal["beginTerminalSpool"]>> | null =
      request.terminalSpool;
    try {
      if (!terminalSpool) {
        try {
          terminalSpool = await this.#journal.beginTerminalSpool(request.requestId);
        } catch (error) {
          if (
            this.#binding.protocolRevision === "r2" &&
            error instanceof ClaimJournalError &&
            error.code === "capacity"
          ) {
            await this.#journal.markCancelled(request.requestId);
            await this.#journal.acknowledgeOutcome(request.requestId);
            this.#reject(
              request.requestId,
              "capacity",
              "Sidecar terminal spool capacity is unavailable",
            );
            return;
          }
          throw error;
        }
      }
      const terminal = await this.#upstream.execute({
        body: request.body,
        bodyIsCanonicalStream: true,
        signal: request.controller.signal,
        beforeFetch: async () => {
          await this.#proveExecutionBoundary(request.executionFence);
          const fetchStartSyncStarted = performance.now();
          try {
            await this.#journal.markFetchStarting(request.requestId);
          } catch (error) {
            this.#reportFatalJournalError(error);
            throw error;
          }
          const fetchStartSyncMs = performance.now() - fetchStartSyncStarted;
          await this.#proveExecutionBoundary(request.executionFence);
          request.fetchStarted = true;
          this.#diagnostics?.fetchStarted(request.requestId, fetchStartSyncMs);
          if (request.executionFence !== null) this.#emitRequestAck(request);
        },
        onPrelude: async prelude => {
          upstreamPrelude = { ...prelude };
          if (this.#binding.protocolRevision !== "r2") return;
          await this.#outputBackpressure?.wait(request.controller.signal);
          this.#emit({
            kind: "upstream_prelude",
            requestId: request.requestId,
            status: prelude.status,
            contentType: prelude.contentType,
            upstreamRequestId: prelude.upstreamRequestId,
          }, "terminal", request.requestId);
        },
        onTranscriptBytes: bytes => terminalSpool!.append(bytes),
        onEvent: async event => {
          if (
            event.event === "response.completed" ||
            event.event === "response.failed" ||
            event.event === "response.incomplete"
          ) {
            if (withheldTerminalEvent) {
              throw new UpstreamExecutionError(
                "Upstream emitted multiple terminal events",
                "duplicate_terminal",
                true,
              );
            }
            withheldTerminalEvent = event;
            return;
          }
          const eventIndex = request.eventIndex++;
          await this.#emitEvent(request.requestId, eventIndex, event, this.#diagnostics?.upstreamEvent(request.requestId, eventIndex), request.controller.signal);
        },
      });
      withheldTerminalEvent = terminal.event ?? withheldTerminalEvent;
      if (
        !withheldTerminalEvent ||
        terminal.finalEventIndex !== request.eventIndex
      ) {
        throw new UpstreamExecutionError(
          "Upstream terminal event ordering is inconsistent",
          "terminal_order",
          true,
        );
      }
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
      await this.#journal.markTerminalFromSpool(
        request.requestId,
        terminal.responseId,
        terminal.transcriptSha256,
        terminal.byteCount,
        terminalSpool!,
        upstreamPrelude ?? undefined,
      );
      // A bounded forced-drain timeout may already have emitted an explicitly
      // unproven ambiguous outcome. If this pre-existing terminal append later
      // wins durability, keep live output single-valued; restart can replay the
      // newly proven terminal without contradicting the current connection.
      if (request.forcedDrainReported) {
        this.#diagnostics?.removeRequest(request.requestId);
        return;
      }
      await this.#emitEvent(
        request.requestId,
        terminal.finalEventIndex,
        withheldTerminalEvent,
        this.#diagnostics?.upstreamEvent(request.requestId, terminal.finalEventIndex),
        undefined,
      );
      request.eventIndex = terminal.finalEventIndex + 1;
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
      if (this.#isFatalExecutionBoundaryError(error)) {
        fatalBoundaryFailure = true;
        return;
      }
      this.#reportFatalJournalError(error);
      const upstream = error instanceof UpstreamExecutionError ? error : null;
      let ambiguous = upstream?.ambiguous ?? true;
      if (upstream && !ambiguous) {
        try {
          await this.#journal.markProviderError(request.requestId, {
            ...(upstream.status === undefined ? {} : { status: upstream.status }),
            code: upstream.code,
            message: upstream.message,
            ...(upstream.rateLimit ? { rateLimit: upstream.rateLimit } : {}),
            ...(upstream.providerError ? { providerError: upstream.providerError } : {}),
          }, upstreamPrelude ?? undefined);
        } catch (journalError) {
          // A provider failure is definitive only after the journal transition
          // is durable. If that proof fails, expose ambiguity and retire the
          // lifecycle so a restart observes the same unresolved fetch truth.
          this.#reportUnprovenDurableOutcome(
            journalError,
            "Provider outcome could not be durably recorded",
          );
          ambiguous = true;
        }
      }
      if (request.forcedDrainReported && request.cancellation === "drain") {
        this.#diagnostics?.removeRequest(request.requestId);
        return;
      }
      if (ambiguous) this.#ambiguous.add(request.requestId);
      this.#emitError(
        request.requestId,
        upstream?.code ?? "internal",
        upstream?.message ?? "Sidecar execution failed after commitment",
        ambiguous,
        upstream?.rateLimit,
        upstream?.providerError,
      );
    } finally {
      releaseAtomicBytePermit(withheldTerminalEvent);
      if (terminalSpool) {
        try { await terminalSpool.abort(); }
        catch (error) { this.#reportFatalJournalError(error); }
      }
      request.detachParentAbort?.();
      releaseRequestBody(request.body);
      request.lease.release();
      this.#active.delete(request.requestId);
      const deferred = request.deferredCancellationOutcome;
      if (fatalBoundaryFailure) {
        // Election/ownership loss is process-fatal and deliberately silent.
        // Never turn it into a client cancellation or ambiguous outcome.
      } else if (deferred?.kind === "client") {
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
      } else if (deferred?.kind === "ambiguous") {
        this.#emitError(
          request.requestId,
          deferred.code,
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
      this.#reportFatalCancellationStorage(error);
      this.#ambiguous.add(request.requestId);
      request.deferredCancellationOutcome = {
        kind: "ambiguous",
        code: "cancellation_unproven",
        message: "Client cancellation could not be durably recorded",
      };
      return;
    }
    request.deferredCancellationOutcome = { kind: "client" };
  }

  async #emitForcedDrainOutcome(
    request: ActiveRequest,
    message: string,
    emitImmediately: boolean,
  ): Promise<void> {
    request.forcedDrainTask ??= this.#persistForcedDrainOutcome(request, message);
    const result = await request.forcedDrainTask;
    if (result === "definitive_won") return;
    if (request.forcedDrainReported) return;
    request.forcedDrainReported = true;
    this.#ambiguous.add(request.requestId);
    const code = result === "forced" ? "forced_drain_cancelled" : "forced_drain_unproven";
    const outputMessage = result === "forced"
      ? message
      : "Forced-drain outcome could not be durably recorded before teardown";
    if (emitImmediately) {
      this.#emitError(
        request.requestId,
        code,
        outputMessage,
        true,
      );
    } else {
      request.deferredCancellationOutcome = result === "forced"
        ? { kind: "drain", message }
        : { kind: "ambiguous", code, message: outputMessage };
    }
  }

  async #persistForcedDrainOutcome(
    request: ActiveRequest,
    message: string,
  ): Promise<"forced" | "definitive_won" | "unproven"> {
    const marking = this.#journal.markForcedAmbiguous(request.requestId, {
      code: "forced_drain_cancelled",
      message,
    }).then(
      () => "forced" as const,
      error => {
        const state = this.#journal.status(request.requestId);
        if (
          error instanceof ClaimJournalError &&
          (error.code === "conflict" || error.code === "transition") &&
          (state === "terminal" || state === "provider_error" || state === "cancelled" || state === "acked")
        ) return "definitive_won" as const;
        this.#reportUnprovenDurableOutcome(
          error,
          "Forced-drain outcome could not be durably recorded",
        );
        return "unproven" as const;
      },
    );
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        marking,
        new Promise<"unproven">(resolve => {
          timer = setTimeout(() => {
            this.#reportUnprovenDurableOutcome(
              new Error("Forced-drain journal transition timed out"),
              "Forced-drain outcome could not be durably recorded",
            );
            resolve("unproven");
          }, this.#drainAbortSettlementMs);
        }),
      ]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  async #emitEvent(requestId: string, eventIndex: number, event: SseFrame, diagnostics?: import("../diagnostics/types").V2FirstEventDiagnostics, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    if (eventIndex !== 0 && this.#batcher) {
      await this.#outputBackpressure?.wait(signal);
      if (this.#batcher.push(requestId, eventIndex, event)) return;
    }
    await streamResponseEventParts(event.data, async part => {
      if (signal?.aborted) throw abortError(signal);
      await this.#outputBackpressure?.wait(signal);
      const frame: SidecarFrame = {
        kind: "response_event",
        requestId,
        eventIndex,
        event: event.event,
        ...part,
        ...(part.partIndex === 0 && diagnostics ? { diagnostics } : {}),
      };
      if (utf8Bytes(JSON.stringify(frame)).byteLength > GP_V2_FRAME_MAX_BYTES) {
        throw new Error("Segmented response event exceeds physical frame limit");
      }
      this.#emit(frame, "data", requestId);
    }, { partBytes: RESPONSE_PART_BYTES });
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
      if (recovered.outcome.error.providerError?.origin === "sse") {
        this.#emitRecoveredPrelude(
          requestId,
          recovered.prelude,
          recovered.outcome.error.providerError.upstreamRequestId,
        );
      }
      this.#emitError(
        requestId,
        recovered.outcome.error.code,
        recovered.outcome.error.message,
        false,
        recovered.outcome.error.rateLimit,
        recovered.outcome.error.providerError,
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
    let recovered: Awaited<ReturnType<ClaimJournal["openRecoveredTerminal"]>> = null;
    let terminal: Awaited<ReturnType<TranscriptProof["createTerminal"]>>;
    try {
      try {
        recovered = await this.#journal.openRecoveredTerminal(requestId);
      } catch (error) {
        readFailed = true;
        throw error;
      }
      if (!recovered || recovered.leaderGeneration !== this.#binding.leaderGeneration) {
        throw new Error("terminal spool is unavailable");
      }
      const transcript = new TranscriptProof(requestId, recovered.requestDigest, {
        retainBytes: false,
      });
      await streamTerminalFrames(recovered.reader, signal, this.#atomicBytePermits, async event => {
        await transcript.appendCanonicalStream(event);
      });
      terminal = await transcript.createTerminal();
      if (
        terminal.responseId !== recovered.responseId ||
        terminal.transcriptSha256 !== recovered.terminalDigest
      ) throw new Error("terminal spool identity mismatch");
      await recovered.reader.rewind();
    } catch (error) {
      recovered?.reader.close();
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
    this.#emitRecoveredPrelude(requestId, recovered.prelude);
    try {
      let eventIndex = 0;
      await streamTerminalFrames(recovered!.reader, signal, this.#atomicBytePermits, async event => {
        await this.#emitEvent(requestId, eventIndex++, event, undefined, signal);
      });
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
    } finally {
      recovered!.reader.close();
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

  #emitRequestAck(request: ActiveRequest): void {
    const diagnostics = this.#diagnostics?.requestAck(request.requestId);
    this.#emit({
      kind: "request_ack",
      requestId: request.requestId,
      requestDigest: request.requestDigest,
      committed: true,
      ...(diagnostics ? { diagnostics } : {}),
    }, "terminal", request.requestId);
  }

  #emitRecoveredPrelude(
    requestId: string,
    prelude?: import("../protocol/shared").UpstreamPrelude,
    fallbackUpstreamRequestId: string | null = null,
  ): void {
    if (this.#binding.protocolRevision !== "r2") return;
    const recovered = prelude ?? {
      status: 200,
      contentType: "text/event-stream; charset=utf-8",
      upstreamRequestId: fallbackUpstreamRequestId,
    };
    this.#emit({
      kind: "upstream_prelude",
      requestId,
      ...recovered,
    }, "terminal", requestId);
  }

  #emitError(
    requestId: string,
    code: string,
    message: string,
    ambiguous: boolean,
    rateLimit?: import("../rate-limit").RateLimitHint,
    providerError?: import("../provider-error").ProviderErrorEnvelope,
  ): void {
    this.#batcher?.flush(requestId,"terminal");
    this.#emit({
      kind: "response_error",
      requestId,
      code: this.#safeCode(code),
      message: this.#safeMessage(message),
      ambiguous,
      ...(rateLimit ? { rateLimit } : {}),
      ...(providerError ? { providerError } : {}),
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

  #reportUnprovenDurableOutcome(value: unknown, fallback: string): void {
    if (this.#reportFatalJournalError(value)) return;
    this.#onFatalError?.(value instanceof Error ? value : new Error(fallback));
  }

  #reportFatalCancellationStorage(value: unknown): boolean {
    if (this.#reportFatalJournalError(value)) return true;
    if (!this.#isOwnershipError(value)) return false;
    this.#onFatalError?.(value as Error);
    return true;
  }

  #isOwnershipError(value: unknown): boolean {
    return value instanceof Error && value.name === "SidecarOwnershipError";
  }

  #isFatalExecutionBoundaryError(value: unknown): boolean {
    return value instanceof Error && (
      value.name === "V2ExecutionFenceError" ||
      this.#isOwnershipError(value)
    );
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

  async #proveExecutionBoundary(
    frame: R1RequestCommitFrame | R2RequestCommitFrame | R2RequestStartFrame | null,
  ): Promise<void> {
    await this.#proveOwnership();
    if (!frame || !("protocolRevision" in frame) || frame.protocolRevision !== "r2") return;
    try {
      await this.#assertExecutionFence!(frame);
    } catch (error) {
      const fenced = new V2ExecutionFenceError(
        "R2 execution fence is not currently elected",
        { cause: error },
      );
      this.#onFatalError?.(fenced);
      throw fenced;
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
    if (
      !this.#draining ||
      this.#active.size > 0 ||
      this.#prepared.size > 0 ||
      this.#preparedCleanups > 0 ||
      this.#unprovenPreparedCleanupIds.size > 0 ||
      this.#committing > 0
    ) return;
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
