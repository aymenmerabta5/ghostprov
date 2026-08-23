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
import { ClaimJournal } from "./journal";
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
}

interface ActiveRequest {
  requestId: string;
  requestDigest: string;
  body: Uint8Array;
  controller: AbortController;
  lease: { release(): void };
  eventIndex: number;
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
  readonly #diagnostics?: SidecarDiagnosticsObserver;
  readonly #batcher: ResponseBatcher | null;
  readonly #outputBackpressure?: OutputBackpressure;
  readonly #active = new Map<string, ActiveRequest>();
  readonly #ambiguous = new Set<string>();
  readonly #terminalDigests = new Map<string, string>();
  readonly #drainWaiters: Array<() => void> = [];
  #ready = false;
  #started = false;
  #draining = false;
  #committing = 0;
  #drainedEmitted = false;
  #drainNotified = false;

  constructor(options: {
    binding: ProtocolBinding;
    assembler: RequestAssembler;
    journal: ClaimJournal;
    admission: SidecarAdmission;
    upstream: SidecarUpstream;
    onDrainRequested?: (reason: string) => void;
    diagnostics?: SidecarDiagnosticsObserver;
    batching?: boolean;
    outputBackpressure?: OutputBackpressure;
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
    this.#diagnostics = options.diagnostics;
    this.#emitFrame = options.emit;
    this.#batcher = options.batching ? new ResponseBatcher({ emit: (frame, priority) => this.#emitFrame(frame, priority, frame.requestId) }) : null;
    this.#outputBackpressure = options.outputBackpressure;
  }

  async start(): Promise<void> {
    if (this.#started) return;
    this.#started = true;
    const recovered = await this.#journal.recover();
    for (const requestId of recovered.terminals) {
      await this.#replayTerminal(requestId);
    }
    for (const requestId of recovered.ambiguous) {
      const identity = this.#journal.identity(requestId);
      if (identity?.leaderGeneration !== this.#binding.leaderGeneration) continue;
      this.#ambiguous.add(requestId);
      this.#emitError(
        requestId,
        "recovered_ambiguous",
        "Upstream execution has no proven terminal after recovery",
        true,
      );
    }
    this.#ready = true;
  }

  async accept(
    frame: ReliableFrame<HostFrame>,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!this.#started) throw new Error("Sidecar engine is not started");
    switch (frame.kind) {
      case "request_open":
        this.#diagnostics?.requestOpened(frame.requestId);
      case "request_data":
        this.#assembler.accept(frame);
        return;
      case "request_commit":
        this.#diagnostics?.commitReceived(frame.requestId);
        this.#committing++;
        try {
          await this.#commit(frame.requestId, signal);
        } finally {
          this.#committing--;
          this.#finishDrainIfIdle();
        }
        return;
      case "request_cancel": {
        this.#batcher?.flush(frame.requestId,"control");
        this.#assembler.cancel(frame.requestId);
        this.#active.get(frame.requestId)?.controller.abort();
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

  async drain(_reason: string): Promise<void> {
    this.#batcher?.flushAll();
    if (!this.#draining) {
      this.#draining = true;
      this.#admission.close();
    }
    if (this.#active.size === 0 && this.#committing === 0) {
      this.#finishDrainIfIdle();
      return;
    }
    await new Promise<void>(resolve => this.#drainWaiters.push(resolve));
  }

  setHostCeiling(value: number): void {
    this.#admission.setHostCeiling(value);
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
    try {
      const digest = await sha256Hex(body);
      const claimStarted = performance.now();
      await this.#journal.claim({
        requestId,
        requestDigest: digest,
        leaderGeneration: this.#binding.leaderGeneration,
      });
      this.#diagnostics?.claimCompleted(requestId, performance.now() - claimStarted);
      const state = this.#journal.status(requestId);
      if (state === "fetch_starting") {
        body.fill(0);
        this.#ambiguous.add(requestId);
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
        await this.#replayTerminal(requestId);
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
        this.#reject(requestId, "capacity", "Sidecar admission capacity is unavailable");
        return;
      }
      const controller = new AbortController();
      if (signal?.aborted) controller.abort();
      else signal?.addEventListener("abort", () => controller.abort(), { once: true });
      const request: ActiveRequest = {
        requestId,
        requestDigest: digest,
        body,
        controller,
        lease,
        eventIndex: 0,
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
    }
  }

  async #execute(request: ActiveRequest): Promise<void> {
    try {
      const terminal = await this.#upstream.execute({
        body: request.body,
        signal: request.controller.signal,
        beforeFetch: async () => { await this.#journal.markFetchStarting(request.requestId); this.#diagnostics?.fetchStarted(request.requestId); },
        onEvent: async event => {
          const eventIndex = request.eventIndex++;
          await this.#emitEvent(request.requestId, eventIndex, event, this.#diagnostics?.upstreamEvent(request.requestId, eventIndex), request.controller.signal);
        },
      });
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
      }, "terminal", request.requestId);
    } catch (error) {
      const upstream = error instanceof UpstreamExecutionError ? error : null;
      const ambiguous = upstream?.ambiguous ?? true;
      if (ambiguous) this.#ambiguous.add(request.requestId);
      this.#emitError(
        request.requestId,
        upstream?.code ?? "internal",
        upstream?.message ?? "Sidecar execution failed after commitment",
        ambiguous,
      );
    } finally {
      request.body.fill(0);
      request.lease.release();
      this.#active.delete(request.requestId);
      this.#finishDrainIfIdle();
    }
  }

  async #emitEvent(requestId: string, eventIndex: number, event: SseFrame, diagnostics?: import("../diagnostics/types").V2FirstEventDiagnostics, signal?: AbortSignal): Promise<void> {
    if (!diagnostics && this.#batcher?.push(requestId, eventIndex, event)) return;
    const bytes = utf8Bytes(event.data);
    const partCount = Math.max(1, Math.ceil(bytes.byteLength / RESPONSE_PART_BYTES));
    for (let partIndex = 0, offset = 0; partIndex < partCount; partIndex++) {
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

  async #replayTerminal(requestId: string): Promise<void> {
    const recovered = await this.#journal.readRecoveredTerminal(requestId);
    if (!recovered || recovered.leaderGeneration !== this.#binding.leaderGeneration) return;
    try {
      const raw = utf8Text(recovered.bytes);
      const parsed = parseFrames(raw, 0);
      if (parsed.nextOffset !== raw.length) throw new Error("terminal spool is incomplete");
      const transcript = new TranscriptProof(requestId, recovered.requestDigest);
      let eventIndex = 0;
      for (const event of parsed.frames) {
        transcript.append(event);
        await this.#emitEvent(requestId, eventIndex++, event);
      }
      const terminal = await transcript.createTerminal();
      if (
        terminal.responseId !== recovered.responseId ||
        terminal.transcriptSha256 !== recovered.terminalDigest
      ) throw new Error("terminal spool identity mismatch");
      this.#terminalDigests.set(requestId, terminal.transcriptSha256);
      this.#batcher?.flush(requestId,"terminal");
      this.#emit({ ...terminal, requestId, requestDigest: recovered.requestDigest }, "terminal", requestId);
    } catch {
      this.#ambiguous.add(requestId);
      this.#emitError(
        requestId,
        "terminal_recovery_failed",
        "Recovered terminal spool did not match durable proof",
        true,
      );
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

  #emitError(
    requestId: string,
    code: string,
    message: string,
    ambiguous: boolean,
  ): void {
    this.#batcher?.flush(requestId,"terminal");
    this.#emit({
      kind: "response_error",
      requestId,
      code: this.#safeCode(code),
      message: this.#safeMessage(message),
      ambiguous,
    }, "terminal", requestId);
  }

  #safeCode(value: string): string {
    return /^[A-Za-z][A-Za-z0-9._-]{0,63}$/.test(value) ? value : "internal";
  }

  #safeMessage(value: string): string {
    return value
      .replace(/\b(token|api[_-]?key|secret|password)\s*[:=]\s*[^;\s,]+/gi, "$1=[REDACTED]")
      .replace(/[A-Za-z0-9+/_=-]{48,}/g, "[REDACTED]")
      .slice(0, 240) || "Sidecar request failed";
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
