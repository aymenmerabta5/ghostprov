import type { SseFrame } from "../../src/relay/sse";
import {
  base64UrlEncode,
  GP_V2_R2_SCHEMA_VERSION,
  sha256Hex,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type R2RequestCommitFrame,
  type R2RequestStartFrame,
  type SidecarFrame,
} from "../../src/v2/protocol/shared";
import { TranscriptProof } from "../../src/v2/protocol/transcript";
import { RequestAssembler } from "../../src/v2/sidecar/assembler";
import {
  ClaimJournal,
  type JournalRecord,
  type JournalStorage,
  type JournalTerminalReader,
  type JournalTerminalSpool,
} from "../../src/v2/sidecar/journal";
import {
  SidecarEngine,
  type SidecarAdmission,
  type SidecarUpstream,
} from "../../src/v2/sidecar/engine";
import {
  UpstreamExecutionError,
  type DirectResponsesExecution,
  type UpstreamTerminal,
} from "../../src/v2/sidecar/upstream";
import type { OutputBackpressure } from "../../src/v2/sidecar/output-sink";
import type { AtomicBytePermitPool } from "../../src/v2/memory/atomic-byte-permits";
import { gzipSync } from "fflate";

export const TEST_BINDING: ProtocolBinding = {
  protocolRevision: "r1",
  launchNonce: "launch-nonce-r1",
  projectId: "e631d12c-d917-4434-b360-41b4715d6679",
  sessionId: "sandbox-session-1",
  leaderGeneration: 3,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};

export const TEST_R2_BINDING: ProtocolBinding = {
  protocolRevision: "r2",
  launchNonce: "launch-nonce-r2",
  projectId: TEST_BINDING.projectId,
  sessionId: "sandbox-session-r2",
  leaderGeneration: 4,
  buildHash: "c".repeat(64),
  schemaHash: "d".repeat(64),
};

export const TEST_R2_HOST_INSTANCE = "host-engine-r2";
export const TEST_R2_SIDECAR_INSTANCE = "sidecar-engine-r2";

export class FakeSidecarStorage implements JournalStorage {
  readonly records: JournalRecord[];
  readonly terminals = new Map<string, Uint8Array>();
  readonly timeline: string[];
  terminalReads = 0;
  terminalExistenceChecks = 0;
  terminalReadPasses = 0;
  terminalMaxChunkBytes = 0;

  constructor(
    timeline: string[],
    records: JournalRecord[] = [],
    terminals: Map<string, Uint8Array> = new Map(),
    readonly beforeAppend?: () => Promise<void>,
    readonly terminalReadError?: Error,
    readonly beforeReadRecords?: () => Promise<void>,
    readonly terminalSpoolError?: Error,
    readonly beforeBeginTerminalSpool?: () => Promise<void>,
    readonly beforePromoteTerminalSpool?: () => Promise<void>,
  ) {
    this.timeline = timeline;
    this.records = [...records];
    for (const [key, value] of terminals) this.terminals.set(key, value.slice());
  }

  async appendAndSync(records: readonly JournalRecord[]): Promise<void> {
    await this.beforeAppend?.();
    this.records.push(...records.map(record => ({ ...record })));
    this.timeline.push(`sync:${records.map(record => record.state).join(",")}`);
  }
  async beginTerminalSpool(id: string, maxBytes: number): Promise<JournalTerminalSpool> {
    await this.beforeBeginTerminalSpool?.();
    if (this.terminalSpoolError) throw this.terminalSpoolError;
    const chunks: Uint8Array[] = [];
    let byteCount = 0;
    let state: "active" | "promoted" | "aborted" = "active";
    const storage = this;
    return {
      requestId: id,
      maxBytes,
      get byteCount() { return byteCount; },
      async append(bytes) {
        if (state !== "active" || byteCount + bytes.byteLength > maxBytes) throw new Error("spool bounds");
        chunks.push(bytes.slice());
        byteCount += bytes.byteLength;
      },
      async promote(proof) {
        if (state !== "active" || proof.byteCount !== byteCount) throw new Error("spool proof");
        await storage.beforePromoteTerminalSpool?.();
        const value = new Uint8Array(byteCount);
        let offset = 0;
        for (const chunk of chunks) { value.set(chunk, offset); offset += chunk.byteLength; }
        if (await sha256Hex(value) !== proof.transcriptSha256) throw new Error("spool digest");
        storage.terminals.set(id, value);
        storage.timeline.push("terminal-spool");
        state = "promoted";
        chunks.length = 0;
      },
      async abort() { state = "aborted"; chunks.length = 0; },
    };
  }
  async writeTerminal(id: string, bytes: Uint8Array): Promise<void> {
    this.terminals.set(id, bytes.slice());
    this.timeline.push("terminal-spool");
  }
  async readTerminal(id: string): Promise<Uint8Array | null> {
    this.terminalReads++;
    if (this.terminalReadError) throw this.terminalReadError;
    return this.terminals.get(id)?.slice() ?? null;
  }
  async openTerminal(id: string): Promise<JournalTerminalReader | null> {
    this.terminalReads++;
    if (this.terminalReadError) throw this.terminalReadError;
    const bytes = this.terminals.get(id);
    if (!bytes) return null;
    const storage = this;
    let closed = false;
    return {
      requestId: id,
      byteCount: bytes.byteLength,
      async *chunks() {
        if (closed) throw new Error("reader closed");
        storage.terminalReadPasses++;
        for (let offset = 0; offset < bytes.byteLength; offset += 64 * 1024) {
          const chunk = bytes.subarray(offset, offset + 64 * 1024);
          storage.terminalMaxChunkBytes = Math.max(
            storage.terminalMaxChunkBytes,
            chunk.byteLength,
          );
          yield chunk;
        }
      },
      async rewind() {
        if (closed) throw new Error("reader closed");
      },
      close() { closed = true; },
    };
  }
  async terminalExists(id: string): Promise<boolean> {
    this.terminalExistenceChecks++;
    return this.terminals.has(id);
  }
  async deleteTerminal(id: string): Promise<void> {
    this.terminals.delete(id);
    this.timeline.push("terminal-delete");
  }
  async readRecords(): Promise<readonly JournalRecord[]> {
    await this.beforeReadRecords?.();
    return this.records;
  }
}

class FakeAdmission implements SidecarAdmission {
  active = 0;
  queued = 0;
  effectiveLimit = 128;
  readonly hostCeilings: number[] = [];

  constructor(readonly timeline: string[]) {}

  async acquire(): Promise<{ release(): void }> {
    this.timeline.push("permit");
    this.active++;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.active--;
        this.timeline.push("release");
      },
    };
  }

  close(): void {}

  setHostCeiling(value: number): void {
    this.hostCeilings.push(value);
    this.effectiveLimit = Math.min(128, value);
  }

  snapshot() {
    return {
      active: this.active,
      queued: this.queued,
      effectiveLimit: this.effectiveLimit,
      physicalMax: 128,
      reservedBytes: this.active * 4 * 1024 * 1024,
      memoryBudgetBytes: 512 * 1024 * 1024,
    };
  }
}

class FakeUpstream implements SidecarUpstream {
  calls = 0;

  constructor(
    readonly timeline: string[],
    readonly behavior: (input: DirectResponsesExecution) => Promise<UpstreamTerminal>,
  ) {}

  async execute(input: DirectResponsesExecution): Promise<UpstreamTerminal> {
    this.calls++;
    return this.behavior(input);
  }
}

export async function completedUpstream(
  input: DirectResponsesExecution,
  delta = "hello",
): Promise<UpstreamTerminal> {
  await input.beforeFetch();
  const final = {
    id: "resp_1",
    object: "response",
    status: "completed",
    output: [{ type: "message", content: [{ type: "output_text", text: delta }] }],
  };
  const events: SseFrame[] = [
    { event: "response.created", data: JSON.stringify({ type: "response.created", response: { id: "resp_1" } }) },
    { event: "response.output_text.delta", data: JSON.stringify({ type: "response.output_text.delta", delta }) },
    { event: "response.completed", data: JSON.stringify({ type: "response.completed", response: final }) },
  ];
  const proof = new TranscriptProof("upstream", "0".repeat(64), { retainBytes: false });
  for (let index = 0; index < events.length; index++) {
    const event = events[index]!;
    await proof.appendCanonicalStream(event, { onChunk: input.onTranscriptBytes });
    if (index !== events.length - 1) await input.onEvent(event);
  }
  const terminal = await proof.createTerminal();
  return {
    responseId: terminal.responseId,
    status: terminal.terminalStatus,
    eventCount: terminal.eventCount,
    byteCount: terminal.byteCount,
    transcriptSha256: terminal.transcriptSha256,
    finalEventIndex: terminal.finalEventIndex,
    finalSha256: terminal.finalSha256,
    final,
    event: events[events.length - 1]!,
  };
}

export interface SidecarHarnessOptions {
  behavior?: (input: DirectResponsesExecution) => Promise<UpstreamTerminal>;
  records?: JournalRecord[];
  terminals?: Map<string, Uint8Array>;
  onDrainRequested?: (reason: string) => void;
  onFatalError?: (error: Error) => void;
  assertOwnership?: () => Promise<void>;
  batching?: boolean;
  outputBackpressure?: OutputBackpressure;
  drainTimeoutMs?: number;
  drainAbortSettlementMs?: number;
  deferAcceptUntilStarted?: boolean;
  emit?: (frame: SidecarFrame) => void;
  beforeAppend?: () => Promise<void>;
  terminalReadError?: Error;
  beforeReadRecords?: () => Promise<void>;
  atomicBytePermits?: AtomicBytePermitPool;
  binding?: ProtocolBinding;
  assertExecutionFence?: (
    frame: R2RequestCommitFrame | R2RequestStartFrame,
  ) => void | Promise<void>;
  preparedTimeoutMs?: number;
  terminalSpoolError?: Error;
  beforeBeginTerminalSpool?: () => Promise<void>;
  beforePromoteTerminalSpool?: () => Promise<void>;
}

export class SidecarHarness {
  readonly timeline: string[] = [];
  readonly frames: SidecarFrame[] = [];
  readonly storage: FakeSidecarStorage;
  readonly journal: ClaimJournal;
  readonly admission: FakeAdmission;
  readonly upstream: FakeUpstream;
  readonly engine: SidecarEngine;
  readonly binding: ProtocolBinding;
  requestId = "req-engine-1";
  requestDigest = "";

  constructor(options: SidecarHarnessOptions = {}) {
    this.binding = options.binding ?? TEST_BINDING;
    this.storage = new FakeSidecarStorage(
      this.timeline,
      options.records,
      options.terminals,
      options.beforeAppend,
      options.terminalReadError,
      options.beforeReadRecords,
      options.terminalSpoolError,
      options.beforeBeginTerminalSpool,
      options.beforePromoteTerminalSpool,
    );
    this.journal = new ClaimJournal({
      storage: this.storage,
      flushIntervalMs: 0,
      maxBatch: 64,
    });
    this.admission = new FakeAdmission(this.timeline);
    this.upstream = new FakeUpstream(
      this.timeline,
      options.behavior ?? (input => completedUpstream(input)),
    );
    this.engine = new SidecarEngine({
      binding: this.binding,
      assembler: new RequestAssembler({
        atomicBytePermits: options.atomicBytePermits,
      }),
      journal: this.journal,
      admission: this.admission,
      upstream: this.upstream,
      onDrainRequested: options.onDrainRequested,
      onFatalError: options.onFatalError,
      assertOwnership: options.assertOwnership,
      ...(this.binding.protocolRevision === "r2"
        ? { assertExecutionFence: options.assertExecutionFence ?? (() => undefined) }
        : {}),
      batching: options.batching,
      outputBackpressure: options.outputBackpressure,
      atomicBytePermits: options.atomicBytePermits,
      drainTimeoutMs: options.drainTimeoutMs,
      drainAbortSettlementMs: options.drainAbortSettlementMs,
      deferAcceptUntilStarted: options.deferAcceptUntilStarted,
      preparedTimeoutMs: options.preparedTimeoutMs,
      emit: frame => {
        this.frames.push(frame);
        this.timeline.push(`emit:${frame.kind}`);
        options.emit?.(frame);
      },
    });
  }

  async sendRequest(
    body: Record<string, unknown>,
    options: { gzip?: boolean } = {},
  ): Promise<void> {
    if (this.binding.protocolRevision === "r2") {
      const start = await this.prepareRequest(body, options);
      await this.startPrepared(start);
      return;
    }
    await this.engine.start();
    const bytes = new TextEncoder().encode(JSON.stringify(body));
    const encoded = options.gzip ? gzipSync(bytes) : bytes;
    this.requestDigest = await sha256Hex(bytes);
    const open: ReliableFrame<HostFrame> = {
      kind: "request_open",
      requestId: this.requestId,
      requestDigest: this.requestDigest,
      decodedBytes: bytes.byteLength,
      encodedBytes: encoded.byteLength,
      encoding: options.gzip ? "gzip+base64" : "identity",
      partCount: 1,
      epoch: 1,
      sequence: 0,
    };
    const data: ReliableFrame<HostFrame> = {
      kind: "request_data",
      requestId: this.requestId,
      partIndex: 0,
      offset: 0,
      dataBase64Url: base64UrlEncode(encoded),
      epoch: 1,
      sequence: 1,
    };
    const commit: ReliableFrame<HostFrame> = {
      kind: "request_commit",
      requestId: this.requestId,
      epoch: 1,
      sequence: 2,
    };
    await this.engine.accept(open);
    await this.engine.accept(data);
    await this.engine.accept(commit);
  }

  async prepareRequest(
    body: Record<string, unknown>,
    options: { gzip?: boolean } = {},
  ): Promise<R2RequestStartFrame> {
    if (this.binding.protocolRevision !== "r2") {
      throw new Error("prepareRequest requires the R2 harness binding");
    }
    await this.engine.start();
    const bytes = new TextEncoder().encode(JSON.stringify(body));
    const encoded = options.gzip ? gzipSync(bytes) : bytes;
    this.requestDigest = await sha256Hex(bytes);
    const manifest = {
      requestDigest: this.requestDigest,
      decodedBytes: bytes.byteLength,
      encodedBytes: encoded.byteLength,
      encoding: options.gzip ? "gzip+base64" as const : "identity" as const,
      partCount: 1,
    };
    const fence = {
      requestId: this.requestId,
      protocolRevision: "r2" as const,
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      projectId: this.binding.projectId,
      sessionId: this.binding.sessionId,
      leaderGeneration: this.binding.leaderGeneration,
      buildHash: this.binding.buildHash,
      schemaHash: this.binding.schemaHash,
      launchNonce: this.binding.launchNonce,
      hostInstanceId: TEST_R2_HOST_INSTANCE,
      targetInstanceId: TEST_R2_SIDECAR_INSTANCE,
      ...manifest,
    };
    await this.engine.accept({
      kind: "request_open",
      requestId: this.requestId,
      ...manifest,
      epoch: 1,
      sequence: 0,
    });
    await this.engine.accept({
      kind: "request_data",
      requestId: this.requestId,
      partIndex: 0,
      offset: 0,
      dataBase64Url: base64UrlEncode(encoded),
      epoch: 1,
      sequence: 1,
    });
    await this.engine.accept({
      kind: "request_commit",
      ...fence,
      epoch: 1,
      sequence: 2,
    });
    return { kind: "request_start", ...fence };
  }

  async startPrepared(start: R2RequestStartFrame): Promise<void> {
    await this.engine.accept({
      ...start,
      epoch: 1,
      sequence: 3,
    });
  }

  async settle(): Promise<void> {
    const deadline = performance.now() + 10_000;
    while (performance.now() < deadline) {
      const snapshot = this.engine.snapshot();
      if (snapshot.active === 0 && snapshot.replaying === 0) return;
      await new Promise(resolve => setTimeout(resolve, 0));
    }
    throw new Error("sidecar harness did not settle");
  }
}

export function resetAfterStart(
  timeline: string[],
): (input: DirectResponsesExecution) => Promise<UpstreamTerminal> {
  return async input => {
    await input.beforeFetch();
    timeline.push("fetch");
    throw new UpstreamExecutionError("reset", "transport", true);
  };
}
