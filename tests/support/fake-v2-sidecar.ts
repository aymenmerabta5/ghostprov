import type { SseFrame } from "../../src/relay/sse";
import {
  base64UrlEncode,
  sha256Hex,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type SidecarFrame,
} from "../../src/v2/protocol/shared";
import { TranscriptProof } from "../../src/v2/protocol/transcript";
import { RequestAssembler } from "../../src/v2/sidecar/assembler";
import {
  ClaimJournal,
  type JournalRecord,
  type JournalStorage,
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

export const TEST_BINDING: ProtocolBinding = {
  projectId: "e631d12c-d917-4434-b360-41b4715d6679",
  sessionId: "sandbox-session-1",
  leaderGeneration: 3,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};

export class FakeSidecarStorage implements JournalStorage {
  readonly records: JournalRecord[];
  readonly terminals = new Map<string, Uint8Array>();
  readonly timeline: string[];

  constructor(
    timeline: string[],
    records: JournalRecord[] = [],
    terminals: Map<string, Uint8Array> = new Map(),
  ) {
    this.timeline = timeline;
    this.records = [...records];
    for (const [key, value] of terminals) this.terminals.set(key, value.slice());
  }

  async appendAndSync(records: readonly JournalRecord[]): Promise<void> {
    this.records.push(...records.map(record => ({ ...record })));
    this.timeline.push(`sync:${records.map(record => record.state).join(",")}`);
  }
  async writeTerminal(id: string, bytes: Uint8Array): Promise<void> {
    this.terminals.set(id, bytes.slice());
    this.timeline.push("terminal-spool");
  }
  async readTerminal(id: string): Promise<Uint8Array | null> {
    return this.terminals.get(id)?.slice() ?? null;
  }
  async deleteTerminal(id: string): Promise<void> {
    this.terminals.delete(id);
    this.timeline.push("terminal-delete");
  }
  async readRecords(): Promise<readonly JournalRecord[]> { return this.records; }
}

class FakeAdmission implements SidecarAdmission {
  active = 0;
  queued = 0;
  effectiveLimit = 128;

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
      },
    };
  }

  close(): void {}

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
  const proof = new TranscriptProof("upstream", "0".repeat(64));
  for (const event of events) {
    proof.append(event);
    await input.onEvent(event);
  }
  const terminal = await proof.createTerminal();
  const snapshot = proof.snapshot();
  return {
    responseId: terminal.responseId,
    status: terminal.terminalStatus,
    eventCount: terminal.eventCount,
    byteCount: terminal.byteCount,
    transcriptSha256: terminal.transcriptSha256,
    finalEventIndex: terminal.finalEventIndex,
    finalSha256: terminal.finalSha256,
    final,
    transcriptBytes: snapshot.canonicalBytes,
  };
}

export interface SidecarHarnessOptions {
  behavior?: (input: DirectResponsesExecution) => Promise<UpstreamTerminal>;
  records?: JournalRecord[];
  terminals?: Map<string, Uint8Array>;
}

export class SidecarHarness {
  readonly timeline: string[] = [];
  readonly frames: SidecarFrame[] = [];
  readonly storage: FakeSidecarStorage;
  readonly journal: ClaimJournal;
  readonly admission: FakeAdmission;
  readonly upstream: FakeUpstream;
  readonly engine: SidecarEngine;
  requestId = "req-engine-1";
  requestDigest = "";

  constructor(options: SidecarHarnessOptions = {}) {
    this.storage = new FakeSidecarStorage(
      this.timeline,
      options.records,
      options.terminals,
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
      binding: TEST_BINDING,
      assembler: new RequestAssembler(),
      journal: this.journal,
      admission: this.admission,
      upstream: this.upstream,
      emit: frame => {
        this.frames.push(frame);
        this.timeline.push(`emit:${frame.kind}`);
      },
    });
  }

  async sendRequest(body: Record<string, unknown>): Promise<void> {
    await this.engine.start();
    const bytes = new TextEncoder().encode(JSON.stringify(body));
    this.requestDigest = await sha256Hex(bytes);
    const open: ReliableFrame<HostFrame> = {
      kind: "request_open",
      requestId: this.requestId,
      requestDigest: this.requestDigest,
      decodedBytes: bytes.byteLength,
      encodedBytes: bytes.byteLength,
      encoding: "identity",
      partCount: 1,
      epoch: 1,
      sequence: 0,
    };
    const data: ReliableFrame<HostFrame> = {
      kind: "request_data",
      requestId: this.requestId,
      partIndex: 0,
      offset: 0,
      dataBase64Url: base64UrlEncode(bytes),
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

  async settle(): Promise<void> {
    for (let index = 0; index < 100; index++) {
      if (this.engine.snapshot().active === 0) return;
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
