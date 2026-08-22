import { sanitizeFinal, type SseFrame } from "../../relay/sse";
import {
  canonicalJson,
  sha256Hex,
  utf8Bytes,
  type SidecarFrame,
  type TerminalStatus,
} from "./shared";

export class V2TerminalProofError extends Error {
  readonly name = "V2TerminalProofError";

  constructor(message: string, readonly code: string) {
    super(message);
  }
}

export type ResponseTerminalProof = Extract<SidecarFrame, {
  kind: "response_terminal";
}>;

export interface TranscriptSnapshot {
  requestId: string;
  requestDigest: string;
  eventCount: number;
  byteCount: number;
  canonicalBytes: Uint8Array;
  final: Record<string, unknown> | null;
  finalEventIndex: number | null;
  terminalStatus: TerminalStatus | null;
}

const TERMINALS = new Map<string, TerminalStatus>([
  ["response.completed", "completed"],
  ["response.failed", "failed"],
  ["response.incomplete", "incomplete"],
]);

function object(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new V2TerminalProofError(`${label} must be an object`, "shape");
  }
  return value as Record<string, unknown>;
}

function concat(chunks: readonly Uint8Array[], total: number): Uint8Array {
  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

function responseIdentity(final: Record<string, unknown>): {
  id: string;
  status: TerminalStatus;
} {
  if (typeof final.id !== "string" || final.id.length === 0) {
    throw new V2TerminalProofError("Final response ID is missing", "response_id");
  }
  if (
    final.status !== "completed" &&
    final.status !== "failed" &&
    final.status !== "incomplete"
  ) {
    throw new V2TerminalProofError("Final response status is invalid", "terminal_status");
  }
  return { id: final.id, status: final.status };
}

export class TranscriptProof {
  readonly #requestId: string;
  readonly #requestDigest: string;
  readonly #maxBytes: number;
  readonly #chunks: Uint8Array[] = [];
  #eventCount = 0;
  #byteCount = 0;
  #final: Record<string, unknown> | null = null;
  #finalEventIndex: number | null = null;
  #terminalStatus: TerminalStatus | null = null;

  constructor(
    requestId: string,
    requestDigest: string,
    options: { maxBytes?: number } = {},
  ) {
    if (!requestId) throw new TypeError("requestId must not be empty");
    if (!/^[a-f0-9]{64}$/.test(requestDigest)) {
      throw new TypeError("requestDigest must be a lowercase SHA-256 digest");
    }
    const maxBytes = options.maxBytes ?? 64 * 1024 * 1024;
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
      throw new RangeError("maxBytes must be a positive safe integer");
    }
    this.#requestId = requestId;
    this.#requestDigest = requestDigest;
    this.#maxBytes = maxBytes;
  }

  append(frame: SseFrame): void {
    if (!frame.event || typeof frame.data !== "string") {
      throw new V2TerminalProofError("SSE frame is invalid", "frame");
    }
    const terminalStatus = TERMINALS.get(frame.event) ?? null;
    let terminalFinal: Record<string, unknown> | null = null;
    if (terminalStatus) {
      if (this.#final) {
        throw new V2TerminalProofError("Transcript contains a duplicate terminal event", "duplicate_terminal");
      }
      let decoded: unknown;
      try {
        decoded = JSON.parse(frame.data);
      } catch {
        throw new V2TerminalProofError("Terminal event data is invalid JSON", "terminal_json");
      }
      const event = object(decoded, "Terminal event");
      if (event.type !== frame.event) {
        throw new V2TerminalProofError("Terminal event type does not match its SSE name", "terminal_type");
      }
      terminalFinal = object(event.response, "Final response");
      const identity = responseIdentity(terminalFinal);
      if (identity.status !== terminalStatus) {
        throw new V2TerminalProofError("Final response status does not match terminal event", "terminal_status");
      }
    }

    const bytes = utf8Bytes(`event: ${frame.event}\ndata: ${frame.data}\n\n`);
    if (this.#byteCount + bytes.byteLength > this.#maxBytes) {
      throw new V2TerminalProofError("Transcript exceeds its byte limit", "byte_limit");
    }
    this.#chunks.push(bytes);
    if (terminalFinal && terminalStatus) {
      this.#final = terminalFinal;
      this.#finalEventIndex = this.#eventCount;
      this.#terminalStatus = terminalStatus;
    }
    this.#eventCount++;
    this.#byteCount += bytes.byteLength;
  }

  async createTerminal(
    expectedFinal?: Record<string, unknown>,
  ): Promise<ResponseTerminalProof> {
    const snapshot = this.snapshot();
    if (
      !snapshot.final ||
      snapshot.finalEventIndex === null ||
      snapshot.terminalStatus === null
    ) {
      throw new V2TerminalProofError("Transcript does not contain a terminal event", "missing_terminal");
    }
    if (
      expectedFinal &&
      canonicalJson(expectedFinal) !== canonicalJson(snapshot.final)
    ) {
      throw new V2TerminalProofError("Expected final object does not match terminal event", "final_object");
    }
    const identity = responseIdentity(snapshot.final);
    return {
      kind: "response_terminal",
      requestId: snapshot.requestId,
      requestDigest: snapshot.requestDigest,
      responseId: identity.id,
      eventCount: snapshot.eventCount,
      byteCount: snapshot.byteCount,
      transcriptSha256: await sha256Hex(snapshot.canonicalBytes),
      terminalStatus: snapshot.terminalStatus,
      finalEventIndex: snapshot.finalEventIndex,
      finalSha256: await sha256Hex(canonicalJson(snapshot.final)),
    };
  }

  snapshot(): TranscriptSnapshot {
    return {
      requestId: this.#requestId,
      requestDigest: this.#requestDigest,
      eventCount: this.#eventCount,
      byteCount: this.#byteCount,
      canonicalBytes: concat(this.#chunks, this.#byteCount),
      final: this.#final,
      finalEventIndex: this.#finalEventIndex,
      terminalStatus: this.#terminalStatus,
    };
  }
}

export async function validateTerminalProof(
  terminal: ResponseTerminalProof,
  snapshot: TranscriptSnapshot,
): Promise<Record<string, unknown>> {
  if (terminal.requestId !== snapshot.requestId) {
    throw new V2TerminalProofError("Terminal request ID does not match", "request_id");
  }
  if (terminal.requestDigest !== snapshot.requestDigest) {
    throw new V2TerminalProofError("Terminal request digest does not match", "request_digest");
  }
  if (terminal.eventCount !== snapshot.eventCount) {
    throw new V2TerminalProofError("Terminal event count does not match", "event_count");
  }
  if (terminal.byteCount !== snapshot.byteCount) {
    throw new V2TerminalProofError("Terminal byte count does not match", "byte_count");
  }
  if (!snapshot.final || snapshot.finalEventIndex === null || !snapshot.terminalStatus) {
    throw new V2TerminalProofError("Local transcript has no terminal event", "missing_terminal");
  }
  const identity = responseIdentity(snapshot.final);
  if (terminal.responseId !== identity.id) {
    throw new V2TerminalProofError("Terminal response ID does not match", "response_id");
  }
  if (
    terminal.terminalStatus !== identity.status ||
    terminal.terminalStatus !== snapshot.terminalStatus
  ) {
    throw new V2TerminalProofError("Terminal response status does not match", "terminal_status");
  }
  if (terminal.finalEventIndex !== snapshot.finalEventIndex) {
    throw new V2TerminalProofError("Terminal final event index does not match", "final_event_index");
  }
  const finalDigest = await sha256Hex(canonicalJson(snapshot.final));
  if (terminal.finalSha256 !== finalDigest) {
    throw new V2TerminalProofError("Terminal final digest does not match", "final_digest");
  }
  const transcriptDigest = await sha256Hex(snapshot.canonicalBytes);
  if (terminal.transcriptSha256 !== transcriptDigest) {
    throw new V2TerminalProofError("Terminal transcript digest does not match", "transcript_digest");
  }
  return sanitizeFinal(snapshot.final);
}
