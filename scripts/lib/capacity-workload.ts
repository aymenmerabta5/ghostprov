import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import WebSocket, { type RawData } from "ws";
import { canonicalJson } from "../../src/v2/protocol/shared";

const LANES = 10, TURNS = 10, ROOT_BYTES = 1024 * 1024, TOOL_RESULT_BYTES = 8 * 1024;
const REQUEST_TIMEOUT_MS = 6 * 60_000, WORKLOAD_TIMEOUT_MS = 30 * 60_000;
const MAX_FRAME_BYTES = 2 * 1024 * 1024, MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
const MAX_TOTAL_RESPONSE_BYTES = LANES * TURNS * MAX_RESPONSE_BYTES, MAX_EVENTS = 32_768;
const MAX_OUTPUT_CHARS = 256 * 1024, QUIET_CHECKPOINT_MS = 15_000;
const SECTIONS = ["admission", "acknowledgements", "continuation", "recovery", "fairness"] as const;
type Section = typeof SECTIONS[number];
type ReasoningEffort = "high" | "xhigh";
type ReasoningSummary = "auto" | "concise" | "detailed";

export type WorkloadFailureCode = "http_error" | "network_error" | "invalid_frame" |
  "response_bound" | "event_bound" | "missing_terminal" | "duplicate_terminal" |
  "provider_failed" | "provider_incomplete" | "invalid_terminal" | "marker_mismatch" |
  "parent_mismatch" | "duplicate_response_id" | "request_deadline" | "workload_deadline" |
  "parent_abort" | "socket_closed" | "send_error" | "missing_tool_call" |
  "tool_call_mismatch" | "unexpected_tool_call" | "wire_proof_mismatch";

export interface WorkloadUsage {
  readonly inputTokens: number | null;
  readonly outputTokens: number | null;
  readonly totalTokens: number | null;
  readonly cachedInputTokens: number | null;
  readonly reasoningOutputTokens: number | null;
}
export interface WorkloadTurnMetric {
  readonly lane: number; readonly turn: number; readonly atMs: number;
  readonly requestBytes: number; readonly responseBytes: number; readonly eventCount: number;
  /** WebSocket open latency on the first turn; zero for an already-open socket. */
  readonly headersMs: number | null;
  readonly firstEventMs: number | null; readonly firstTextMs: number | null;
  readonly firstToolMs: number | null; readonly toolReadyMs: number | null;
  readonly totalMs: number; readonly maxEventGapMs: number;
  /** Observed upgrade status, not the upstream provider status. */
  readonly httpStatus: number | null;
  readonly providerStatus: number | null;
  readonly terminalStatus: "completed" | "failed" | "incomplete" | "missing";
  readonly failureCode?: WorkloadFailureCode;
  readonly previousIdPresent: boolean; readonly parentMatched: boolean;
  readonly providerParentMatched: boolean | null;
  readonly markerMatched: boolean; readonly markerExpected: boolean;
  readonly outputChars: number; readonly usage: WorkloadUsage;
  readonly transport: "websocket"; readonly turnKind: "tool_call" | "tool_result";
  readonly toolCallCount: number; readonly toolArgumentsMatched: boolean;
  readonly toolResultBytes: number; readonly socketCloseCode: number | null;
  readonly outOfTurnEvents: number;
  readonly canonicalRequestBytes: number;
  /** Fixture compression measurement; actual V2 wire measurements come from host instrumentation. */
  readonly fixtureGzipBytes: number; readonly fixtureGzipBase64Bytes: number;
  readonly canonicalRequestSha256: string;
}
export interface WorkloadProgress {
  readonly lane: number; readonly turn: number; readonly atMs: number;
  readonly phase: "started" | "first_event" | "first_text" | "quiet_checkpoint" | "settled";
  readonly checkpoint: boolean;
}
export interface WorkloadOptions {
  readonly baseUrl: string; readonly apiKey: string; readonly model?: string;
  readonly signal: AbortSignal;
  readonly reasoningEffort?: ReasoningEffort;
  readonly reasoningSummary?: ReasoningSummary;
  readonly rootBytes?: 262144 | 1048576;
  /** Internal evidence callbacks must never serialize provider IDs or input. */
  readonly onPrepared?: (digest: string, lane: number, turn: number) => void;
  readonly onTerminal?: (digest: string, providerId: string) => void;
  readonly onMetric?: (metric: WorkloadTurnMetric) => void;
  readonly onProgress?: (progress: WorkloadProgress) => void;
}
export interface WorkloadSummary {
  readonly expected: number; readonly started: number; readonly completed: number;
  readonly failed: number; readonly skipped: number; readonly stoppedOnFailure: boolean;
  readonly aborted: boolean; readonly callbackFailures: number; readonly durationMs: number;
  readonly metrics: readonly WorkloadTurnMetric[];
}

const INSTRUCTIONS = "You are analyzing a synthetic repository and its operational reference. Treat all code as reference data; do not execute it. On tool-call turns call report_fixture exactly once with the requested lane, cycle and section. On tool-result turns use the provided result and the earlier reference to write about 450 to 600 words of useful technical analysis, including concrete observations and uncertainties. Do not call another tool on a tool-result turn. End each text answer with exactly the project codename established in the root user message, a space, and the requested TURN suffix. Tool-call turns do not need this text marker. Think normally at the configured reasoning effort.";
const TOOLS = [{
  type: "function", name: "report_fixture", strict: true,
  description: "Return synthetic diagnostic records from the test harness. This tool performs no external action.",
  parameters: { type: "object", additionalProperties: false,
    properties: {
      lane: { type: "integer", minimum: 1, maximum: 10 },
      cycle: { type: "integer", minimum: 0, maximum: 4 },
      section: { type: "string", enum: [...SECTIONS] },
    }, required: ["lane", "cycle", "section"],
  },
}] as const;

function object(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : null;
}
function count(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= 1_000_000_000
    ? value : null;
}
function status(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 100 && value <= 599 ? value : null;
}
function usageOf(value: unknown): WorkloadUsage {
  const usage = object(value);
  return Object.freeze({ inputTokens: count(usage?.input_tokens), outputTokens: count(usage?.output_tokens),
    totalTokens: count(usage?.total_tokens), cachedInputTokens: count(object(usage?.input_tokens_details)?.cached_tokens),
    reasoningOutputTokens: count(object(usage?.output_tokens_details)?.reasoning_tokens) });
}

/** Deterministic varied code, numeric samples, and prose; no live files are read. */
function readableReference(bytes: number, seed: number): string {
  const roles = ["admission", "delivery", "provenance", "journal", "scheduler", "recovery", "metrics", "ownership"];
  const actions = ["reserve", "release", "commit", "inspect", "measure", "cancel", "reconcile", "validate"];
  const nouns = ["packet", "permit", "record", "window", "cursor", "lease", "queue", "receipt"];
  let state = seed >>> 0;
  const next = () => { state ^= state << 13; state ^= state >>> 17; state ^= state << 5; return state >>> 0; };
  const parts: string[] = [];
  let length = 0;
  for (let index = 0; length < bytes; index++) {
    const role = roles[next() % roles.length]!, action = actions[next() % actions.length]!;
    const noun = nouns[next() % nouns.length]!, identifier = `${role}_${noun}_${index.toString(36)}`;
    const samples = Array.from({ length: 24 }, () => String(next() % 1_000_003));
    const row = `\n// Reference module ${index}: ${role} ${action} ${noun}.\n` +
      `export const ${identifier} = { limit: ${next() % 2048 + 1}, generation: ${next() % 65536}, ` +
      `deadlineMs: ${next() % 180000 + 1000}, samples: [${samples.join(", ")}] };\n` +
      `// Review note ${next() % 1000000}: before ${action}, compare the exact ${noun} owner and record a bounded ${role} observation. ` +
      `The measured offset ${next() % 100000} and sequence ${next() % 100000} describe this fixture only; neither authorizes a retry.\n`;
    parts.push(row);
    length += row.length;
  }
  return parts.join("").slice(0, bytes);
}

type Prepared = {
  encoded: string; streamId: string; bytes: number; canonicalBytes: number;
  gzipBytes: number; gzipBase64Bytes: number; sha256: string;
  expectedMarker: string; kind: "tool_call" | "tool_result"; toolResultBytes: number;
  parentMatched: boolean;
  expectedLane: number; expectedCycle: number; expectedSection: Section;
};

function prepare(
  lane: number, turn: number, previousId: string | null, callId: string | null,
  model: string, effort: ReasoningEffort, summary: ReasoningSummary,
  rootBytes: number = ROOT_BYTES,
): Prepared {
  const cycle = Math.floor(turn / 2), section = SECTIONS[cycle]!;
  const kind = turn % 2 === 0 ? "tool_call" : "tool_result";
  const codename = `PROJECT-${String(lane + 1).padStart(2, "0")}-REFERENCE`;
  const input: Array<Record<string, unknown>> = [];
  let toolResultBytes = 0;
  if (kind === "tool_result") {
    if (!callId) throw new Error("Missing prior fixture tool identity");
    const prefix = `Synthetic report for lane ${lane + 1}, cycle ${cycle}, section ${section}.\n`;
    const output = prefix + readableReference(TOOL_RESULT_BYTES - prefix.length, 0x913ab723 ^ lane * 19 ^ turn * 131);
    toolResultBytes = Buffer.byteLength(output);
    input.push({ type: "function_call_output", call_id: callId, output });
    input.push({ type: "message", role: "user", content: [{ type: "input_text",
      text: `Analyze this ${section} result using the earlier reference. Identify useful evidence, risks, and the next discriminating measurement. Finish the text with the original project codename and TURN-${turn + 1}.` }] });
  } else {
    const text = `${turn === 0 ? `The project codename is ${codename}. Remember it for future turns. ` : ""}` +
      `Inspect the ${section} aspect of the reference. Call report_fixture once with lane ${lane + 1}, cycle ${cycle}, section ${section}. ` +
      `Wait for its result before giving a text answer.${turn === 0 ? "\nSynthetic repository and reference:\n" : ""}`;
    input.push({ type: "message", role: "user", content: [{ type: "input_text", text }] });
  }
  const body: Record<string, unknown> = {
    model, stream: true, store: true, service_tier: "fast", max_output_tokens: 8192,
    reasoning: { effort, summary }, instructions: INSTRUCTIONS, tools: TOOLS, parallel_tool_calls: false,
    tool_choice: kind === "tool_call" ? { type: "function", name: "report_fixture" } : "none",
    input, ...(previousId === null ? {} : { previous_response_id: previousId }),
  };
  const streamId = `realistic-${lane}-${turn}`;
  const event = { type: "response.create", stream_id: streamId, ...body };
  if (turn === 0) {
    const part = (input[0]!.content as Array<{ type: string; text: string }>)[0]!;
    const remaining = rootBytes - Buffer.byteLength(JSON.stringify(event));
    if (remaining < 1) throw new Error("Fixture root envelope is too large");
    // JSON escapes the newlines in readable code. Construct, then trim only the
    // ASCII reference suffix to reach an exact encoded request byte count.
    const reference = readableReference(remaining, 0x6d2b79f5 ^ (lane + 1) * 0x85ebca6b);
    const prefix = part.text;
    let low = 0, high = reference.length;
    while (low < high) {
      const length = Math.ceil((low + high) / 2);
      part.text = prefix + reference.slice(0, length);
      if (Buffer.byteLength(JSON.stringify(event)) <= rootBytes) low = length;
      else high = length - 1;
    }
    part.text = prefix + reference.slice(0, low);
    part.text += " ".repeat(rootBytes - Buffer.byteLength(JSON.stringify(event)));
  }
  const encoded = JSON.stringify(event), bytes = Buffer.byteLength(encoded);
  const canonical = canonicalJson(body), canonicalBytes = Buffer.byteLength(canonical);
  const compressed = gzipSync(canonical, { level: 6 });
  if ((turn === 0 && bytes !== rootBytes) || (turn > 0 && kind === "tool_call" && bytes > 4096) ||
    (kind === "tool_result" && (toolResultBytes !== TOOL_RESULT_BYTES || bytes > 12 * 1024))) {
    throw new Error("Fixture request byte invariant failed");
  }
  if (turn === 0 && compressed.byteLength < 16 * 1024) throw new Error("Fixture reference compressed unrealistically");
  return { encoded, streamId, bytes, canonicalBytes, gzipBytes: compressed.byteLength,
    gzipBase64Bytes: 4 * Math.ceil(compressed.byteLength / 3),
    sha256: createHash("sha256").update(canonical).digest("hex"),
    parentMatched: previousId === null ? !Object.hasOwn(body, "previous_response_id") : body.previous_response_id === previousId,
    expectedMarker: `${codename} TURN-${turn + 1}`, kind, toolResultBytes,
    expectedLane: lane + 1, expectedCycle: cycle, expectedSection: section };
}

/** Offline fixture inspection returns only byte counts and hashes; never opens a socket. */
export function inspectCapacityFixtures(rootBytes: 262144 | 1048576 = 1048576) {
  return Array.from({ length: LANES }, (_, lane) => Array.from({ length: TURNS }, (_, turn) => {
    const request = prepare(lane, turn, turn === 0 ? null : `resp_fixture_${lane}_${turn - 1}`,
      turn % 2 === 1 ? `call_fixture_${lane}_${turn - 1}` : null, "gpt-6-astra", "xhigh", "auto", rootBytes);
    return { lane, turn, kind: request.kind, requestBytes: request.bytes,
      canonicalRequestBytes: request.canonicalBytes, fixtureGzipBytes: request.gzipBytes,
      fixtureGzipBase64Bytes: request.gzipBase64Bytes, canonicalRequestSha256: request.sha256,
      toolResultBytes: request.toolResultBytes, parentMatched: request.parentMatched };
  })).flat();
}

function rawBytes(raw: RawData): number {
  return Array.isArray(raw) ? raw.reduce((total, part) => total + part.byteLength, 0) : raw.byteLength;
}
function rawText(raw: RawData): string {
  return Array.isArray(raw) ? Buffer.concat(raw).toString("utf8")
    : raw instanceof ArrayBuffer ? Buffer.from(raw).toString("utf8") : raw.toString("utf8");
}

/** At most one active turn per socket, ten sockets, one hundred requests, and no retries. */
export async function runWorkload(options: WorkloadOptions): Promise<WorkloadSummary> {
  const base = new URL(options.baseUrl), model = options.model ?? "gpt-6-astra";
  const effort = options.reasoningEffort ?? "xhigh", summary = options.reasoningSummary ?? "auto";
  const rootBytes = options.rootBytes ?? ROOT_BYTES;
  if (base.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(base.hostname) ||
    base.port !== "18888" || base.username || base.password || base.pathname !== "/" || base.search || base.hash ||
    !options.apiKey || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(model) ||
    model !== "gpt-6-astra" || effort !== "xhigh" || ![262144, 1048576].includes(rootBytes) ||
    !["auto", "concise", "detailed"].includes(summary)) {
    throw new Error("Invalid isolated realistic workload options");
  }
  const rootFixtures = Array.from({ length: LANES }, (_, lane) => prepare(lane, 0, null, null, model, effort, summary, rootBytes));
  const url = new URL("/v1/responses", base); url.protocol = "ws:";
  const begin = performance.now(), overall = new AbortController();
  const overallTimer = setTimeout(() => overall.abort(), WORKLOAD_TIMEOUT_MS);
  const metrics: WorkloadTurnMetric[] = [], responseIds = new Set<string>(), sockets: WebSocket[] = [];
  let started = 0, stopped = false, callbackFailures = 0, totalResponseBytes = 0;
  const notify = (lane: number, turn: number, phase: WorkloadProgress["phase"], checkpoint: boolean) => {
    try { options.onProgress?.({ lane, turn, atMs: performance.now() - begin, phase, checkpoint }); }
    catch { callbackFailures++; stopped = true; }
  };

  async function runLane(lane: number) {
    let socket: WebSocket | undefined, upgradeStatus: number | null = null;
    let previousId: string | null = null, previousCallId: string | null = null;
    try {
      for (let turn = 0; turn < TURNS; turn++) {
        if (stopped || options.signal.aborted || overall.signal.aborted) break;
        const request = turn === 0 ? rootFixtures[lane]! : prepare(lane, turn, previousId, previousCallId, model, effort, summary, rootBytes);
        const start = performance.now();
        started++;
        notify(lane, turn, "started", false);
        const result = await new Promise<{ responseId: string | null; callId: string | null; failed: boolean }>(resolve => {
          let done = false, sent = false, responseBytes = 0, eventCount = 0, outOfTurnEvents = 0;
          let headersMs: number | null = null, firstEventMs: number | null = null, firstTextMs: number | null = null;
          let firstToolMs: number | null = null, toolReadyMs: number | null = null, lastEventAt: number | null = null;
          let maxEventGapMs = 0, socketCloseCode: number | null = null, providerStatus: number | null = null;
          let terminalStatus: WorkloadTurnMetric["terminalStatus"] = "missing";
          let providerParentMatched: boolean | null = null, markerMatched = false, outputChars = 0;
          let toolCallCount = 0, toolArgumentsMatched = false, usage = usageOf(null);
          let quietBefore = false, quietAfter = false;
          const finish = (failureCode?: WorkloadFailureCode, responseId: string | null = null, callId: string | null = null) => {
            if (done) return;
            done = true;
            clearTimeout(timer); clearInterval(quiet);
            options.signal.removeEventListener("abort", parentAbort);
            overall.signal.removeEventListener("abort", overallAbort);
            socket?.off("message", message); socket?.off("error", socketError); socket?.off("close", closed);
            socket?.off("open", send);
            if (failureCode) stopped = true;
            const metric = Object.freeze<WorkloadTurnMetric>({
              lane, turn, atMs: start - begin, requestBytes: request.bytes, responseBytes, eventCount,
              headersMs, firstEventMs, firstTextMs, firstToolMs, toolReadyMs, totalMs: performance.now() - start,
              maxEventGapMs, httpStatus: upgradeStatus, providerStatus, terminalStatus,
              ...(failureCode ? { failureCode } : {}), previousIdPresent: previousId !== null,
              parentMatched: request.parentMatched,
              providerParentMatched, markerMatched, markerExpected: request.kind === "tool_result", outputChars, usage,
              transport: "websocket", turnKind: request.kind, toolCallCount, toolArgumentsMatched,
              toolResultBytes: request.toolResultBytes, socketCloseCode, outOfTurnEvents,
              canonicalRequestBytes: request.canonicalBytes, fixtureGzipBytes: request.gzipBytes,
              fixtureGzipBase64Bytes: request.gzipBase64Bytes, canonicalRequestSha256: request.sha256,
            });
            metrics.push(metric);
            try { options.onMetric?.(metric); } catch { callbackFailures++; stopped = true; }
            notify(lane, turn, "settled", failureCode !== undefined);
            resolve({ responseId, callId, failed: failureCode !== undefined });
          };
          const terminate = () => { try { socket?.terminate(); } catch {} };
          const parentAbort = () => { finish("parent_abort"); terminate(); };
          const overallAbort = () => { finish("workload_deadline"); terminate(); };
          const socketError = () => finish("network_error");
          const closed = (code: number) => {
            socketCloseCode = Number.isInteger(code) && code >= 1000 && code <= 4999 ? code : null;
            finish("socket_closed");
          };
          const send = () => {
            if (done || sent) return;
            if (socket?.readyState !== WebSocket.OPEN) { finish("socket_closed"); return; }
            sent = true;
            headersMs = turn === 0 ? performance.now() - start : 0;
            try { options.onPrepared?.(request.sha256, lane, turn); }
            catch { callbackFailures++; finish("wire_proof_mismatch"); return; }
            try { socket.send(request.encoded, error => { if (error) finish("send_error"); }); }
            catch { finish("send_error"); }
          };
          const message = (raw: RawData) => {
            if (done) return;
            const bytes = rawBytes(raw);
            responseBytes += bytes; totalResponseBytes += bytes;
            if (bytes > MAX_FRAME_BYTES || responseBytes > MAX_RESPONSE_BYTES || totalResponseBytes > MAX_TOTAL_RESPONSE_BYTES) {
              finish("response_bound"); terminate(); return;
            }
            if (++eventCount > MAX_EVENTS) { finish("event_bound"); terminate(); return; }
            let event: Record<string, unknown> | null;
            try { event = object(JSON.parse(rawText(raw))); } catch { finish("invalid_frame"); terminate(); return; }
            if (!event || typeof event.type !== "string") { finish("invalid_frame"); return; }
            if (event.stream_id !== request.streamId) {
              if (event.type === "error" && event.stream_id === undefined) {
                providerStatus = status(event.status) ?? status(object(event.error)?.status);
                finish("provider_failed"); return;
              }
              if (++outOfTurnEvents > 32) finish("invalid_frame");
              return;
            }
            const now = performance.now();
            if (lastEventAt !== null) maxEventGapMs = Math.max(maxEventGapMs, now - lastEventAt);
            lastEventAt = now;
            if (firstEventMs === null) { firstEventMs = now - start; notify(lane, turn, "first_event", false); }
            if ((event.type === "response.output_text.delta" || event.type === "response.refusal.delta") &&
              typeof event.delta === "string" && event.delta.length > 0 && firstTextMs === null) {
              firstTextMs = now - start; notify(lane, turn, "first_text", false);
            }
            if ((event.type === "response.function_call_arguments.delta" ||
              event.type === "response.output_item.added" && object(event.item)?.type === "function_call") && firstToolMs === null) {
              firstToolMs = now - start;
            }
            if (event.type === "response.function_call_arguments.done") toolReadyMs ??= now - start;
            if (event.type === "error" || event.type === "response.failed") {
              terminalStatus = "failed"; providerStatus = status(event.status) ?? status(object(event.error)?.status);
              finish("provider_failed"); return;
            }
            if (event.type === "response.incomplete") { terminalStatus = "incomplete"; finish("provider_incomplete"); return; }
            if (event.type !== "response.completed") return;
            terminalStatus = "completed";
            const final = object(event.response);
            if (final?.status !== "completed" || typeof final.id !== "string" || !final.id || final.id.length > 256 ||
              !Array.isArray(final.output) || final.output.length > 4096) { finish("invalid_terminal"); return; }
            if (Object.hasOwn(final, "previous_response_id")) {
              providerParentMatched = (final.previous_response_id ?? null) === previousId;
              if (!providerParentMatched) { finish("parent_mismatch"); return; }
            }
            usage = usageOf(final.usage);
            let tail = "", callId: string | null = null;
            for (const value of final.output) {
              const item = object(value);
              if (item?.type === "function_call") {
                toolCallCount++;
                if (item.name !== "report_fixture" || typeof item.call_id !== "string" || !item.call_id ||
                  item.call_id.length > 512 || typeof item.arguments !== "string" || item.arguments.length > 8192) {
                  finish("tool_call_mismatch"); return;
                }
                let args: Record<string, unknown> | null;
                try { args = object(JSON.parse(item.arguments)); } catch { finish("tool_call_mismatch"); return; }
                toolArgumentsMatched = args !== null && Object.keys(args).length === 3 &&
                  args.lane === request.expectedLane && args.cycle === request.expectedCycle && args.section === request.expectedSection;
                if (!toolArgumentsMatched) { finish("tool_call_mismatch"); return; }
                callId = item.call_id;
              }
              if (!Array.isArray(item?.content)) continue;
              if (item.content.length > 4096) { finish("response_bound"); return; }
              for (const value of item.content) {
                const part = object(value);
                if (part?.type !== "output_text" || typeof part.text !== "string") continue;
                outputChars += part.text.length;
                if (outputChars > MAX_OUTPUT_CHARS) { finish("response_bound"); return; }
                tail = (tail + part.text).slice(-256);
              }
            }
            markerMatched = tail.trimEnd().endsWith(request.expectedMarker);
            if (request.kind === "tool_call" && (toolCallCount !== 1 || callId === null)) { finish("missing_tool_call"); return; }
            if (request.kind === "tool_result" && toolCallCount !== 0) { finish("unexpected_tool_call"); return; }
            if (request.kind === "tool_result" && !markerMatched) { finish("marker_mismatch"); return; }
            if (responseIds.has(final.id)) { finish("duplicate_response_id"); return; }
            try { options.onTerminal?.(request.sha256, final.id); }
            catch { callbackFailures++; finish("wire_proof_mismatch"); return; }
            responseIds.add(final.id);
            finish(undefined, final.id, callId);
          };
          const timer = setTimeout(() => { finish("request_deadline"); terminate(); }, REQUEST_TIMEOUT_MS);
          const quiet = setInterval(() => {
            const reported = firstEventMs === null ? quietBefore : quietAfter;
            if (!reported && performance.now() - (lastEventAt ?? start) >= QUIET_CHECKPOINT_MS) {
              if (firstEventMs === null) quietBefore = true; else quietAfter = true;
              notify(lane, turn, "quiet_checkpoint", true);
            }
          }, 1000);
          options.signal.addEventListener("abort", parentAbort, { once: true });
          overall.signal.addEventListener("abort", overallAbort, { once: true });
          if (!socket) {
            socket = new WebSocket(url.toString(), { headers: { authorization: `Bearer ${options.apiKey}` },
              maxPayload: MAX_FRAME_BYTES, perMessageDeflate: false });
            sockets.push(socket);
            socket.on("error", () => {}); // Errors between turns must not become process-level exceptions.
            socket.on("upgrade", response => { upgradeStatus = status(response.statusCode); });
            socket.on("unexpected-response", (_request, response) => {
              upgradeStatus = status(response.statusCode); response.resume(); finish("http_error"); terminate();
            });
          }
          socket.on("message", message); socket.on("error", socketError); socket.on("close", closed);
          if (options.signal.aborted) parentAbort();
          else if (overall.signal.aborted) overallAbort();
          else if (socket.readyState === WebSocket.OPEN) send();
          else if (socket.readyState === WebSocket.CONNECTING) socket.once("open", send);
          else finish("socket_closed");
        });
        if (result.failed || result.responseId === null) break;
        previousId = result.responseId;
        previousCallId = result.callId;
      }
    } finally {
      if (socket && socket.readyState === WebSocket.OPEN) socket.close(1000);
      else if (socket && socket.readyState !== WebSocket.CLOSED) socket.terminate();
    }
  }
  try { await Promise.all(Array.from({ length: LANES }, (_, lane) => runLane(lane))); }
  finally {
    clearTimeout(overallTimer);
    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN) socket.close(1000);
      if (socket.readyState !== WebSocket.CLOSED) {
        const fence = setTimeout(() => { try { socket.terminate(); } catch {} }, 2_000);
        fence.unref();
      }
    }
  }
  const completed = metrics.filter(metric => metric.failureCode === undefined && metric.terminalStatus === "completed").length;
  return Object.freeze({ expected: LANES * TURNS, started, completed, failed: metrics.length - completed,
    skipped: LANES * TURNS - started, stoppedOnFailure: stopped,
    aborted: options.signal.aborted || overall.signal.aborted, callbackFailures,
    durationMs: performance.now() - begin, metrics: Object.freeze(metrics) });
}
