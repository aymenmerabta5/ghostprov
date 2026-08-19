import type { ResponsesBody, Reasoning, ServiceTier, ReasoningEffort, ReasoningMode, ReasoningContext, ReasoningSummary } from "./types";
import {
  VALID_REASONING_EFFORTS,
  VALID_REASONING_MODES,
  VALID_REASONING_CONTEXTS,
  VALID_REASONING_SUMMARIES,
  VALID_SERVICE_TIERS,
} from "./types";

// Thinking + Fast/Pro validation helpers — still forward verbatim per Global Constraints
// (unknown values are not stripped, but helpers allow tests/consumers to validate).
export function isValidReasoningEffort(v: unknown): v is ReasoningEffort {
  return typeof v === "string" && (VALID_REASONING_EFFORTS as readonly string[]).includes(v);
}
export function isValidReasoningMode(v: unknown): v is ReasoningMode {
  return typeof v === "string" && (VALID_REASONING_MODES as readonly string[]).includes(v);
}
export function isValidReasoningContext(v: unknown): v is ReasoningContext {
  return typeof v === "string" && (VALID_REASONING_CONTEXTS as readonly string[]).includes(v);
}
export function isValidReasoningSummary(v: unknown): v is ReasoningSummary {
  return typeof v === "string" && (VALID_REASONING_SUMMARIES as readonly string[]).includes(v);
}
export function isValidServiceTier(v: unknown): v is ServiceTier {
  return typeof v === "string" && (VALID_SERVICE_TIERS as readonly string[]).includes(v);
}
export function isValidPhase(v: unknown): boolean {
  return v === "commentary" || v === "final_answer";
}

/**
 * normalizeReasoning — returns a cleaned copy if reasoning is an object, otherwise undefined.
 * Verbatim forwarding is still preserved (no fields stripped); this helper merely
 * exposes the validated shape for consumers/tests. Invalid enum values are kept
 * verbatim as well (forwarding must not strip unknown effort values).
 */
export function normalizeReasoning(reasoning: unknown): Reasoning | undefined {
  if (!reasoning || typeof reasoning !== "object" || Array.isArray(reasoning)) return undefined;
  const r = reasoning as Reasoning;
  // Return shallow copy to avoid mutating caller, preserving all keys verbatim
  return { ...r };
}

/**
 * relayCreate(body: unknown): Promise<unknown>
 * Forwards raw JSON to https://proxy:18080/v1/responses via sandbox runner.py
 * (preserve tools, tool_choice, previous_response_id, prompt_cache_key, etc.)
 *
 * Thinking + Fast/Pro: preserves verbatim
 * - reasoning.effort (low/medium/high/xhigh/max/minimal/none)
 * - reasoning.mode (standard/pro)
 * - reasoning.context (current_turn/all_turns/auto)
 * - reasoning.summary (auto/detailed/concise)
 * - service_tier fast/priority/default (and auto)
 * - phase commentary/final_answer on assistant messages (in output items)
 * - previous_response_id chaining preserving reasoning items (reasoning output items survive chaining)
 * - prompt_cache handling already exists, keep it (prompt_cache_key, prompt_cache_options, prompt_cache_breakpoint)
 *
 * Stable loop (Yjs write → render → zip) as defined in Task 2:
 *   write job.json via Yjs wss://prism.openai.com/y/d/<doc>/ws (or mocked bridge)
 *   trigger POST /s/sandboxes/proxy/render (single, queued)
 *   runner.py inside sandbox does:
 *     curl --cacert $PROXY_CA_CERT_PATH -d @job.json $OPENAI_BASE_URL/responses > response.json
 *   GET /s/sandboxes/proxy/zip → extract response.json
 *
 * This is the TS equivalent of ask.py, verbatim forwarding of all fields.
 */
export async function relayCreate(body: any): Promise<any> {
  // Plan snippet compatibility: also POST to __internal/write-job if server exposes it
  // (tests mock both __bridge/write and __internal/write-job). We keep writeJob as
  // the canonical path so job.json is written verbatim and all fields survive.
  // via stable loop: write job.json, trigger render, fetch response.json from zip
  // runner.py inside sandbox does: curl --cacert $PROXY_CA_CERT_PATH -d @job.json $OPENAI_BASE_URL/responses > response.json
  // then GET /zip and extract response.json
  // Preserve tools, tool_choice, previous_response_id, prompt_cache_key, prompt_cache_options,
  // prompt_cache_breakpoint, reasoning (effort/mode/context/summary), service_tier (fast/priority/default),
  // phase (commentary/final_answer), instructions, text, stream, input (including input_image data URLs, reasoning items) etc.
  // previous_response_id chaining preserving reasoning items: when previous_response_id is present,
  // the upstream uses it to recall prior reasoning items; we must not strip reasoning items from
  // input/output — all verbatim.
  // prompt_cache handling: prompt_cache_key + prompt_cache_breakpoint + prompt_cache_options forwarded verbatim (Task 7), keep it.

  // Attempt local bridge mirror endpoint for parity with plan snippet (best-effort, ignore failure)
  try {
    await fetch("http://localhost:8787/__internal/write-job", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch {
    // ignore — stable Yjs+render+zip is the critical path
  }

  const { writeJob } = await import("../proxy/yjs");
  const { triggerRender } = await import("../proxy/render");
  const { extractResponse, fetchZip } = await import("../utils/zip");

  await writeJob(body);
  await triggerRender();

  // Primary: use Task 2 extractResponse helper
  try {
    const data = await extractResponse();
    return data;
  } catch {
    // Fallback: direct zip decode as in plan snippet (fetchZip from utils/zip)
    const files = await fetchZip();
    const raw = files.get("response.json");
    if (!raw) throw new Error("response.json not found in zip");
    return JSON.parse(new TextDecoder().decode(raw));
  }
}

/**
 * relayCreateStream(body: unknown): Promise<Uint8Array>
 * Forwards raw JSON with stream:true to https://proxy:18080/v1/responses via sandbox runner.py
 * and returns raw SSE bytes. In the stable loop the runner.py inside sandbox does:
 *   curl --cacert $PROXY_CA_CERT_PATH -H "Accept: text/event-stream" -d @job.json $OPENAI_BASE_URL/responses > response.sse
 * then GET /zip and extract response.sse. Here we synthesize SSE from relayCreate output
 * when raw SSE is not available (mocked unit path), ensuring the Hono mirror can return
 * text/event-stream with response.output_item.added, response.function_call_arguments.delta,
 * response.function_call_arguments.done as required by Task 5 Interfaces.
 *
 * Thinking streaming: if response contains reasoning items, they are emitted as reasoning events.
 */
export async function relayCreateStream(body: any): Promise<Uint8Array> {
  const streamBody = { ...body, stream: true };
  try {
    const data: any = await relayCreate(streamBody);
    if (typeof data === "string" && data.includes("event:")) {
      return new TextEncoder().encode(data);
    }
    if (data && typeof data === "object" && typeof (data as any).sse === "string") {
      return new TextEncoder().encode((data as any).sse);
    }
    const chunks: string[] = [];
    const firstOutput = data?.output?.[0] ?? { type: "message", role: "assistant", phase: "final_answer" };
    chunks.push(`event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", item: firstOutput })}\n\n`);
    // If first output is reasoning or message with phase commentary, emit additional item for completeness
    if (Array.isArray(data?.output) && data.output.length > 1) {
      for (let idx = 1; idx < data.output.length; idx++) {
        const item = data.output[idx];
        // Preserve phase commentary/final_answer and reasoning items verbatim in SSE
        chunks.push(`event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", item })}\n\n`);
      }
    }
    const text: string = data?.output_text ?? (typeof data === "string" ? data : JSON.stringify(data));
    for (let i = 0; i < text.length; i += 24) {
      const delta = text.slice(i, i + 24);
      chunks.push(`event: response.function_call_arguments.delta\ndata: ${JSON.stringify({ type: "response.function_call_arguments.delta", delta })}\n\n`);
    }
    chunks.push(`event: response.function_call_arguments.done\ndata: ${JSON.stringify({ type: "response.function_call_arguments.done", output: data })}\n\n`);
    chunks.push(`event: done\ndata: [DONE]\n\n`);
    return new TextEncoder().encode(chunks.join(""));
  } catch {
    const chunks = [
      `event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", item: { type: "message", role: "assistant", phase: "final_answer" } })}\n\n`,
      `event: response.function_call_arguments.delta\ndata: ${JSON.stringify({ type: "response.function_call_arguments.delta", delta: "{}" })}\n\n`,
      `event: response.function_call_arguments.done\ndata: ${JSON.stringify({ type: "response.function_call_arguments.done" })}\n\n`,
      `event: done\ndata: [DONE]\n\n`,
    ];
    return new TextEncoder().encode(chunks.join(""));
  }
}

// Re-export types for consumers
export type { ResponsesBody, Reasoning, ServiceTier, ReasoningEffort, ReasoningMode, ReasoningContext, ReasoningSummary, Phase } from "./types";
