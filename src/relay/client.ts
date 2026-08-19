import type { ResponsesBody } from "./types";

/**
 * relayCreate(body: unknown): Promise<unknown>
 * Forwards raw JSON to https://proxy:18080/v1/responses via sandbox runner.py
 * (preserve tools, tool_choice, previous_response_id, prompt_cache_key, etc.)
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
  // prompt_cache_breakpoint, reasoning, instructions, text, stream, input (including input_image data URLs) etc.

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

// Re-export type for consumers
export type { ResponsesBody } from "./types";
