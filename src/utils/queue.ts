/**
 * Concurrency + Codex Integration (Task 8)
 *
 * p-queue, max 5 concurrent, queue render
 *
 * Global Constraints:
 * - One render at a time on the Deno backend — concurrent POST /render share the active render
 * - PQUEUE_CONCURRENCY=5 for subagents (concurrent POST /v1/responses)
 * - GET /zip and GET /logs are concurrent-safe
 * - prompt_cache_key + prompt_cache_breakpoint {mode:"explicit"} + prompt_cache_options {mode:"explicit", ttl:"30m"} forwarded verbatim
 *   cache writes are 1.25×, reads 0.1×; cached_tokens / cache_write_tokens in usage.input_tokens_details
 */

import PQueue from "p-queue";

// PQUEUE_CONCURRENCY=5 — max concurrent POST /v1/responses (subagents)
// Env override allowed for tuning: PQUEUE_CONCURRENCY env var, default 5 per plan
export const PQUEUE_CONCURRENCY: number = (() => {
  const v = Number(process.env.PQUEUE_CONCURRENCY);
  return Number.isFinite(v) && v > 0 ? v : 5;
})();

// Main queue for concurrent subagent requests — max 5 concurrent
// Interfaces: concurrent POST /v1/responses with different prompt_cache_key
export const queue = new PQueue({ concurrency: PQUEUE_CONCURRENCY });

// Render queue is single-threaded — concurrent POST /render share the active render
// Global Constraints: "One render at a time on the Deno backend — concurrent POST /render share the active render; GET /zip and GET /logs are concurrent-safe"
export const renderQueue = new PQueue({ concurrency: 1 });

/**
 * Queue a render call — ensures only one render runs at a time.
 * Concurrent callers are queued with p-queue (render is single-threaded).
 * Use for POST /s/sandboxes/proxy/render → poll /render-status.
 */
export async function queueRender<T>(fn: () => Promise<T>): Promise<T> {
  return renderQueue.add(fn) as Promise<T>;
}

/** Typed enqueue on the main request queue (p-queue's void-union typing is awkward). */
export function enqueue<T>(fn: () => Promise<T>): Promise<T> {
  return queue.add(fn) as unknown as Promise<T>;
}

// Re-export PQueue for testability and for consumers that need custom queues
export { PQueue };
