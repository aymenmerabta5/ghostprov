import "dotenv/config";

function num(v: string | undefined, d: number): number {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : d;
}

export const config = {
  port: num(process.env.PORT, 8787),
  docId: (process.env.PRISM_DOC_ID ?? "69288e8b-c183-45f3-9131-d64b05b98928").trim(),
  prismOrigin: "https://prism.openai.com",
  gatewayBase: "/s/sandboxes/proxy",
  /** Yjs browser->sandbox sync lag varies (2-15s); scale with payload size */
  syncTimeoutMs: num(process.env.GP_SYNC_TIMEOUT_MS, 30_000),
  /**
   * Fail only when the stream is STALLED (no bytes flowing) for this long.
   * Healthy streams run indefinitely regardless of total duration — huge
   * reasoning/refactors are accepted whatever time they take.
   * NOTE: reasoning WITHOUT summary streaming can be silent for many minutes
   * (agentic max-effort turns); keep this generous.
   */
  stallTimeoutMs: num(process.env.GP_STALL_TIMEOUT_MS, 900_000),
  /**
   * Max wait for the daemon's FIRST ack frame after the job hit disk. The
   * daemon acks within ~seconds of dispatch, so exceeding this means the job
   * was never accepted (zombie worker holding the slot, or a boot-baseline
   * swallow). We attempt in-place recovery rewrites, then fail LOUDLY instead
   * of hanging the agent for stallTimeoutMs (15min) with zero feedback.
   */
  unlatchTimeoutMs: num(process.env.GP_UNLATCH_TIMEOUT_MS, 45_000),
  /** zip poll interval ceiling while waiting (adaptive: 150ms when active) */
  pollIntervalMs: num(process.env.GP_POLL_INTERVAL_MS, 400),
  /** emit SSE comment keepalive every N ms during silent stretches */
  keepAliveMs: num(process.env.GP_KEEPALIVE_MS, 15_000),
  /** max concurrent /v1/responses handled end-to-end */
  concurrency: num(process.env.PQUEUE_CONCURRENCY, 5),
  /** slot pool size for the daemon pipeline (must match sandbox GP_SLOTS) */
  slots: num(process.env.GP_SLOTS, 8),
  /** warn threshold for inline data URLs routed through Yjs */
  inlinePayloadWarnBytes: num(process.env.GP_INLINE_WARN_BYTES, 512 * 1024),
} as const;

export type Config = typeof config;
