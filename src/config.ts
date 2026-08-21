import "dotenv/config";

function num(v: string | undefined, d: number): number {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : d;
}

export const config = {
  port: num(process.env.PORT, 8787),
  docId: (process.env.PRISM_DOC_ID ?? "2a153042-dc25-4ca5-b290-260eed5e8705").trim(),
  prismOrigin: "https://prism.openai.com",
  gatewayBase: "/s/sandboxes/proxy",
  /** Yjs browser->sandbox sync lag varies (2-15s); scale with payload size */
  syncTimeoutMs: num(process.env.GP_SYNC_TIMEOUT_MS, 30_000),
  /**
   * Fail only when the stream is STALLED (no bytes flowing) for this long.
   * Healthy streams run indefinitely regardless of total duration — huge
   * reasoning/refactors are accepted whatever time they take.
   */
  stallTimeoutMs: num(process.env.GP_STALL_TIMEOUT_MS, 300_000),
  /** zip poll interval ceiling while waiting (adaptive: 150ms when active) */
  pollIntervalMs: num(process.env.GP_POLL_INTERVAL_MS, 400),
  /** emit SSE comment keepalive every N ms during silent stretches */
  keepAliveMs: num(process.env.GP_KEEPALIVE_MS, 15_000),
  /** max concurrent /v1/responses handled end-to-end */
  concurrency: num(process.env.PQUEUE_CONCURRENCY, 5),
  /** warn threshold for inline data URLs routed through Yjs */
  inlinePayloadWarnBytes: num(process.env.GP_INLINE_WARN_BYTES, 512 * 1024),
} as const;

export type Config = typeof config;
