import "dotenv/config";

function num(v: string | undefined, d: number): number {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : d;
}

function nonnegative(v: string | undefined, d: number): number {
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 ? n : d;
}

function integer(v: string | undefined, d: number): number {
  const n = Number(v);
  return Number.isInteger(n) && n > 0 ? n : d;
}

export const CONFIG_DEFAULTS = {
  port: 8787,
  slots: 8,
  zipBatchMs: 10,
  wsActiveResponses: 16,
  wsNamedStreams: 32,
} as const;

const slots = integer(process.env.GP_SLOTS, CONFIG_DEFAULTS.slots);
const requestedConcurrency = process.env.PQUEUE_CONCURRENCY
  ? integer(process.env.PQUEUE_CONCURRENCY, slots)
  : slots;
const responseConcurrency = Math.min(slots, requestedConcurrency);

export const config = {
  port: num(process.env.PORT, CONFIG_DEFAULTS.port),
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
  /** max concurrent /v1/responses held end-to-end across HTTP and WebSocket */
  concurrency: responseConcurrency,
  responseConcurrency,
  /** slot pool size for the daemon pipeline (must match sandbox GP_SLOTS) */
  slots,
  zipBatchMs: nonnegative(process.env.GP_ZIP_BATCH_MS, CONFIG_DEFAULTS.zipBatchMs),
  wsActiveResponses: integer(process.env.GP_WS_ACTIVE_RESPONSES, CONFIG_DEFAULTS.wsActiveResponses),
  wsNamedStreams: integer(process.env.GP_WS_NAMED_STREAMS, CONFIG_DEFAULTS.wsNamedStreams),
  /** warn threshold for inline data URLs routed through Yjs */
  inlinePayloadWarnBytes: num(process.env.GP_INLINE_WARN_BYTES, 512 * 1024),
} as const;

export type Config = typeof config;
