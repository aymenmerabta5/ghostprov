import { ApiError } from "./errors";

function tooLarge(maxBytes: number): ApiError {
  return new ApiError(413, `Request body exceeds the configured ${maxBytes}-byte limit.`, {
    type: "invalid_request_error",
    param: "body",
    code: "request_too_large",
  });
}

export function contentLengthExceeds(
  value: string | string[] | undefined,
  maxBytes: number,
): boolean {
  if (!Number.isFinite(maxBytes)) return false;
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw === undefined || !/^\d+$/.test(raw.trim())) return false;
  return Number(raw) > maxBytes;
}

export function declaredContentLength(
  value: string | string[] | undefined,
): number | null {
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw === undefined || !/^\d+$/.test(raw.trim())) return null;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

export interface RequestBodyCollectionOptions {
  signal?: AbortSignal;
  /** Called before a received chunk becomes retained request memory. */
  beforeRetain?(totalBytes: number): void | Promise<void>;
}

function signalError(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("Request body collection aborted", "AbortError");
}

export async function collectRequestBody(
  chunks: AsyncIterable<Uint8Array>,
  maxBytes: number,
  options: RequestBodyCollectionOptions = {},
): Promise<Buffer> {
  if (!(maxBytes > 0) && maxBytes !== Number.POSITIVE_INFINITY) {
    throw new RangeError("maxBytes must be positive or Infinity");
  }
  if (options.signal?.aborted) throw signalError(options.signal);
  const buffers: Buffer[] = [];
  let total = 0;
  const iterator = chunks[Symbol.asyncIterator]();
  let abort!: (error: unknown) => void;
  const aborted = options.signal
    ? new Promise<never>((_resolve, reject) => { abort = reject; })
    : null;
  const onAbort = () => abort(signalError(options.signal!));
  let completed = false;
  options.signal?.addEventListener("abort", onAbort, { once: true });
  try {
    for (;;) {
      const next = aborted
        ? await Promise.race([iterator.next(), aborted])
        : await iterator.next();
      if (next.done) break;
      const chunk = next.value;
      const buffer = Buffer.isBuffer(chunk)
        ? chunk
        : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
      const nextTotal = total + buffer.length;
      if (nextTotal > maxBytes) throw tooLarge(maxBytes);
      await options.beforeRetain?.(nextTotal);
      if (options.signal?.aborted) throw signalError(options.signal);
      total = nextTotal;
      buffers.push(buffer);
    }
    completed = true;
  } finally {
    options.signal?.removeEventListener("abort", onAbort);
    if (!completed && iterator.return) {
      void Promise.resolve(iterator.return()).catch(() => undefined);
    }
  }
  return buffers.length === 0 ? Buffer.alloc(0) : Buffer.concat(buffers, total);
}
