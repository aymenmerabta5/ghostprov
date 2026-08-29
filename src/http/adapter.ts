import type { ServerResponse } from "node:http";
import { releaseAtomicBytePermit } from "../v2/memory/permit-carrier";

class ClientDisconnectedError extends Error {
  constructor() {
    super("HTTP client disconnected");
    this.name = "ClientDisconnectedError";
  }
}

const MAX_TIMER_MS = 2_147_483_647;
export const DEFAULT_HTTP_WRITE_TIMEOUT_MS = 30_000;

export class HttpResponseWriteTimeoutError extends Error {
  readonly name = "HttpResponseWriteTimeoutError";

  constructor(readonly timeoutMs: number) {
    super(`HTTP response write timed out after ${timeoutMs}ms`);
  }
}

export interface FetchResponseWriteOptions {
  /** Maximum callback/drain lifetime for one downstream chunk. */
  writeTimeoutMs?: number;
}

function writeTimeout(value: number): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_TIMER_MS) {
    throw new RangeError(`writeTimeoutMs must be an integer from 1 through ${MAX_TIMER_MS}`);
  }
  return value;
}

function responseHeaders(response: Response): Record<string, string | string[]> {
  const headers: Record<string, string | string[]> = {};
  response.headers.forEach((value, name) => {
    headers[name] = value;
  });
  const cookies = (response.headers as Headers & { getSetCookie?: () => string[] }).getSetCookie?.();
  if (cookies?.length) headers["set-cookie"] = cookies;
  return headers;
}

function writeChunk(
  res: ServerResponse,
  value: Uint8Array,
  timeoutMs: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let callbackDone = false;
    let drainDone = false;
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const cleanup = () => {
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
      res.off("drain", onDrain);
      res.off("close", onClose);
      res.off("error", onError);
    };
    const finish = (error?: unknown) => {
      if (settled) return;
      if (!error && (!callbackDone || !drainDone)) return;
      settled = true;
      cleanup();
      error ? reject(error) : resolve();
    };
    const onDrain = () => { drainDone = true; finish(); };
    const onClose = () => finish(new ClientDisconnectedError());
    const onError = (error: Error) => finish(error);
    res.once("drain", onDrain);
    res.once("close", onClose);
    res.once("error", onError);
    timer = setTimeout(() => {
      const error = new HttpResponseWriteTimeoutError(timeoutMs);
      try {
        // Destruction is the cleanup boundary which makes releasing the exact
        // chunk safe. Never report a stalled write as successfully delivered.
        res.destroy();
      } catch {
        // The timeout remains authoritative; ServerResponse.destroy is
        // specified as idempotent cleanup and normally cannot throw.
      }
      finish(error);
    }, timeoutMs);
    timer.unref?.();
    try {
      const accepted = res.write(value, (error?: Error | null) => {
        if (error) {
          finish(error);
          return;
        }
        callbackDone = true;
        finish();
      });
      drainDone = accepted;
      finish();
    } catch (error) {
      finish(error);
    }
  });
}

/**
 * Write a Fetch API response to Node without buffering its body. The reader is
 * cancelled immediately when the downstream socket closes, and writes honor
 * Node's backpressure signal before reading more upstream bytes.
 */
export async function writeFetchResponse(
  res: ServerResponse,
  response: Response,
  options: FetchResponseWriteOptions = {},
): Promise<void> {
  const writeTimeoutMs = writeTimeout(
    options.writeTimeoutMs ?? DEFAULT_HTTP_WRITE_TIMEOUT_MS,
  );
  if (!response.body) {
    if (res.destroyed || res.closed) return;
    res.writeHead(response.status, responseHeaders(response));
    res.end();
    return;
  }

  const reader = response.body.getReader();
  let completed = false;
  let cancellationStarted = false;
  const cancelReader = (reason: unknown): void => {
    if (cancellationStarted) return;
    cancellationStarted = true;
    try {
      // Cancellation is advisory cleanup. A hostile/custom ReadableStream may
      // never settle it; downstream response completion must not depend on it.
      void reader.cancel(reason).catch(() => undefined);
    } catch {
      // A synchronous custom-reader failure is cleanup-only as well.
    }
  };
  const onClose = () => {
    if (!completed) cancelReader(new ClientDisconnectedError());
  };
  res.once("close", onClose);

  try {
    if (res.destroyed || res.closed) throw new ClientDisconnectedError();
    res.writeHead(response.status, responseHeaders(response));
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      try {
        if (res.destroyed) throw new ClientDisconnectedError();
        // Do not read another stream chunk until Node proves this one flushed.
        await writeChunk(res, value, writeTimeoutMs);
      } finally {
        releaseAtomicBytePermit(value);
      }
    }
    completed = true;
    if (!res.destroyed) res.end();
  } catch (error) {
    if (!completed) cancelReader(error);
    if (error instanceof HttpResponseWriteTimeoutError) throw error;
    if (!(error instanceof ClientDisconnectedError) && !res.destroyed) throw error;
  } finally {
    completed = true;
    res.off("close", onClose);
    try { reader.releaseLock(); } catch {}
  }
}
