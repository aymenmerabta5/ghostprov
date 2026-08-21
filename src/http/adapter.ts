import type { ServerResponse } from "node:http";

class ClientDisconnectedError extends Error {
  constructor() {
    super("HTTP client disconnected");
    this.name = "ClientDisconnectedError";
  }
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

function waitForDrain(res: ServerResponse): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      res.off("drain", onDrain);
      res.off("close", onClose);
      res.off("error", onError);
    };
    const onDrain = () => { cleanup(); resolve(); };
    const onClose = () => { cleanup(); reject(new ClientDisconnectedError()); };
    const onError = (error: Error) => { cleanup(); reject(error); };
    res.once("drain", onDrain);
    res.once("close", onClose);
    res.once("error", onError);
  });
}

/**
 * Write a Fetch API response to Node without buffering its body. The reader is
 * cancelled immediately when the downstream socket closes, and writes honor
 * Node's backpressure signal before reading more upstream bytes.
 */
export async function writeFetchResponse(res: ServerResponse, response: Response): Promise<void> {
  res.writeHead(response.status, responseHeaders(response));
  if (!response.body) {
    res.end();
    return;
  }

  const reader = response.body.getReader();
  let completed = false;
  const onClose = () => {
    if (!completed) void reader.cancel(new ClientDisconnectedError()).catch(() => undefined);
  };
  res.once("close", onClose);

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (res.destroyed) throw new ClientDisconnectedError();
      if (!res.write(Buffer.from(value))) await waitForDrain(res);
    }
    completed = true;
    if (!res.destroyed) res.end();
  } catch (error) {
    if (!(error instanceof ClientDisconnectedError) && !res.destroyed) throw error;
  } finally {
    completed = true;
    res.off("close", onClose);
    reader.releaseLock();
  }
}
