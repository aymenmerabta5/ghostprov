import { releaseAtomicBytePermit } from "../v2/memory/permit-carrier";

export interface PermittedJsonResponseOptions {
  status?: number;
  headers?: HeadersInit;
  /** Objects carrying every physical allocation used by the translated value. */
  owners?: readonly unknown[];
}

/**
 * Serialize only while all source allocations remain charged. The second pull
 * proves the one JSON chunk has left the Fetch queue; GhostProv's Node adapter
 * requests it only after the prior socket write drains.
 */
export function permittedJsonResponse(
  value: unknown,
  options: PermittedJsonResponseOptions = {},
): Response {
  let retainedValue: unknown = value;
  let owners = [...(options.owners ?? [value])];
  let chunk: Uint8Array | null = null;
  let enqueued = false;
  let closed = false;
  const release = () => {
    for (const owner of owners) releaseAtomicBytePermit(owner);
    owners = [];
    retainedValue = null;
    chunk = null;
  };
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (closed) return;
      if (enqueued) {
        closed = true;
        release();
        controller.close();
        return;
      }
      try {
        chunk = new TextEncoder().encode(JSON.stringify(retainedValue));
        controller.enqueue(chunk);
        enqueued = true;
      } catch (error) {
        closed = true;
        release();
        controller.error(error);
      }
    },
    cancel() {
      if (closed) return;
      closed = true;
      release();
    },
  }, { highWaterMark: 0 });

  try {
    const headers = new Headers(options.headers);
    if (!headers.has("content-type")) {
      headers.set("content-type", "application/json; charset=UTF-8");
    }
    return new Response(stream, { status: options.status ?? 200, headers });
  } catch (error) {
    release();
    throw error;
  }
}
