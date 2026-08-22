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

export async function collectRequestBody(
  chunks: AsyncIterable<Uint8Array>,
  maxBytes: number,
): Promise<Buffer> {
  if (!(maxBytes > 0) && maxBytes !== Number.POSITIVE_INFINITY) {
    throw new RangeError("maxBytes must be positive or Infinity");
  }
  const buffers: Buffer[] = [];
  let total = 0;
  for await (const chunk of chunks) {
    const buffer = Buffer.isBuffer(chunk)
      ? chunk
      : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
    total += buffer.length;
    if (total > maxBytes) throw tooLarge(maxBytes);
    buffers.push(buffer);
  }
  return buffers.length === 0 ? Buffer.alloc(0) : Buffer.concat(buffers, total);
}
