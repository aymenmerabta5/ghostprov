import { gzipSync } from "node:zlib";

interface TransformRequest {
  id: number;
  operation: "gzip";
  data: Uint8Array;
}

self.onmessage = (event: MessageEvent<TransformRequest>) => {
  const request = event.data;
  try {
    if (request.operation !== "gzip") throw new Error(`Unsupported CPU transform: ${request.operation}`);
    const result = gzipSync(request.data, { level: 1 });
    if (!(result.buffer instanceof ArrayBuffer)) {
      throw new Error("CPU transform output is not transferable");
    }
    const compressed = result.byteOffset === 0 && result.byteLength === result.buffer.byteLength
      ? result
      : Uint8Array.from(result);
    // Never transfer a larger zlib/Buffer slab: the receiver gets only the
    // compressed bytes and the transfer does not retain unrelated allocation.
    self.postMessage({ id: request.id, data: compressed }, { transfer: [compressed.buffer] });
  } catch (error) {
    self.postMessage({ id: request.id, error: error instanceof Error ? error.message : String(error) });
  }
};
