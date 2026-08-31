import { gzipSync } from "node:zlib";

interface TransformRequest {
  id: number;
  operation: "gzip";
  data: Uint8Array;
  priority: "latency" | "bulk";
}

self.onmessage = (event: MessageEvent<TransformRequest>) => {
  const request = event.data;
  try {
    if (request.operation !== "gzip") throw new Error(`Unsupported CPU transform: ${request.operation}`);
    if (request.priority !== "latency" && request.priority !== "bulk") {
      throw new Error("CPU transform priority is invalid");
    }
    // Bun 1.4's level-1 zlib path barely compresses high-entropy image data
    // after it has been expanded to base64. Large/bulk requests can spend a
    // few extra worker milliseconds to recover that expansion, reducing V2
    // frame count and ACK rounds without moving CPU work onto the event loop.
    const result = gzipSync(request.data, {
      level: request.priority === "bulk" ? 6 : 1,
    });
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
