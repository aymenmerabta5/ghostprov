import { gzipSync } from "node:zlib";

interface TransformRequest {
  id: number;
  operation: "gzip";
  data: Uint8Array;
}

self.onmessage = (event: MessageEvent<TransformRequest>) => {
  const request = event.data;
  try {
    const compressed = new Uint8Array(gzipSync(request.data, { level: 1 }));
    self.postMessage({ id: request.id, data: compressed }, { transfer: [compressed.buffer] });
  } catch (error) {
    self.postMessage({ id: request.id, error: error instanceof Error ? error.message : String(error) });
  }
};
