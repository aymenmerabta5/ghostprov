/**
 * Deep probe: duck-type exploration of the synced doc.
 */
import "dotenv/config";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";

const docId = (process.env.PRISM_DOC_ID ?? "").trim();
const token = process.env.GP_FRESH_TOKEN ?? "";

const doc = new Y.Doc();
const provider = new WebsocketProvider(
  `wss://prism.openai.com/y/d/${docId}/ws`,
  docId,
  doc,
  {
    connect: true,
    WebSocketPolyfill: class extends (globalThis as any).WebSocket {
      constructor(url: any, protocols?: any) {
        super(`${url}?token=${token}`, protocols);
      }
    },
  } as any,
);

await new Promise<boolean>((resolve) => {
  const t = setTimeout(() => resolve(false), 20000);
  provider.on("sync", (s: boolean) => { if (s) { clearTimeout(t); resolve(true); } });
});
console.log("synced ✓\n");

function explore(obj: any, path: string, depth: number): void {
  if (depth > 4) return;
  const ctor = obj?.constructor?.name ?? "undefined";

  // Duck-type checks
  const isMapLike = typeof obj?.get === "function" && typeof obj?.set === "function" && typeof obj?.keys === "function";
  const isArrayLike = typeof obj?.length === "number" && typeof obj?.get === "function" && typeof obj?.push === "function";
  const isTextLike = typeof obj?.toString === "function" && typeof obj?.insert === "function" && !isArrayLike;

  if (isMapLike && !isArrayLike) {
    console.log(`${path} → Map-like (${obj.size ?? "?"} entries)`);
    try {
      const keys = [...obj.keys()];
      console.log(`  keys: ${keys.slice(0, 20).map((k: any) => JSON.stringify(k)).join(", ")}`);
      for (const key of keys.slice(0, 10)) {
        const val = obj.get(key);
        explore(val, `${path}.${key}`, depth + 1);
      }
    } catch (e: any) { console.log(`  err: ${e.message}`); }
  } else if (isArrayLike) {
    console.log(`${path} → Array-like (len=${obj.length})`);
    for (let i = 0; i < Math.min(obj.length, 5); i++) {
      explore(obj.get(i), `${path}[${i}]`, depth + 1);
    }
  } else if (isTextLike) {
    const s = obj.toString();
    console.log(`${path} → Text-like (${s.length} chars) head=${JSON.stringify(s.slice(0, 80))}`);
  } else {
    console.log(`${path} → ${ctor} ${JSON.stringify(obj)?.slice(0, 80) ?? ""}`);
  }
}

console.log("=== shared types ===");
for (const [name, type] of doc.share.entries()) {
  console.log(`\n--- "${name}" ---`);
  console.log(`  constructor: ${type.constructor.name}`);
  console.log(`  _map size: ${(type as any)._map?.size ?? "n/a"}`);
  console.log(`  _length: ${(type as any)._length ?? "n/a"}`);

  // Try duck-typing
  const isMapLike = typeof type.get === "function" && typeof type.keys === "function";
  if (isMapLike) {
    try {
      const keys = [...type.keys()];
      console.log(`  keys(${keys.length}): ${keys.slice(0, 20).map((k: any) => JSON.stringify(k)).join(", ")}`);
      for (const key of keys.slice(0, 8)) {
        explore(type.get(key), `  ${name}["${key}"]`, 1);
      }
    } catch (e: any) { console.log(`  keys err: ${e.message}`); }
  }

  // Also try toJSON
  try {
    const json = type.toJSON();
    const s = JSON.stringify(json);
    console.log(`  toJSON: ${s.slice(0, 200)}${s.length > 200 ? "…" : ""}`);
  } catch (e: any) { console.log(`  toJSON err: ${e.message}`); }
}

provider.destroy();
process.exit(0);
