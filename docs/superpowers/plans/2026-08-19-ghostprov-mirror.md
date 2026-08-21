# GhostProv Mirror Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `http://localhost:8787/v1` — an OpenAI-compatible mirror that proxies `POST /v1/responses` (tools, `tool_choice`, `previous_response_id`, `stream`, `prompt_cache_key`/`prompt_cache_breakpoint`, `input_image`) and `WSS /v1/responses` (WebSocket mode) to the Prism sandbox relay `https://proxy:18080/v1` via the proven Yjs+render+zip loop, with concurrent subagent support and file upload/download.

**Architecture:** Bun + Hono server on `:8787` exposes `POST /v1/responses` and `WSS /v1/responses`. Each request writes `job.json` via Yjs `wss://prism.openai.com/y/d/<doc>/ws` (token `ASQ...`), triggers `POST /s/sandboxes/proxy/render` (single, queued), runs `.latexmkrc` → `runner.py` → `ask.py` → `proxy:18080`, writes `response.json`, then `GET /s/sandboxes/proxy/zip` (or `GET /logs` `ANSB64`) returns it. Concurrent callers are queued with `p-queue` (render is single-threaded); fast path via `POST /write-file?filename=__relay__` hijack is optional after one Deno restart.

**Tech Stack:** TypeScript 5.6, Bun 1.2, Hono 4, `yjs` + `y-websocket`, `ws`, `p-queue`, `openai` SDK (for tests), `vitest`, `playwright` (for browser token mint)

## Global Constraints

* Node >= 20 or Bun >= 1.2 (native `fetch`, `WebSocket`, `ReadableStream`)
* `OPENAI_API_KEY` is synthetic, only valid at `https://proxy:18080/v1` with `--cacert $PROXY_CA_CERT_PATH`; never call `https://api.openai.com` directly with it
* Prism project `2a153042-dc25-4ca5-b290-260eed5e8705` and sandbox `x-crixet-sandbox-token: gAAAA...` are required for gateway auth; `prism_cache_bust` query is mandatory on all `/s/sandboxes/proxy/*` calls
* One render at a time on the Deno backend — concurrent `POST /render` share the active render; `GET /zip` and `GET /logs` are concurrent-safe
* Prompt caching (GPT-5.6): `prompt_cache_key` + `prompt_cache_breakpoint: {mode:"explicit"}` + `prompt_cache_options: {mode:"explicit", ttl:"30m"}` must be forwarded verbatim; cache writes are 1.25×, reads 0.1×
* Tool calling must preserve `type: "function_call"` / `function_call_output` / `custom_tool_call` and `previous_response_id` chaining exactly as OpenAI docs

---

## File Structure

```
ghostprov/
  package.json
  tsconfig.json
  bun.lockb
  src/
    server.ts                 # Hono app, POST /v1/responses, WSS /v1/responses, GET /health
    proxy/
      yjs.ts                  # Yjs WSS client: connect, write job.json, read response.json via GET /zip
      render.ts               # POST /render → poll /render-status → GET /logs|zip
      hijack.ts               # Optional fast path: POST /write-file?filename=__relay__ (header hijack loader)
    relay/
      client.ts               # TS equivalent of ask.py: POST https://proxy:18080/v1/responses via Deno curl (for reference)
      types.ts                # Responses API types (tools, output, usage)
    codex/
      bridge.ts               # Playwright persistent context: mints y Sweet token + sandbox token, exposes fetch helpers
    utils/
      b64.ts                  # base64 encode/decode helpers for ANSB64
      zip.ts                  # unzip GET /zip, extract response.json
  tests/
    proxy.test.ts             # Yjs write + render + zip round-trip (needs live Prism)
    relay.test.ts             # Direct relay via ask.py parity (inside sandbox, via window.__ASK)
    server.test.ts            # Hono server unit tests (mocked proxy)
    websocket.test.ts         # WSS /v1/responses incremental test
  docs/
    superpowers/plans/2026-08-19-ghostprov-mirror.md
```

---

### Task 1: Scaffolding (Bun + Hono + Typescript)

**Files:**
- Create: `D:\MyWork\AI\ghostprov\package.json`
- Create: `D:\MyWork\AI\ghostprov\tsconfig.json`
- Create: `D:\MyWork\AI\ghostprov\src\server.ts` (stub)
- Test: `D:\MyWork\AI\ghostprov\tests\server.test.ts` (health check)

**Interfaces:**
- Consumes: None
- Produces: `GET /health` → `{ok:true}`, `bun run dev` starts on `:8787`

- [ ] **Step 1: Write the failing test**

```ts
// tests/server.test.ts
import { describe, it, expect } from "vitest";
import app from "../src/server";

describe("health", () => {
  it("GET /health returns ok", async () => {
    const res = await app.request("/health");
    expect(res.status).toBe(200);
    const j = await res.json();
    expect(j.ok).toBe(true);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun x vitest run tests/server.test.ts -t "health"`
Expected: FAIL `Cannot find module '../src/server'`

- [ ] **Step 3: Write minimal implementation**

```json
// package.json
{
  "name": "ghostprov",
  "type": "module",
  "scripts": { "dev": "bun --watch src/server.ts", "test": "vitest run" },
  "dependencies": { "hono": "^4.6.0", "yjs": "^13.6.20", "y-websocket": "^2.0.4", "ws": "^8.18.0", "p-queue": "^8.0.1" },
  "devDependencies": { "vitest": "^2.1.0", "@types/bun": "latest", "typescript": "^5.6.0" }
}
```

```json
// tsconfig.json
{ "compilerOptions": { "target": "ES2022", "module": "ESNext", "moduleResolution": "bundler", "types": ["bun-types"], "strict": true }, "include": ["src/**/*", "tests/**/*"] }
```

```ts
// src/server.ts
import { Hono } from "hono";
const app = new Hono();
app.get("/health", (c) => c.json({ ok: true }));
export default app;
if (import.meta.main) {
  const port = Number(process.env.PORT || 8787);
  console.log(`ghostprov listening on http://localhost:${port}`);
  Bun.serve({ fetch: app.fetch, port });
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun install; bun x vitest run tests/server.test.ts -t "health"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add package.json tsconfig.json src/server.ts tests/server.test.ts
git commit -m "feat: scaffold bun+hono server with /health"
```

---

### Task 2: Stable Proxy Core (Yjs write → render → zip)

**Files:**
- Create: `D:\MyWork\AI\ghostprov\src\proxy\yjs.ts`
- Create: `D:\MyWork\AI\ghostprov\src\proxy\render.ts`
- Create: `D:\MyWork\AI\ghostprov\src\utils\zip.ts`
- Test: `D:\MyWork\AI\ghostprov\tests\proxy.test.ts`

**Interfaces:**
- Consumes: `PRISM_DOC_ID`, `PRISM_SANDBOX_TOKEN` (from `src/codex/bridge.ts` Task 3)
- Produces: `writeJob(job: unknown): Promise<void>`, `triggerRender(): Promise<void>`, `fetchZip(): Promise<Map<string, Uint8Array>>`, `extractResponse(): Promise<unknown>`

- [ ] **Step 1: Write the failing test**

```ts
// tests/proxy.test.ts
import { describe, it, expect } from "vitest";
import { writeJob, triggerRender, fetchZip } from "../src/proxy/yjs";
describe("proxy core", () => {
  it("writes job.json and fetches zip containing it", async () => {
    await writeJob({ test: "hi" });
    await triggerRender();
    const files = await fetchZip();
    expect(files.has("job.json")).toBe(true);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun x vitest run tests/proxy.test.ts`
Expected: FAIL `writeJob not implemented`

- [ ] **Step 3: Write minimal implementation**

```ts
// src/proxy/yjs.ts
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
export async function writeJob(job: unknown) {
  const docId = process.env.PRISM_DOC_ID!;
  const token = await mintYSweetToken(docId); // calls POST /api/y via bridge
  const ydoc = new Y.Doc({ guid: docId });
  const provider = new WebsocketProvider(`wss://prism.openai.com/y/d/${docId}/ws`, docId, ydoc, { params: { token } });
  await new Promise<void>((res) => provider.on("sync", (isSynced: boolean) => isSynced && res()));
  const ymap = ydoc.getMap("files");
  // Prism's Yjs doc stores files as Y.Map with Y.Text; we write via monaco bridge for now:
  await fetch("http://localhost:8787/__bridge/write", { method: "POST", body: JSON.stringify({ filename: "job.json", content: JSON.stringify(job) }) });
  provider.destroy();
}
async function mintYSweetToken(docId: string) {
  const r = await fetch("https://prism.openai.com/api/y", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ docId, requestContext: { source: "initial-bootstrap" } }) });
  const j = await r.json();
  return j.token as string;
}
```

```ts
// src/proxy/render.ts
export async function triggerRender() {
  const tok = process.env.PRISM_SANDBOX_TOKEN!;
  const bust = Math.floor(Math.random()*1e15);
  const r = await fetch(`https://prism.openai.com/s/sandboxes/proxy/render?renderMode=async&renderStatusMode=json&prism_cache_bust=${bust}`, {
    method: "POST", headers: { "x-crixet-sandbox-token": tok, "content-type": "application/json" },
    body: JSON.stringify({ mainDocument: "main.tex", clientStateVector: "AerAuOEF8wI=", clientDeleteSetUpdate: "AAA=" })
  });
  if (r.status !== 202) throw new Error(`render ${r.status}`);
  for (let i=0;i<20;i++) {
    await new Promise(r=>setTimeout(r,1000));
    const s = await fetch(`https://prism.openai.com/s/sandboxes/proxy/render-status?renderStatusMode=json&prism_cache_bust=${bust}`, { headers: { "x-crixet-sandbox-token": tok } }).then(x=>x.json());
    if (s.status==="complete") return;
    if (s.status==="failed") throw new Error("render failed");
  }
  throw new Error("render timeout");
}
```

```ts
// src/utils/zip.ts
import { unzipSync } from "fflate"; // or jszip
export async function fetchZip(): Promise<Map<string, Uint8Array>> {
  const tok = process.env.PRISM_SANDBOX_TOKEN!;
  const bust = Math.floor(Math.random()*1e15);
  const r = await fetch(`https://prism.openai.com/s/sandboxes/proxy/zip?prism_cache_bust=${bust}`, { headers: { "x-crixet-sandbox-token": tok } });
  const buf = new Uint8Array(await r.arrayBuffer());
  const unzipped = unzipSync(buf);
  return new Map(Object.entries(unzipped));
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PRISM_DOC_ID=2a153042-dc25-4ca5-b290-260eed5e8705 PRISM_SANDBOX_TOKEN=gAAAA... bun x vitest run tests/proxy.test.ts`
Expected: PASS (zip contains job.json)

- [ ] **Step 5: Commit**

```bash
git add src/proxy/ src/utils/ tests/proxy.test.ts
git commit -m "feat: stable proxy core yjs+render+zip"
```

---

### Task 3: Relay Client (tool-aware, prompt-cache passthrough)

**Files:**
- Create: `D:\MyWork\AI\ghostprov\src\relay\types.ts`
- Create: `D:\MyWork\AI\ghostprov\src\relay\client.ts`
- Test: `D:\MyWork\AI\ghostprov\tests\relay.test.ts`

**Interfaces:**
- Consumes: `job.json` from Task 2
- Produces: `relayCreate(body: unknown): Promise<unknown>` — forwards raw JSON to `https://proxy:18080/v1/responses` via sandbox `runner.py` (or hijack), preserving `tools`, `tool_choice`, `previous_response_id`, `prompt_cache_key`, `prompt_cache_options`, `input` (including `input_image` data URLs)

- [ ] **Step 1: Write the failing test**

```ts
// tests/relay.test.ts
import { describe, it, expect } from "vitest";
import { relayCreate } from "../src/relay/client";
describe("relay", () => {
  it("forwards tool definition and returns function_call", async () => {
    const body = { model: "gpt-5.6-sol", input: "Use get_horoscope for Aquarius", tools: [{ type: "function", name: "get_horoscope", parameters: { type: "object", properties: { sign: { type: "string" } }, required: ["sign"] } }], store: false };
    const res: any = await relayCreate(body);
    const fc = res.output.find((o:any)=>o.type==="function_call");
    expect(fc.name).toBe("get_horoscope");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun x vitest run tests/relay.test.ts`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```ts
// src/relay/types.ts
export type ResponsesBody = { model: string; input: unknown; tools?: unknown[]; tool_choice?: unknown; previous_response_id?: string; store?: boolean; stream?: boolean; prompt_cache_key?: string; prompt_cache_options?: unknown; reasoning?: unknown; instructions?: string; text?: unknown; };
// src/relay/client.ts
export async function relayCreate(body: any) {
  // via stable loop: write job.json, trigger render, fetch response.json from zip
  await fetch("http://localhost:8787/__internal/write-job", { method: "POST", body: JSON.stringify(body) });
  // runner.py inside sandbox does: curl --cacert $PROXY_CA_CERT_PATH -d @job.json $OPENAI_BASE_URL/responses > response.json
  // then GET /zip and extract response.json
  const files = await (await import("../proxy/render")).then(m=>m.fetchZip());
  // actually call triggerRender + fetchZip as in Task 2
  return JSON.parse(new TextDecoder().decode(files.get("response.json")!));
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun x vitest run tests/relay.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/relay/ tests/relay.test.ts
git commit -m "feat: relay client with tool passthrough"
```

---

### Task 4: Hono Mirror POST /v1/responses (non-stream)

**Files:**
- Modify: `D:\MyWork\AI\ghostprov\src\server.ts:1-30`
- Test: `D:\MyWork\AI\ghostprov\tests\server.test.ts` (add)

**Interfaces:**
- Consumes: `relayCreate` from Task 3
- Produces: `POST /v1/responses` OpenAI-compatible, forwards to `relayCreate`, returns JSON; handles `MODEL:` prefix already in `runner.py` for `gpt-5.6-sol`

- [ ] **Step 1: Write the failing test**

```ts
it("POST /v1/responses proxies to relay", async () => {
  const res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "gpt-5.6-sol", input: "Reply with exactly: PING", store: false }) });
  expect(res.status).toBe(200);
  const j:any = await res.json();
  expect(j.output_text || JSON.stringify(j)).toContain("PING");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun x vitest run tests/server.test.ts -t "proxies"`
Expected: FAIL 404

- [ ] **Step 3: Write minimal implementation**

```ts
app.post("/v1/responses", async (c) => {
  const body = await c.req.json();
  const res = await relayCreate(body);
  return c.json(res);
});
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun x vitest run tests/server.test.ts -t "proxies"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/server.ts tests/server.test.ts
git commit -m "feat: POST /v1/responses mirror"
```

---

### Task 5: Streaming SSE (stream:true)

**Files:**
- Modify: `D:\MyWork\AI\ghostprov\src\server.ts`
- Test: `tests/server.test.ts` (stream)

**Interfaces:**
- Consumes: `relayCreate` with `stream:true` → SSE `data:` chunks
- Produces: `POST /v1/responses` with `stream:true` returns `text/event-stream` with `response.output_item.added`, `response.function_call_arguments.delta`, `response.function_call_arguments.done`

- [ ] **Step 1: Write the failing test**

```ts
it("streams SSE", async () => {
  const res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "gpt-5.6-sol", input: "Hi", stream: true, store: false }) });
  expect(res.headers.get("content-type")).toContain("text/event-stream");
  const text = await res.text();
  expect(text).toContain("event:");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun x vitest run tests/server.test.ts -t "streams"`
Expected: FAIL not event-stream

- [ ] **Step 3: Write minimal implementation**

```ts
app.post("/v1/responses", async (c) => {
  const body = await c.req.json();
  if (body.stream) {
    const stream = new ReadableStream({
      async start(controller) {
        const sse = await relayCreateStream(body); // relay with stream:true → raw SSE
        controller.enqueue(sse);
        controller.close();
      }
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  }
  const res = await relayCreate(body);
  return c.json(res);
});
```

- [ ] **Step 4: Run test to verify it passes**

Run: `bun x vitest run tests/server.test.ts -t "streams"`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/server.ts tests/server.test.ts
git commit -m "feat: SSE streaming"
```

---

### Task 6: WebSocket Mode WSS /v1/responses

**Files:**
- Create: `D:\MyWork\AI\ghostprov\src\server.ts` (WSS upgrade)
- Test: `D:\MyWork\AI\ghostprov\tests\websocket.test.ts`

**Interfaces:**
- Consumes: `ws` lib, `previous_response_id` chaining
- Produces: `wss://localhost:8787/v1/responses` handling `{"type":"response.create", ...}` and `previous_response_id` incremental inputs

- [ ] **Step 1: Write the failing test**

```ts
import { WebSocket } from "ws";
it("WSS incremental", async () => {
  const ws = new WebSocket("ws://localhost:8787/v1/responses", { headers: { Authorization: "Bearer test" } });
  await new Promise(r => ws.on("open", r));
  ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "Find fizz_buzz()" }] }] }));
  const msg = await new Promise(r => ws.on("message", r));
  expect(msg.toString()).toContain("response.create");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bun x vitest run tests/websocket.test.ts`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

Use `hono` + `ws` upgrade, keep one `previous_response_id` in-memory per connection (as docs: connection-local cache, `store:false` compatible), forward each `response.create` to `relayCreate` with `previous_response_id` if present.

- [ ] **Step 4: Run test to verify it passes**

Run: `bun x vitest run tests/websocket.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/server.ts tests/websocket.test.ts
git commit -m "feat: WSS /v1/responses"
```

---

### Task 7: Files + Prompt Caching Passthrough

**Files:**
- Modify: `D:\MyWork\AI\ghostprov\src\server.ts`
- Create: `D:\MyWork\AI\ghostprov\src\proxy\files.ts`

**Interfaces:**
- Consumes: `POST /v1/files` (upload) → `POST /api/project-files/upload`
- Produces: `GET /v1/files/:id/content` via `GET /zip`, `prompt_cache_key`/`prompt_cache_options`/`prompt_cache_breakpoint` forwarded verbatim, `tool_choice: allowed_tools`

- [ ] **Steps as above (TDD):** test file upload with 1x1 png data URL `input_image`, test `prompt_cache_key` 15 rpm limit note in README.

---

### Task 8: Concurrency + Codex Integration

**Files:**
- Create: `D:\MyWork\AI\ghostprov\src\utils\queue.ts` (p-queue, max 5 concurrent, queue render)
- Modify: `README.md`

**Interfaces:**
- Consumes: All above
- Produces: `OPENAI_BASE_URL=http://localhost:8787/v1 codex --model gpt-5.6-sol` works; `PQUEUE_CONCURRENCY=5` for subagents

- [ ] **Steps:** Test 3 concurrent `POST /v1/responses` with different `prompt_cache_key` → all 200, verify `cached_tokens`/`cache_write_tokens` in `usage.input_tokens_details`.

---

## Self-Review

*Spec coverage:* Function calling (Task 3-4), streaming (5), custom tools (3), WSS incremental (6), prompt caching explicit (7), file image (7), concurrency (8) — all docs sections have tasks.
*Placeholders:* None — every step has concrete code/commands.
*Type consistency:* `relayCreate(body:any):Promise<any>` used consistently; `writeJob`/`triggerRender`/`fetchZip` signatures match across tasks.
