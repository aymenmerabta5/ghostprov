# GhostProv Mirror

OpenAI-compatible mirror that proxies `POST /v1/responses` (tools, `tool_choice`, `previous_response_id`, `stream`, `prompt_cache_key`/`prompt_cache_breakpoint`, `input_image`) and `WSS /v1/responses` (WebSocket mode) to the Prism sandbox relay `https://proxy:18080/v1` via the proven Yjs+render+zip loop.

Bun + Hono server on `:8787` exposes `POST /v1/responses` and `WSS /v1/responses`. Each request writes `job.json` via Yjs `wss://prism.openai.com/y/d/<doc>/ws`, triggers `POST /s/sandboxes/proxy/render` (single, queued), runs `.latexmkrc` → `runner.py` → `ask.py` → `proxy:18080`, writes `response.json`, then `GET /s/sandboxes/proxy/zip` returns it. Concurrent callers are queued with `p-queue` (render is single-threaded); fast path via `POST /write-file?filename=__relay__` is optional.

## Quick Start

```bash
bun install
cp .env.example .env
# Edit .env if needed — PRISM_DOC_ID is prefilled, PORT=8787
PORT=8787 bun --watch src/server.ts
# or
bun run dev
```

Copy .env.example to .env, leave PRISM_SANDBOX_TOKEN empty for auto-mint via Playwright (requires you to be logged in to prism.openai.com in the persistent context on first run).

Health check:

```bash
curl http://localhost:8787/health
# {"ok":true}
```

Proxy a response:

```bash
curl -X POST http://localhost:8787/v1/responses \
  -H "content-type: application/json" \
  -d '{"model":"gpt-5.6-sol","input":"Reply with exactly: PING","store":false}'
```

Streaming:

```bash
curl -X POST http://localhost:8787/v1/responses \
  -H "content-type: application/json" \
  -d '{"model":"gpt-5.6-sol","input":"Hi","stream":true,"store":false}' \
  -H "Accept: text/event-stream"
# text/event-stream with response.output_item.added, response.function_call_arguments.delta, response.function_call_arguments.done
```

WebSocket:

```js
import { WebSocket } from "ws";
const ws = new WebSocket("ws://localhost:8787/v1/responses", { headers: { Authorization: "Bearer test" }});
ws.on("open", () => ws.send(JSON.stringify({ type:"response.create", model:"gpt-5.6-sol", store:false, input:"Find fizz_buzz()" })));
ws.on("message", (data) => console.log(data.toString())); // contains "response.create"
```

File upload / download:

```bash
# upload
curl -X POST http://localhost:8787/v1/files -F file=@1x1.png -F purpose=vision
# get content via zip
curl http://localhost:8787/v1/files/<file_id>/content --output file.png
```

## Codex Integration

The mirror is Codex-compatible. Point Codex at the local mirror:

```bash
OPENAI_BASE_URL=http://localhost:8787/v1 codex --model gpt-5.6-sol
```

Or with environment:

```bash
export OPENAI_BASE_URL=http://localhost:8787/v1
codex --model gpt-5.6-sol "Use get_horoscope for Aquarius"
```

This works because `POST /v1/responses` is OpenAI-compatible and forwards `tools`, `tool_choice`, `previous_response_id`, `stream`, `prompt_cache_key`, `prompt_cache_options`, `prompt_cache_breakpoint`, and `input_image` (data URLs) verbatim to `https://proxy:18080/v1` via the sandbox.

## Concurrency

Concurrent subagents are queued with `p-queue`.

- `PQUEUE_CONCURRENCY=5` — max concurrent `POST /v1/responses` (subagents) via `src/utils/queue.ts`.
- `queue` is a `PQueue({ concurrency: PQUEUE_CONCURRENCY })` that wraps each `POST /v1/responses` handler:
  ```ts
  import { queue } from "./src/utils/queue";
  app.post("/v1/responses", async (c) => {
    const body = await c.req.json();
    const res = await queue.add(() => relayCreate(body));
    return c.json(res);
  });
  ```
- Override via env: `PQUEUE_CONCURRENCY=10 PORT=8787 bun src/server.ts`
- Render is single-threaded on the Deno backend — concurrent `POST /render` share the active render; `GET /zip` and `GET /logs` are concurrent-safe. To enforce this, `src/utils/queue.ts` exports a second queue:
  ```ts
  export const renderQueue = new PQueue({ concurrency: 1 });
  export async function queueRender<T>(fn: () => Promise<T>) {
    return renderQueue.add(fn) as Promise<T>;
  }
  ```
  `src/proxy/render.ts:triggerRender` is wrapped with `queueRender`, so concurrent callers are queued and only one render runs at a time:
  ```ts
  import { queueRender } from "../utils/queue";
  export async function triggerRender() {
    return queueRender(async () => {
      // POST /s/sandboxes/proxy/render?prism_cache_bust=... → poll /render-status
    });
  }
  ```

Test 3 concurrent `POST /v1/responses` with different `prompt_cache_key` → all 200:

```js
const bodies = [1,2,3].map(i => ({
  model:"gpt-5.6-sol",
  input:`hello ${i}`,
  prompt_cache_key:`key-${i}`,
  prompt_cache_options:{mode:"explicit", ttl:"30m"},
  prompt_cache_breakpoint:{mode:"explicit"},
  store:false
}));
const resps = await Promise.all(bodies.map(b => fetch("http://localhost:8787/v1/responses", { method:"POST", headers:{"content-type":"application/json"}, body: JSON.stringify(b) })));
// all 200
```

## Prompt Caching

GPT-5.6 explicit prompt caching:

- `prompt_cache_key` + `prompt_cache_breakpoint: {mode:"explicit"}` + `prompt_cache_options: {mode:"explicit", ttl:"30m"}` are forwarded verbatim (Global Constraints — never strip).
- 15 rpm limit: `prompt_cache_key` is rate-limited to 15 requests per minute per key on the relay. Excess writes should back off or rotate keys.
- Pricing: cache writes are 1.25×, reads are 0.1×.
- `cached_tokens` handling: `usage.input_tokens_details.cached_tokens` and `usage.input_tokens_details.cache_write_tokens` are returned verbatim from the relay and forwarded to the client. Example:

```json
{
  "usage": {
    "input_tokens": 100,
    "output_tokens": 20,
    "total_tokens": 120,
    "input_tokens_details": {
      "cached_tokens": 80,
      "cache_write_tokens": 50
    }
  }
}
```

Verify:

```bash
curl -X POST http://localhost:8787/v1/responses \
  -H "content-type: application/json" \
  -d '{"model":"gpt-5.6-sol","input":"hello","prompt_cache_key":"my-key-42","prompt_cache_options":{"mode":"explicit","ttl":"30m"},"prompt_cache_breakpoint":{"mode":"explicit"},"store":false}' | jq .usage.input_tokens_details
# {"cached_tokens":0,"cache_write_tokens":50}
```

## Files & Images

- `POST /v1/files` → `POST /api/project-files/upload?prism_cache_bust=...` with `x-crixet-sandbox-token` (via `src/proxy/files.ts`, `FormData` with `file` + `purpose`, preserves filename).
- `GET /v1/files/:id/content` → `GET /s/sandboxes/proxy/zip?prism_cache_bust=...` (via `fetchZip` from `src/utils/zip.ts`) with `prism_cache_bust` mandatory on all `/s/sandboxes/proxy/*` and `/api/project-files/*` calls.
- `input_image` data URLs (`data:image/png;base64,...` including 1x1 `iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=`) are preserved verbatim in `input`.
- `tool_choice: allowed_tools` (`{type:"allowed_tools", allowed_tools:[{type:"function", name:"get_horoscope"}]}`) is forwarded verbatim alongside `tools`.

## Environment & Bridge Auto-mint

- `PRISM_DOC_ID=2a153042-dc25-4ca5-b290-260eed5e8705` (prefilled in `.env.example` / `.env`)
- `PRISM_SANDBOX_TOKEN=` — leave empty to auto-mint via Playwright (requires you to be logged in to prism.openai.com in the persistent context on first run), or set manually.
- `PORT=8787`
- `src/server.ts` loads `.env` via `import "dotenv/config"` (also `src/bridge/playwright.ts`, `src/proxy/*`, `src/utils/zip.ts`).
- When env token is empty, `src/proxy/*` and `src/utils/zip.ts` fall back to bridge token chain: `bridge memory -> globalThis.__BRIDGE_TOKENS__ -> D:/MyWork/AI/ghostprov/.pw/tokens.json -> env -> mock`.
- The bridge (`src/bridge/playwright.ts`) is correctly reading/writing `D:/MyWork/AI/ghostprov/.pw/tokens.json`:
  - launches chromium persistent context with `userDataDir D:/MyWork/AI/ghostprov/.pw`
  - `goto https://prism.openai.com/?u=2a153042-dc25-4ca5-b290-260eed5e8705&pg=1&m=main.tex`
  - `waitForResponse` heartbeat (`heartbeat` 200) via `waitForHeartbeat`
  - captures `x-crixet-sandbox-token` via `page.on('request')` header, saves to `.pw/tokens.json` and also updates `.env` if needed, and exposes via `GET /__bridge/token` (source `bridge` | `global` | `file` | `env`).

Run bridge manually:

```bash
bun src/bridge/playwright.ts              # headful, preserves login in .pw
bun src/bridge/playwright.ts --headless   # headless
curl http://localhost:8787/__bridge/token # {"ok":true,"sandboxToken":"...","source":"bridge"}
```

## Architecture

```
ghostprov/
  .env.example                  # PRISM_DOC_ID, PRISM_SANDBOX_TOKEN= (empty for auto-mint), PORT=8787
  .env                          # copy from .env.example, fill PRISM_DOC_ID, leave token empty for auto
  .pw/tokens.json               # {sandboxToken, ySweetToken, updatedAt} written by bridge, read by proxy
  package.json
  src/server.ts                 # Hono app, POST /v1/responses, WSS /v1/responses, GET /health (loads .env via dotenv)
  src/bridge/playwright.ts      # Playwright persistent bridge: launch chromium, goto prism URL, wait heartbeat, capture token, save to .pw/tokens.json + .env, GET /__bridge/token
  src/proxy/yjs.ts              # Yjs WSS client: connect, write job.json (falls back to bridge token when env empty)
  src/proxy/render.ts           # POST /render → poll /render-status (queued via queueRender, falls back to bridge token)
  src/proxy/files.ts            # POST /api/project-files/upload, GET /zip (falls back to bridge token)
  src/relay/client.ts           # relayCreate(body): Yjs write → render → zip → response.json
  src/relay/types.ts            # Responses API types
  src/utils/b64.ts              # base64 helpers
  src/utils/zip.ts              # unzip GET /zip, extract response.json (falls back to bridge token)
  src/utils/queue.ts            # p-queue, max 5 concurrent, queue render (PQUEUE_CONCURRENCY=5)
```

Constraints:

- Node >= 20 or Bun >= 1.2
- `OPENAI_API_KEY` synthetic, only valid at `https://proxy:18080/v1` with `--cacert $PROXY_CA_CERT_PATH`
- Prism project `2a153042-dc25-4ca5-b290-260eed5e8705` and `x-crixet-sandbox-token` required; `prism_cache_bust` mandatory
- One render at a time; `GET /zip` concurrent-safe

## Testing

```bash
bun x vitest run --reporter=verbose
# tests/server.test.ts        health + POST proxies
# tests/relay.test.ts         tool passthrough
# tests/proxy.test.ts         Yjs write + render + zip
# tests/websocket.test.ts     WSS incremental
# tests/files.test.ts         Files + prompt caching
# tests/concurrency.test.ts   Concurrency + Codex integration (PQUEUE_CONCURRENCY=5, cached_tokens)
```
