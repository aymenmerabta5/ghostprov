# GhostProv

GhostProv is a local OpenAI Responses API mirror backed by an authenticated Prism sandbox. It supports non-streaming HTTP, true incremental SSE, Codex-compatible Responses WebSockets, file transfer, concurrent sandbox slots, and `store:false` continuation through connection-local response graphs.

The project is pinned to Bun 1.4.0, uses Bun's isolated native test runner, and typechecks with the native TypeScript 7.0.2 compiler.

## Quick start

```powershell
bun install
Copy-Item .env.example .env
bun run dev
```

The Playwright bridge opens the configured Prism project, keeps login state in a persistent profile, captures sandbox/Yjs credentials, and routes authenticated gateway calls through the live page.

```powershell
curl.exe http://127.0.0.1:8787/health
# {"ok":true}
```

Point Codex at the mirror:

```powershell
$env:OPENAI_BASE_URL = "http://127.0.0.1:8787/v1"
codex --model gpt-5.6-sol
```

## Responses HTTP

```powershell
curl.exe -N -X POST http://127.0.0.1:8787/v1/responses `
  -H "content-type: application/json" `
  -d '{"model":"gpt-5.6-sol","store":false,"stream":true,"input":"Say hello"}'
```

The Node adapter forwards each `ReadableStream` chunk with backpressure. It does not buffer the response before sending it to the client.

## Responses WebSocket mode

WebSocket mode follows the [official OpenAI Responses WebSocket contract](https://developers.openai.com/api/docs/guides/websocket-mode/):

- clients send direct `response.create` objects;
- `stream` is implicit and must be omitted;
- server events are direct event objects;
- named events echo `stream_id`; default-lane events omit it;
- requests sharing a lane execute FIFO and never overlap;
- different lanes execute concurrently;
- `previous_response_id` controls lineage independently of the lane;
- connection-local state supports `store:false` continuation and forks;
- `previous_session_id` is not a public Codex/OpenAI field.

```js
import { WebSocket } from "ws";

const ws = new WebSocket("ws://127.0.0.1:8787/v1/responses");
ws.on("message", data => {
  const event = JSON.parse(data.toString());
  console.log(event.type, event.stream_id, event);
});
ws.on("open", () => {
  ws.send(JSON.stringify({
    type: "response.create",
    stream_id: "codex-main",
    model: "gpt-5.6-sol",
    store: false,
    input: "Find fizz_buzz()",
  }));
});
```

For a `store:false` continuation, GhostProv resolves the client-supplied `previous_response_id` in the socket's bounded response graph, reconstructs stateless input for the internal HTTP relay, and retains encrypted reasoning internally. It never changes `store:false` to `store:true`.

## Concurrency architecture

One global FIFO scheduler owns all end-to-end relay capacity across HTTP and WebSocket callers. A lease remains active until `RunHandle.final` settles; creating a streaming handle does not release capacity early.

The slot daemon executes one upstream request per physical lane. Large requests use segmented transport:

- v6.3 is selected only when `gp_status.json` advertises `segmented-v6.3-embedded-commit`;
- parts `1..K-1` are published first and embedded-header part zero commits last;
- `job_sN.json` is not rewritten by v6.3;
- v6.2 remains the compatibility fallback.

Concurrent response/status reads are coalesced by `ZipPollCoordinator`: filenames requested in one short window share one `/zip` archive fetch instead of downloading the archive once per lane.

| Setting | Default | Purpose |
| --- | ---: | --- |
| `GP_SLOTS` | `8` | Global end-to-end relay capacity and requested daemon lanes |
| `PQUEUE_CONCURRENCY` | unset | Compatibility cap; when set, the smaller value with `GP_SLOTS` wins |
| `GP_ZIP_BATCH_MS` | `10` | Archive-read coalescing window |
| WebSocket active responses | `16` | Per-connection active response ceiling (`GP_WS_ACTIVE_RESPONSES`) |
| WebSocket named lanes | `32` | Per-connection distinct named-lane ceiling (`GP_WS_NAMED_STREAMS`) |
| `GP_WS_CACHE_NODES` | `256` | Cached response nodes per WebSocket |
| `GP_WS_CACHE_BYTES` | `67108864` | Cached response bytes per WebSocket |
| `GP_WS_CACHE_TOTAL_BYTES` | `536870912` | Process-wide WebSocket graph byte budget |

## Bridge configuration

| Variable | Meaning |
| --- | --- |
| `PRISM_DOC_ID` | Prism document/project identifier |
| `PRISM_PROJECT_URL` | Optional explicit Prism URL; otherwise derived from `PRISM_DOC_ID` |
| `PRISM_SANDBOX_TOKEN` | Optional initial sandbox credential; the bridge refreshes it |
| `PRISM_Y_TOKEN` | Optional initial Yjs credential |
| `GP_USER_DATA_DIR` | Persistent Chromium profile; defaults to `<repo>/.pw` |
| `GP_TOKEN_FILE` | Token cache; defaults to `<GP_USER_DATA_DIR>/tokens.json` |
| `GP_ENV_FILE` | Environment file updated by the bridge; defaults to `<repo>/.env` |
| `BRIDGE_HEADLESS` | Set to `1` for a headless persistent context |
| `BRIDGE_DISABLE` | Set to `1` to disable bridge auto-start |

Headful and headless modes use the same persistent context. Launch recovery closes only contexts owned by GhostProv; it does not terminate unrelated browser processes, and Chromium is not launched with `--no-sandbox`.

## Files

```powershell
curl.exe -X POST http://127.0.0.1:8787/v1/files -F file=@image.png -F purpose=vision
curl.exe http://127.0.0.1:8787/v1/files/<file_id>/content --output image.png
```

Image/audio data URLs and Responses fields such as `tools`, `tool_choice`, `reasoning`, `prompt_cache_key`, `prompt_cache_options`, and `prompt_cache_breakpoint` pass through the relay body.

## Active source layout

```text
src/server.ts                 HTTP routes and server startup
src/http/adapter.ts           Fetch-to-Node incremental response adapter
src/ws/server.ts              WebSocket upgrade boundary
src/ws/session.ts             lanes, limits, event forwarding, disconnect cleanup
src/ws/response-graph.ts      bounded store:false continuation/fork state
src/relay/scheduler.ts        global FIFO end-to-end capacity
src/relay/pipeline.ts         slot-first relay with serial fallback
src/relay/slots.ts            daemon lanes and transport negotiation
src/relay/segments.ts         v6.2/v6.3 segmented request plans
src/prism/zip-coordinator.ts  coalesced archive polling
src/prism/gateway.ts          authenticated in-page gateway calls
src/prism/ydoc.ts             persistent Yjs document writer
src/bridge/config.ts          portable bridge paths and launch policy
src/bridge/playwright.ts      persistent authenticated browser bridge
sandbox/gp_daemon.py          Linux concurrent slot daemon
```

`src/relay/client.ts`, `src/proxy/yjs.ts`, `src/proxy/render.ts`, and `src/utils/zip.ts` are compatibility helpers for older direct callers. The server's active Responses path is `relay/pipeline.ts` → `relay/slots.ts`.

## Tests

```powershell
bun test --isolate
bun run test:unit
bun run test:integration
bun run typecheck
```

The default suite is isolated from live credentials and includes:

- `tests/bridge-config.test.ts`
- `tests/bridge.test.ts`
- `tests/concurrency.test.ts`
- `tests/documentation.test.ts`
- `tests/files.test.ts`
- `tests/http-streaming.test.ts`
- `tests/mirror.test.ts`
- `tests/response-graph.test.ts`
- `tests/scheduler.test.ts`
- `tests/segments.test.ts`
- `tests/sse.test.ts`
- `tests/websocket.test.ts`
- `tests/zip-coordinator.test.ts`

`tests/http-streaming.test.ts` and `tests/websocket.test.ts` use real local TCP/WebSocket boundaries. Bridge tests use temporary credential/profile paths and verify no live `.env` or `.pw/tokens.json` mutation.

## Optional live verification

Live verification writes to the configured Prism project and consumes upstream capacity, so it requires an explicit flag:

```powershell
$env:GP_LIVE_VERIFY = "1"
bun run verify:live
```

It checks health, first-event streaming, a multi-turn WebSocket continuation, and concurrent named lanes against the running server. Start GhostProv separately before running it.
