# GhostProv

GhostProv is a browserless OpenAI-compatible router over independent Prism sandbox projects. One local HTTP/WebSocket endpoint distributes work across healthy per-project daemons while preserving terminal proof, streaming replay, image compatibility, and Codex `store:false` continuation.

The project is pinned to Bun 1.4.0, TypeScript 7.0.2, and `bun test --isolate`. Playwright and Chromium are not runtime dependencies.

## Browserless configuration

Every sandbox uses a separate explicitly allowlisted Prism project under the same authenticated account cookies.

```powershell
$env:PRISM_PROJECT_IDS = "project-uuid-1,project-uuid-2,project-uuid-3,project-uuid-4"
$env:PRISM_COOKIE_FILE = "D:\secrets\prism-cookies.json"
$env:GP_CANDIDATE = "1"
$env:PORT = "18787"
bun run dev:candidate
```

`PRISM_COOKIE` accepts an initial Cookie header when a persisted cookie jar is unavailable. `PRISM_DOC_ID` remains a single-project compatibility fallback. The account session consumes `Set-Cookie`, persists rotations atomically, and hot-reloads a newer cookie file.

Candidate mode refuses port 8787. Development and load testing use port 18787 so an existing `localhost:8787` process remains untouched.

## Architecture

Each project owns an isolated `SandboxRuntime`:

- Direct Prism provisioning and token synchronization
- One Node Yjs WebSocket and filename index
- One direct sandbox gateway and ZIP coordinator
- One epoch-based workspace-sync coordinator that coalesces concurrent publications
- One daemon controller and daemon build check
- One physical slot pool and terminal-recovery state
- One independent heartbeat every 10 seconds
- Workload-class latency and failure metrics

The global router evaluates every healthy runtime and selects the lowest predicted completion time using active work, reservations, daemon capacity, ACK/completion EWMA, failures, quarantines, heartbeat age, credential horizon, payload size, and memory pressure.

Failure rules:

- Before publication, one safe reroute is allowed.
- After publication, the request remains pinned to its original sandbox.
- A lane is released only after matching ACK, request identity, and terminal proof.
- Missing incremental data is replayed from the authoritative terminal transcript.
- Unproven lanes remain quarantined and reduce advertised capacity.
- One failed project never stops healthy projects.

## Concurrency and latency

Four 30-slot projects provide a physical ceiling of 120 simultaneous requests. Actual admission is:

```text
min(healthy non-quarantined lanes, work fitting live RAM/CPU reservations)
```

Hardware detection uses physical/free RAM, process RSS, and logical processors:

```text
system reserve = max(4 GiB, 20% total RAM)
process ceiling = min(60% total RAM, startup RSS + free RAM - reserve)
pause admission below max(2 GiB, 10% total RAM) free
resume above max(3 GiB, 15% total RAM) free
CPU transform workers = clamp(2, 8, floor(logical processors / 2))
```

Large HTTP bodies reserve memory before buffering. Gzip runs in bounded workers. ZIP downloads are coalesced and limited to one active archive per sandbox while different sandboxes operate in parallel.

Fresh projects are initialized with Prism's current Yjs root/settings schema. Daemon status and lane files are tracked before launch, render requests use live Yjs state vectors, and each publication crosses a coalesced `wait-for-sync` barrier before polling. Writes arriving during an active barrier receive the next epoch, so batching never weakens publication ordering.

Latency acceptance targets:

- Router decision p95 below 2 ms
- Queue p95 below 250 ms while capacity is available
- ≤2 MiB job publish p50/p95 below 600/1500 ms
- First-event overhead within 10% of a one-project candidate

One sequential Codex task normally uses one lane. High utilization requires multiple tasks, API callers, named WebSocket lanes, or image fan-out.

## HTTP Responses

```powershell
curl.exe -X POST http://127.0.0.1:18787/v1/responses `
  -H "content-type: application/json" `
  -d '{"model":"gpt-5.6-sol","store":false,"input":"Reply with hello"}'
```

Use `stream:true` for incremental SSE. Reasoning fields including `effort:"max"`, `mode:"pro"`, `context`, and `summary` pass through verbatim.

## Codex WebSocket mode

WebSocket mode follows the [official OpenAI Responses WebSocket contract](https://developers.openai.com/api/docs/guides/websocket-mode/). Clients send direct `response.create` events to `ws://127.0.0.1:18787/v1/responses`. Named lanes execute independently, same-lane work remains FIFO, and `previous_response_id` lineage is separate from lane identity.

```toml
[model_providers.ghostprov]
name = "GhostProv"
env_key = "OPENAI_API_KEY"
base_url = "http://127.0.0.1:18787/v1"
supports_websockets = true
```

## Images compatibility

- `POST /v1/images/generations`
- `POST /v1/images/edits`

`gpt-image-2` and its pinned snapshot are supported. Native `n=1..10` fans children across independent sandbox runtimes. GPT Image responses return `b64_json`. Image streaming, URL emulation, audio, and DALL-E variation routes return explicit unsupported-capability errors.

## Monitoring

- `GET /health` returns aggregate capacity, active, available, queued, and runtime count; zero routable capacity returns HTTP 503 with `ok:false`.
- `GET /__sandboxes` returns sanitized per-project lifecycle, slot, heartbeat, latency, failure, quarantine, and resource snapshots.

No endpoint returns cookies, sandbox tokens, resource tokens, or Y-Sweet tokens.

## Operational settings

| Setting | Default | Meaning |
| --- | ---: | --- |
| `PORT` in candidate mode | `18787` | Protected candidate listener; `8787` is rejected |
| `GP_SLOTS` | `30` | Per-project lanes; configurable from 1 through the daemon maximum of 30 |
| `GP_MAX_PROJECTS` | `4` minimum | Maximum allowlisted projects |
| `GP_HEARTBEAT_MS` | `10000` | Exact per-sandbox heartbeat period |
| `GP_ZIP_BATCH_MS` | `10` | Same-runtime ZIP coalescing window |
| WebSocket active responses | `16` | Per-connection active response ceiling |
| WebSocket named lanes | `32` | Per-connection named-lane ceiling |
| `GP_IMAGE_EDIT_MAX_INPUT_BYTES` | `41943040` | Raw multipart edit-input budget |
| `GP_IMAGE_EDIT_HTTP_MAX_BODY_BYTES` | `44040192` | Multipart envelope ceiling |

## Tests

```powershell
bun test --isolate
bun run test:unit
bun run test:integration
bun run test:daemon
bun run typecheck
bun audit --production
```

The isolated suite includes:

- `tests/account-session.test.ts`
- `tests/concurrency.test.ts`
- `tests/cpu-transform-pool.test.ts`
- `tests/daemon-controller.test.ts`
- `tests/documentation.test.ts`
- `tests/file-lease.test.ts`
- `tests/fleet-manifest.test.ts`
- `tests/heartbeat.test.ts`
- `tests/http-body.test.ts`
- `tests/http-errors.test.ts`
- `tests/http-streaming.test.ts`
- `tests/images-compat.test.ts`
- `tests/images-http.test.ts`
- `tests/images-service.test.ts`
- `tests/multi-sandbox-config.test.ts`
- `tests/multi-sandbox-integration.test.ts`
- `tests/multi-sandbox-router.test.ts`
- `tests/no-playwright.test.ts`
- `tests/prism-protocol.test.ts`
- `tests/provisioning-client.test.ts`
- `tests/projects-client.test.ts`
- `tests/resource-governor.test.ts`
- `tests/response-graph.test.ts`
- `tests/router-http.test.ts`
- `tests/router-policy.test.ts`
- `tests/sandbox-gateway.test.ts`
- `tests/sandbox-runtime.test.ts`
- `tests/sandbox-ydoc.test.ts`
- `tests/scheduler.test.ts`
- `tests/session-manager.test.ts`
- `tests/segments.test.ts`
- `tests/slot-runner.test.ts`
- `tests/sse.test.ts`
- `tests/terminal-rescue.test.ts`
- `tests/unlatch-recovery.test.ts`
- `tests/websocket.test.ts`
- `tests/workload.test.ts`
- `tests/ydoc.test.ts`
- `tests/zip-coordinator.test.ts`

The deterministic integration test fills 120 lanes as `30 + 30 + 30 + 30`, verifies terminal settlement and unique execution IDs, and proves a draining project leaves the other 90 lanes usable.

## Live proof

Create dedicated test projects only with explicit authorization:

```powershell
$env:GP_LIVE_CREATE_TEST_PROJECTS = "1"
$env:GP_LIVE_PROJECT_COUNT = "4"
bun run create:test-projects
```

Run direct browserless provisioning/Yjs/heartbeat proof:

```powershell
$env:GP_LIVE_VERIFY = "1"
bun run verify:browserless
```

Start the candidate on 18787, then run the staged `16 → 32 → 64 → 96 → 120` load proof:

```powershell
$env:GP_LIVE_VERIFY = "1"
$env:OPENAI_BASE_URL = "http://127.0.0.1:18787/v1"
bun run verify:multi-sandbox
```

The load script verifies exact output markers, unique response IDs, peak physical occupancy, per-runtime occupancy, zero under-capacity queueing, zero quarantines, and port 8787 health before and after the candidate run.

Run Codex-compatible HTTP streaming, `store:false` WebSocket tool continuation, and eight concurrent named lanes:

```powershell
$env:GP_LIVE_VERIFY = "1"
$env:OPENAI_BASE_URL = "http://127.0.0.1:18787/v1"
bun run verify:live
```

Native GPT Image generation and multipart editing are separately opt-in because they incur image usage:

```powershell
$env:GP_LIVE_VERIFY = "1"
$env:GP_LIVE_IMAGE = "1"
$env:GP_LIVE_IMAGE_EDIT = "1"
$env:OPENAI_BASE_URL = "http://127.0.0.1:18787/v1"
bun run verify:live
```

## ScriptC evaluation

Bun remains the production runtime. After Playwright removal, ScriptC 0.0.35 still reports 16 unsupported type-surface sites (web/worker globals, URL/Headers overloads, and crypto typing), so no native binary is produced for a meaningful A/B test. Its dynamic npm tier also uses QuickJS with copied boundaries. Optional coverage can be rerun as ScriptC matures:

```powershell
$env:GP_BENCHMARK_SCRIPTC = "1"
bun run benchmark:scriptc
```

ScriptC is adopted only if full compatibility passes and A/B results improve p95 local latency by at least 10% or RSS by at least 20%, with no more than 5% throughput regression.
