# GhostProv

GhostProv is a browserless OpenAI-compatible router over independent Prism sandbox projects. V2 distributes work through per-project Deno Awareness sidecars while preserving terminal proof, streaming replay, GPT Image compatibility, and Codex `store:false` continuation. The file-backed V1 daemon remains only as a lazy authenticated `auto` fallback during rollout.

The project is pinned to Bun 1.4.0, TypeScript 7.0.2, and `bun test --isolate`. Playwright and Chromium are not runtime dependencies.

## Browserless configuration

Every sandbox uses a separate explicitly allowlisted Prism project under the same authenticated account cookies.

```powershell
$env:PRISM_PROJECT_IDS = "project-uuid-1,project-uuid-2,project-uuid-3,project-uuid-4"
$env:PRISM_COOKIE_FILE = "D:\secrets\prism-cookies.json"
$env:GP_TRANSPORT = "v2"
$env:GP_PRISM_SESSION = "authenticated"
$env:GP_TARGET_CONCURRENCY = "512"
$env:GP_CANDIDATE = "1"
$env:PORT = "18787"
bun run dev
```

`PRISM_COOKIE` accepts an initial Cookie header when a persisted cookie jar is unavailable. `PRISM_DOC_ID` remains a single-project compatibility fallback. The account session consumes `Set-Cookie`, persists rotations atomically, and hot-reloads a newer cookie file.

Candidate mode refuses port 8787. Development and load testing use port 18787 so an existing `localhost:8787` process remains untouched. Stable project UUIDs and worker indexes are persisted in `.ghostprov/fleet.json`; restarts reuse them and never create a new fleet blindly.

Project-less automatic creation is available only when explicitly enabled with `GP_AUTO_CREATE_PROJECTS=1`. The signed-in session is the production default. Guest mode requires `GP_PRISM_SESSION=anonymous`, `GP_ALLOW_ANONYMOUS=1`, and `GP_TRANSPORT=v2`; it defaults to one reusable project and is capped separately by `GP_GUEST_MAX_PROJECTS`.

## Architecture

Each V2 project owns an isolated `V2SandboxRuntime`:

- Direct Prism provisioning and token synchronization
- A scoped Node Yjs connection used only for one offline sidecar bundle and bootstrap ciphertext
- Independent ephemeral Y-Sweet Awareness states: `ghostprovV2Data` for reliable frames and `ghostprovV2Control` for frame-free ACK/capacity state
- A separately launched Deno sidecar that calls the sandbox `OPENAI_BASE_URL` directly
- Append-only claims and terminal spools under `/tmp/ghostprov-v2`
- P-256 ECDH/HKDF/AES-GCM bootstrap and AES-GCM credential rotation
- ACK-clocked 32-frame/128 KiB data flights inside the 16 KiB frame and 128-frame/512 KiB hard ceilings
- Up to 128 physical upstream calls plus fair byte-bounded admission
- One independently phased heartbeat on an exact 10-second cadence
- Live RTT, completion, failure, quarantine, credential, and memory metrics

V2 never requests a project export or calls the sandbox ZIP endpoint. ZIP, immutable project chunks, and 30 daemon lanes belong only to the optional V1 fallback.

The global router evaluates every healthy runtime and selects the lowest predicted completion time using active work, reservations, daemon capacity, ACK/completion EWMA, failures, quarantines, heartbeat age, credential horizon, payload size, and memory pressure.

Failure rules:

- Before committed ACK, one safe router retry is allowed.
- After committed ACK, the request remains pinned to its original sandbox and is never retried.
- `FETCH_STARTING` is durable before fetch; a crash afterward is ambiguous and quarantined.
- A request is released only after exact request/response/transcript terminal proof.
- Proven terminal spools replay after restart without another upstream call.
- Unproven requests remain quarantined and reduce advertised capacity.
- One failed project never stops healthy projects.
- Global zero-capacity queueing fails with a typed 503 after 30 seconds instead of waiting for a client WebSocket idle timeout.
- Client cancellations and definitive upstream errors are routing-neutral; only transport-attributable failures feed a sixty-second decaying sandbox health score.
- ACK-stall age starts with the active reliable flight; idle time never suppresses credits, and a fully drained transport restores credits immediately.

## Concurrency and latency

One V2 sidecar retains a hard physical ceiling of 128 simultaneous upstream calls, while transport admission starts at 8 and grows only through healthy RTT samples to a maximum of 16. Four projects therefore expose 32–64 transport-safe calls and still retain 512 physical safety slots; fleets are dynamically tested at 1, 4, 8, 16, and 32 projects and have no fixed-four implementation limit. Actual admission is:

```text
min(sidecar credits, healthy non-quarantined calls, work fitting live RAM/CPU reservations)
```

Hardware detection uses physical/free RAM, process RSS, and logical processors:

```text
system reserve = max(4 GiB, 20% total RAM)
process ceiling = min(60% total RAM, startup RSS + free RAM - reserve)
pause admission below max(2 GiB, 10% total RAM) free
resume above max(3 GiB, 15% total RAM) free
CPU transform workers = clamp(2, 8, floor(logical processors / 2))
```

Large HTTP bodies reserve memory before buffering. Gzip runs in bounded workers and is used only when it reduces frame count. Response ACK publication stops when a client buffer is full, providing end-to-end backpressure without unbounded memory. Each V2 peer shares one 25 ms data-publication cadence across all streams, keeps ACK/control traffic urgent and frame-free, freezes each 128 KiB data flight until cumulative ACK, and derives bounded retransmission backoff from first-send sequence RTT instead of a fixed timer.

WebSocket `store:false` continuation caches retain only byte-budgeted deltas; full histories are materialized per request and never cached quadratically.

Per-runtime completion estimates cap extreme model durations and decay halfway toward a neutral ten-second baseline every idle minute.

Fresh projects are initialized with Prism's current Yjs root/settings schema. Non-secret sidecar sources are content-addressed and written only when changed. The temporary launcher and encrypted credential entry are tombstoned after matching Awareness readiness; no cookie, OpenAI key, resource token, or Y-Sweet token is written in plaintext.

Latency acceptance targets:

- Router decision p95 below 2 ms
- Queue p95 below 250 ms while capacity is available
- Small Awareness transport p95 below 1 second under 512 accepted/128 active load
- One-project 128-call first-event p95 below 4 seconds
- Four-project 512-call first-event p95 below 7 seconds per sandbox

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

Streaming is implicit in WebSocket mode: clients must omit the `stream` field. GhostProv adds `stream:true` only to its internal sandbox HTTP request.

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

## Performance observability

- `GET /health` returns aggregate capacity, active, available, queued, and runtime count; zero routable capacity returns HTTP 503 with `ok:false`.
- `GET /__sandboxes` returns sanitized per-project lifecycle, slot, heartbeat, latency, failure, quarantine, and resource snapshots.

Start the read-only external monitor against an already-running router without restarting it:

```powershell
bun run monitor:v2
```

It polls only `GET /health` and `GET /__sandboxes`, retries failures with capped exponential jitter, and writes `monitor.sample` or sanitized `monitor.error` JSONL events. The default target is `http://127.0.0.1:18787`; non-loopback roots and roots containing paths, credentials, queries, or fragments are rejected. Ctrl+C stops only the monitor and flushes its log.

Built-in tracing starts with the upgraded router after the operator's next manual restart. It records `request.accepted`, `admission.acquired`, `request.queued`, `route.selected`, `transport.committed`, `response.first_event`, and exactly one `request.completed` or `request.failed`. A periodic `fleet.sample` adds host memory, CPU, event-loop delay, logger pressure, aggregate capacity, and per-runtime health. The external monitor provides immediate fleet visibility, but only built-in tracing can attribute per-request queue, commit, first-event, and total latency.

Logs default to `.ghostprov/logs`, rotate at 64 MiB, and retain five rotated files per process and prefix. The bounded asynchronous queue preserves terminal events ahead of progress and sample events. Observability never logs prompts, responses, tool arguments, request bodies, headers, cookies, tokens, URLs, or client identifiers. No monitoring endpoint returns cookies, sandbox tokens, resource tokens, or Y-Sweet tokens.

Inspect the latest records in PowerShell:

```powershell
$latest = Get-ChildItem -LiteralPath $env:GP_LOG_DIR -Filter 'ghostprov-v2*.jsonl' |
  Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
Get-Content -LiteralPath $latest.FullName -Tail 50
```

Diagnose a slow multi-agent run in this order:

1. High `admission_wait_ms` or `memory_paused=true` means host memory pressure.
2. High queue wait with zero available capacity means the fleet is saturated; queueing with available capacity points to scheduling or stale capacity.
3. High `commit_ack_ms` points to Y-Sweet/Awareness or sandbox event-loop latency.
4. Normal commit ACK with high `post_commit_first_event_ms` points to upstream model latency.
5. Normal first-event latency with high total duration means long reasoning or generation.
6. One runtime with worse RTT, completion EWMA, failures, or quarantine indicates sandbox imbalance.
7. High event-loop p95/max points to local CPU or synchronous work.

For boundary-level V2 diagnostics, explicitly enable instrumentation, manually restart once so the sidecars redeploy, then run the paid controlled 1/4/8/16 WebSocket probe:

```powershell
$env:GP_V2_DIAGNOSTICS = "1"
$env:GP_LIVE_V2_DIAGNOSTICS = "1"
$env:GP_V2_DIAGNOSTIC_ROOT = "http://127.0.0.1:18787"
bun run diagnose:v2-concurrency
```

The probe uses identical 256 KiB request bodies, three waves per stage, waits for fleet idle, never touches port 8787, and writes only timing/counter summaries under `.ghostprov/diagnostics`. It incurs real API usage and never records request or response content.

## Lifecycle and recovery

Startup resolves and locks one persisted Prism identity, reconciles the durable fleet manifest, and binds the public listener as soon as one runtime is ready; remaining projects continue provisioning independently. Shutdown stops listener admission, rejects queued uncommitted work with HTTP 503, waits for existing terminal proofs, drains sidecars, clears Awareness, closes the identity lease, and never creates or deletes a project.

Operational recovery states are intentionally distinct:

- pre-commit rejection: safe for the router's single retry;
- committed definitive failure: pinned, no fallback;
- committed ambiguous failure or lost instance: quarantined, no retry;
- proven terminal spool: replayed and ACKed without a second fetch;
- expired heartbeat/credential: runtime drains without affecting healthy projects.

Project deletion is never automatic. Preview managed targets first:

```powershell
bun run decommission:v2 -- --project <managed-project-uuid>
```

Execution requires the exact preview digest and a separate destructive opt-in:

```powershell
$env:GP_ALLOW_PROJECT_DELETE = "1"
bun run decommission:v2 -- --project <managed-project-uuid> --confirm <sha256-digest>
```

Only manifest records with `managed:true` can be deleted. Explicit user projects are excluded. Cleanup and tombstoning do not erase or conceal Prism-visible project/account activity; they only maintain local lifecycle hygiene.

## Operational settings

| Setting | Default | Meaning |
| --- | ---: | --- |
| `PORT` in candidate mode | `18787` | Protected candidate listener; `8787` is rejected |
| `GP_TRANSPORT` | `auto` | `v2`, `v1`, or authenticated rollout fallback; guests are V2-only |
| `GP_V2_SANDBOX_CAPACITY` | `128` | Hard per-sidecar physical call ceiling |
| `GP_V2_INITIAL_ACTIVE_PER_SANDBOX` | `8` | Initial transport-safe active request limit |
| `GP_V2_MAX_ACTIVE_PER_SANDBOX` | `16` | Maximum adaptive active request limit |
| `GP_TARGET_CONCURRENCY` | `512` | Fleet sizing target; divided by per-sandbox V2 capacity |
| `GP_MAX_PROJECTS` | `32` minimum | Safety ceiling, not a fixed fleet size |
| `GP_HEARTBEAT_MS` | `10000` | Exact per-sandbox heartbeat period |
| `GP_ROUTER_QUEUE_TIMEOUT_MS` | `30000` | Maximum global wait for any sandbox capacity |
| `GP_V2_TRANSPORT_RESERVATION_BYTES` | `16777216` | Host capacity estimate per active V2 stream |
| `GP_V2_SIDECAR_MEMORY_BUDGET_BYTES` | `536870912` | Sidecar request/queue admission budget |
| `GP_V2_YSWEET_REFRESH_MS` | `2700000` | Encrypted reconnect-credential rotation horizon |
| WebSocket active responses | `16` | Per-connection active response ceiling |
| WebSocket named lanes | `32` | Per-connection named-lane ceiling |
| `GP_IMAGE_EDIT_MAX_INPUT_BYTES` | `41943040` | Raw multipart edit-input budget |
| `GP_IMAGE_EDIT_HTTP_MAX_BODY_BYTES` | `44040192` | Multipart envelope ceiling |
| `GP_LOG_LEVEL` | `info` | `off`, `error`, `info`, or `debug` bounded JSONL tracing |
| `GP_LOG_DIR` | `.ghostprov/logs` | Absolute or working-directory-relative log directory |
| `GP_LOG_ROTATE_BYTES` | `67108864` | Active-file rotation threshold (64 MiB) |
| `GP_LOG_RETAIN_FILES` | `5` | Rotated files retained per process/prefix |
| `GP_LOG_QUEUE_MAX_EVENTS` | `10000` | Maximum queued observability events |
| `GP_LOG_QUEUE_MAX_BYTES` | `8388608` | Maximum encoded observability queue bytes |
| `GP_LOG_FLUSH_MS` | `250` | Maximum normal batch delay |
| `GP_METRICS_INTERVAL_MS` | `5000` | Built-in `fleet.sample` interval |
| `GP_SLOW_REQUEST_MS` | `10000` | Slow terminal threshold |
| `GP_SLOW_FIRST_EVENT_MS` | `5000` | Slow first-event threshold |
| `GP_MONITOR_ROOT` | `http://127.0.0.1:18787` | Loopback router origin for the external monitor |
| `GP_MONITOR_INTERVAL_MS` | `2000` | Successful external polling interval |
| `GP_MONITOR_TIMEOUT_MS` | `5000` | Per-poll timeout |
| `GP_V2_DIAGNOSTICS` | `0` | Opt-in host/sidecar boundary diagnostics |
| `GP_V2_DIAGNOSTICS_INTERVAL_MS` | `1000` | Fixed-memory sidecar sample interval |
| `GP_V2_BATCHING` | `0` | Opt-in bounded response batching during live acceptance |

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
- `tests/anthropic-foundations.test.ts`
- `tests/anthropic-http.test.ts`
- `tests/anthropic-request-contract.test.ts`
- `tests/anthropic-request.test.ts`
- `tests/anthropic-response-stop.test.ts`
- `tests/anthropic-response-tool.test.ts`
- `tests/anthropic-response-usage.test.ts`
- `tests/anthropic-response-validation.test.ts`
- `tests/anthropic-response.test.ts`
- `tests/anthropic-stream-bounds.test.ts`
- `tests/anthropic-stream-protocol.test.ts`
- `tests/anthropic-stream-safety.test.ts`
- `tests/anthropic-stream.test.ts`
- `tests/anthropic-token-count.test.ts`
- `tests/concurrency.test.ts`
- `tests/cpu-transform-pool.test.ts`
- `tests/daemon-controller.test.ts`
- `tests/decommission-script.test.ts`
- `tests/decommission.test.ts`
- `tests/documentation.test.ts`
- `tests/file-lease.test.ts`
- `tests/fleet-manifest.test.ts`
- `tests/fleet-manager.test.ts`
- `tests/graceful-shutdown.test.ts`
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
- `tests/observability-config.test.ts`
- `tests/observability-logger.test.ts`
- `tests/observability-redaction.test.ts`
- `tests/observability-rotation.test.ts`
- `tests/observability-run-handle.test.ts`
- `tests/observability-sampler.test.ts`
- `tests/prism-protocol.test.ts`
- `tests/project-inspector.test.ts`
- `tests/provisioning-client.test.ts`
- `tests/projects-client.test.ts`
- `tests/resource-governor.test.ts`
- `tests/response-graph.test.ts`
- `tests/router-http.test.ts`
- `tests/router-observability.test.ts`
- `tests/router-policy.test.ts`
- `tests/sandbox-gateway.test.ts`
- `tests/sandbox-runtime.test.ts`
- `tests/sandbox-ydoc.test.ts`
- `tests/scheduler.test.ts`
- `tests/session-manager.test.ts`
- `tests/segments.test.ts`
- `tests/slot-runner.test.ts`
- `tests/sse.test.ts`
- `tests/system-v2.test.ts`
- `tests/terminal-rescue.test.ts`
- `tests/transport-selector.test.ts`
- `tests/unlatch-recovery.test.ts`
- `tests/v2-awareness-channel.test.ts`
- `tests/v2-adaptive-capacity.test.ts`
- `tests/v2-batching-throughput.test.ts`
- `tests/v2-admission.test.ts`
- `tests/v2-assembler.test.ts`
- `tests/v2-bootstrap-crypto.test.ts`
- `tests/v2-bootstrap-binding.test.ts`
- `tests/v2-bootstrap-controller.test.ts`
- `tests/v2-bootstrap-probe.test.ts`
- `tests/v2-concurrency.test.ts`
- `tests/v2-congestion.test.ts`
- `tests/v2-credential-rotation.test.ts`
- `tests/v2-delivery-timing.test.ts`
- `tests/v2-diagnostics-classifier.test.ts`
- `tests/v2-diagnostics-observability.test.ts`
- `tests/v2-diagnostics-overhead.test.ts`
- `tests/v2-diagnostics-probe.test.ts`
- `tests/v2-diagnostics-protocol.test.ts`
- `tests/v2-diagnostics-statistics.test.ts`
- `tests/v2-diagnostics-types.test.ts`
- `tests/v2-dynamic-fleet.test.ts`
- `tests/v2-deno-awareness-peer.test.ts`
- `tests/v2-faults.test.ts`
- `tests/v2-flight.test.ts`
- `tests/v2-host-observer.test.ts`
- `tests/v2-journal.test.ts`
- `tests/v2-large-payload.test.ts`
- `tests/v2-live-guards.test.ts`
- `tests/v2-long-run-stability.test.ts`
- `tests/v2-monitor.test.ts`
- `tests/v2-output-sink.test.ts`
- `tests/v2-protocol.test.ts`
- `tests/v2-public-api.test.ts`
- `tests/v2-publication-pacer.test.ts`
- `tests/v2-response-batch.test.ts`
- `tests/v2-response-batcher.test.ts`
- `tests/v2-run-handle.test.ts`
- `tests/v2-runtime-completion.test.ts`
- `tests/v2-runtime.test.ts`
- `tests/v2-runtime-health.test.ts`
- `tests/v2-scheduler.test.ts`
- `tests/v2-sidecar-assets.test.ts`
- `tests/v2-sidecar-diagnostics.test.ts`
- `tests/v2-sidecar-observer.test.ts`
- `tests/v2-sidecar-bootstrap.test.ts`
- `tests/v2-sidecar-engine.test.ts`
- `tests/v2-sidecar-rotation.test.ts`
- `tests/v2-split-awareness-channel.test.ts`
- `tests/v2-split-flight-integration.test.ts`
- `tests/v2-transcript.test.ts`
- `tests/v2-transport-session.test.ts`
- `tests/v2-upstream.test.ts`
- `tests/v2-window.test.ts`
- `tests/websocket.test.ts`
- `tests/websocket-telemetry.test.ts`
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

## V2 verification

The deterministic candidate smoke uses an OS-assigned port and verifies that the existing service on 8787 stays healthy:

```powershell
bun run verify:v2-smoke
```

The secure bootstrap probe is separately guarded and never deletes its project:

```powershell
$env:GP_LIVE_V2_BOOTSTRAP = "1"
$env:GP_LIVE_V2_PROJECT_ID = "<dedicated-project-uuid>"
$env:GP_CANDIDATE = "1"
$env:GP_TRANSPORT = "v2"
$env:PORT = "18787"
bun run verify:v2-bootstrap
```

The single-project canary performs two complete bootstrap/drain/relaunch cycles plus HTTP and WebSocket checks. GPT Image usage remains separately opt-in with `GP_LIVE_IMAGE=1`:

```powershell
$env:GP_LIVE_V2 = "1"
$env:GP_LIVE_PROJECT_COUNT = "1"
$env:GP_TRANSPORT = "v2"
$env:PORT = "18787"
bun run verify:v2-single
```

The paid four-project scale canary additionally requires `GP_LIVE_SCALE=1`. It runs 128 direct, 256 accepted, 512 routed, and 4 MiB integrity stages without creating or deleting projects:

```powershell
$env:GP_LIVE_V2 = "1"
$env:GP_LIVE_SCALE = "1"
$env:GP_LIVE_PROJECT_COUNT = "4"
$env:GP_TRANSPORT = "v2"
$env:PORT = "18787"
bun run verify:v2-fleet
```

Set `GP_LIVE_V2_SOAK=1` only for the additional lifecycle soak. Missing live opt-ins are reported as “not run”; deterministic tests are never represented as live Prism proof.

## ScriptC evaluation

Bun remains the production runtime. After Playwright removal, ScriptC 0.0.35 still reports 16 unsupported type-surface sites (web/worker globals, URL/Headers overloads, and crypto typing), so no native binary is produced for a meaningful A/B test. Its dynamic npm tier also uses QuickJS with copied boundaries. Optional coverage can be rerun as ScriptC matures:

```powershell
$env:GP_BENCHMARK_SCRIPTC = "1"
bun run benchmark:scriptc
```

ScriptC is adopted only if full compatibility passes and A/B results improve p95 local latency by at least 10% or RSS by at least 20%, with no more than 5% throughput regression.
