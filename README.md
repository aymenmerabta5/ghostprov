# GhostProv

GhostProv is a browserless OpenAI-compatible router over independent Prism sandbox projects. Per-project Deno Awareness sidecars distribute work while preserving terminal proof, provider-persisted continuation, GPT Image compatibility, and Claude/Codex streaming.

The project is pinned to Bun 1.4.0, TypeScript 7.0.2, and `bun test --isolate`. Playwright and Chromium are not runtime dependencies.

## Browserless configuration

Every sandbox uses a separate explicitly allowlisted Prism project under the same authenticated account cookies.

```powershell
$env:PRISM_PROJECT_IDS = "project-uuid-1,project-uuid-2,project-uuid-3,project-uuid-4"
$env:PRISM_COOKIE_FILE = "D:\secrets\prism-cookies.json"
$env:GP_PRISM_SESSION = "authenticated"
$env:GP_CANDIDATE = "1"
$env:PORT = "18888"
bun run dev
```

`PRISM_COOKIE` accepts an initial Cookie header when a persisted cookie jar is unavailable. `PRISM_DOC_ID` remains a single-project compatibility fallback. The account session consumes `Set-Cookie`, persists rotations atomically, and hot-reloads a newer cookie file.

Port `18787` is the protected existing inference gateway and is never a candidate or test listener. Development and live verification use `18888` or an OS-assigned port; lifecycle commands must not target the protected service. Stable project UUIDs and worker indexes are persisted in `.ghostprov/fleet.json`; restarts reuse them and never create a new fleet blindly.

V2-only mode requires an explicit allowlist of existing `PRISM_PROJECT_IDS`. GhostProv never creates or adopts an unlisted project. The signed-in session is the production default. Guest mode requires `GP_PRISM_SESSION=anonymous` and `GP_ALLOW_ANONYMOUS=1`, and remains capped separately by `GP_GUEST_MAX_PROJECTS`.

## Architecture

Each V2 project owns an isolated `V2SandboxRuntime`:

- Direct Prism provisioning and token synchronization
- A scoped Node Yjs connection used only for one offline sidecar bundle and bootstrap ciphertext
- Independent ephemeral Y-Sweet Awareness states: `ghostprovV2Data` for reliable frames and `ghostprovV2Control` for frame-free ACK/capacity state
- A separately launched Deno sidecar that calls the sandbox `OPENAI_BASE_URL` directly
- Append-only claims and terminal spools under `/tmp/ghostprov-v2`
- P-256 ECDH/HKDF/AES-GCM bootstrap and AES-GCM credential rotation
- Revision-bound ACK-clocked flights: frozen R1 is 128 KiB in both directions; R2 permits a 512 KiB host request flight while keeping sidecar response flights at the proven 128 KiB ceiling, all inside the 16 KiB frame and 128-frame/512 KiB window bounds
- Up to 128 physical upstream calls plus fair byte-bounded admission
- One independently phased heartbeat on an exact 10-second cadence
- Live RTT, completion, failure, quarantine, credential, and memory metrics

The request path never requests a project export or calls the sandbox ZIP endpoint. Yjs and Render are used only to bootstrap the sidecar; live requests use ephemeral Awareness frames.

The global router evaluates every healthy runtime and selects the lowest predicted completion time using active work, reservations, sidecar capacity, ACK/completion EWMA, failures, quarantines, heartbeat age, credential horizon, payload size, and memory pressure.

Failure rules:

- Safe router failover is allowed only before committed ACK and inside the original admission deadline. A selected runtime has 15 seconds to take transport ownership, followed by a fixed 25 ms cancellation-proof fence, before another healthy incarnation is tried; any committed or ambiguous execution-risk signal disables failover. A non-cooperative runtime cannot extend the absolute deadline beyond that bounded fence.
- After committed ACK, the request remains pinned to its original sandbox and is never retried.
- Top-level Responses `function_call.arguments` strings and decoded argument values are validated before memory admission. The same pass rejects lone Unicode surrogates anywhere in the request, so provider-incompatible JSON receives an exact local 400 path instead of consuming a sandbox request.
- `FETCH_STARTING` is durable before fetch; a crash afterward is ambiguous and quarantined.
- A request is released only after exact request/response/transcript terminal proof.
- Proven terminal spools replay after restart without another upstream call.
- Unproven requests remain quarantined and reduce advertised capacity.
- One failed project never stops healthy projects.
- Global zero-capacity queueing fails with a typed 503 after 30 seconds instead of waiting for a client WebSocket idle timeout.
- Client cancellations and definitive upstream errors are routing-neutral; only transport-attributable failures feed a sixty-second decaying sandbox health score.
- ACK-stall age starts with the active reliable flight; idle time never suppresses credits, and a fully drained transport restores credits immediately. A reliable stall first sampled at ten seconds removes capacity, then a second independent sample two seconds later starts same-session R2 recovery; one slow sample never relaunches, and fresh ACK progress restores the normal ten-second cadence.
- Every project runtime is supervised. A dead child is replaced with a fresh sandbox session for the **same Prism project and worker index**; recovery never creates or deletes a project.
- One isolated sidecar failure recovers immediately. A second distinct project failing within five seconds activates the existing fleet single-probe circuit, preventing an account outage from reprovisioning every sandbox at once.

## Concurrency and latency

One V2 sidecar retains a hard protocol ceiling of 128 simultaneous upstream calls, while the proven 512 MiB terminal-spool quota reserves exactly 64 MiB per prepared request and therefore caps production admission at 8. `GP_V2_RESPONSE_MAX_BYTES` is fixed to exactly 64 MiB in this release and configuration rejects any other value. Four projects expose 32 durability-backed calls; fleets are dynamically tested at 1, 4, 8, 16, and 32 projects and have no fixed-four implementation limit. Actual admission is:

The sidecar claim journal is bounded at 64 MiB and 100,000 checksummed records during both runtime and restart. New claims reserve their worst-case remaining transition/outcome space before acceptance, and exclusive compaction retains one minimal state snapshot per unresolved fence, terminal proof, provider outcome, or acknowledged duplicate fence. Per-request mutation lanes serialize terminal/provider/forced outcomes, with forced-deadline intent revalidated after terminal promotion. Torn unterminated UTF-8 tails are removed only after every complete line passes fatal decoding and checksum validation, and bounded orphan rewrite temps are cleaned under ownership. On POSIX, hierarchy creation, the first claim append, journal replacement, and terminal promotion fsync their directory entries before reporting durability.

Host response events use one process-wide atomic byte-permit pool reserved in full through the global resource governor before startup. For each active slot, its ordinary region covers one in-progress assembly (`512 KiB × 4 + 1 MiB`), the full retained payload queue (`512 KiB × 4`), and metadata for at most 512 buffered events (`512 × 4 KiB`); that per-slot sum is multiplied by configured projects and the proven active limit. Its single jumbo lane is `GP_V2_RESPONSE_MAX_BYTES × 4 + 1 MiB`. Startup fails closed when that complete reservation cannot fit. A response queue is bounded by both 512 KiB and 512 events, while a larger canonical SSE event is accepted only with exclusive jumbo coverage. When either bound is full, the host withholds its cumulative data ACK and propagates backpressure through the sidecar window instead of turning a temporary consumer pause into committed ambiguity. Permit ownership follows the event through the run handle, final object, and HTTP/WebSocket delivery or cache eviction, and fleet samples expose ordinary/jumbo usage and waiters.

Pre-commit request transfer is byte-weighted independently from upstream model concurrency. Standard profiles retain the established four-way per-runtime admission; R5/R6/R7/R8 can pipeline six medium bodies per runtime only behind an independent four-`request_start` execution-risk fence. A separate fleet-wide four-unit gate admits at most two observed 2–3 MiB transfers, while 256 KiB–2 MiB starts use a 500 ms weighted pacer and requests through 256 KiB bypass both controls. This prevents synchronized bodies from collapsing the shared Prism/Y-Sweet link without reducing committed model concurrency or the tiny-request fast path.

Each bulk-profile runtime can retain six pre-commit bodies of at most 2 MiB while exposing at most four `request_start` execution risks. Any admitted request through 256 KiB keeps the four-request standard until that latency-critical request leaves the transport; requests above 3 MiB remain exclusive, so two true bulk transfers remain forbidden. Any latency-critical admission, retransmission, reconnectable publication failure, or transport stall returns flights to the standard bound, and two clean request flights are required before acceleration resumes.

```text
min(sidecar credits, healthy non-quarantined calls, work fitting live RAM/CPU reservations)
```

GhostProv performs no provider/model rate-limit admission control. Requests continue through the normal local memory, transport-credit, and client-backpressure safety limits until the upstream itself returns a 429. HTTP, SSE, and WebSocket callers receive the original credential-scrubbed upstream status and error identity; GhostProv does not sleep, probe, retry, reroute, flush queued work, or synthesize a cooldown 429. Parsed organization/model/TPM evidence is retained only on the sandbox that observed it as `upstreamRateLimit` telemetry and never changes routing.

Hardware detection uses physical/free RAM, process RSS, and logical processors:

```text
system reserve = max(4 GiB, 20% total RAM)
process ceiling = min(60% total RAM, startup RSS + free RAM - reserve)
pause admission below max(2 GiB, 10% total RAM) free
resume above max(3 GiB, 15% total RAM) free
CPU transform workers = clamp(2, 8, floor(logical processors / 2))
```

Large HTTP bodies reserve memory before buffering. Gzip runs in bounded workers and is used only when it reduces frame count. Response ACK publication stops when a client buffer is full, providing end-to-end backpressure without unbounded memory. Each V2 peer shares one 25 ms data-publication cadence across all streams, keeps ACK/control traffic urgent and frame-free, freezes one revision/direction-specific flight until cumulative ACK, and derives a five-second-bounded SRTT/RTT-variance retransmission clock from first-send sequence timing. A live control update fast-retransmits a stale flight. A measured two-second no-progress interval temporarily advertises zero new credits; one recovered stall restores full concurrency, while repeated stalls within one minute adaptively derate the affected sandbox. R2's larger 512 KiB flight applies only from host to sidecar; sidecar-to-host response traffic remains 128 KiB.

## Persisted continuation policy

Every Responses body sent beyond the router is normalized, copy-on-write, to explicit `stream:true` and `store:true`. Provider-side response state and its approximately 30-day retention window are accepted product behavior. Caller-facing HTTP mode remains independent: a client may request a normal JSON final while the internal upstream call still streams. An exact caller-supplied `previous_response_id` is preserved unless GhostProv is expanding one of its reserved synthetic WebSocket warmup aliases.

Generated WebSocket turns are provider-backed. GhostProv forwards only the current input delta with the provider `previous_response_id`; it does not retain or replay generated prompts, images, reasoning, tool arguments, or outputs. Connection-local graph storage is limited to explicit `generate:false` warmup aliases, with count, byte, process, scope, and connection-lifetime bounds. A generated child expands that warmup prefix once and then continues by its provider response ID.

Native Responses HTTP clients that resend an exact full input array use a separate app-scoped, content-free prefix index. When `prior input + exact provider output` matches an item-boundary prefix under identical request settings, GhostProv forwards only the nonempty suffix with the stored provider response ID. Any client rewrite is a safe miss, and an explicit caller `previous_response_id` always bypasses inference. A definitive pre-event stale inferred parent is compare-deleted and retried once with the immutable full input; explicit IDs and ambiguous failures are never replayed.

The Anthropic adapter owns one app-scoped, process-local continuation index shared by stable and beta Messages routes. It retains only keyed rolling digests, provider response IDs, counts, and bounded lifecycle metadata: at most 4,096 records, a conservative approximately 6 MiB heap estimate, and a 60-minute absolute TTL. A completed, losslessly round-trippable Claude history can reuse the longest unique prefix and send only its nonempty suffix. Digest preparation charges only memory not already covered by the global ingress lease, so a bounded 16-agent large-history burst does not silently fall back to bulk relay while the count and governor limits remain authoritative. A definitive HTTP `previous_response_not_found` before any public event permits one full-history fallback; ambiguous, streamed, generic, and explicit-ID failures are never replayed.

One opaque authenticated scope is constructed for the application and passed through HTTP, WebSocket, Claude, and router composition. The router reserves bounded concurrent commit capacity, commits completed provider response IDs to their exact project/incarnation before public terminal exposure, and prefers known continuations there. Provenance is a one-hour, content-free routing hint bounded to 4,096 records and 1 MiB; after immutable conflict checks, commit-time oldest-hint eviction prevents metadata pressure from becoming a global request quota. Unknown or expired provider IDs route normally, while missing scope, cross-scope IDs, and conflicting live bindings fail closed.

Per-runtime completion estimates cap extreme model durations and decay halfway toward a neutral ten-second baseline every idle minute. Successful responses update that estimate even when they include rate-limit budget headers. Routing throughput is learned only from uniquely acknowledged request-data flights, never cumulative publications, retransmissions, or idle ACK/control heartbeats; a stale goodput sample returns to neutral after one idle minute so a recovered sandbox cannot remain permanently starved. Host transcript hashing uses exact-sized buffers for ordinary events and a bounded scratch path for jumbo events, while WebSocket reasoning redaction uses copy-on-write so ordinary streaming events are not deep-cloned.

Fresh projects are initialized with Prism's current Yjs root/settings schema. Non-secret sidecar sources are content-addressed and written only when changed. The temporary launcher and encrypted credential entry are tombstoned after matching Awareness readiness; no cookie, OpenAI key, resource token, or Y-Sweet token is written in plaintext.

The historical live-load baseline measured successful total latency around 6.9/19.4 seconds p50/p95 and showed that host CPU was not saturated. The isolated R6 fleet subsequently completed 40/40 incompressible 1 MiB requests without body corruption, 15/15 incompressible 2.9 MiB requests in three-request waves without capacity timeouts (6.26/20.27 seconds p50/p95), a 14-agent mixed 128 KiB/1 MiB/2.9 MiB wave in 27.94 seconds, and 14 simultaneous 128 KiB requests in 8.94 seconds. The isolated R7 fleet then completed five exact 14-agent observed-distribution waves (70/70) with zero capacity timeouts, zero `invalid_json`, zero load-time recoveries, and 4.89/25.79/29.41 seconds total p50/p95/max while all eight runtimes remained healthy. Eight simultaneous incompressible 2.9 MiB bodies exceed the measured shared-link throughput inside the hard 30-second bound and are intentionally treated as true overload rather than fictitious sandbox capacity.

Current acceptance targets remain router decision p95 below 2 ms, queue p95 below 250 ms whenever local capacity is available, and no CPU/event-loop or memory saturation. Interpret high `commit_ack_ms` with RTT/retransmission counters as transport-link pressure; interpret high CPU/event-loop delay with normal RTT as local work; interpret normal commit with high first-event/total latency as model/upstream pressure.

One sequential Codex task normally uses one lane. High utilization requires multiple tasks, API callers, named WebSocket lanes, or image fan-out.

## HTTP Responses

```powershell
$env:OPENAI_BASE_URL = "http://127.0.0.1:18787/v1"
$env:OPENAI_API_KEY = "GHOST_IS_KING"
curl.exe -X POST http://127.0.0.1:18787/v1/responses `
  -H "Authorization: Bearer GHOST_IS_KING" `
  -H "content-type: application/json" `
  -d '{"model":"gpt-5.6-sol","store":true,"input":"Reply with hello"}'
```

Every `/v1/*` OpenAI-compatible route requires `Authorization: Bearer <GHOSTPROV_API_KEY>`, including the Responses WebSocket upgrade. Health and sandbox-monitor endpoints remain available without credentials. Use `stream:true` for incremental SSE; omitted or false client `stream` still produces a JSON final while the upstream body is normalized to `stream:true`. Client `store` hints are accepted for compatibility but the upstream body is always `store:true`. Every non-image upstream request is forced to `service_tier:"fast"`; image-tool requests continue to omit unsupported service tiers. Reasoning fields including `effort:"max"`, `mode:"pro"`, `context`, and `summary` pass through verbatim.

## Anthropic Messages and Claude Code

The production Anthropic adapter is part of the same `createGhostProvApp` and uses the same router, queue, sandbox capacity, and listener as the OpenAI routes. Point Claude Code directly at the GhostProv origin; `ANTHROPIC_BASE_URL` is a root URL and must not end in `/v1`:

```powershell
$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:18787"
$env:ANTHROPIC_API_KEY = "GHOST_IS_KING"
claude
```

The equivalent values are `ANTHROPIC_BASE_URL=http://127.0.0.1:18787` and `ANTHROPIC_API_KEY=GHOST_IS_KING`. Anthropic routes require either an exact `x-api-key: <GHOSTPROV_API_KEY>` or `Authorization: Bearer <GHOSTPROV_API_KEY>`. The SDK adds the required `anthropic-version: 2023-06-01` header itself, so normal Claude Code setup does not configure that header separately.

Supported Anthropic routes are:

- `POST /v1/messages`
- `POST /v1/messages/count_tokens`
- `POST /v1/messages?beta=true`
- `POST /v1/messages/count_tokens?beta=true`

The beta query and nonempty `anthropic-beta` headers enable current beta response fields. Unknown beta names and additive Claude Code/Stainless headers are tolerated. Authentication runs before request parsing or sandbox admission.

Claude model names are selected by tier, case-insensitively:

- Opus → `gpt-5.6-sol`
- Sonnet → `gpt-5.6-terra`
- Haiku → `gpt-5.6-luna`
- Native `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, and `gpt-5.5` IDs pass through.
- Unknown model names use `gpt-5.6-sol`.

Override those defaults with `GP_ANTHROPIC_OPUS_MODEL`, `GP_ANTHROPIC_SONNET_MODEL`, `GP_ANTHROPIC_HAIKU_MODEL`, and `GP_ANTHROPIC_DEFAULT_MODEL`. Responses continue to echo the Claude model name requested by the client.

Current mid-conversation `system` messages are enabled for the exact normalized model IDs `claude-fable-5`, `claude-mythos-5`, `claude-opus-4-8`, and `claude-opus-5`. A system section must immediately follow a user turn and be final or immediately followed by an assistant turn. Consecutive `system` messages are coalesced into one section before placement validation. Other `claude-*` models reject message-level `system`; top-level `system` remains the ordinary instructions field. For Claude Code compatibility with GhostProv model names, message-level `system` is accepted for custom model IDs, while `developer` is accepted only for custom non-`claude-*` model IDs.

Messages supports streaming and non-streaming text, client tool use/results (including parallel calls), provider-persisted encrypted-reasoning continuity, current image/document inputs, structured output, prompt-cache annotations, local token estimates, cancellation, and Anthropic error envelopes. The translated Responses traffic retains OpenAI's default prompt caching; live acceptance reports cache-read and cache-write tokens rather than claiming a separate Claude-side cache. Stable and beta routes share the app-scoped digest index described above; public JSON and `message_stop` terminals are emitted only after the authoritative router provenance and eligible Claude index commits. Token counts are conservative local estimates rather than provider billing counts. Anthropic server-hosted tools and exact manual thinking-budget enforcement are non-goals; unsupported content/tool variants return a bounded `invalid_request_error`. Legacy Completions, Message Batches, and Anthropic Files/Admin APIs are not implemented. `GET /v1/models` remains the OpenAI-compatible GhostProv route, not an Anthropic Models response.

### Separate-port forwarding verifier

`bun run verify:claude-code` is a disposable branch verifier, not a production proxy. It defaults to `127.0.0.1:18788` and forwards translated Responses streams to the loopback root in `GP_CLAUDE_VERIFY_UPSTREAM` (default `http://127.0.0.1:18787`). Override the listener with `GP_CLAUDE_VERIFY_PORT` only when another non-protected loopback port is needed:

```powershell
$env:GP_CLAUDE_VERIFY_PORT = "18788"
$env:GP_CLAUDE_VERIFY_UPSTREAM = "http://127.0.0.1:18787"
bun run verify:claude-code
```

In a second terminal, point Claude Code at the verifier rather than the production listener:

```powershell
$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:18788"
$env:ANTHROPIC_API_KEY = "GHOST_IS_KING"
claude
```

The verifier refuses listener ports `18787` and `8787`, non-loopback upstream URL credentials, paths, queries, fragments, and self-forwarding. It forwards only the configured `GHOSTPROV_API_KEY` bearer credential to the loopback GhostProv upstream. It never enumerates, starts, stops, restarts, or signals the upstream process. Ctrl+C closes only the verifier listener and its outbound requests. Live Claude Code calls through this verifier belong to the separate live-verification task; automated tests import it without opening a listener or contacting the upstream.

Reliability defaults allow eight concurrent forwards and reserve four times the 2 MiB per SSE event parser cap, enforcing a 64 MiB aggregate parser-memory ceiling. Admission permits at most 32 forward waiters and 32 incoming-body waiters, with a 128 MiB aggregate incoming-body byte budget; excess waiters receive a typed sanitized overload. Every admitted request has a 300-second forward deadline covering response headers through stream completion, and each streamed run has a 1 MiB per-run output queue with upstream backpressure.

## Codex WebSocket mode

WebSocket mode follows the [official OpenAI Responses WebSocket contract](https://developers.openai.com/api/docs/guides/websocket-mode/). Clients send direct `response.create` events to `ws://127.0.0.1:18787/v1/responses`. Named lanes execute independently, same-lane work remains FIFO, and `previous_response_id` lineage is separate from lane identity.

Streaming is implicit in WebSocket mode. Clients normally omit `stream`; `stream:true` is accepted as a compatibility hint, while `stream:false` is rejected. GhostProv normalizes persistence before graph resolution and sends `stream:true` and `store:true` to the sandbox.

The normal WebSocket fast path remains 4 MiB and every upgraded connection pre-reserves a complete 17 MiB raw/decode/parse working set. The fixed 768 MiB ingress region also preserves one 48 MiB promotion lane, guaranteeing 42 simultaneous default connections while any one of them accepts an authenticated message through 16 MiB without changing sockets. The independent 512 MiB queued-request ceiling and the ingress ceiling are both charged to the global resource governor. Promotion is synchronous; exceptional custom-budget exhaustion closes only that connection with code `1013` instead of retaining an unbounded or reordered queue. Bun 1.4's current `node:http` compatibility path fixes its parser at 16 MiB even though native `Bun.serve` can configure a larger limit. The legacy `x-ghostprov-ws-jumbo: 1` header therefore maps to normal bounded admission on Bun instead of wasting a 257 MiB reservation. Payloads above the compatibility-path bound are rejected before relay admission. Telemetry reports interval and cumulative promotion/rejection counts, parser and upgrade rejections, byte peaks, and current/peak ingress reservations without request content. Server ping/pong detects half-open peers, and an otherwise idle connection expires after five minutes; a live active relay is never expired merely because upstream reasoning is temporarily silent. Generated responses retain no local history copy: ordinary provider IDs carry only the current delta, while explicit `generate:false` warmup aliases are scoped and byte-bounded. GhostProv neither resizes nor recompresses images, and it withholds every public terminal until the authoritative final, router provenance, and any eligible continuation-index commit have completed.

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

- `GET /health` returns aggregate capacity, active, available, queued, desired/ready/unavailable fleet counts, bounded exact unavailable project UUIDs, runtime count, and an exact release summary (`releaseRevision`, `protocolRevision`, `buildHash`, and `schemaHash`); zero routable capacity returns HTTP 503 with `ok:false`.
- `GET /__sandboxes` returns sanitized per-project lifecycle, exact verified release, slot, heartbeat, latency, failure, quarantine, and resource snapshots. A runtime which has not proven a V2 release reports all three release fields as `null`; sandbox instance/session IDs and credentials are never exposed. The originating runtime retains its latest observation-only `upstreamRateLimit` problem and parsed organization/model/limit/used/requested/remaining/retry evidence; no unsupported `/v1/me` call is made through the scoped sandbox proxy.

Routing treats either a fresh direct sandbox heartbeat or a fresh envelope from the exact bootstrap-bound V2 sidecar as liveness proof; lifecycle state and credential expiry still fail closed. Admission-credit ownership does not end R2 failover by itself: the per-route deadline remains active until `request_start` enters the cumulative reliable window. For R5/R6/R7/R8 requests above 2 MiB through 8 MiB, the unchanged first attempt boundary is 15 seconds: no recent request-data ACK retries another runtime, while a fresh cumulative ACK or exact `request_prepared` proof keeps the already-transferred bytes on that runtime. Freshness is four times the selected RTT clamped to 5–10 seconds, and can never extend the unchanged absolute 30-second deadline. A late-admitted request with proven transfer progress also retains its current attempt when less than one freshness window remains, because a full retransmit cannot establish stronger evidence before the same deadline. Requests through 2 MiB and standard/R4 rollback retain their exact prior attempt behavior. Work stalled before the execution fence can move to another runtime, while work at or beyond the fence is never duplicated.

Start the read-only external monitor against an already-running router without restarting it:

```powershell
bun run monitor:v2
```

It polls only `GET /health` and `GET /__sandboxes`, retries failures with capped exponential jitter, and writes `monitor.sample` or sanitized `monitor.error` JSONL events. The default target is `http://127.0.0.1:18787`; non-loopback roots and roots containing paths, credentials, queries, or fragments are rejected. Ctrl+C stops only the monitor and flushes its log.

Built-in tracing starts with the upgraded router after the operator's next manual restart. It records `request.accepted`, `admission.acquired`, `request.queued`, `route.selected`, `route.attempt_expired`, `transport.committed`, `response.first_event`, and exactly one `request.completed` or `request.failed`. Selected routes and expired attempts include a bounded capacity reason without request content. Commit events include decoded/encoded request bytes, compression ratio, part count, encoding, assembly, durable claim, prepare-to-start, fetch-start sync, scheduler, and transport-residual timing. A periodic `fleet.sample` adds separately measured host external/ArrayBuffer memory, CPU, event-loop delay, logger pressure, aggregate capacity, lifecycle-ready and actually-routable runtime counts, per-runtime availability reasons, interval requests/second and success rate, response bytes, retries, uniquely acknowledged goodput, retransmitted bytes, stall transitions, observation-only upstream rate-limit evidence, and bounded p50/p95/max distributions for queue wait, commit ACK, first byte, and total latency. When the sidecar runtime exposes them, the same sample includes process CPU deltas, ArrayBuffer bytes, and bounded rolling GC/event-loop observations; missing platform metrics stay `null` rather than being estimated. Full samples remain in JSONL while the console prints a fixed-size fleet rollup. The external monitor provides immediate fleet visibility, but only built-in tracing can attribute per-request queue, commit, first-event, and total latency.

Recovery tracing adds `sandbox.recovery.started`, `sandbox.recovery.retry_scheduled`, `sandbox.recovery.completed`, `sandbox.recovery.aborted`, `sandbox.credential_refresh`, and transition-only `prism.circuit.changed`. Recovery events report `attempt_duration_ms` separately from total `duration_ms`, so retry backoff is never mistaken for a slow provisioning attempt. These events contain only project/worker indexes, bounded categories, counters, durations, and HTTP status codes—never sandbox/session IDs, URLs, cookies, tokens, prompts, or responses. `/__sandboxes` exposes process-local runtime incarnations, bounded recovery counters, and sidecar peer age; periodic `fleet.sample` additionally records the sanitized provisioning-circuit state.

Logs default to `.ghostprov/logs`, rotate at 64 MiB, and retain five rotated files per process and prefix. The bounded asynchronous head-index queue preserves terminal events ahead of progress and sample events, validates and serializes only during batched drain, and keeps full records in JSONL. Console output batches terminal/error records and rate-limits progress/sample records to a fixed cadence. Observability never logs prompts, responses, tool arguments, request bodies, headers, cookies, tokens, URLs, or client identifiers. No monitoring endpoint returns cookies, sandbox tokens, resource tokens, or Y-Sweet tokens.

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
8. A runtime `rate_limit_observation` identifies upstream provider pressure; it is evidence only and must never coincide with GhostProv provider-gate queueing.
9. `ambiguous_quarantined >= ambiguity_limit` should be followed by one supervised runtime replacement; cancellation-only churn must leave `ambiguous_quarantined=0`.

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
- heartbeat `401`/`403`: account cookies are revalidated once fleet-wide, `/api/backend/1/new` reacquires the sandbox credential, and an exactly matching session rotates its token in place; a changed or unverifiable session is handed directly to a fresh generation without a second acquisition;
- heartbeat `404`/`409`/`410`, three consecutive transient heartbeat failures, a stale/missing heartbeat, or an expired resource credential: the child fails closed and is replaced;
- V2 proves the exact bootstrap-bound sidecar over an out-of-band control-lane ping before readiness and every ten seconds while healthy; one missed pong removes capacity, suspect proofs repeat every two seconds, and three independent misses replace the sidecar generation without confusing request backpressure with liveness;
- replacement always keeps the same persisted Prism project UUID and worker index, increments only the leader generation, and reacquires a sandbox through `/api/backend/1/new` before resynchronizing that project;
- shared auth, Prism provisioning 429, and Prism 5xx failures pass through one fleet-wide revalidation/half-open probe circuit, preventing a provisioning stampede; provider/model 429s never enter recovery, while local `404`/`409`/`410` recovery bypasses that circuit;
- failed replacement attempts retry forever with capped exponential jitter while respecting the fleet provisioning-concurrency limit. Every replacement is a fresh V2 generation in the same Prism project. Healthy projects continue serving throughout recovery.

Heartbeat requests have a five-second deadline inside the exact ten-second cadence, so a hung proxy request cannot freeze liveness detection. The stable registry entry exposes zero capacity while its disposable child is recovering. Recovery can replace only the disposable sandbox session inside the same explicitly configured Prism project; it never creates, adopts, or deletes a project.

## Host supervision and rolling upgrades

`bun run start:supervised` runs a cross-platform supervisor that owns the public loopback port and proxies to one internal GhostProv worker. It restarts crashed workers with bounded backoff. A worker becomes selectable only after its own IPC channel proves the exact PID, allocated port, and supervisor generation; an unrelated local listener returning HTTP 200/503 can never satisfy readiness or receive queued API bodies. A planned `bun run upgrade` stops new worker dispatch, lets existing HTTP responses finish, closes persistent Responses WebSockets with restart code `1012`, gracefully stops the old worker, starts exactly one replacement, then releases requests queued at the stable public listener. The restart queue is bounded by both count and declared request bytes; overflowed HTTP and WebSocket handshakes receive an immediate credential-free `503 host_queue_full` instead of retaining bodies and timers for the full wait. Old and new workers never manage the same Prism projects concurrently. This rolling worker restart deliberately keeps the supervisor's immutable startup environment; it does **not** reload an edited `.env`.

The supervisor exposes `GET /__supervisor`, writes its current state to `.ghostprov/supervisor.json`, and appends bounded structured history to `.ghostprov/logs/supervisor.jsonl`. Status persistence coalesces bursts to at most one in-flight write and one latest dirty snapshot instead of writing once per forwarded request. `host.supervisor.sample` records request rate, proxy errors, exact queued request/byte pressure, rejections, active requests, WebSockets, generations, crashes, restart backoff, and upgrade duration. The first migration from a directly bound `18787` process still requires the operator to stop that old process once; after the supervisor owns the port, upgrades do not require rebinding it.

Each supervisor process also atomically publishes `.ghostprov/supervisor-control.json`, a non-secret discovery record written with mode `0600` where supported and containing its PID, start time, random instance nonce, configured control/status paths, and lifecycle timeouts. Every upgrade/shutdown request has an exact schema, random request ID, action, timestamp, and descriptor-derived PID/start/nonce target. The supervisor parses content identity rather than trusting filesystem timestamp ordering, so stale-generation writes, duplicate IDs, equal/coarse timestamps, and backward timestamps cannot control a fresh process. Helpers re-read the descriptor after their atomic request rename and retry against a replacement rather than claiming a stale write succeeded. Lifecycle waits are capped at 1,800 seconds; systemd retains a finite 1,860-second cgroup-owned last resort, so it never signals a bare PID from the descriptor and cannot wait forever on a hung supervisor.

Foreground controls work on both Windows and Linux:

```text
bun run start:supervised
bun run upgrade
bun run stop:supervised
```

For a foreground configuration change, run `bun run stop:supervised`, wait for the existing `bun run start:supervised` command to exit, and then launch `bun run start:supervised` again. This intentionally has a brief public-listener outage.

Windows user service (Task Scheduler, restarted on failure):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ghostprov_service.ps1 install
powershell -ExecutionPolicy Bypass -File scripts/ghostprov_service.ps1 restart
powershell -ExecutionPolicy Bypass -File scripts/ghostprov_service.ps1 config-restart
powershell -ExecutionPolicy Bypass -File scripts/ghostprov_service.ps1 status
```

Linux user service (systemd):

```bash
bash scripts/ghostprov_service.sh install
bash scripts/ghostprov_service.sh restart   # systemctl --user reload
bash scripts/ghostprov_service.sh config-restart  # systemctl --user restart
bash scripts/ghostprov_service.sh status
journalctl --user -u ghostprov.service -f
```

Use `restart` only for a rolling code restart with the current startup configuration. After editing `GP_V2_SIDECAR_REVISION`, `GP_V2_SIDECAR_PROJECT_REVISIONS`, project IDs, credentials, or any other `.env` setting, use `config-restart`. It requests a graceful drain of the whole supervisor/fleet, applies the configured bounded fallback if a drain cannot finish, launches a fresh supervisor that reloads `.env`, and on both Windows and Linux waits boundedly for a new descriptor nonce plus matching ready status, supervisor PID, worker PID, start time, and readiness time. A timeout exits nonzero rather than reporting a Type=simple/systemd launch as ready. Windows also reports whether the old process proved a clean drain instead of silently calling a forced stop graceful. Every new sandbox session is acquired inside the same explicitly configured Prism project; this path never creates, adopts, deletes, or silently falls back to another project or protocol revision. Environment variables explicitly supplied by the service manager still take precedence over `.env` and must be updated there as well.

`GP_V2_SIDECAR_REVISION=r8` selects the frozen production release for every configured project. R8 keeps R7's fenced R2 `bulk-v1` wire profile and immutable, accounted request-body snapshot, defensively forces upstream `stream:true` and `store:true`, and never replays a provider request after durable `FETCH_STARTING`. A definitive HTTP rejection, including `invalid_json`, is returned exactly once. Encoded requests through 256 KiB retain the exact R4 32-frame/512-KiB host flight, while isolated medium and bulk transfer may use 64 frames/1 MiB. Each runtime retains the six-unit pre-commit budget and four-`request_start` execution-risk fence; the router independently smooths the shared fleet link with a four-unit transfer gate and a 500 ms weighted start pacer. Requests above 3 MiB remain exclusive bulk work. R7 is the immediate immutable same-wire rollback but retains the superseded post-`FETCH_STARTING` `invalid_json` replay; R4 is the standard-profile rollback. `GP_V2_SIDECAR_PROJECT_REVISIONS` accepts exact configured `project-id=r1|r2|r3|r4|r5|r6|r7|r8` overrides. Rollback uses `config-restart`, a fresh sandbox session in each existing project, and exact build/schema/profile proof; it never falls back in place or creates projects. Older frozen releases remain only as explicit compatibility/forensic rollback fixtures.

Before shipping or starting a pinned R2 release, run both independent release gates:

```text
bun run check:v2-sidecar-release
bun run check:v2-sidecar-deno
```

The first deterministically rebuilds current sidecar source twice and compares the result, build hash, bundle hash, and R2 `bulk-v1` schema-profile hash with the checked-in R8 artifact. The second asks pinned Deno 2.9.5 to type/check that frozen offline bundle. Both are read-only and fail closed. Maintainers create a replacement artifact only through the explicit `bun run build:v2-sidecar-release` write command after source is frozen; the ordinary/default release command never overwrites an artifact.

For the long soak test, retain the application JSONL logs, `supervisor.jsonl`, `supervisor.json`, and monitor output. These contain only bounded counters/categories and no prompts, responses, cookies, API keys, or sandbox credentials.

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
| `GHOSTPROV_API_KEY` | `GHOST_IS_KING` | Shared inbound key required by every OpenAI/Anthropic `/v1/*` route and Responses WebSocket upgrade |
| `GP_SUPERVISOR_READY_TIMEOUT_MS` | `120000` | Maximum internal-worker startup time |
| `GP_SHUTDOWN_DRAIN_MS` | `300000` | Maximum wait for active terminal proofs during graceful shutdown |
| `GP_SUPERVISOR_GRACE_MS` | `330000` | Graceful drain before force-stopping only the owned worker |
| `GP_SUPERVISOR_REQUEST_WAIT_MS` | `480000` | Maximum public-listener queue wait during restart |
| `GP_SUPERVISOR_QUEUE_MAX_REQUESTS` | `256` | Maximum HTTP plus WebSocket handshakes retained while no worker is ready |
| `GP_SUPERVISOR_QUEUE_MAX_BYTES` | `67108864` | Maximum declared request/header bytes retained by that restart queue |
| `GP_V2_SANDBOX_CAPACITY` | `128` | Hard per-sidecar physical call ceiling |
| `GP_V2_INITIAL_ACTIVE_PER_SANDBOX` | `8` | Initial transport-safe active request limit |
| `GP_V2_MAX_ACTIVE_PER_SANDBOX` | `8` | Maximum active request limit backed by eight 64 MiB terminal-spool reservations |
| `GP_V2_RESPONSE_MAX_BYTES` | `67108864` | Fixed proven response/transcript maximum for this release |
| `GP_V2_SIDECAR_REVISION` | `r8` | Frozen global sidecar release; R8 uses R2 `bulk-v1`, R7 is the immediate immutable rollback, and R4 is the standard-profile rollback |
| `GP_V2_SIDECAR_PROJECT_REVISIONS` | unset | Exact configured `project-id=r1|r2|r3|r4|r5|r6|r7|r8` overrides; no wildcard or fallback |
| `GP_MAX_PROJECTS` | `32` minimum | Safety ceiling, not a fixed fleet size |
| `GP_HEARTBEAT_MS` | `10000` | Exact per-sandbox heartbeat period |
| `GP_ROUTER_QUEUE_TIMEOUT_MS` | `30000` | Exact ingress-to-transport admission deadline through 8 MiB; R5/R6/R7/R8 >2–8 MiB transfers retain useful ACK/prepared progress after the unchanged 15 s first-attempt boundary but never beyond 30 s; larger payloads scale linearly (13 MiB: 53.75 s, 64 MiB: 245 s) |
| `GP_V2_TRANSPORT_RESERVATION_BYTES` | `16777216` | Host capacity estimate per active V2 stream |
| `GP_V2_SIDECAR_MEMORY_BUDGET_BYTES` | `536870912` | Sidecar request/queue admission budget |
| `GP_V2_YSWEET_REFRESH_MS` | `2700000` | Encrypted reconnect-credential rotation horizon |
| WebSocket active responses | `16` | Per-connection active response ceiling |
| WebSocket named lanes | `32` | Per-connection named-lane ceiling |
| `GP_IMAGE_EDIT_MAX_INPUT_BYTES` | `41943040` | Raw multipart edit-input budget |
| `GP_IMAGE_EDIT_HTTP_MAX_BODY_BYTES` | `44040192` | Multipart envelope ceiling |
| `GP_ANTHROPIC_OPUS_MODEL` | `gpt-5.6-sol` | Default target for Claude Opus names |
| `GP_ANTHROPIC_SONNET_MODEL` | `gpt-5.6-terra` | Default target for Claude Sonnet names |
| `GP_ANTHROPIC_HAIKU_MODEL` | `gpt-5.6-luna` | Default target for Claude Haiku names |
| `GP_ANTHROPIC_DEFAULT_MODEL` | `gpt-5.6-sol` | Fallback for unknown model names |
| `GP_CLAUDE_VERIFY_PORT` | `18788` | Separate loopback-only verifier listener |
| `GP_CLAUDE_VERIFY_UPSTREAM` | `http://127.0.0.1:18787` | Loopback root used only by the verifier |
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
| `GP_V2_BATCHING` | `1` | Bounded 25 ms response batching; set `0` for explicit rollback |

## Tests

```powershell
bun test --isolate
bun run test:unit
bun run test:integration
bun run typecheck
bun audit --production
```

The continuous performance branch compares every transport experiment against a checked-in best-known baseline spanning short, medium, long, large, mixed, 40-agent, and 1,000 ms RTT workloads. Exact-size fixtures include the final upstream `stream:true` and `store:true` fields before padding, so canonical byte counts, digests, framing, and baseline sizes describe the bytes actually sent. The comparator rejects any exactness, concurrency, retransmission, 1 MiB window, useful-byte, tiny-p95, overall-p95, virtual-duration, or wire-volume regression. A candidate is marked `keep` only when it clears every regression gate and improves p95 or total modeled duration by at least two percent; otherwise it is `stable` or `reject`.

```powershell
# Normal no-regression comparison.
bun run benchmark:v2-research

# Experimental acceptance requires a measurable improvement.
bun scripts/v2_performance_research.ts --compare benchmarks/v2-performance-baseline.json --require-improvement

# Baseline replacement is intentionally explicit and is allowed only after a keep decision.
bun scripts/v2_performance_research.ts --record benchmarks/v2-performance-baseline.json --replace
```

The isolated suite includes:

- `tests/account-session.test.ts`
- `tests/api-key-auth.test.ts`
- `tests/anthropic-continuation-http.test.ts`
- `tests/anthropic-continuation.test.ts`
- `tests/anthropic-foundations.test.ts`
- `tests/anthropic-http.test.ts`
- `tests/anthropic-request-contract.test.ts`
- `tests/anthropic-request.test.ts`
- `tests/anthropic-response-stop.test.ts`
- `tests/anthropic-response-tool.test.ts`
- `tests/anthropic-response-usage.test.ts`
- `tests/anthropic-response-validation.test.ts`
- `tests/anthropic-response.test.ts`
- `tests/anthropic-stop-filter.test.ts`
- `tests/anthropic-stream-bounds.test.ts`
- `tests/anthropic-stream-protocol.test.ts`
- `tests/anthropic-stream-safety.test.ts`
- `tests/anthropic-stream.test.ts`
- `tests/anthropic-token-count.test.ts`
- `tests/claude-code-verifier.test.ts`
- `tests/compat-final-permit.test.ts`
- `tests/cpu-transform-pool.test.ts`
- `tests/decommission-script.test.ts`
- `tests/decommission.test.ts`
- `tests/documentation.test.ts`
- `tests/file-lease.test.ts`
- `tests/final-object-permit.test.ts`
- `tests/fleet-manifest.test.ts`
- `tests/fleet-manager.test.ts`
- `tests/graceful-shutdown.test.ts`
- `tests/heartbeat.test.ts`
- `tests/host-supervisor-service-control.test.ts`
- `tests/host-supervisor.test.ts`
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
- `tests/persisted-continuation-live.test.ts`
- `tests/preparation-scheduler.test.ts`
- `tests/prism-protocol.test.ts`
- `tests/project-inspector.test.ts`
- `tests/provisioning-client.test.ts`
- `tests/projects-client.test.ts`
- `tests/resource-governor.test.ts`
- `tests/recovery-chaos.test.ts`
- `tests/recovery-coordinator.test.ts`
- `tests/recovery-observability.test.ts`
- `tests/response-graph.test.ts`
- `tests/response-graph-governor.test.ts`
- `tests/responses-continuation.test.ts`
- `tests/responses-validation.test.ts`
- `tests/router-http.test.ts`
- `tests/router-observability.test.ts`
- `tests/router-policy.test.ts`
- `tests/router-provenance.test.ts`
- `tests/router-rate-limit.test.ts`
- `tests/sandbox-gateway.test.ts`
- `tests/sandbox-liveness.test.ts`
- `tests/supervised-runtime.test.ts`
- `tests/sandbox-ydoc.test.ts`
- `tests/session-manager.test.ts`
- `tests/server-main-lifecycle.test.ts`
- `tests/sse.test.ts`
- `tests/system-v2.test.ts`
- `tests/upstream-policy.test.ts`
- `tests/v2-awareness-channel.test.ts`
- `tests/v2-adaptive-capacity.test.ts`
- `tests/v2-batching-throughput.test.ts`
- `tests/v2-admission.test.ts`
- `tests/v2-assembler.test.ts`
- `tests/v2-atomic-byte-permits.test.ts`
- `tests/v2-atomic-event-large.test.ts`
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
- `tests/v2-native-transcript-hasher.test.ts`
- `tests/v2-output-sink.test.ts`
- `tests/v2-performance-harness.test.ts`
- `tests/v2-performance-large.test.ts`
- `tests/v2-performance-research.test.ts`
- `tests/v2-permit-carrier.test.ts`
- `tests/v2-protocol.test.ts`
- `tests/v2-provider-error.test.ts`
- `tests/v2-r2-execution-fence.test.ts`
- `tests/v2-r2-protocol.test.ts`
- `tests/v2-r2-start-handshake.test.ts`
- `tests/v2-rate-limit-protocol.test.ts`
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
- `tests/v2-sidecar-ownership.test.ts`
- `tests/v2-sidecar-release-build.test.ts`
- `tests/v2-sidecar-revisions.test.ts`
- `tests/v2-sidecar-recovery.test.ts`
- `tests/v2-sidecar-rotation.test.ts`
- `tests/v2-sidecar-storage-paths.test.ts`
- `tests/v2-split-awareness-channel.test.ts`
- `tests/v2-split-flight-integration.test.ts`
- `tests/v2-transcript.test.ts`
- `tests/v2-transport-session.test.ts`
- `tests/v2-upstream.test.ts`
- `tests/v2-upstream-incremental.test.ts`
- `tests/v2-window.test.ts`
- `tests/websocket.test.ts`
- `tests/websocket-telemetry.test.ts`
- `tests/workload.test.ts`
- `tests/ydoc.test.ts`

The deterministic integration test fills 120 lanes as `30 + 30 + 30 + 30`, verifies terminal settlement and unique execution IDs, and proves a draining project leaves the other 90 lanes usable.

## Live proof

The persisted-continuation verifier is the only live command authorized by the local protected-port allowlist. It hardcodes candidate port `18888`, requires 2-8 explicitly named dedicated test projects, never creates or deletes projects, and touches `18787` only with read-only `/health` checks. It proves explicit and inferred HTTP suffixes, public WebSocket and Claude full-history continuation, same-project restart hydration, different-project hydration, and 1/4/8/16-agent two-turn isolation:

```powershell
$env:GP_PERSISTED_CONTINUATION_LIVE = "1"
$env:GP_CONTINUATION_TEST_CONFIRM = "dedicated-test-projects"
$env:GP_CONTINUATION_TEST_PROJECT_IDS = "<test-uuid-1>,<test-uuid-2>"
$env:GP_CONTINUATION_TEST_COOKIE_FILE = "D:\secrets\test-prism-cookies.json"
bun run verify:persisted-continuation
```

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

Start the candidate on 18888, then run the staged `16 → 32 → 64 → 96 → 120` load proof:

```powershell
$env:GP_LIVE_VERIFY = "1"
$env:OPENAI_BASE_URL = "http://127.0.0.1:18888/v1"
bun run verify:multi-sandbox
```

The load script verifies exact output markers, unique response IDs, peak physical occupancy, per-runtime occupancy, zero under-capacity queueing, zero quarantines, and port 8787 health before and after the candidate run.

Run Codex-compatible HTTP streaming, provider-persisted WebSocket tool continuation, and eight concurrent named lanes:

```powershell
$env:GP_LIVE_VERIFY = "1"
$env:OPENAI_BASE_URL = "http://127.0.0.1:18888/v1"
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
$env:PORT = "18888"
bun run verify:v2-bootstrap
```

The single-project canary performs two complete bootstrap/drain/relaunch cycles plus HTTP and WebSocket checks. GPT Image usage remains separately opt-in with `GP_LIVE_IMAGE=1`:

```powershell
$env:GP_LIVE_V2 = "1"
$env:GP_LIVE_PROJECT_COUNT = "1"
$env:PORT = "18888"
bun run verify:v2-single
```

The paid four-project scale canary additionally requires `GP_LIVE_SCALE=1`. It runs 128 direct, 256 accepted, 512 routed, and 4 MiB integrity stages without creating or deleting projects:

```powershell
$env:GP_LIVE_V2 = "1"
$env:GP_LIVE_SCALE = "1"
$env:GP_LIVE_PROJECT_COUNT = "4"
$env:PORT = "18888"
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
