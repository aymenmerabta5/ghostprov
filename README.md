# GhostProv

GhostProv is a browserless OpenAI-compatible router over independent Prism sandbox projects. Per-project Deno Awareness sidecars distribute work while preserving terminal proof, provider-persisted continuation, GPT Image compatibility, and Claude/Codex streaming.

The project is pinned to Bun 1.4.0, TypeScript 7.0.2, and the hermetic `bun run test` gate. Playwright and Chromium are not runtime dependencies.

## Browserless configuration

Every sandbox uses a separate Prism project under the same authenticated account cookies. GhostProv owns the managed fleet registry and creates missing projects automatically.

```powershell
$env:PRISM_COOKIE_FILE = "D:\secrets\prism-cookies.json"
$env:GP_PRISM_SESSION = "authenticated"
$env:GP_TARGET_SANDBOXES = "8"
$keyBytes = New-Object byte[] 32
$rng = [Security.Cryptography.RandomNumberGenerator]::Create()
try { $rng.GetBytes($keyBytes) } finally { $rng.Dispose() }
$env:GHOSTPROV_API_KEY = [Convert]::ToBase64String($keyBytes)
$env:GP_CANDIDATE = "0"
$env:PORT = "18787"
bun run dev
```

`PRISM_COOKIE` accepts an initial Cookie header when a persisted cookie jar is unavailable. The account session consumes `Set-Cookie`, persists rotations atomically, and hot-reloads a newer cookie file. New installations do not configure project IDs. `PRISM_PROJECT_IDS` and `PRISM_DOC_ID` are accepted only as one-time migration seeds: imported projects are recorded as unmanaged and are never automatically deleted.

Port `18787` is the normal production listener used by `bun run dev`. Candidate and automated-test listeners must use `18888` or an OS-assigned port. Safety guards prevent candidate binding and destructive lifecycle commands from targeting the running production service; they do not prohibit normal production startup. Stable project UUIDs and worker indexes are persisted by Prism identity under `~/.ghostprov/fleets/<identity-fingerprint>/fleet.json`; restarts reuse that registry and do not create a second fleet blindly.

V2-only mode defaults to eight managed projects (`GP_TARGET_SANDBOXES=8`, bounded by `GP_MAX_PROJECTS=32`). Create intent is persisted before each idempotent UUID-addressed Prism request, and a restart resumes incomplete creation from the same record. GhostProv never adopts arbitrary account projects. The signed-in session is the production default. Explicit guest mode uses `GP_PRISM_SESSION=anonymous` and remains capped separately by `GP_GUEST_MAX_PROJECTS`; `GP_ALLOW_ANONYMOUS=1` only permits `GP_PRISM_SESSION=auto` to create a guest when no authenticated session is available.

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

R10 stages immutable assets through Yjs and starts a persistent supervisor through Prism's direct execution API by default. The original resumed Codex-session bootstrap remains selectable with `GP_V2_SUPERVISOR_BOOTSTRAP_MODE=llm`; R10 also supports `git-fsmonitor`, `git-ssh-signing`, `git-clean-filter`, and `git-textconv` through temporary Git callbacks. The supervisor owns sidecar lifecycle through authenticated control messages. Live requests use ephemeral Awareness frames; they do not request project exports or sandbox ZIP files.

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
- Global zero-capacity queueing fails with `router_capacity_timeout`; a request that selected a runtime but could not cross transport admission fails as `transport_admission_timeout`. The base is 30 seconds through 2 MiB, while observed long histories receive a bounded payload-aware deadline.
- Client cancellations and definitive upstream errors are routing-neutral; only transport-attributable failures feed a sixty-second decaying sandbox health score.
- ACK-stall age starts with the active reliable flight; idle time never suppresses credits, and a fully drained transport restores credits immediately. A reliable stall first sampled at ten seconds removes capacity, then a second independent sample two seconds later starts same-session R2 recovery; one slow sample never relaunches, and fresh ACK progress restores the normal ten-second cadence.
- Every project runtime is supervised. A dead child first receives fresh sandbox sessions for the **same Prism project and worker index**. Only repeated identical project-local R9/R10 bootstrap failures can nominate the project itself for replacement. The content-free failure signature, observation identity, count, timestamps, duration, and durable leader generation are persisted, duplicate callbacks do not advance the threshold, and any verified readiness clears the evidence.
- R10 ownership conflicts and unproven bootstrap cleanup use an automatic capacity-restoration path after the configured identical-failure threshold. The runtime fences the failed project, persists `bootstrap_hold`, and creates a fresh managed successor through normal fleet reconciliation. Its old project, journal, and continuation hints are retained; no old request is replayed and no held project is automatically deleted or reactivated. Ordinary holds survive restarts and are limited to `min(4, GP_MAX_PROJECTS)`, within the 128-record manifest bound. The explicitly prepared legacy migration ledger described below can grant at most two additional exact project holds, with a hard total of six. Healthy-majority, zero active/quarantined request work, and closed outage/mutation circuits are required. `GP_MAX_PROJECTS` continues to bound active projects; held records use the existing retirement allowance. `/__sandboxes.heldProjects` and `fleet.sample.held_project_count` expose retained projects. This runs automatically in normal `bun dev`; it does not depend on a repair script.
- One isolated sidecar failure recovers immediately. A second distinct project failing within five seconds activates the existing fleet single-probe circuit, preventing an account outage from reprovisioning every sandbox at once.
- Replacement-intent admission is serialized and fail-closed: a healthy majority running the configured supervisor release, closed fleet and project-mutation circuits, zero active/quarantined/ambiguous work, and zero retained continuation bindings are required. A verified successor may unblock the next failed minority even while an older remote deletion is pending, so stale retirement records cannot deadlock recovery. GhostProv persists replacement intent, creates and verifies the successor, drains the old runtime, and then deletes only an automatically managed old project. Imported projects are retired locally but never remotely deleted. Common-mode internet, account, rate-limit, timeout, or Prism failures freeze create/delete with bounded jittered backoff.
- Projects that have never reached verified readiness use a separate two-start permit pool before entering the general four-start pool. Persisted `hasReachedReady` proof survives later failed states and restarts, so ordinary warm recovery keeps the full general headroom without allowing a cold Y-Sweet/render wave to start all projects simultaneously.

## Concurrency and latency

One V2 sidecar retains a hard protocol ceiling of 128 simultaneous upstream calls, while the proven 512 MiB terminal-spool quota reserves exactly 64 MiB per prepared request and therefore caps production admission at 8. `GP_V2_RESPONSE_MAX_BYTES` is fixed to exactly 64 MiB in this release and configuration rejects any other value. The default eight-project fleet exposes 64 durability-backed calls; fleets are dynamically tested at 1, 4, 8, 16, and 32 projects and have no fixed-four implementation limit. Actual admission is:

The sidecar claim journal is bounded at 64 MiB and 100,000 checksummed records during both runtime and restart. New claims reserve their worst-case remaining transition/outcome space before acceptance, and exclusive compaction retains one minimal state snapshot per unresolved fence, terminal proof, provider outcome, or acknowledged duplicate fence. Per-request mutation lanes serialize terminal/provider/forced outcomes, with forced-deadline intent revalidated after terminal promotion. Torn unterminated UTF-8 tails are removed only after every complete line passes fatal decoding and checksum validation, and bounded orphan rewrite temps are cleaned under ownership. On POSIX, hierarchy creation, the first claim append, journal replacement, and terminal promotion fsync their directory entries before reporting durability.

Host response events use one process-wide atomic byte-permit pool reserved in full through the global resource governor before startup. For each active slot, its ordinary region covers one in-progress assembly (`512 KiB × 4 + 1 MiB`), the full retained payload queue (`512 KiB × 4`), and metadata for at most 512 buffered events (`512 × 4 KiB`); that per-slot sum is multiplied by configured projects and the proven active limit. Its single jumbo lane is `GP_V2_RESPONSE_MAX_BYTES × 4 + 1 MiB`. Startup fails closed when that complete reservation cannot fit. A response queue is bounded by both 512 KiB and 512 events, while a larger canonical SSE event is accepted only with exclusive jumbo coverage. Competing sessions acquire that lane through a bounded FIFO instead of failing merely because another large image is being delivered; teardown and same-sandbox recovery cancel outstanding waits before joining frame processing. When either bound is full, the host withholds its cumulative data ACK and propagates backpressure through the sidecar window instead of turning a temporary consumer pause into committed ambiguity. Malformed same-request reordering that could wait behind its own retained event fails closed rather than deadlocking the reliable lane. Permit ownership follows the event through the run handle, final object, and HTTP/WebSocket delivery or cache eviction, and fleet samples expose ordinary/jumbo usage and waiters.

Pre-commit request transfer is byte-weighted independently from upstream model concurrency. Standard profiles retain the established four-way per-runtime admission; R5/R6/R7/R8/R9/R10 can pipeline six medium bodies per runtime only behind an independent four-`request_start` execution-risk fence. A separate fleet-wide four-unit gate admits at most two observed 2–3 MiB transfers, while 256 KiB–2 MiB starts use a 500 ms weighted pacer and requests through 256 KiB bypass both controls. This prevents synchronized bodies from collapsing the shared Prism/Y-Sweet link without reducing committed model concurrency or the tiny-request fast path.

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

Large HTTP bodies reserve memory before buffering. Gzip runs in bounded workers and is used only when it reduces frame count. Inline-image requests through 256 KiB retain the zero-compression latency path; larger single- or multi-image data-URL bodies use balanced compression to recover base64 expansion before transport without changing image bytes. Response ACK publication stops when a client buffer is full, providing end-to-end backpressure without unbounded memory. Each V2 peer shares one 25 ms data-publication cadence across all streams, keeps ACK/control traffic urgent and frame-free, freezes one revision/direction-specific flight until cumulative ACK, and derives a five-second-bounded SRTT/RTT-variance retransmission clock from first-send sequence timing. A live control update fast-retransmits a stale flight. A measured two-second no-progress interval temporarily advertises zero new credits; one recovered stall restores full concurrency, while repeated stalls within one minute adaptively derate the affected sandbox. R2's larger 512 KiB flight applies only from host to sidecar; sidecar-to-host response traffic remains 128 KiB.

## Persisted continuation policy

Every Responses body sent beyond the router is normalized, copy-on-write, to explicit `stream:true` and `store:true`. Provider-side response state and its approximately 30-day retention window are accepted product behavior. Caller-facing HTTP mode remains independent: a client may request a normal JSON final while the internal upstream call still streams. An exact caller-supplied `previous_response_id` is preserved unless GhostProv is expanding one of its reserved synthetic WebSocket warmup aliases.

Generated WebSocket turns are provider-backed. GhostProv forwards only the current input delta with the provider `previous_response_id`; it does not retain or replay generated prompts, images, reasoning, tool arguments, or outputs. Connection-local graph storage is limited to explicit `generate:false` warmup aliases, with count, byte, process, scope, and connection-lifetime bounds. A generated child expands that warmup prefix once and then continues by its provider response ID. If a Codex fork supplies a fully reasoning-free legacy warmup containing only strict text assistant output messages, GhostProv copy-on-write projects those retained `msg_*` items into ordinary assistant input messages and starts a new stored text lineage; it never fabricates reasoning, changes the current generated delta, rewrites a provider-backed ancestor, or retries a rejected request. Exact reasoning/output sequences and unsafe or lossy shapes remain unchanged for definitive validation.

Native Responses HTTP clients that resend an exact full input array use a separate app-scoped, content-free prefix index, including histories up to the existing 64 MiB canonical limit. When `prior input + lossless provider-output replay` matches an item-boundary prefix under identical model settings, GhostProv forwards only the nonempty suffix with the stored provider response ID. Known Codex tracing labels in `client_metadata` are forwarded unchanged but excluded from matching; unknown metadata remains part of the exact settings digest. Both the original output and Codex's replay spelling (omitted completed status and empty annotations/logprobs) share the same bounded index budget. Text, reasoning ciphertext, IDs, nonempty metadata, tools, and unknown fields remain exact. Other client rewrites are safe misses, and an explicit caller `previous_response_id` always bypasses inference. A definitive pre-event stale inferred parent is compare-deleted and retried once with the immutable full input; explicit IDs and ambiguous failures are never replayed.

Request canonicalization, hashing, and compression have a separate 15-second preparation deadline, still bounded by the original routing deadline and cancellation signal. Preparation expiry returns `request_preparation_timeout` once instead of repeatedly redoing CPU work on another sandbox. Fleet transfer admission retains four ordinary weighted units, with a six-unit ceiling for one four-unit bulk upload plus two units for medium traffic. An aged bulk waiter preserves that medium headroom; queue passes consider each waiting request at most once. Runtime limits, memory reservations, preparation proof, and execution-risk fences remain authoritative.

The Anthropic adapter owns one app-scoped, process-local continuation index shared by stable and beta Messages routes. It retains only keyed rolling digests, provider response IDs, counts, and bounded lifecycle metadata: at most 4,096 records, a conservative approximately 6 MiB heap estimate, and a 60-minute absolute TTL. A completed, losslessly round-trippable Claude history can reuse the longest unique prefix and send only its nonempty suffix. Digest preparation charges only memory not already covered by the global ingress lease, so a bounded 16-agent large-history burst does not silently fall back to bulk relay while the count and governor limits remain authoritative. A definitive HTTP `previous_response_not_found` before any public event permits one full-history fallback; ambiguous, streamed, generic, and explicit-ID failures are never replayed.

One opaque authenticated scope is constructed for the application and passed through HTTP, WebSocket, Claude, and router composition. The router reserves bounded concurrent commit capacity, commits completed provider response IDs to their exact project/incarnation before public terminal exposure, and prefers known continuations there. Provenance is a one-hour, content-free routing hint bounded to 4,096 records and 1 MiB; after immutable conflict checks, commit-time oldest-hint eviction prevents metadata pressure from becoming a global request quota. Unknown or expired provider IDs route normally, while missing scope, cross-scope IDs, and conflicting live bindings fail closed.

Per-runtime completion estimates cap extreme model durations and decay halfway toward a neutral ten-second baseline every idle minute. Successful responses update that estimate even when they include rate-limit budget headers. Routing throughput is learned only from uniquely acknowledged request-data flights, never cumulative publications, retransmissions, or idle ACK/control heartbeats; a stale goodput sample returns to neutral after one idle minute so a recovered sandbox cannot remain permanently starved. Host transcript hashing uses exact-sized buffers for ordinary events and a bounded scratch path for jumbo events, while WebSocket reasoning redaction uses copy-on-write so ordinary streaming events are not deep-cloned.

Fresh projects are initialized with Prism's current Yjs root/settings schema. Non-secret sidecar sources are content-addressed and written only when changed. The temporary launcher and encrypted credential entry are tombstoned after matching Awareness readiness; no cookie, OpenAI key, resource token, or Y-Sweet token is written in plaintext.

The historical live-load baseline measured successful total latency around 6.9/19.4 seconds p50/p95 and showed that host CPU was not saturated. The isolated R6 fleet subsequently completed 40/40 incompressible 1 MiB requests without body corruption, 15/15 incompressible 2.9 MiB requests in three-request waves without capacity timeouts (6.26/20.27 seconds p50/p95), a 14-agent mixed 128 KiB/1 MiB/2.9 MiB wave in 27.94 seconds, and 14 simultaneous 128 KiB requests in 8.94 seconds. The isolated R7 fleet then completed five exact 14-agent observed-distribution waves (70/70) with zero capacity timeouts, zero `invalid_json`, zero load-time recoveries, and 4.89/25.79/29.41 seconds total p50/p95/max while all eight runtimes remained healthy. Later incident telemetry showed healthy 3.68-3.82 MiB histories compressing to roughly 1.72-1.78 MiB and progressing near 50 KiB/s, which cannot reliably cross a fixed 30-second admission deadline. The payload-aware bound now begins above 2 MiB while preserving the 15-second no-progress failover attempt and the established 245-second legal 64 MiB ceiling.

Current acceptance targets remain router decision p95 below 2 ms, queue p95 below 250 ms whenever local capacity is available, and no CPU/event-loop or memory saturation. Interpret high `commit_ack_ms` with RTT/retransmission counters as transport-link pressure; interpret high CPU/event-loop delay with normal RTT as local work; interpret normal commit with high first-event/total latency as model/upstream pressure.

One sequential Codex task normally uses one lane. High utilization requires multiple tasks, API callers, named WebSocket lanes, or image fan-out.

## HTTP Responses

```powershell
$env:OPENAI_BASE_URL = "http://127.0.0.1:18787/v1"
$env:OPENAI_API_KEY = $env:GHOSTPROV_API_KEY
curl.exe -X POST http://127.0.0.1:18787/v1/responses `
  -H "Authorization: Bearer $env:GHOSTPROV_API_KEY" `
  -H "content-type: application/json" `
  -d '{"model":"gpt-5.6-sol","store":true,"input":"Reply with hello"}'
```

Every `/v1/*` OpenAI-compatible route requires `Authorization: Bearer <GHOSTPROV_API_KEY>`, including the Responses WebSocket upgrade. `GHOSTPROV_API_KEY` must be explicitly configured as a single-token value of at least 12 characters; a random 32-byte value remains recommended. There is no shared development-key fallback. `GET /health` remains unauthenticated, while `GET /__sandboxes` and the supervisor's `GET /__supervisor` require the same bearer credential. Use `stream:true` for incremental SSE; omitted or false client `stream` still produces a JSON final while the upstream body is normalized to `stream:true`. Client `store` hints are accepted for compatibility but the upstream body is always `store:true`. Every non-image upstream request is forced to `service_tier:"fast"`; image-tool requests continue to omit unsupported service tiers. Reasoning fields including `effort:"max"`, `mode:"pro"`, `context`, and `summary` pass through verbatim.

## Codex standalone web search

GhostProv implements Codex's unary `POST /v1/alpha/search` contract. It does not forward the private sandbox `/alpha/search` shape. Instead, the validated Codex command batch is translated into a native stored Responses request with the hosted `web_search` tool and is executed through the same bounded router, exact-once V2 transport, sandbox fleet, authentication scope, and `service_tier:"fast"` policy as ordinary Responses traffic. No separate search API key or second proxy is required.

The route accepts current `search_query`, `image_query`, `open`, `click`, `find`, `screenshot`, finance, weather, sports, and time command objects. A request ID gets tenant-scoped, content-free serialization and provider `previous_response_id` continuity for follow-up reference commands; the index retains no search prompt or result content. Request collection is capped at 16 MiB and the public unary JSON result at 8 MiB. Unsupported or malformed commands fail before sandbox admission, while provider status and bounded error metadata are preserved.

Point Codex at the normal GhostProv provider and enable live search in `config.toml`:

```toml
model_provider = "ghostprov"
web_search = "live"

[model_providers.ghostprov]
name = "GhostProv"
env_key = "OPENAI_API_KEY"
base_url = "http://127.0.0.1:18787/v1"
supports_websockets = true
supports_standalone_web_search = true

[features]
standalone_web_search = true
```

The explicit feature flag also enables the standalone tool for non-Responses-Lite models such as `gpt-5.5`; GhostProv's Sol, Terra, and Luna catalog entries advertise Responses Lite and can expose it from provider capability alone.

## Anthropic Messages and Claude Code

The production Anthropic adapter is part of the same `createGhostProvApp` and uses the same router, queue, sandbox capacity, and listener as the OpenAI routes. Point Claude Code directly at the GhostProv origin; `ANTHROPIC_BASE_URL` is a root URL and must not end in `/v1`:

```powershell
$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:18787"
$env:ANTHROPIC_API_KEY = $env:GHOSTPROV_API_KEY
claude
```

The equivalent values are `ANTHROPIC_BASE_URL=http://127.0.0.1:18787` and `ANTHROPIC_API_KEY=<GHOSTPROV_API_KEY>`. Anthropic routes require either an exact `x-api-key: <GHOSTPROV_API_KEY>` or `Authorization: Bearer <GHOSTPROV_API_KEY>`. The SDK adds the required `anthropic-version: 2023-06-01` header itself, so normal Claude Code setup does not configure that header separately.

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
- Native `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-6-astra`, and `gpt-5.5` IDs pass through.
- Unknown model names use `gpt-5.6-sol`.

`gpt-6-astra` appears in both the OpenAI-compatible and Codex model lists at `/v1/models`. Select that exact ID for native Responses or Messages requests, including tool-result turns. Its Codex entry uses standard Responses and advertises reasoning efforts `low`, `medium`, `high`, `xhigh`, and `max`, with a maximum context window of 1,050,000 tokens from the [official Astra model documentation](https://developers.openai.com/api/docs/models/gpt-6-astra). The initial Codex context budget remains the existing 272,000-token default.

Override those defaults with `GP_ANTHROPIC_OPUS_MODEL`, `GP_ANTHROPIC_SONNET_MODEL`, `GP_ANTHROPIC_HAIKU_MODEL`, and `GP_ANTHROPIC_DEFAULT_MODEL`. Responses continue to echo the Claude model name requested by the client.

Current mid-conversation `system` messages are enabled for the exact normalized model IDs `claude-fable-5`, `claude-mythos-5`, `claude-opus-4-8`, and `claude-opus-5`. A system section must immediately follow a user turn and be final or immediately followed by an assistant turn. Consecutive `system` messages are coalesced into one section before placement validation. Other `claude-*` models reject message-level `system`; top-level `system` remains the ordinary instructions field. For Claude Code compatibility with GhostProv model names, message-level `system` is accepted for custom model IDs, while `developer` is accepted only for custom non-`claude-*` model IDs.

Messages supports streaming and non-streaming text, client tool use/results (including parallel calls), provider-persisted encrypted-reasoning continuity, current image/document inputs, structured output, prompt-cache annotations, local token estimates, cancellation, and Anthropic error envelopes. The translated Responses traffic retains OpenAI's default prompt caching; live acceptance reports cache-read and cache-write tokens rather than claiming a separate Claude-side cache. Stable and beta routes share the app-scoped digest index described above; public JSON and `message_stop` terminals are emitted only after the authoritative router provenance and eligible Claude index commits. Token counts are conservative local estimates rather than provider billing counts. Anthropic server-hosted tools and exact manual thinking-budget enforcement are non-goals; unsupported content/tool variants return a bounded `invalid_request_error`. Legacy Completions, Message Batches, and Anthropic Files/Admin APIs are not implemented. `GET /v1/models` remains the OpenAI-compatible GhostProv route, not an Anthropic Models response.

### Separate-port forwarding verifier

`bun run verify:claude-code` is a disposable branch verifier, not a production proxy. It defaults to `127.0.0.1:18788` and forwards translated Responses streams to the isolated candidate root in `GP_CLAUDE_VERIFY_UPSTREAM` (default `http://127.0.0.1:18888`). It refuses the protected service port `18787`. Override the listener with `GP_CLAUDE_VERIFY_PORT` only when another non-protected loopback port is needed:

```powershell
$env:GP_CLAUDE_VERIFY_PORT = "18788"
$env:GP_CLAUDE_VERIFY_UPSTREAM = "http://127.0.0.1:18888"
bun run verify:claude-code
```

In a second terminal, point Claude Code at the verifier rather than the production listener:

```powershell
$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:18788"
$env:ANTHROPIC_API_KEY = Read-Host "Enter the same explicit GHOSTPROV_API_KEY"
claude
```

The verifier refuses listener ports `18787` and `8787`, non-loopback upstream URL credentials, paths, queries, fragments, and self-forwarding. It forwards only the configured `GHOSTPROV_API_KEY` bearer credential to the loopback GhostProv upstream. It never enumerates, starts, stops, restarts, or signals the upstream process. Ctrl+C closes only the verifier listener and its outbound requests. Live Claude Code calls through this verifier belong to the separate live-verification task; automated tests import it without opening a listener or contacting the upstream.

Reliability defaults allow eight concurrent forwards and reserve four times the 2 MiB per SSE event parser cap, enforcing a 64 MiB aggregate parser-memory ceiling. Admission permits at most 32 forward waiters and 32 incoming-body waiters, with a 128 MiB aggregate incoming-body byte budget; excess waiters receive a typed sanitized overload. Every admitted request has a 300-second forward deadline covering response headers through stream completion, and each streamed run has a 1 MiB per-run output queue with upstream backpressure.

## Codex WebSocket mode

WebSocket mode follows the [official OpenAI Responses WebSocket contract](https://developers.openai.com/api/docs/guides/websocket-mode/). Clients send direct `response.create` events to `ws://127.0.0.1:18787/v1/responses`. Named lanes execute independently, same-lane work remains FIFO, and `previous_response_id` lineage is separate from lane identity.

Streaming is implicit in WebSocket mode. Clients normally omit `stream`; `stream:true` is accepted as a compatibility hint, while `stream:false` is rejected. GhostProv normalizes persistence before graph resolution and sends `stream:true` and `store:true` to the sandbox.

Large Codex histories automatically fall back to HTTP/SSE after exceeding the WebSocket payload limit. A local parser rejection on an authenticated, otherwise idle connection records a five-minute transport hint for the task. The next authenticated upgrade for that task returns `426`, which Codex recognizes as an immediate HTTP fallback even when its stream retry limit is high. Other tasks continue using WebSockets. Codex keeps HTTP fallback for the remainder of that client session; GhostProv does not switch it back mid-session. The listener retains at most 1,024 keyed digests and expiry times, never raw task identifiers or history, and clears them on shutdown. This hint cannot authorize a request or replay an active relay: host/origin, API-key, and exact trusted-scope checks remain mandatory, and HTTP requests retain the original input and explicit `previous_response_id`. Unsampled `websocket.payload_rejected` and `websocket.http_fallback` events expose the byte limit and fallback outcome without request content. The HTTP body limit remains 64 MiB by default. Run `bun run verify:codex-ws-fallback --run --codex-executable <installed-codex>` for the isolated installed-client verifier; it uses an empty temporary home, dummy authentication, an OS-assigned loopback port, and a fake router with no provider calls.

The normal WebSocket fast path remains 4 MiB and every upgraded connection pre-reserves a complete 17 MiB raw/decode/parse working set. The hard 768 MiB ingress ceiling also preserves one shared 48 MiB promotion lane, supporting 42 simultaneous default connections while any one of them accepts an authenticated message through 16 MiB without changing sockets. The independent 512 MiB queued-request ceiling remains a hard process bound. Actual ingress, the shared promotion guarantee, queued request bytes, and synthetic warmup graph bytes acquire elastic global-governor backing before they may be retained; unused theoretical ceilings consume no admission headroom, and every release, disconnect, eviction, abort, and shutdown returns its backing. Promotion is synchronous; process-budget or governor exhaustion closes only that connection with code `1013` instead of retaining an unbounded or reordered queue. Bun 1.4's current `node:http` compatibility path fixes its parser at 16 MiB even though native `Bun.serve` can configure a larger limit. The legacy `x-ghostprov-ws-jumbo: 1` header therefore maps to normal bounded admission on Bun instead of wasting a 257 MiB reservation. Payloads above the compatibility-path bound are rejected before relay admission. Telemetry reports interval and cumulative promotion/rejection counts, parser and upgrade rejections, byte peaks, current ingress reservations, global reservation headroom, and elastic pool current/maximum bytes without request content. It also records a fixed first-wins close-cause taxonomy plus the number of sessions and already-active responses closed; peer close reasons, codes, identifiers, URLs, and error text are not retained. Server ping/pong detects half-open peers, and an otherwise idle connection expires after five minutes; a live active relay is never expired merely because upstream reasoning is temporarily silent. Generated responses retain no local history copy: ordinary provider IDs carry only the current delta, while explicit `generate:false` warmup aliases are scoped and byte-bounded. GhostProv neither resizes nor recompresses images, and it withholds every public terminal until the authoritative final, router provenance, and any eligible continuation-index commit have completed.

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

The Images routes and `/v1/models` support these exact model IDs:

| Model IDs | Quality | Transparent background |
| --- | --- | --- |
| `gpt-image-2`, `gpt-image-2-2026-04-21` | `low`, `medium`, `high`, `auto` | Not enabled in this compatibility route |
| `gpt-image-2.5-flare`, `gpt-image-2.5-flare-2026-09-08` | `low`, `medium`, `high`, `xhigh`, `max`, `auto` | PNG or WebP |
| `gpt-image-2.5-sunburst`, `gpt-image-2.5-sunburst-2026-09-08` | `low`, `medium`, `high`, `xhigh`, `max`, `auto` | PNG or WebP |

Omitting `model` selects `gpt-image-2`. [Flare](https://developers.openai.com/api/docs/models/gpt-image-2.5-flare) is the speed-oriented image option; [Sunburst](https://developers.openai.com/api/docs/models/gpt-image-2.5-sunburst) emphasizes editing precision. Both are selected through the existing Responses `image_generation` tool. Image 2.5 custom sizes require edges divisible by 16, at most 3840 pixels per edge, a 1:3 to 3:1 aspect ratio, and 655,360–8,294,400 total pixels. Transparent output uses `background: "transparent"` with PNG (the default format) or WebP. These parameters follow the [official image-generation guide](https://developers.openai.com/api/docs/guides/image-generation).

Native `n=1..10` fans children across independent sandbox runtimes. GPT Image responses return `b64_json`, including the provider's `xhigh`/`max` quality metadata when present. Image streaming, URL emulation, audio, and DALL-E variation routes return explicit unsupported-capability errors. Compatibility validation is offline; availability of a new image model on a particular Prism upstream requires separate authorized live verification.

## Performance observability

- `GET /health` returns aggregate capacity, active, available, queued, desired/ready/unavailable fleet counts, bounded exact unavailable project UUIDs, runtime count, and an exact release summary (`releaseRevision`, `protocolRevision`, `buildHash`, and `schemaHash`); zero routable capacity returns HTTP 503 with `ok:false`.
- `GET /__sandboxes` returns sanitized per-project lifecycle, exact verified release, slot, heartbeat, latency, failure, quarantine, and resource snapshots. A runtime which has not proven a V2 release reports all three release fields as `null`; sandbox instance/session IDs and credentials are never exposed. The originating runtime retains its latest observation-only `upstreamRateLimit` problem and parsed organization/model/limit/used/requested/remaining/retry evidence; no unsupported `/v1/me` call is made through the scoped sandbox proxy.

Routing treats either a fresh direct sandbox heartbeat or a fresh envelope from the exact bootstrap-bound V2 sidecar as liveness proof; lifecycle state and credential expiry still fail closed. Admission-credit ownership does not end R2 failover by itself: the per-route deadline remains active until `request_start` enters the cumulative reliable window. For R5/R6/R7/R8/R9/R10 requests above 2 MiB through 8 MiB, the first no-progress attempt remains 15 seconds: no recent request-data ACK retries another runtime, while a fresh cumulative ACK or exact `request_prepared` proof keeps the already-transferred bytes on that runtime. Freshness is four times the selected RTT clamped to 5–10 seconds and never extends the one payload-scaled global deadline. If stale progress leaves enough time for a complete retransmit, failover remains available; otherwise a provably finishable current copy is retained. Requests through 2 MiB and standard/R4 rollback retain their exact prior attempt behavior. Work stalled before the execution fence can move to another runtime, while work at or beyond the fence is never duplicated.

Start the read-only external monitor against an already-running router without restarting it:

```powershell
bun run monitor:v2
```

It polls only `GET /health` and authenticated `GET /__sandboxes`, retries failures with capped exponential jitter, and writes `monitor.sample` or sanitized `monitor.error` JSONL events. The default target is `http://127.0.0.1:18787`; it reads `GHOSTPROV_API_KEY` for the sandbox snapshot, and non-loopback roots or roots containing paths, credentials, queries, or fragments are rejected. Ctrl+C stops only the monitor and flushes its log.

Built-in tracing starts with the upgraded router after the operator's next manual restart. It records `request.accepted`, `admission.acquired`, `request.queued`, `route.selected`, `route.attempt_expired`, `transport.committed`, `response.first_event`, and exactly one `request.completed` or `request.failed`. Selected routes and expired attempts include a bounded capacity reason without request content. Commit events include decoded/encoded request bytes, compression ratio, part count, encoding, assembly, durable claim, prepare-to-start, fetch-start sync, scheduler, and transport-residual timing. A periodic `fleet.sample` adds separately measured host external/ArrayBuffer memory, CPU, event-loop delay, logger pressure, aggregate capacity, lifecycle-ready and actually-routable runtime counts, pending retirements, project-mutation circuit state/retry time, per-runtime availability reasons, interval requests/second and success rate, response bytes, retries, uniquely acknowledged goodput, retransmitted bytes, stall transitions, observation-only upstream rate-limit evidence, and bounded p50/p95/max distributions for queue wait, commit ACK, first byte, and total latency. When the sidecar runtime exposes them, the same sample includes process CPU deltas, ArrayBuffer bytes, and bounded rolling GC/event-loop observations; missing platform metrics stay `null` rather than being estimated. Full samples remain in JSONL while the console prints a fixed-size fleet rollup. The external monitor provides immediate fleet visibility, but only built-in tracing can attribute per-request queue, commit, first-event, and total latency.

Recovery tracing adds `sandbox.recovery.started`, `sandbox.recovery.retry_scheduled`, `sandbox.recovery.completed`, `sandbox.recovery.aborted`, `sandbox.credential_refresh`, and transition-only `prism.circuit.changed`. Retry records are always visible on stdout and expose only normalized bootstrap/YDoc categories. Project replacement additionally emits `fleet.replacement.qualified`, `.started`, `.successor_reserved`, `.successor_created`, `.successor_activated`, `.old_retired`, transition-only `fleet.mutation.blocked`, and `fleet.startup_permits.changed`. Recovery events report `attempt_duration_ms` separately from total `duration_ms`, so retry backoff is never mistaken for a slow provisioning attempt. These events contain only project/worker indexes, bounded categories, counters, durations, and HTTP status codes—never sandbox/session IDs, URLs, cookies, tokens, prompts, or responses. `/__sandboxes` exposes process-local runtime incarnations, bounded recovery counters, sidecar peer age, pending retirements, and the sanitized project-mutation circuit; periodic `fleet.sample` additionally records both provisioning and project-mutation circuit state.

Transition-only `sandbox.liveness` records identify Prism heartbeat HTTP, timeout, and transport failures; exact-sidecar probe timeouts and reliable-window stalls; same-session recovery outcomes; and Y-Sweet Awareness failures by data/control lane. Awareness records retain only a bounded numeric connection code, such as WebSocket close code `1006` or HTTP-style `401`/`403`, while liveness records include current heartbeat/sidecar-peer ages and transport connectivity. Routine successful heartbeats and probes remain silent; only recovery from a prior failure is recorded.

Transient Awareness loss first receives one reconnect attempt using the existing credential, bounded to three seconds and the original scheduled renewal deadline. The host must resynchronize and receive a pong from the exact bootstrap-bound sidecar before restoring routing. Authentication/state failures, unsuccessful proof, or due renewal use the existing credential-rotation path. The transport retains its in-flight requests and reliable windows; reconnection does not submit another provider request. Successful reconnect diagnostics include `reconnect_phase` to distinguish restored host transport from credential rotation.

An R2 runtime owns one liveness-recovery episode for at most 150 seconds from its first failed probe or Awareness disconnect. Repeated preflight failures cannot extend that deadline. The outer supervisor respects this ownership while capacity is zero; definitive lifecycle failures and expiring credentials still take precedence. Expired recovery returns to the existing recovery path, and both `heartbeat_stale` and `heartbeat_missing` use the shared outage circuit. Liveness events distinguish `waiting_for_transport_recovery` and `recovery_deadline_expired`; full fleet samples include `liveness_recovery_remaining_ms`. This coordination does not reclaim unproven bootstrap journals or erase held projects.

The dedicated `scripts/live_transport_outage_recovery.ts` verifier requires `GP_LIVE_TRANSPORT_OUTAGE_RECOVERY=1`, `GP_LIVE_ALLOW_TEST_PROJECT_MUTATION=1`, and an explicit `PRISM_COOKIE_FILE`. It is fenced to port `18888`, creates eight disposable projects with an isolated manifest and cookie copy, and applies bounded heartbeat/control/data loss only to their host transport lanes. Real provider requests exercise 30-, 60-, and 90-second interruptions. It verifies retained generations, successful terminals, public Responses/Messages routes, readiness after recovery, supervisor shutdown, and dedicated-project cleanup. Run it with `bun --no-env-file scripts/live_transport_outage_recovery.ts` only for an authorized live task.

`request.accepted.reasoning_effort` records only a recognized effort label, `unspecified`, or `unknown`. Join it with terminal `first_text_ms`/`first_tool_ms` and upstream/transport timing to distinguish heavy reasoning from delayed delivery. This telemetry does not change the requested effort or retain reasoning content.

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
4. Compare `post_commit_first_event_ms` with `upstream_first_event_ms` and host/sidecar delivery diagnostics to separate upstream waiting from transport delay. The first event may contain only metadata.
5. Compare `first_text_ms`, `first_tool_ms`, and `upstream_total_ms` with total duration to distinguish text/tool events and provider execution from later delivery or finalization. Null means unobserved; these overlapping diagnostics must not be summed.
6. One runtime with worse RTT, completion EWMA, failures, or quarantine indicates sandbox imbalance.
7. High event-loop p95/max points to local CPU or synchronous work.
8. A runtime `rate_limit_observation` identifies upstream provider pressure; it is evidence only and must never coincide with GhostProv provider-gate queueing.
9. `ambiguous_quarantined >= ambiguity_limit` should be followed by one supervised runtime replacement; cancellation-only churn must leave `ambiguous_quarantined=0`.

For boundary-level V2 diagnostics, explicitly enable instrumentation, manually restart once so the sidecars redeploy, then run the paid controlled 1/4/8/16 WebSocket probe:

```powershell
$env:GP_V2_DIAGNOSTICS = "1"
$env:GP_LIVE_V2_DIAGNOSTICS = "1"
$env:GP_V2_DIAGNOSTIC_ROOT = "http://127.0.0.1:18888"
bun run diagnose:v2-concurrency
```

The probe uses identical 256 KiB request bodies, three waves per stage, waits for the candidate fleet to become idle, refuses protected port `18787`, and writes only timing/counter summaries under `.ghostprov/diagnostics`. It incurs real API usage and never records request or response content.

## Lifecycle and recovery

Startup resolves and locks one persisted Prism identity, reconciles its durable fleet manifest, resumes bounded project creation when needed, and binds the public listener as soon as one runtime is ready; remaining projects continue provisioning independently. Shutdown itself never creates or deletes a project: it stops listener admission, rejects queued uncommitted work with HTTP 503, waits for existing terminal proofs, drains sidecars, clears Awareness, and closes the identity lease.

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
- failed sandbox-session recovery attempts retry forever with capped exponential jitter while respecting the fleet provisioning-concurrency limit. Healthy projects continue serving throughout recovery; the separately guarded project-replacement transaction is serialized.

Heartbeat requests have a five-second deadline inside the exact ten-second cadence, so a hung proxy request cannot freeze liveness detection. The stable registry entry exposes zero capacity while its disposable child is recovering. Ordinary recovery replaces only the disposable sandbox session inside the same manifest project. Project creation/deletion belongs exclusively to the higher-level managed-fleet transaction and never adopts an arbitrary account project.

## Host supervision and rolling upgrades

`bun run start:supervised` runs a cross-platform supervisor that owns the public loopback port and proxies to one internal GhostProv worker. It restarts crashed workers with bounded backoff. A worker becomes selectable only after its own IPC channel proves the exact PID, allocated port, and supervisor generation; an unrelated local listener returning HTTP 200/503 can never satisfy readiness or receive queued API bodies. A planned `bun run upgrade` stops new worker dispatch, lets existing HTTP responses finish, closes persistent Responses WebSockets with restart code `1012`, gracefully stops the old worker, starts exactly one replacement, then releases requests queued at the stable public listener. The restart queue is bounded by both count and declared request bytes; overflowed HTTP and WebSocket handshakes receive an immediate credential-free `503 host_queue_full` instead of retaining bodies and timers for the full wait. Old and new workers never manage the same Prism projects concurrently. This rolling worker restart deliberately keeps the supervisor's immutable startup environment; it does **not** reload an edited `.env`.

The supervisor exposes bearer-authenticated `GET /__supervisor`, writes its current state to `.ghostprov/supervisor.json`, and appends bounded structured history to `.ghostprov/logs/supervisor.jsonl`. Status persistence coalesces bursts to at most one in-flight write and one latest dirty snapshot instead of writing once per forwarded request. `host.supervisor.sample` records request rate, proxy errors, exact queued request/byte pressure, rejections, active requests, WebSockets, generations, crashes, restart backoff, and upgrade duration. The first migration from a directly bound `18787` process still requires the operator to stop that old process once; after the supervisor owns the port, upgrades do not require rebinding it.

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

Use `restart` only for a rolling code restart with the current startup configuration. After editing `GP_V2_SIDECAR_REVISION`, `GP_V2_SIDECAR_PROJECT_REVISIONS`, `GP_TARGET_SANDBOXES`, credentials, or any other `.env` setting, use `config-restart`. It requests a graceful drain of the whole supervisor/fleet, applies the configured bounded fallback if a drain cannot finish, launches a fresh supervisor that reloads `.env`, and on both Windows and Linux waits boundedly for a new descriptor nonce plus matching ready status, supervisor PID, worker PID, start time, and readiness time. A timeout exits nonzero rather than reporting a Type=simple/systemd launch as ready. Windows also reports whether the old process proved a clean drain instead of silently calling a forced stop graceful. Every ordinary sandbox-session recovery remains in its manifest project; managed project replacement follows the separately persisted and outage-fenced transaction and never changes the selected immutable protocol revision. Environment variables explicitly supplied by the service manager still take precedence over `.env` and must be updated there as well.

`GP_V2_SIDECAR_REVISION=r10` selects the production release with persistent sandbox logging for every persisted project. R10 keeps R9's fenced R2 `bulk-v1` wire profile and immutable, accounted request-body snapshot, defensively forces upstream `stream:true` and `store:true`, and never replays a provider request after durable `FETCH_STARTING`. A definitive HTTP rejection, including `invalid_json`, is returned exactly once. Encoded requests through 256 KiB retain the exact R4 32-frame/512-KiB host flight, while isolated medium and bulk transfer may use 64 frames/1 MiB. Each runtime retains the six-unit pre-commit budget and four-`request_start` execution-risk fence; the router independently smooths the shared fleet link with a four-unit transfer gate and a 500 ms weighted start pacer. Requests above 3 MiB remain exclusive bulk work. R9 is the immediate immutable rollback without sandbox log files; R8 retains the historical 30 s credential window; R7 retains the superseded post-`FETCH_STARTING` `invalid_json` replay; R4 is the standard-profile rollback. `GP_V2_SIDECAR_PROJECT_REVISIONS` accepts exact persisted `project-id=r1|r2|r3|r4|r5|r6|r7|r8|r9|r10` overrides; dynamically created projects use the immutable default. Rollback uses `config-restart`, a fresh sandbox session in each existing project, and exact build/schema/profile proof; release selection never falls back in place. Older frozen releases remain only as explicit compatibility/forensic rollback fixtures, and automatic project replacement supports R9/R10 and is disabled for older rollback selections.

Before shipping or starting a pinned R2 release, run both independent release gates:

```text
bun run check:v2-sidecar-release
bun run check:v2-sidecar-deno
bun run check:v2-sidecar-release-r9
bun run check:v2-sidecar-deno-r9
bun run check:v2-sidecar-release-r10
bun run check:v2-sidecar-deno-r10
```

The release checks build source twice and compare the bytes, build hash, bundle hash, and R2 `bulk-v1` schema-profile hash with the registered R8, R9, and R10 identities. The Deno checks validate all three offline bundles with pinned Deno 2.9.5. These checks are read-only and fail closed. Maintainers create an R10 artifact through `bun run build:v2-sidecar-release-r10` after explicitly preparing its immutable identity; ordinary checks never overwrite artifacts.

R9 and R10 default to `GP_V2_SUPERVISOR_BOOTSTRAP_MODE=direct`. It submits one authenticated `codex/codex_exec_async` request with `codex_path: "/bin/sh"`, `prompt: "Hiii"`, and the service-required `gpt-5.6-sol` label. The shell runs the staged supervisor installer without invoking a model. A private host-generated execution session seals the bootstrap credential; the returned execution session and workspace must match exactly, and signed supervisor readiness remains authoritative. The temporary `exec` file, installer, and encrypted credential are removed from the project after readiness. An existing user `exec` file is preserved and causes a closed failure.

The original LLM bootstrap remains available through `GP_V2_SUPERVISOR_BOOTSTRAP_MODE=llm`. It creates one project conversation, discovers one reusable Codex session, and runs the installer through the existing resumed model turn. Mode selection applies on process startup. Direct execution never silently falls back to the LLM path, and a failed or ambiguous launch never authorizes another submission.

R10 direct bootstrap records one bounded private recovery attempt per project beside the configured fleet manifest, in `<fleet.json>.bootstrap/<project-id>.json`. Normal `bun dev` startup reconciles these records automatically. An attempt recorded before submission can clean its exact owned files; a submitted attempt must reconnect to its exact authenticated supervisor and prove shutdown before another launch. A recovering host authenticates the retained command sequence and then requires a fresh liveness challenge; it never replays the previous lifecycle command. Staged files are checked again after workspace synchronization and immediately before submission, so a Yjs reconnect cannot silently replace the expected launch files. Cleanup completes before the project Yjs connection closes. Records remain through successful startup and the supervisor's lifetime, and disappear only after signed shutdown and verified bootstrap-file cleanup. They contain sensitive execution identity and encrypted bootstrap material, so retain the local directory's permissions and do not delete records to force a retry. Legacy artifacts without a record, foreign edits, and unproven execution remain blocked. Recovery diagnostics expose allowlisted `bootstrap_phase`, `bootstrap_code`, and `bootstrap_reason` fields, including missing receipts, ownership mismatches, lost staging, and the failed supervisor-handshake phase, without recording credentials or raw exception details.

System shutdown separately retains a bounded registry of supervisor cleanup owners after a runtime's bounded stop waiter finishes. It joins their exact shutdown/close operations before closing account/session resources, within the original system shutdown deadline. Deadline expiry or a failed shutdown is reported as incomplete cleanup; it does not erase the recovery receipt or authorize another launch.

`GP_V2_SUPERVISOR_BOOTSTRAP_MODE=git-fsmonitor` selects the third method for an all-R10 fleet with relay bootstrap. It stages the same frozen supervisor in a private execution workspace and triggers a temporary Git filesystem-monitor hook during sandbox workspace synchronization. The hook holds Git's configuration lock, preserves the original configuration, restores it before launch, and releases the lock. Foreign configuration or lock changes fail closed. A bounded, one-shot claim prevents replay; an expired hook restores configuration without launching. Cancellation after a trigger may have been submitted remains ambiguous and never authorizes replay. Signed supervisor readiness remains authoritative, and shutdown removes the private workspace only after authenticated child cleanup. This method uses the sandbox's existing Python, Git, and filesystem control APIs and makes no LLM, direct-exec, or Render launch request. It never falls back to another method. Recovery and ordinary Responses/Anthropic traffic use the existing R10 supervisor and transport.

`bun run check:git-fsmonitor-hook` runs the offline Linux hook checks with Python and Git (on Windows it uses the installed `Ubuntu-24.04` WSL distribution). `bun run verify:git-fsmonitor` is an opt-in live verifier: it requires `GP_LIVE_GIT_FSMONITOR=1`, `GP_LIVE_ALLOW_TEST_PROJECT_MUTATION=1`, and `PORT=18888`, creates and deletes one dedicated project, and exercises eight small model requests, concurrent Responses/Anthropic traffic, two reconnect fault injections, one signed sidecar recovery, and a two-minute stability window. A passing offline check is not live Prism proof.

`GP_V2_SUPERVISOR_BOOTSTRAP_MODE=git-ssh-signing` selects the fourth method for an all-R10 fleet with relay bootstrap. Git's `gpg.ssh.defaultKeyCommand` and `gpg.ssh.program` callbacks restore the original configuration, produce and verify a real Ed25519 SSH signature, and claim one launch of the same frozen supervisor. Signing uses the sandbox's existing Python and Deno WebCrypto; it requires neither root nor `ssh-keygen`. The hook and signer are content-addressed, and signer and launcher bytes are sealed before execution. Key and message input is bounded and passed through stdin; signing keys are removed before launch. A signed Git commit is only submission evidence: authenticated supervisor readiness remains authoritative. Failed or ambiguous launches never switch methods or replay. Private workspace deletion requires signed shutdown and a valid restored-config receipt, then verifies absence. This callback shares the Git initialization and filesystem dependencies of `git-fsmonitor`.

`bun run check:git-ssh-signing-hook` runs the offline Linux Git acceptance and OpenSSH interoperability checks. It requires installed Python, Git, OpenSSH, and Deno 2.9.5; Windows uses `Ubuntu-24.04` WSL and `/tmp/ghostprov-deno/deno`. `GP_GIT_SSH_DENO_PATH` may select `/usr/local/bin/deno` instead. OpenSSH is a verification dependency only. `bun run verify:git-ssh-signing` requires `GP_LIVE_GIT_SSH_SIGNING=1`, `GP_LIVE_ALLOW_TEST_PROJECT_MUTATION=1`, `GP_LIVE_ALLOW_MODEL_REQUEST=1`, `GP_CANDIDATE=1`, `PORT=18888`, and an explicitly supplied `PRISM_COOKIE_FILE`. It disables automatic `.env` loading, creates one disposable project, makes at most one small model request, checks an ordinary reconnect plus lost-challenge and lost-reply recovery, exercises same-supervisor child recovery, observes two minutes of idle readiness, and verifies signed shutdown and cleanup. See the [validation record](docs/validation/2026-09-08-git-ssh-signing.md) for measured results and limits.

`GP_V2_SUPERVISOR_BOOTSTRAP_MODE=git-clean-filter` selects the fifth method for an all-R10 fleet with relay bootstrap. One harmless Render request triggers Git snapshot indexing of a private probe document. A temporary clean filter preserves its bytes, restores the original Git configuration, releases its lock, and claims one launch of the frozen supervisor. The launcher is sealed before execution; signed supervisor readiness remains authoritative. A persistent project ownership record prevents abandoned attempts from accumulating probe documents. Signed shutdown removes only the exact owned document, record, and execution workspace; ambiguous work is never replayed or switched to another method. This mode requires the sandbox's normal Python/Git/Linux execution facilities and Render, without an executable `.latexmkrc` bootstrap payload. It accepts an absent `.latexmkrc` or GhostProv's exact inactive configuration; custom project `.latexmkrc`, `.gitattributes`, and `.gitignore` files fail closed. It shares Git/filesystem dependencies with the other Git methods. The default remains `direct`.

`bun run check:git-clean-filter-hook` runs real Git/process checks as an unprivileged Linux user; Windows uses `Ubuntu-24.04` WSL user `nobody`. `bun run verify:git-clean-filter` requires `GP_LIVE_GIT_CLEAN_FILTER=1`, the project/model live guards, `GP_CANDIDATE=1`, `PORT=18888`, and an explicitly supplied `PRISM_COOKIE_FILE`. It creates one disposable project, makes one small Responses request and one small Anthropic Messages request, checks reconnect faults and same-supervisor recovery, observes two minutes of readiness, and verifies signed shutdown plus document/workspace/project/credential cleanup. Automatic `.env` loading is disabled. See the [validation record](docs/validation/2026-09-08-git-clean-filter.md) for measured proof and its limits.

`GP_V2_SUPERVISOR_BOOTSTRAP_MODE=git-textconv` selects the sixth method for an all-R10 fleet with relay bootstrap. The Git display callback runs during `git show HEAD`, after the snapshot commit preceding one harmless Render request. A pinned Python adapter accepts only the expected owned regular file, bounds it to 8 KiB, rejects symlinks/FIFOs/hard links, and seals its bytes before invoking the same pinned content executor used by `git-clean-filter`. Only the Git attributes, driver configuration, and configuration digest differ; the R10 authentication, durable launch claim, project ownership record, and signed cleanup are shared. Text-conversion caching is disabled. The callback preserves the stored document and returns its original bytes for display. The same project-file restrictions apply, and unresolved ownership prevents switching between these two content callbacks after an ambiguous attempt. This method shares Git/filesystem/Render dependencies and does not provide independent protection against failure of those facilities.

`bun run check:git-textconv-hook` runs the shared executor checks plus real Linux textconv/file-input checks. `bun run verify:git-textconv` requires `GP_LIVE_GIT_TEXTCONV=1`, `GP_LIVE_ALLOW_TEST_PROJECT_MUTATION=1`, `GP_LIVE_ALLOW_MODEL_REQUEST=1`, `GP_CANDIDATE=1`, `PORT=18888`, and an explicitly supplied `PRISM_COOKIE_FILE`. It uses the actual registered mode, creates/deletes one disposable project, makes one small Responses and one small Anthropic Messages request, and checks reconnects, recovery, readiness, signed shutdown, and all cleanup. Automatic `.env` loading is disabled. See the [validation record](docs/validation/2026-09-08-git-textconv.md).

Both modes use the selected immutable bundle and session-bound encrypted credential delivery. The supervisor keeps the exact Deno child resource and verifies its wrapper identity, canonical PID record, held owner lock, and static configuration before delivering credentials or stopping it. Recovery replaces the child within the same supervisor. Shutdown allows the full child cleanup budget and records exact retirement proof; missing proof blocks replacement. Control queues retain at most four host operations and one active plus one pending notification.

After a supervisor connection drops, the host obtains fresh authenticated proof using a new credential-rotation challenge supported by the existing R9/R10 supervisor peer. Cached signed replies can establish the sequence of a previously consumed command, but cannot restore liveness. Commands with an unproven outcome are never replayed. Reconnect waits are bounded and cancellable, and shutdown fences late replies. If reconnect completes while child recovery is waiting, GhostProv checks exact sidecar liveness and reliable-window progress before deciding whether a child restart is still needed.

The initial supervisor handshake also survives transient transport errors within its original deadline. It requires transport synchronization and signed readiness on the current connection, without another bootstrap submission.

A fresh reconnect challenge can be reannounced up to three times within that same connection and deadline if its update or reply is lost. Its authenticated command stays byte-for-byte identical; a bounded outer Awareness marker triggers the existing peer's exact-command reply cache even when Yjs would ignore an identical state renewal. This does not reissue an ordinary lifecycle command or a provider request, change the deadline, or accept old startup readiness as fresh proof.

Liveness failure records retain `V2SupervisorError` and fixed `reconnect_phase` / `supervisor_phase` categories. These distinguish credential minting, supervisor rotation, transport restoration, and the fresh proof without recording credentials or message contents. Periodic credential-refresh failures are observable even before a transport disconnect.

Sidecar source changes require rebuilding the selected release and verifying its bundle identity before live testing. The checked-in R1-R9 identities are frozen: a build that differs from a registered release must use a separately authorized revision, rather than overwrite that release. Starting `bun dev` uses the registered bundle; it does not compile sandbox source changes automatically.

R10 writes bounded JSONL logs inside its execution workspace. It keeps two 128 KiB segments for the supervisor, two for the current child, and two for the previous child. Each segment holds at most 1,024 records. A replacement child archives its predecessor's files only after exact exit proof, keeping the total at six files across repeated child recovery. Filesystem errors disable logging without failing model execution.

The logs contain startup/credential phases, request preparation and journal timings, upstream header timing, first text/tool/argument readiness, event gaps, delivery time, token usage, terminal and error categories. Summaries account for all events. Routine event records are sampled after the first four (every 32nd); first text/tool, tool readiness, gaps of at least 250 ms, delivery delays of at least 50 ms, and terminal/failure records are retained. `GP_V2_DIAGNOSTICS=1` also enables the existing resource sampler. Logs exclude prompts, generated text, tool arguments, headers, credentials, URLs, and client identifiers.

Download a file through the authenticated `GET /v1/diagnostics/sandboxes/<project-id>/logs/<file>` endpoint. File names are limited to `supervisor.jsonl`, `supervisor.1.jsonl`, `sidecar.jsonl`, `sidecar.1.jsonl`, `sidecar.previous.jsonl`, and `sidecar.previous.1.jsonl`. The host reads the exact execution session through Prism's file API; it makes no model call. Each download reserves memory before reading and retains that reservation until delivery or cancellation. At most two downloads run at once, independently of inference scheduling.

For an explicitly running candidate on port 18888, using a project ID from its authenticated `/__sandboxes` response:

```powershell
$logHeaders = @{ Authorization = "Bearer $env:GHOSTPROV_API_KEY" }
Invoke-WebRequest -Uri "http://127.0.0.1:18888/v1/diagnostics/sandboxes/$projectId/logs/sidecar.jsonl" -Headers $logHeaders -OutFile sidecar.jsonl
Get-Content -LiteralPath sidecar.jsonl -Tail 40
```

Use `sidecar.previous.jsonl` to inspect the child that just exited. File rotation bounds retention within the current execution workspace; these files are diagnostics, not a permanent request history. Compare durations within each process. Host and sandbox wall clocks can differ, so subtracting their timestamps directly is not a valid network-latency measurement.

Supervisor bootstrap readiness failures also reach the existing failed-session tracker through deployment-error wrappers. Repeated failures therefore fence the exact sandbox session using the established bounded policy; generic workspace deployment errors do not mark a healthy sandbox as failed.

For the long soak test, retain the application JSONL logs, `supervisor.jsonl`, `supervisor.json`, and monitor output. These contain only bounded counters/categories and no prompts, responses, cookies, API keys, or sandbox credentials.

For an explicitly reviewed stranded legacy R10 bootstrap, the offline migration command can stage at most two exact managed project grants beside the manifest. It verifies the account, worker identity, archived release, and private submitted journal, then writes an immutable `0600` `<manifest>.bootstrap-migration.json` containing only identities and digests. It does not clear journals, delete projects, replay work, or restart a service:

```powershell
bun --no-env-file scripts/prepare_bootstrap_migration.ts --manifest <fleet.json> --identity-fingerprint <sha256> --project <legacy-project-uuid> --project <second-legacy-project-uuid> --confirm-legacy-bootstrap-migration
```

The new fleet code retains the ordinary four-hold budget plus at most these two exact migration holds, with a hard total of six. Existing majority, outage, and zero-work gates still apply. A granted held project consumes its slot permanently; successor IDs inherit nothing, and its continuation metadata remains retained. Older running code ignores the ledger. Busy journal leases temporarily withhold an unused grant without blocking reconciliation for more than 250 ms per grant.

General project deletion remains a manual operation. The sole automatic case is cleanup of an old `managed:true` project after its fully verified replacement is active and every retirement fence passes. To decommission any other managed target, preview it first:

```powershell
bun run decommission:v2 -- --project <managed-project-uuid>
```

Execution requires the exact preview digest and a separate destructive opt-in:

```powershell
$env:GP_ALLOW_PROJECT_DELETE = "1"
bun run decommission:v2 -- --project <managed-project-uuid> --confirm <sha256-digest>
```

Only manifest records with `managed:true` can be deleted. Legacy imported user projects are excluded. Cleanup and tombstoning do not erase or conceal Prism-visible project/account activity; they only maintain local lifecycle hygiene.

## Operational settings

| Setting | Default | Meaning |
| --- | ---: | --- |
| `PORT` in candidate mode | `18888` | Dedicated candidate listener; protected port `18787` is rejected |
| `GHOSTPROV_API_KEY` | required | Explicit single-token key of at least 12 characters; a random 32-byte value is recommended |
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
| `GP_V2_SIDECAR_REVISION` | `r9` | Global sidecar release; R9 uses R2 `bulk-v1` with a host-configured credential window, R8 is the immediate immutable rollback, and R4 is the standard-profile rollback |
| `GP_V2_SUPERVISOR_BOOTSTRAP_MODE` | `direct` | `direct` starts the R9/R10 supervisor without model inference; `llm` selects the retained Codex conversation bootstrap; `git-fsmonitor`, `git-ssh-signing`, `git-clean-filter`, and `git-textconv` select temporary Git callbacks for R10. Clean-filter and textconv require one Render request. No automatic fallback |
| `GP_V2_TERMINAL_RECEIPT_COALESCING` | `1` | Combines a verified response receipt with an immediate next request using a maximum 10 ms hold; `0` restores eager receipts. Receive ACKs, immutable flights, and execution fences stay authoritative |
| `GP_V2_SIDECAR_PROJECT_REVISIONS` | unset | Exact persisted `project-id=r1|r2|r3|r4|r5|r6|r7|r8` overrides; no wildcard or fallback |
| `GP_TARGET_SANDBOXES` | `8` | Desired identity-scoped managed fleet size |
| `GP_MAX_PROJECTS` | `32` | Safety ceiling, not a fixed fleet size |
| `GP_PROJECT_REPLACEMENT_FAILURES` | `3` | Minimum identical local R9 bootstrap failures before guarded replacement can be considered |
| `GP_COLD_START_CONCURRENCY` | `2` | Maximum projects that have never reached verified readiness bootstrapping simultaneously; previously healthy recovery still uses `GP_PROVISION_CONCURRENCY` |
| `GP_HEARTBEAT_MS` | `10000` | Exact per-sandbox heartbeat period |
| `GP_ROUTER_QUEUE_TIMEOUT_MS` | `30000` | Exact ingress-to-transport deadline through 2 MiB; observed 2–8 MiB histories scale to at most four base windows while the no-progress route attempt remains 15 s; 13 MiB receives 125 s and 64 MiB retains the established 245 s ceiling |
| `GP_V2_TRANSPORT_RESERVATION_BYTES` | `16777216` | Host capacity estimate per active V2 stream |
| `GP_V2_SIDECAR_MEMORY_BUDGET_BYTES` | `536870912` | Sidecar request/queue admission budget |
| `GP_V2_YSWEET_REFRESH_MS` | `2700000` | Encrypted reconnect-credential rotation horizon |
| WebSocket active responses | `16` | Per-connection active response ceiling |
| WebSocket named lanes | `32` | Per-connection named-lane ceiling |
| Image edit aggregate raw input | `41943040` | Fixed production limit across edit images and mask; not an environment setting |
| Image edit HTTP request body | `67108864` | Fixed production envelope ceiling inherited from the shared HTTP body limit |
| `GP_ANTHROPIC_OPUS_MODEL` | `gpt-5.6-sol` | Default target for Claude Opus names |
| `GP_ANTHROPIC_SONNET_MODEL` | `gpt-5.6-terra` | Default target for Claude Sonnet names |
| `GP_ANTHROPIC_HAIKU_MODEL` | `gpt-5.6-luna` | Default target for Claude Haiku names |
| `GP_ANTHROPIC_DEFAULT_MODEL` | `gpt-5.6-sol` | Fallback for unknown model names |
| `GP_CLAUDE_VERIFY_PORT` | `18788` | Separate loopback-only verifier listener |
| `GP_CLAUDE_VERIFY_UPSTREAM` | `http://127.0.0.1:18888` | Isolated candidate root used only by the verifier; protected `18787` is rejected |
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
bun run test
bun run test:unit
bun run test:integration
bun run typecheck
bun --no-env-file node_modules/typescript/bin/tsc --project tsconfig.scripts.json
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
- `tests/codex-exec.test.ts`
- `tests/compat-final-permit.test.ts`
- `tests/cpu-transform-pool.test.ts`
- `tests/decommission-script.test.ts`
- `tests/decommission.test.ts`
- `tests/documentation.test.ts`
- `tests/file-lease.test.ts`
- `tests/final-object-permit.test.ts`
- `tests/bootstrap-migration.test.ts`
- `tests/fleet-manifest.test.ts`
- `tests/fleet-manager.test.ts`
- `tests/graceful-shutdown.test.ts`
- `tests/heartbeat.test.ts`
- `tests/host-supervisor-service-control.test.ts`
- `tests/host-supervisor.test.ts`
- `tests/http-body.test.ts`
- `tests/http-errors.test.ts`
- `tests/http-streaming.test.ts`
- `tests/images-batch-memory.test.ts`
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
- `tests/prism-direct-exec.test.ts`
- `tests/tool-latency-guards.test.ts`
- `tests/project-inspector.test.ts`
- `tests/provisioning-client.test.ts`
- `tests/projects-client.test.ts`
- `tests/resource-governor.test.ts`
- `tests/recovery-chaos.test.ts`
- `tests/recovery-coordinator.test.ts`
- `tests/recovery-observability.test.ts`
- `tests/live-transport-outage-guard.test.ts`
- `tests/live-retired-supervisor-guard.test.ts`
- `tests/live-retirement-preflight.test.ts`
- `tests/inspect-existing-retirement.test.ts`
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
- `tests/security-observability-boundaries.test.ts`
- `tests/session-manager.test.ts`
- `tests/server-main-lifecycle.test.ts`
- `tests/sse.test.ts`
- `tests/standalone-search-adapter.test.ts`
- `tests/standalone-search-http.test.ts`
- `tests/standalone-search-probe.test.ts`
- `tests/standalone-search-session.test.ts`
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
- `tests/v2-relay-launcher.test.ts`
- `tests/v2-release-observability.test.ts`
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
- `tests/prism-conversation-client.test.ts`
- `tests/v2-r9-recovery-guards.test.ts`
- `tests/v2-r10-upstream-logging.test.ts`
- `tests/v2-r10-release.test.ts`
- `tests/v2-sandbox-log.test.ts`
- `tests/v2-sandbox-log-service.test.ts`
- `tests/v2-supervisor-channel.test.ts`
- `tests/v2-supervisor-controller.test.ts`
- `tests/v2-supervisor-direct.test.ts`
- `tests/v2-supervisor-bootstrap-recovery.test.ts`
- `tests/v2-supervisor-git-fsmonitor.test.ts`
- `tests/v2-supervisor-git-ssh-signing.test.ts`
- `tests/git-ssh-signing-live-guards.test.ts`
- `tests/v2-supervisor-git-clean-filter.test.ts`
- `tests/git-clean-filter-live-guards.test.ts`
- `tests/v2-supervisor-git-textconv.test.ts`
- `tests/v2-supervisor-git-textconv-deployment.test.ts`
- `tests/git-textconv-live-guards.test.ts`
- `tests/v2-supervisor-launcher.test.ts`
- `tests/v2-supervisor-lifecycle.test.ts`
- `tests/v2-supervisor-mailbox.test.ts`
- `tests/v2-supervisor-process.test.ts`
- `tests/v2-supervisor-protocol.test.ts`
- `tests/v2-supervisor-reconnect.test.ts`
- `tests/v2-supervisor-retirement.test.ts`
- `tests/v2-supervisor-retirement-receipt.test.ts`
- `tests/v2-supervisor-session-bootstrap.test.ts`
- `tests/v2-transcript.test.ts`
- `tests/v2-transport-session.test.ts`
- `tests/v2-upstream.test.ts`
- `tests/v2-upstream-incremental.test.ts`
- `tests/v2-window.test.ts`
- `tests/websocket.test.ts`
- `tests/websocket-http-fallback.test.ts`
- `tests/websocket-telemetry.test.ts`
- `tests/workload.test.ts`
- `tests/ydoc.test.ts`

The deterministic integration test fills 120 lanes as `30 + 30 + 30 + 30`, verifies terminal settlement and unique execution IDs, and proves a draining project leaves the other 90 lanes usable.

## Live proof

The R9 supervisor release has a dedicated one- or eight-project stability proof. It creates only fresh UUIDs, copies account cookies to an isolated directory, binds `18888`, checks mixed Responses/Anthropic requests and provider continuation, stops each idle sidecar through its authenticated supervisor, and verifies recovery without another bootstrap. Direct mode blocks the LLM bootstrap endpoints and checks the exact `Hiii` execution request; explicit `llm` mode verifies the retained two-turn path. Set `GP_LIVE_BENCHMARK_CONCURRENCY=1` to add bounded request bursts up to 32 concurrent requests on eight sandboxes, with latency and throughput measurements. It records bounded diagnostics and deletes every project in its own ledger during cleanup:

```powershell
$env:GP_LIVE_R9_SUPERVISOR_RECOVERY = "1"
$env:GP_LIVE_ALLOW_TEST_PROJECT_MUTATION = "1"
$env:GP_LIVE_PROJECT_COUNT = "8" # use 1 for the first gate
$env:PORT = "18888"
bun scripts/live_r9_supervisor_recovery.ts
```

The persisted-continuation verifier is the only live verification command authorized by the local protected-port allowlist. Normal production startup is separate. The verifier hardcodes candidate port `18888`, requires 2-8 explicitly named dedicated test projects, never creates or deletes projects, and never contacts the protected listener on `18787`. It proves explicit and inferred HTTP suffixes, public WebSocket and Claude full-history continuation, same-project restart hydration, different-project hydration, and 1/4/8/16-agent two-turn isolation:

```powershell
$env:GP_PERSISTED_CONTINUATION_LIVE = "1"
$env:GP_CONTINUATION_TEST_CONFIRM = "dedicated-test-projects"
$env:GP_CONTINUATION_TEST_PROJECT_IDS = "<test-uuid-1>,<test-uuid-2>"
$env:GP_CONTINUATION_TEST_COOKIE_FILE = "D:\secrets\test-prism-cookies.json"
bun run verify:persisted-continuation
```

Standalone search has a separate destructive live verifier. It creates exactly one disposable Prism project, provisions one sandbox, performs one native stored Responses `web_search` request, and deletes that exact project in cleanup. It never enumerates or mutates configured fleet projects and never contacts, restarts, or signals the protected listener on `18787`. Because it creates/deletes a project and incurs provider search usage, all four explicit guards are required:

```powershell
$env:GP_LIVE_STANDALONE_SEARCH = "1"
$env:GP_LIVE_CREATE_TEST_PROJECTS = "1"
$env:GP_LIVE_SANDBOX_PROBE = "1"
$env:GP_ALLOW_PROJECT_DELETE = "1"
$env:GP_LIVE_PROJECT_COUNT = "1"
$env:PRISM_COOKIE_FILE = "D:\secrets\test-prism-cookies.json"
bun run verify:standalone-search
```

The verifier records only bounded metadata under `.ghostprov/standalone-search-probes`; it never writes search prompts, result text, cookies, or credentials. A cleanup failure retains the exact project ID in that state directory for deliberate operator recovery rather than deleting any unrelated project.

Create dedicated test projects only with explicit authorization:

```powershell
$env:GP_LIVE_CREATE_TEST_PROJECTS = "1"
$env:GP_LIVE_PROJECT_COUNT = "4"
bun run create:test-projects
```

The tool-latency verifier traces relay arrival and client delivery for HTTP Responses, WebSocket Responses, and Anthropic streams. It records every observed event's relative timestamp and character counts, provider timing diagnostics, final token-usage counts, and tool-result round trips with small and 32 KiB results. It does not save prompts, output text, arguments, response IDs, or credentials. These are observed stream-event times, not model-internal per-token timestamps. The verifier creates and deletes one dedicated project, binds only `18888`, and writes its bounded report under `.ghostprov/tool-latency`:

```powershell
$env:GP_LIVE_TOOL_LATENCY_TRACE = "1"
$env:GP_LIVE_ALLOW_TEST_PROJECT_MUTATION = "1"
$env:PORT = "18888"
bun run verify:tool-latency
```

For paired comparisons, set `GP_V2_TERMINAL_RECEIPT_COALESCING=0` to test eager receipts, or `GP_TRACE_BATCHING=0` to test without sidecar event batching. These are independent controls; compare the recorded diagnostics and latency distributions before changing a production setting. The separate R9 recovery verifier accepts `GP_LIVE_RECOVERY_CYCLES=1` through `3` for repeated stop/recovery checks within the same supervisor instances and reports bounded, content-free failure-phase diagnostics.

For supervisor-connection recovery, the same guarded verifier accepts `GP_LIVE_CONNECTION_DROP_ROUNDS=1` or `2`. It closes only the exact supervisor sockets created by its dedicated candidate, requires fresh authenticated proof, and checks that the original supervisor and child are retained. `GP_LIVE_RECONNECT_SOAK_MS=7800000` adds a 130-minute soak spanning the observed hourly connection lifetime. The soak checks the original instances, sends two small model requests every five minutes, maintains a stored Responses continuation, and fails if usable capacity is not restored within 30 seconds. This is a separate long-run check; the existing short stability gate remains unchanged. Connection diagnostics record only bounded counters, timing, and close/error codes.

`GP_LIVE_COLD_START_CONCURRENCY=1` serializes only the verifier's initial sandbox setup when investigating setup contention. Its default is `2` for an eight-project proof and `1` for a single project. It does not change production startup defaults, the target fleet size, request concurrency, or any reconnect/recovery assertion.

The [2026-09-05 validation report](docs/validation/2026-09-05-direct-exec-and-tool-latency.md) records the direct-bootstrap comparison, paired tool-result latencies, repeated eight-sandbox recoveries, concurrency measurements, and failed attempts with their limits.

The [reconnect investigation](docs/validation/2026-09-05-r9-reconnect.md) separates the reproduced host defect from connection-establishment failures and records incomplete fleet gates. The [sandbox logging validation](docs/validation/2026-09-05-sidecar-diagnostics.md) distinguishes the early prototype from the integrated R10 release, with measured processing, tool timing, failure capture, and recovery results.

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

The load script verifies exact output markers, unique response IDs, peak physical occupancy, per-runtime occupancy, zero under-capacity queueing, and zero quarantines against the candidate only; it refuses and never probes protected port `18787`.

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
$env:OPENAI_BASE_URL = "http://127.0.0.1:18888/v1"
bun run verify:live
```

## V2 verification

The deterministic candidate smoke uses an OS-assigned port and never contacts the protected service on `18787`:

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

The memory-accounting proof creates exactly one random throwaway project, runs one HTTP and one WebSocket response on candidate port `18888`, requires idle and normal single-session governor reservations and process RSS to remain below 1 GiB, verifies that all elastic bytes return to the fixed host event pool, and deletes only that exact project after shutdown:

```powershell
$env:GP_LIVE_MEMORY_ACCOUNTING = "1"
$env:GP_LIVE_ALLOW_TEST_PROJECT_MUTATION = "1"
$env:PORT = "18888"
bun run verify:memory-accounting-live
```

Its report exposes only a hashed project identity, bounded memory totals, timings, cleanup proof, and `protectedPortTouched: false`.

The paid four-project scale canary additionally requires `GP_LIVE_SCALE=1`. It runs 128 direct, 256 accepted, 512 routed, and 4 MiB integrity stages without creating or deleting projects:

```powershell
$env:GP_LIVE_V2 = "1"
$env:GP_LIVE_SCALE = "1"
$env:GP_LIVE_PROJECT_COUNT = "4"
$env:PORT = "18888"
bun run verify:v2-fleet
```

Set `GP_LIVE_V2_SOAK=1` only for the additional lifecycle soak. Missing live opt-ins are reported as “not run”; deterministic tests are never represented as live Prism proof.

The guarded fresh-fleet recovery proof creates an isolated eight-project manifest with no `PRISM_PROJECT_IDS`, verifies the two-project cold-start ceiling, runs one request directly through every real sandbox, replaces two controlled stopped test runtimes through the full managed-project lifecycle, creates a stored continuation before restarting every sandbox incarnation, completes that continuation after restart, runs repeated six-agent router waves, holds 8/8 readiness through multiple sidecar heartbeat periods, and then deletes only the exact project UUIDs recorded by that run. It is destructive only to those dedicated projects and is hard-fenced to `18888`:

```powershell
$env:GP_LIVE_FRESH_FLEET_RECOVERY = "1"
$env:GP_LIVE_ALLOW_TEST_PROJECT_MUTATION = "1"
$env:PORT = "18888"
bun run verify:fresh-fleet-recovery
```

Its saved report contains hashed project identities, lifecycle counts, permit maxima, wave timings, cleanup proof, and `protectedPortTouched: false`.

## ScriptC evaluation

Bun remains the production runtime. After Playwright removal, ScriptC 0.0.35 still reports 16 unsupported type-surface sites (web/worker globals, URL/Headers overloads, and crypto typing), so no native binary is produced for a meaningful A/B test. Its dynamic npm tier also uses QuickJS with copied boundaries. Optional coverage can be rerun as ScriptC matures:

```powershell
$env:GP_BENCHMARK_SCRIPTC = "1"
bun run benchmark:scriptc
```

ScriptC is adopted only if full compatibility passes and A/B results improve p95 local latency by at least 10% or RSS by at least 20%, with no more than 5% throughput regression.
