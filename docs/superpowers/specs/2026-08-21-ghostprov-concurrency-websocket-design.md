# GhostProv Concurrency and WebSocket Compatibility Design

**Date:** 2026-08-21

**Status:** Approved for written-spec review

## Goal

Make GhostProv a concurrency-first, OpenAI-compatible Responses API mirror whose HTTP streaming and WebSocket modes work with Codex while preserving `store:false`. The active implementation must deliver real incremental events, support concurrent independent WebSocket lanes, serialize work within each lane, and use the sandbox slot daemon efficiently.

## Authoritative Protocol

The implementation follows the current official OpenAI documentation:

- [Responses API WebSocket mode](https://developers.openai.com/api/docs/guides/websocket-mode/)
- [Responses WebSocket events](https://developers.openai.com/api/reference/resources/responses/websocket-events/)
- [Streaming Responses](https://developers.openai.com/api/docs/guides/streaming-responses/)

The required observable behavior is:

- Clients send a `response.create` JSON object.
- `stream` is implicit and must not be present in the WebSocket request sent to the internal HTTP relay.
- `background` is rejected because it is unsupported in WebSocket mode.
- Server events are direct JSON event objects, never wrappers such as `{type, parsed}`.
- A named request's events include the originating `stream_id`; default-lane events omit it.
- Requests in the same lane execute first-in, first-out and never overlap.
- Requests in different lanes may execute concurrently.
- One connection permits at most 16 active responses and 32 distinct named stream IDs.
- `previous_response_id` is resolved from connection-local memory for `store:false` continuations and forks.
- Closing the WebSocket destroys all connection-local response state.

## Constraints

- Never change a client request from `store:false` to `store:true`.
- Do not require any Codex configuration beyond pointing it at GhostProv's base URL.
- Preserve the user's decision to leave token and debug-route security behavior unchanged.
- Keep Playwright; Patchright is outside this change.
- Negotiate daemon features from `gp_status.json`; never assume a locally bundled daemon matches the live sandbox daemon.
- The active slot daemon path is authoritative. Legacy relay code must not define or test server behavior.
- Concurrency must be bounded globally so HTTP and WebSocket traffic cannot over-subscribe daemon slots.

## Considered Approaches

### 1. Pass `previous_response_id` directly to HTTP

This is small but incorrect for Codex WebSocket sessions using `store:false`. The upstream HTTP request has no WebSocket-local cache, so incremental continuation can return `previous_response_not_found`.

### 2. Connection-local replay over the existing HTTP daemon

GhostProv stores an immutable response graph per WebSocket connection. When a client references a cached response, GhostProv reconstructs the complete Responses input chain, appends the new incremental input, removes the internal `previous_response_id`, forces the internal relay request to stream, and forwards direct events to the client. This preserves client-visible `store:false` semantics and works with the existing daemon.

This is the selected first release.

### 3. Persistent upstream WebSocket sessions in the daemon

This most closely matches OpenAI and may reduce continuation latency further, but it requires proof that `https://proxy:18080/v1/responses` supports WebSocket upgrade semantics plus a daemon deployment protocol. It is a measured follow-up, not a prerequisite for correctness.

## Architecture

### HTTP adapter

The Node HTTP adapter streams `Response.body` chunks to `ServerResponse` with backpressure. It must not call `response.arrayBuffer()` for streaming responses. Non-streaming JSON and binary responses may retain buffered handling.

The adapter watches request abort and response close events so it can cancel local readers and release scheduler capacity promptly.

### SSE extraction

The SSE parser operates on accumulated visible text and maintains one absolute cursor at the end of the last complete frame. It supports LF and CRLF frame boundaries and never advances past an incomplete frame. Sliding windows that move beyond the cursor cause an explicit protocol-gap error instead of silently dropping events.

The sandbox protocol's offsets are Unicode code-point offsets because Python calculates them with `len(str)`. GhostProv converts code-point offsets to JavaScript string indices before slicing. This prevents non-BMP text such as emoji from corrupting the cursor.

Each emitted event is parsed once into an object. Invalid event JSON becomes a request-scoped protocol error; it is never wrapped and forwarded as an opaque string.

### Global response scheduler

A single scheduler owns all active relay executions, regardless of whether they originated from HTTP or WebSocket traffic.

- Capacity defaults to `GP_SLOTS` and cannot exceed the daemon-reported live slot count.
- `PQUEUE_CONCURRENCY` becomes a compatibility alias; if both variables are set, the smaller positive value is used.
- Capacity is held until the response reaches a terminal event or failure, not merely until a `RunHandle` is created.
- Waiting requests are FIFO.
- An aborted HTTP request or closed WebSocket removes queued work and stops forwarding active work.
- Metrics expose active, queued, completed, failed, and cancelled counts without exposing credentials.

The local scheduler prevents WebSocket multiplexing and HTTP callers from separately assuming the same daemon capacity.

### WebSocket connection

Each accepted socket owns a `ResponsesWebSocketSession` with:

- one implicit default lane;
- up to 32 named lanes;
- a FIFO promise tail per lane;
- a connection-wide active-response semaphore of 16;
- a response graph indexed by response ID;
- a bounded memory budget;
- an outbound send queue that respects `ws.bufferedAmount`.

Incoming `response.create` messages are validated before enqueueing. Different lanes can acquire global scheduler capacity concurrently. Same-lane work is chained to the prior lane promise even if messages arrive back-to-back.

For every upstream SSE event, GhostProv parses the event JSON, adds the request's `stream_id` when named, retains the upstream `type`, and sends that object directly. Terminal events update or evict connection state according to the result.

### Connection-local response graph

Each successful response node stores:

- the response ID;
- a parent pointer, if the request referenced a cached response;
- the request's incremental input items;
- output items required for the next turn;
- the effective model used for the response;
- the lane that produced the response;
- an approximate byte cost and last-access timestamp.

Nodes use structural sharing: a continuation stores only its delta and parent pointer. Full replay input is materialized only when dispatching the next internal HTTP request. Materialization walks parent pointers once, memoizes the flattened chain while it is hot, and invalidates memoized arrays when their node is evicted.

For a cached `previous_response_id`, the internal relay body:

- retains `store:false`;
- removes `previous_response_id` and `stream_id`;
- sets `stream:true` because WebSocket events are implicit streaming;
- combines prior request inputs, prior response output items, and the new incremental input in chronological order;
- takes all generation settings from the current `response.create`; only `model` falls back to the parent response's model when omitted;
- never inherits prior `instructions`, matching the Responses API rule that previous instructions are not carried into the next response.

For internally reconstructed `store:false` requests, GhostProv adds `reasoning.encrypted_content` to the internal `include` list. The encrypted reasoning item is retained in the response graph so stateless replay is complete. If the client did not request that include value, GhostProv removes the encrypted field from outbound events and the terminal response while keeping the internal cached copy.

For an unknown ID with `store:false`, GhostProv emits a request-scoped `previous_response_not_found` error. For an unknown ID with `store:true`, GhostProv leaves `previous_response_id` on the internal request so the upstream HTTP API may hydrate persisted state. A failed same-lane continuation evicts the referenced local node; a failed cross-lane fork preserves its parent, matching documented behavior.

`generate:false` creates a warm-up response node without model generation. The first implementation prepares and caches the normalized request state locally, emits a synthetic completed response with a GhostProv response ID, and allows that ID to be used as the parent of a later generated turn.

`previous_session_id` is not an OpenAI Responses or Codex WebSocket field and is never required from clients. GhostProv assigns an internal connection ID and may associate it with Codex `client_metadata.session_id` for scheduler fairness, metrics, cancellation, and cache ownership. Conversation lineage and forks remain keyed exclusively by the Codex-supplied `previous_response_id`.

### Memory bounds

Connection state is bounded by both count and approximate bytes:

- default maximum: 256 cached response nodes per connection;
- default maximum: 64 MiB per connection;
- default process-wide maximum: 512 MiB across all WebSocket connections;
- environment overrides: `GP_WS_CACHE_NODES`, `GP_WS_CACHE_BYTES`, and `GP_WS_CACHE_TOTAL_BYTES`;
- least-recently-used leaf nodes are evicted first;
- ancestors referenced by live descendants are retained;
- all state is released immediately on socket close.

If the cache cannot admit a new node without evicting a required ancestor, the completed response is still delivered but is not continuation-cacheable. A later reference returns `previous_response_not_found`.

### Zip polling coordinator

The slot client currently performs one `/zip` archive fetch per active lane per poll. A process-wide `ZipPollCoordinator` batches filename requests arriving in the same short scheduling window into one `fetchZipFiles([...names])` call and distributes results to callers.

- batch window defaults to 10 ms and is configurable with `GP_ZIP_BATCH_MS`;
- duplicate filename requests are coalesced;
- one failed batch rejects only its current waiters;
- no stale file result is cached across batches;
- daemon status, response polling, and disk verification all use the coordinator.

This makes gateway archive work approximately one fetch per poll interval instead of one fetch per active response.

### Bridge repair

Bridge paths derive from the repository root or explicit environment variables:

- `GP_USER_DATA_DIR` for the persistent browser profile;
- `GP_TOKEN_FILE` for token persistence;
- `PRISM_PROJECT_URL` or a URL generated from `PRISM_DOC_ID`;
- the repository `.env` path, resolved relative to the project.

Headful and headless modes both use the same persistent context so authenticated state survives. Launch arguments remove `--no-sandbox`. Launch recovery closes only the Playwright browser/context created by GhostProv; it never invokes a global `taskkill`.

Every `/api/y` mint and project-access check uses the configured `PRISM_DOC_ID`. The persistent Yjs client calls the bridge directly to mint a token instead of testing a nonexistent gateway export. Browser operations receive explicit timeouts and surface typed errors rather than silently falling back to raw requests that are known to fail Cloudflare binding.

### Daemon v6.3 transport and coordination boundary

The user-provided daemon advertises `segmented-v6.3-embedded-commit`. GhostProv prefers this protocol when it appears in `gp_status.json`:

1. choose a free physical slot and a random request ID;
2. gzip and base64 the request when that reduces part count;
3. create immutable parts `1..K-1` first under `gp/`;
4. publish part zero last as `GP63 {manifest-json}\n<payload0>`, which atomically commits the generation without rewriting `job_sN.json`;
5. keep the existing acknowledgement and terminal response protocol;
6. clear completed part contents during lane cleanup;
7. fall back to segmented v6.2 when the capability is absent.

Part-zero payload sizing accounts for the embedded header so the complete file remains within `GP_MAX_JOB_PART_BYTES`.

Before relying on v6.3 for sustained high concurrency, the sandbox agent should harden three daemon behaviors:

1. deterministically sort simultaneous discoveries and retain only the newest pending commit per physical slot;
2. queue malformed-commit failures while a slot worker is active instead of overwriting that worker's shared `resp_sN.json`;
3. bound historical part-zero scanning and `v63_seen` growth through processed-artifact cleanup, a bounded index, or event-driven directory notification.

After local correctness and concurrency benchmarks, the sandbox agent should evaluate three further protocol upgrades:

1. cancellation by slot plus request acknowledgement so disconnects free upstream work;
2. sequence-numbered response events or byte-safe framed segments instead of character-offset windows;
3. persistent upstream Responses WebSockets if the proxy endpoint supports upgrades.

Every daemon change advertises a new `daemon_build` and protocol capability in `gp_status.json`. GhostProv selects new behavior only when that capability is present and otherwise keeps the compatible fallback.

## Error Handling

- Invalid JSON emits a direct WebSocket `error` object and leaves the connection open.
- Unknown event types emit a request-scoped error.
- Invalid or excessive `stream_id` values emit the documented connection-level error shape.
- Unsupported `background` and explicit WebSocket `stream` fields produce request-scoped validation errors.
- Queue exhaustion, daemon sync failure, timeout, upstream rejection, protocol gap, and cancellation have distinct stable error codes.
- Named request errors include `stream_id`; default-lane errors omit it.
- An error in one lane does not terminate other lanes.
- Socket close stops outbound sends, removes queued work, releases state, and requests cancellation from the local handle when supported.

## Tests

Testing uses real local boundaries and avoids live credential files.

### Pure tests

- SSE frames split at every possible character boundary.
- LF and CRLF event streams.
- sliding-window overlap, duplicate polls, and explicit gap failure.
- emoji and other non-BMP content across window boundaries.
- response-graph continuation, forks, setting inheritance, eviction, and unknown IDs.
- lane FIFO ordering and cross-lane concurrency.
- scheduler fairness, capacity, cancellation, and mixed HTTP/WebSocket callers.
- zip request coalescing and failure isolation.

### Local integration tests

- a real Node HTTP server proves the first SSE event arrives before the terminal event;
- a real `ws` client sends Codex-style `response.create` messages and receives direct event objects;
- same-lane requests never overlap;
- different lanes overlap up to configured capacity;
- named events echo `stream_id` and default events omit it;
- `store:false` tool-call continuation uses `previous_response_id` externally while the internal relay receives reconstructed full history;
- cross-lane fork continuation works;
- disconnect removes queued work and prevents additional sends;
- file routes use an isolated in-memory upstream fixture;
- bridge tests use temporary directories supplied through environment variables and never touch the live `.pw/tokens.json` or `.env`.

### Live verification

Live Prism verification is a separate opt-in command because it mutates the project and consumes upstream capacity. It must test one HTTP stream, one Codex-like WebSocket tool loop, one same-lane continuation, and at least eight concurrent independent lanes while recording first-event latency, total latency, zip-fetch count, and daemon slot utilization.

## Documentation and Cleanup

- README architecture describes the active slot daemon and connection-local WebSocket replay.
- Documented concurrency defaults come from the same configuration module as runtime behavior.
- Documented test files must exist.
- Legacy `relay/client.ts`, `proxy/yjs.ts`, `proxy/render.ts`, and `utils/zip.ts` code is either removed from the advertised runtime or explicitly labeled compatibility-only; tests must not use it to claim active-server coverage.
- Package scripts include `typecheck`, focused unit/integration tests, and the opt-in live verification command.

## Acceptance Criteria

- Codex can complete a multi-turn `store:false` WebSocket tool-call loop through GhostProv using incremental inputs and `previous_response_id`.
- HTTP SSE delivers at least one event before upstream completion.
- A split SSE frame is emitted exactly once after completion.
- Eight named WebSocket lanes execute concurrently when eight daemon slots are available.
- When the live daemon advertises v6.3, concurrent segmented requests commit through immutable part zero without rewriting `job_sN.json`; an unadvertised daemon uses v6.2.
- Two requests on one lane execute in FIFO order without overlap.
- Mixed HTTP and WebSocket load never exceeds the configured global slot capacity.
- Concurrent response polling produces one coalesced `/zip` fetch per batch rather than one per lane.
- Headless bridge restart retains the authenticated profile and never terminates unrelated Chrome processes.
- Unit, integration, typecheck, Python syntax, and dependency audit checks pass.
- The default test suite never reads, overwrites, or deletes live token files.
