# GhostProv v2 Awareness Sidecar Transport

**Status:** Proposed, empirically validated prototype
**Date:** 2026-08-22
**Branch:** `br/v2-awareness-sidecar`
**Supersedes:** Only the v1 sandbox file/ZIP hot path. The v1 transport remains a recovery fallback.

## 1. Decision

GhostProv v2 will preserve the existing host API, router, Codex WebSocket behavior, image compatibility, project provisioning, resource governor, terminal validation, and quarantine rules.

It will replace the request-time transport:

```text
v1: host Yjs file write -> wait-for-sync -> sandbox disk -> daemon -> disk response -> ZIP poll

v2: host Y-Sweet Awareness frames -> sandbox sidecar -> direct Responses API stream
    -> Y-Sweet Awareness frames -> host
```

The v2 sandbox sidecar requires no public port. It connects to the same authenticated Y-Sweet project document as the host, but carries request traffic through the ephemeral Awareness protocol rather than persistent Yjs document structures.

The supervised Crixet Deno process remains alive on port 8080. GhostProv never patches or restarts it.

## 2. Proven constraints

Read-only sandbox diagnostics established:

- `/code/crixet` is writable, but belongs to the ephemeral sandbox root filesystem.
- `server.sh` is PID 1 and supervises the original Deno server.
- If the original Deno PID exits, PID 1 shuts down the complete sandbox.
- Prism then reprovisions the clean image, so runtime edits to `/code/crixet` disappear.
- The sandbox user is UID/GID 5000 and has no effective Linux capabilities.
- Crixet exposes a fixed route table on port 8080 and no generic sidecar forwarder.
- Sidecar ports are not publicly routed.
- The sandbox has no general internet access.
- `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `PROXY_CA_CERT_PATH` are present in the render environment.
- The configured proxy CA is readable.
- Existing sandbox code can call the configured internal OpenAI proxy successfully.
- Crixet vendors Deno, Yjs 13.6.27, `@y-sweet/client` 0.9.1, and all required transitive modules.
- `/api/y` rejects arbitrary non-project document UUIDs with HTTP 404, so disposable Y-Sweet documents cannot be minted.

The sidecar must therefore:

1. keep Crixet alive;
2. use the existing project Y-Sweet connection;
3. use only vendored dependencies;
4. use ephemeral Awareness frames to avoid persistent CRDT growth;
5. call the configured OpenAI proxy through the provided environment and CA.

## 3. Prototype evidence

### 3.1 v1 baseline

The committed v1 browserless router proved:

- 30 active lanes per sandbox;
- 120 active lanes across four projects;
- exact `30 + 30 + 30 + 30` occupancy;
- zero queue and zero quarantine during the final 328-request ramp;
- full-capacity request p95 of approximately 3.33 seconds for short prompts.

### 3.2 Persistent CRDT bus — rejected

A first prototype used persistent Y.Map request and response entries.

It proved functional direct proxy access and high concurrency, but repeated stress accumulated Yjs history. A later 512-command p95 degraded to approximately 44 seconds. Arbitrary ephemeral document minting was unavailable.

**Decision:** persistent Yjs request/response maps are rejected for v2 traffic.

### 3.3 Awareness bus — accepted foundation

The Awareness prototype produced no persistent request entries.

| Proof | Result |
| --- | --- |
| 512 delayed echo commands | 512/512 completed; 128-active cap; 0 errors |
| Repeated echo wall time | approximately 0.72–0.89 seconds on healthy starts |
| One sandbox, 64 direct Responses | 64/64 unique terminals; 0 errors; first-event p95 1.23 s; terminal p95 1.97 s |
| One sandbox, 128 direct Responses | 128/128 unique terminals; 0 errors; first-event p95 2.23–3.70 s across repeated runs |
| Unbounded 256 direct Responses | proxy connection reset above approximately 128 streams; rejected |
| Bounded 256 accepted / 128 active | 256/256 unique terminals; 0 errors; two automatic waves |
| Bidirectional 4 MiB Awareness payload | exact byte count and SHA-256 in both directions; 4.22 s |
| Sidecar crash during 64 active commands | replacement instance completed 64/64; 0 errors; fresh token; two launch attempts |
| Five lifecycle cycles | 5/5 succeeded; four first-attempt launches, one third-attempt launch |
| Two synchronized sandboxes | 256/256 unique terminals; both started within 1 ms; 0 errors |
| Four synchronized sandboxes | 512/512 unique terminals; all started at the same millisecond; 0 errors |

The unbounded 256 test demonstrated that 128 is the correct default active cap for one Deno process/proxy connection. Higher accepted concurrency must queue locally or be distributed across projects.

The four-sandbox proof establishes a v2 physical target of 512 active upstream Responses calls, compared with v1's 120.

### 3.4 Prototype limitation corrected by this design

The prototype placed all completed results in one growing Awareness state. Under the 512-call aggregate run, first events remained prompt, but one sandbox's final combined state became a 39.5-second straggler.

Production v2 will not publish a growing result array. It will use bounded, acknowledged frames and credit-based flow control.

## 4. System architecture

```text
                         GhostProv host
  HTTP / SSE / WS / Images / Codex continuation / router / governor
                               |
                  V2 Awareness Transport Session
                 request frames | response frames
                               |
                       Y-Sweet WebSocket
                               |
                    sandbox Deno v2 sidecar
        claim journal / concurrency semaphore / direct fetch streaming
                               |
       OPENAI_BASE_URL + OPENAI_API_KEY + PROXY_CA_CERT_PATH
```

### 4.1 Host components

#### `AwarenessTransportSession`

- Owns one host Y-Sweet provider per sandbox runtime.
- Identifies the current sidecar instance and protocol build.
- Publishes bounded frames and consumes sidecar frames.
- Maintains epochs, sequence numbers, acknowledgements, retransmission timers, and credits.
- Never stores complete traffic in persistent Yjs types.

#### `V2SlotRunner`

- Exposes the existing `RunHandle` interface.
- Converts a Responses body into request frames.
- Emits SSE frames incrementally.
- Resolves only after matching terminal proof.
- Quarantines ambiguous committed requests exactly as v1 does.

#### `V2RuntimeController`

- Writes or updates the tracked bootstrap source/config files.
- Forces a render launch with a unique build hash and cleaned LaTeX artifacts.
- Retries launch up to three times with condition-based readiness.
- Mints and installs a fresh Y-Sweet token before replacement.
- Drains the old sidecar before planned rotation.
- Falls back to v1 before request commitment when v2 cannot become ready.

#### Existing host layers retained

- `MultiSandboxRouter`
- `ResourceGovernor`
- HTTP and SSE adapters
- Responses WebSocket session and `store:false` response graph
- Images generation/edit compatibility routes
- typed OpenAI-shaped errors
- sanitized monitoring

### 4.2 Sandbox sidecar components

#### `AwarenessPeer`

- Uses vendored `@y-sweet/client` and Yjs.
- Announces instance ID, build, capacity, credits, active count, and health.
- Reads bounded host frames and publishes bounded response frames.

#### `RequestAssembler`

- Validates protocol version, session, request ID, sequence, sizes, and checksums.
- Reassembles segmented request bodies with strict byte and segment limits.
- Decompresses `gzip+base64` only after full integrity validation.

#### `DirectResponsesClient`

- Reads only `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `PROXY_CA_CERT_PATH` for upstream access.
- Sends `POST {OPENAI_BASE_URL}/responses` according to the official Responses API contract.
- Forces upstream HTTP streaming for incremental delivery.
- Preserves model, reasoning, tools, service tier, input, metadata, and all supported fields.
- Never logs credentials or complete sensitive request bodies.

#### `AdmissionController`

- Defaults to 128 active upstream calls per sandbox.
- Queues additional accepted requests in bounded FIFO/fair queues.
- Exposes credits to the host.
- Reduces capacity temporarily after connection-reset bursts.
- Never attempts the rejected unbounded 256-active mode.

#### `ClaimJournal`

- Stores a small local-disk record under `/workspace` for committed requests.
- Records request ID, digest, claim state, upstream response ID when known, and terminal digest.
- Does not carry request/response payloads in the hot path.
- Allows sidecar-process recovery inside the same sandbox.
- Treats a crash after upstream commitment without a terminal as ambiguous; it does not silently replay.

## 5. Awareness frame protocol

### 5.1 Envelope

Every local Awareness state contains one bounded transport envelope:

```text
{
  protocol: "gp-awareness-v2",
  sessionId,
  peerId,
  instanceId,
  epoch,
  ackEpoch,
  credits,
  frames: [...bounded unacknowledged frames]
}
```

The state contains only a sliding window. Acknowledged frames are removed from the next state, preventing the growing-state behavior observed in the prototype.

### 5.2 Frame types

Host to sidecar:

- `REQUEST_OPEN`
- `REQUEST_DATA`
- `REQUEST_COMMIT`
- `REQUEST_CANCEL`
- `RESPONSE_ACK`
- `DRAIN`
- `PING`

Sidecar to host:

- `REQUEST_ACK`
- `REQUEST_REJECT`
- `RESPONSE_EVENT`
- `RESPONSE_TERMINAL`
- `RESPONSE_ERROR`
- `CREDIT`
- `HEARTBEAT`
- `DRAINED`

### 5.3 Bounds

- Maximum encoded frame: 64 KiB.
- Maximum unacknowledged frames per peer: 32 initially, adaptive within a fixed hard ceiling.
- Maximum request body: 64 MiB, matching the daemon transport bound.
- Maximum response assembly: configurable and reservation-backed.
- Request segments use `gzip+base64` when it reduces segment count.
- Each complete message carries decoded byte length and SHA-256.
- Sequence numbers are monotonic per request.

Although a raw 4 MiB Awareness message passed, production uses segmentation for bounded memory, retransmission, cancellation, and predictable latency.

### 5.4 Commitment and rerouting

The sidecar sends `REQUEST_ACK(committed=true)` only after:

1. all request segments are present;
2. decoded size and SHA-256 match;
3. a claim journal entry is durably written;
4. an upstream concurrency permit is reserved.

Rules:

- Failure before committed ACK: host may reroute once.
- Failure after committed ACK: request stays pinned to the sandbox.
- Missing terminal after commitment: lane is quarantined.
- A sidecar crash does not automatically replay an ambiguous upstream call.
- A proven terminal may be replayed from the local journal/recovery artifact.

## 6. Streaming and terminal proof

The sidecar parses upstream SSE incrementally and sends each complete event as one or more `RESPONSE_EVENT` frames.

The host preserves the existing idempotent SSE extractor. It rejects gaps, duplicate conflicting frames, wrong request IDs, and mismatched terminal digests.

`RESPONSE_TERMINAL` contains:

- request ID;
- request digest;
- upstream response ID;
- final Responses object;
- total event count and byte count;
- digest of the canonical complete SSE transcript;
- terminal status (`completed`, `failed`, or `incomplete`).

A lane is released only when terminal identity and digest match the committed request.

## 7. Lifecycle and token rotation

### 7.1 Bootstrap

1. Provision the Prism sandbox and resource/Y-Sweet credentials.
2. Synchronize two tracked bootstrap files: sidecar source and sidecar config.
3. Write a temporary `.latexmkrc` launcher with a unique build marker.
4. Remove LaTeX result artifacts so the launch command is guaranteed to run.
5. Trigger render.
6. Restore the normal launcher immediately.
7. Wait for an Awareness readiness announcement matching session, instance, and build.
8. Retry up to three times before marking v2 unavailable.

### 7.2 Token refresh

- Host refreshes the Y-Sweet token before its expiry horizon.
- Host writes the new token atomically to the tracked sidecar config.
- Host asks the old sidecar to drain.
- Host launches a replacement and requires a new instance ID.
- Host switches traffic only after the replacement advertises readiness.

The crash prototype proved that a fresh token plus forced render launch can restore a replacement sidecar and recover uncommitted work.

### 7.3 Sandbox loss

If the complete Prism sandbox is reprovisioned, local claim/terminal journals disappear. The host treats committed requests without received terminal proof as ambiguous and quarantines them. It never guesses that an upstream call did not happen.

## 8. Concurrency and routing

Defaults:

- 128 active upstream calls per healthy sandbox.
- Four sandboxes advertise up to 512 physical active calls.
- Additional requests queue through existing per-client fairness and resource admission.
- RAM/CPU governor remains authoritative and may advertise less than the physical ceiling.

The host router continues selecting the lowest predicted completion time. Runtime metrics become real v2 measurements rather than constants:

- Awareness RTT;
- request assembly time;
- upstream first-event EWMA;
- upstream terminal EWMA;
- active and queued counts;
- proxy reset rate;
- sidecar restart rate;
- frame retransmission rate;
- credential horizon.

## 9. API behavior

GhostProv's public API does not change:

- `POST /v1/responses`
- HTTP streaming SSE
- Responses WebSocket mode
- named WebSocket lanes
- `store:false` continuation reconstructed on the host
- `previous_response_id`
- reasoning modes and service tier
- native-shaped GPT Image generation/edit routes

The sidecar forwards the normalized Responses body after host-side continuation reconstruction. It does not implement public API compatibility itself.

Official upstream contract: [Create a response](https://developers.openai.com/api/reference/resources/responses/methods/create).

## 10. Failure handling

| Failure | Required behavior |
| --- | --- |
| Sidecar not ready | Three launch attempts, then v1 fallback before commitment |
| Y-Sweet disconnect before commitment | Reconnect/retransmit or reroute once |
| Y-Sweet disconnect after commitment | Stay pinned; resume frames; quarantine if terminal remains unproven |
| Sidecar crash with queued/uncommitted request | Replacement may reclaim from host Awareness state |
| Sidecar crash after committed ACK | Consult journal; never blind replay |
| Proxy connection reset | Retry only before committed upstream response; reduce advertised capacity |
| Token expiry | Drain, refresh config, replace sidecar |
| Complete sandbox loss | Mark committed in-flight requests ambiguous and quarantine |
| Oversized frame/body | Reject with typed validation error before upstream call |
| Host disconnect | Cancel queued/uncommitted work; preserve committed terminal recovery |

## 11. Migration and fallback

The rollout is opt-in:

```text
GP_TRANSPORT=v1          existing file/ZIP transport
GP_TRANSPORT=v2          Awareness sidecar only; fail closed after commitment
GP_TRANSPORT=auto        prefer v2, use v1 only before request commitment
```

Rollout order:

1. deterministic in-memory protocol tests;
2. one test project in `v2` mode;
3. one production project in `auto` mode;
4. four-project mixed canary;
5. default `auto` after sustained metrics pass;
6. retain v1 recovery for at least one release cycle.

There is no dual execution of the same request.

## 12. Test strategy

### Unit and property tests

- frame encode/decode round trips;
- segmentation across every byte boundary;
- gzip integrity and decompression bounds;
- sequence/epoch wrap and stale-frame rejection;
- ACK window advancement;
- credit enforcement;
- duplicate and conflicting frame handling;
- terminal digest validation;
- journal state transitions;
- cancellation before/after commitment;
- capacity reduction after proxy resets.

### Deterministic integration

- fake Awareness peers with loss, duplication, reordering, and reconnect;
- 512 simultaneous logical requests across four fake runtimes;
- exact unique terminal IDs;
- no physical oversubscription;
- sidecar replacement with queued work;
- ambiguous committed crash quarantines instead of replaying;
- v2 bootstrap failure falls back to v1 only before commitment.

### Live gates

- 4 MiB segmented bidirectional integrity;
- 128 active direct Responses on one sandbox;
- 256 accepted with 128 active and zero errors;
- 512 active across four synchronized sandboxes;
- repeated lifecycle launch/replacement cycles;
- sidecar crash with queued and committed cases;
- token rotation during idle and active periods;
- HTTP SSE, Codex WebSocket continuation, and named lanes;
- GPT Image generation and multipart editing;
- production port 8787 health before and after every candidate run.

## 13. Acceptance criteria

Correctness:

- zero duplicate upstream execution caused by host rerouting after commitment;
- zero lane release without matching terminal proof;
- exact response/request identity and digest validation;
- 100% success for bounded 128-active test waves;
- 100% success for 512 aggregate active canary;
- crash ambiguity is quarantined, not silently retried.

Performance:

- one-sandbox physical active capacity: 128;
- four-sandbox physical active capacity: 512;
- one-sandbox 128-call first-event p95 below 4 seconds for the proof prompt;
- 512-call aggregate first-event p95 below 7 seconds per sandbox for the proof prompt;
- small Awareness transport p95 below 1 second at 512 accepted / 128 active;
- queueing occurs only at configured credits/resource limits;
- no request-time ZIP polling or workspace synchronization in healthy v2 mode.

Lifecycle:

- first-attempt sidecar launch target at least 95% over a 100-cycle soak;
- three-attempt launch success 100% in the same soak;
- token replacement produces a new ready instance before traffic switches;
- Crixet PID 1 and its supervised Deno child remain untouched.

## 14. Explicit non-goals

- Replacing or restarting Crixet's supervised Deno server.
- Exposing a new public sandbox port.
- Depending on sandbox internet package installation.
- Replacing the host API/router with the sidecar.
- Realtime audio support in this phase.
- Removing v1 recovery before v2 has completed its canary period.

## 15. Conclusion

The prototype proves that the Awareness sidecar is materially more scalable than v1: 128 active calls per sandbox and 512 across four sandboxes, compared with 30 and 120 respectively.

The production design must not copy the prototype's growing result array. Its bounded acknowledged frame protocol, strict commitment boundary, local claim journal, and v1 pre-commit fallback are required to preserve GhostProv's strongest property: no lane is released or rerouted after an unproven committed request.
