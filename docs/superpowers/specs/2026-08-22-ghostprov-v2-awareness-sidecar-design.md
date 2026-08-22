# GhostProv v2 Awareness Sidecar Transport

**Status:** Proposed, empirically validated prototype
**Date:** 2026-08-22
**Branch:** `br/v2-awareness-sidecar`
**Supersedes:** The v1 sandbox file/ZIP hot path. V1 remains a signed-in, pre-commit migration fallback only.

## 1. Decision

GhostProv v2 will preserve the existing host API, router, Codex WebSocket behavior, image compatibility, project provisioning, resource governor, terminal validation, and quarantine rules.

Prism sessions may be signed-in or backend-owned anonymous guest sessions. Signed-in sessions remain the preferred production mode during migration. Guest sessions are an explicit, capability-gated V2 mode; they are not described as an authentication bypass and never scrape browser cookies or storage.

Fleet size is dynamic. Four sandboxes remain the live-tested default, not an architectural limit. The desired fleet is derived from configured target concurrency or an explicit sandbox count, and the control plane may reconcile any operator-approved number of projects within a configurable safety ceiling.

It will replace the request-time transport:

```text
v1: host Yjs file write -> wait-for-sync -> sandbox disk -> daemon -> disk response -> ZIP poll

v2: host Y-Sweet Awareness frames -> sandbox sidecar -> direct Responses API stream
    -> Y-Sweet Awareness frames -> host
```

The v2 sandbox sidecar requires no public port. It connects to the same authenticated Y-Sweet project document as the host, but carries request traffic through the ephemeral Awareness protocol rather than persistent Yjs document structures.

The supervised Crixet Deno process remains alive on port 8080. GhostProv never patches or restarts it.

V2 does not use either Prism's project-export archive endpoint or the sandbox proxy `/zip` endpoint. ZIP remains only inside the legacy v1 implementation. An anonymous worker is V2-only: if its sidecar is unavailable before commitment, that worker is unavailable rather than falling back to v1.

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
- Prism exposes a backend-owned guest bootstrap at `POST /api/auth/anonymous-session` and persists the identity in HttpOnly cookies.
- One clean guest session created and listed four distinct projects, provisioned a sandbox, minted project-resource and Y-Sweet credentials, reached `wait-for-sync=syncing -> synced`, and completed a heartbeat.
- Guest-owned binary upload and Yjs text-file creation were accepted, and a fresh Y-Sweet client observed the newly created project files.
- Chrome deliberately disables its Create/Upload File controls for guest users. Guest multi-file automation is therefore an internal capability, not a stable public UI contract, and must be startup-probed before use.
- The sandbox proxy `/zip` returned HTTP 500 for a newly automated guest project even after a render was accepted. This does not block V2 because ZIP is absent from its bootstrap and request paths, but it prohibits v1 fallback for guest workers.

The sidecar must therefore:

1. keep Crixet alive;
2. use the existing project Y-Sweet connection;
3. use only vendored dependencies;
4. use ephemeral Awareness frames to avoid persistent CRDT growth;
5. call the configured OpenAI proxy through the provided environment and CA.
6. avoid every ZIP/export dependency in V2.

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

### 3.5 Anonymous control-plane proof

A clean cookie jar reproduced Prism's backend-owned anonymous-session flow without browser state:

| Proof | Result |
| --- | --- |
| Guest bootstrap | HTTP 200; anonymous user; HttpOnly device/session cookies |
| Four projects under one guest identity | 4/4 created, listed, and matched by UUID |
| Binary object upload | HTTP 200 with a project-scoped file object |
| Sandbox allocation | HTTP 200; approximately 0.5 seconds in the observed run |
| Resource and Y-Sweet credentials | HTTP 200; project and document identity matched |
| Sandbox credential installation | both resource and Y-Sweet token pushes returned HTTP 200 |
| Workspace readiness | `syncing -> synced` in approximately 1.15 seconds |
| Heartbeat | HTTP 200 with a sandbox session identifier |
| Yjs text tree | a second fresh client observed `main.tex` and the probe file |

Every probe used a dedicated guest identity and exact probe project UUIDs; the created probe projects were deleted after the test. Production reconciliation never deletes managed projects automatically.

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

#### `PrismSessionManager`

- Loads one durable Prism cookie jar and refreshes it under an exclusive account lock.
- Reuses a valid signed-in or anonymous session before creating any identity.
- In anonymous mode, calls only the backend-owned `POST /api/auth/anonymous-session` flow and resolves an existing-session conflict through `/auth/session`.
- Persists the cookie jar atomically with owner-only permissions; losing this jar means losing access to its guest-owned project fleet.
- Never reads Chrome cookies, local storage, profiles, or passwords.
- Runs a bounded startup capability probe before guest workers enter the registry.

#### `AwarenessTransportSession`

- Owns one host Y-Sweet provider per sandbox runtime.
- Identifies the current sidecar instance and protocol build.
- Publishes bounded frames and consumes sidecar frames.
- Maintains epochs, sequence numbers, acknowledgements, retransmission timers, and credits.
- Enforces a host-issued, manifest-persisted leader generation so a stale sidecar cannot accept or emit traffic.
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
- Falls back to v1 before request commitment only for signed-in workers in `auto` transport mode.
- Marks an anonymous worker unavailable when V2 cannot become ready; it never invokes a ZIP-based path.

#### `ProjectFleetManager`

- Inventories Prism projects through the current persisted signed-in or guest session.
- Reuses explicit project IDs first.
- Reuses previously managed GhostProv worker projects next.
- May reuse an unmarked owned project only when it is empty, writable, and not leased by another runtime.
- Creates exactly the missing number of Prism projects when automatic creation is enabled.
- Persists creation intents and project IDs atomically so reconciliation is idempotent after crashes.
- Scaffolds and starts runtimes independently with bounded provisioning concurrency.
- Adds ready runtimes to the registry without restarting the HTTP server.
- Drains removed/excess runtimes but never deletes a Prism project automatically.
- Continuously reconciles desired, provisioning, ready, draining, and failed fleet state.

#### Shared protocol package

- Defines the frame schema, protocol version, state-machine transitions, limits, and canonical digest rules once for the host and generated Deno sidecar.
- Produces a deterministic build hash embedded in both peers.
- Rejects readiness, claims, or frames when protocol version, schema hash, build hash, project, session, or leader generation does not match.

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
- Persists `FETCH_STARTING` before calling upstream. Once `fetch()` may have started, no automatic retry or reroute is permitted, even when no response headers arrived.
- Never logs credentials or complete sensitive request bodies.

#### `AdmissionController`

- Defaults to 128 active upstream calls per sandbox.
- Queues additional accepted requests in bounded FIFO/fair queues.
- Exposes credits to the host.
- Accepts a live capacity ceiling from the host `ResourceGovernor`; 128 is a physical upper bound, not a promise to oversubscribe a constrained machine.
- Reduces capacity temporarily after connection-reset bursts.
- Never attempts the rejected unbounded 256-active mode.

#### `ClaimJournal`

- Stores append-only records under `/tmp/ghostprov-v2`; it never pollutes the synchronized `/workspace` project tree.
- Group-commits bounded batches and acknowledges commitment only after the relevant journal boundary is durable.
- Records request ID, digest, host leader generation, claim state, `FETCH_STARTING`, upstream response ID when known, and terminal digest.
- Does not carry request/response payloads in the hot path.
- Stores a completed terminal transcript in a separate bounded recovery spool only until the host acknowledges it.
- Allows sidecar-process recovery inside the same sandbox.
- Treats a crash after upstream commitment without a terminal as ambiguous; it does not silently replay.

## 5. Awareness frame protocol

### 5.1 Envelope

Every local Awareness state contains one bounded transport envelope:

```text
{
  protocol: "gp-awareness-v2",
  schemaHash,
  buildHash,
  sessionId,
  peerId,
  instanceId,
  leaderGeneration,
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

- Hard maximum encoded frame: 16 KiB, including envelope overhead; payload sizing reserves space for JSON fields.
- Maximum unacknowledged state per peer: 32 frames and 512 KiB total; both limits must pass before publication.
- Maximum request body: 64 MiB, matching the daemon transport bound.
- Maximum response assembly: configurable and reservation-backed.
- Request segments use `gzip+base64` when it reduces segment count.
- Each complete message carries decoded byte length and SHA-256.
- Sequence numbers are monotonic per request.

Although a raw 4 MiB Awareness message passed, production uses segmentation for bounded memory, retransmission, cancellation, and predictable latency.

### 5.4 Frame scheduling

Each peer uses weighted fair scheduling rather than draining one large request or response at a time:

1. terminal, rejection, cancellation, and ACK control frames;
2. one frame from each active response stream in round-robin order;
3. request commit/control frames;
4. request and response data frames weighted by reserved workload cost.

No stream may consume the complete 32-frame window while another ready stream is waiting. Retransmissions retain their original sequence identity and do not create a second logical frame.

### 5.5 Commitment and rerouting

The sidecar sends `REQUEST_ACK(committed=true)` only after:

1. all request segments are present;
2. decoded size and SHA-256 match;
3. the host leader generation matches the active sidecar fence;
4. a claim journal entry is durably group-committed;
5. an upstream concurrency permit is reserved.

Rules:

- Failure before committed ACK: host may reroute once.
- Failure after committed ACK: request stays pinned to the sandbox.
- The sidecar appends `FETCH_STARTING` before invoking upstream `fetch()`.
- Any failure after `FETCH_STARTING` is ambiguous unless a matching terminal is recovered; neither peer retries it automatically.
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

1. Resolve one persisted signed-in or guest Prism session and acquire its single-writer account lock.
2. Provision the Prism sandbox and resource/Y-Sweet credentials.
3. Synchronize the tracked text bootstrap files through Yjs: sidecar source, sidecar config, and launcher.
4. Write a temporary `.latexmkrc` launcher with a unique build marker.
5. Remove LaTeX result artifacts so the launch command is guaranteed to run.
6. Trigger render.
7. Restore the normal launcher immediately.
8. Wait for an Awareness readiness announcement matching project, session, leader generation, instance, schema, and build.
9. Retry up to three times before marking V2 unavailable.

No bootstrap step downloads a project export or calls the sandbox `/zip` endpoint.

### 7.2 Token refresh

- Host refreshes the Y-Sweet token before its expiry horizon.
- Host writes the new token atomically to the tracked sidecar config.
- Host asks the old sidecar to drain.
- Host launches a replacement and requires a new instance ID.
- Host switches traffic only after the replacement advertises readiness.

The crash prototype proved that a fresh token plus forced render launch can restore a replacement sidecar and recover uncommitted work.

### 7.3 Guest-session lifecycle

- One durable anonymous cookie jar owns the complete guest fleet; workers do not create independent guest identities.
- The jar and fleet manifest are written atomically under an exclusive process lock and use owner-only filesystem permissions.
- Startup reuses `/auth/session` when the jar is valid and calls `POST /api/auth/anonymous-session` only when no session exists.
- Guest capability gating proves project create/list, Y-Sweet mint, resource mint, sandbox allocation, credential installation, workspace readiness, Yjs text synchronization, and V2 sidecar readiness.
- Capability gating does not require ZIP and does not delete an existing Prism project.
- A lost or rejected guest session drains its workers and requires recovery of the same cookie jar; GhostProv does not silently create a replacement identity and orphan the old fleet.

### 7.4 Sandbox loss

If the complete Prism sandbox is reprovisioned, local claim/terminal journals disappear. The host treats committed requests without received terminal proof as ambiguous and quarantines them. It never guesses that an upstream call did not happen.

## 8. Concurrency and routing

Defaults:

- 128 active upstream calls per healthy sandbox.
- Four sandboxes advertise up to 512 physical active calls.
- Additional requests queue through existing per-client fairness and resource admission.
- RAM/CPU governor remains authoritative and may advertise less than the physical ceiling.

Four is only the default produced by `ceil(512 / 128)`. No router, registry, monitoring structure, or protocol field may use a fixed four-element array or assume four project IDs.

### 8.1 Fleet sizing

Configuration:

```text
GP_TARGET_CONCURRENCY=512       desired aggregate active capacity
GP_TARGET_SANDBOXES=<optional>  explicit desired worker count; overrides derived count
GP_V2_SANDBOX_CAPACITY=128      advertised active capacity per healthy v2 sidecar
GP_MAX_PROJECTS=32              operator safety ceiling, not a protocol/architecture limit
GP_AUTO_CREATE_PROJECTS=1       create missing managed Prism projects
GP_PROJECT_REUSE=managed        managed | eligible-owned | explicit
GP_PROVISION_CONCURRENCY=4      maximum simultaneous sandbox bootstraps
GP_PRISM_SESSION=auto           auto | authenticated | anonymous
GP_PRISM_COOKIE_FILE=<path>     durable owner-only signed-in or guest cookie jar
```

Desired count:

```text
desired = GP_TARGET_SANDBOXES
       ?? ceil(GP_TARGET_CONCURRENCY / GP_V2_SANDBOX_CAPACITY)
```

The desired count must be positive and no greater than the operator's `GP_MAX_PROJECTS`. Increasing `GP_MAX_PROJECTS` requires no code change.

`auto` reuses whichever valid identity is already stored in `GP_PRISM_COOKIE_FILE`; when the file is absent it creates one backend-owned guest session. `authenticated` fails if that jar is not signed in. `anonymous` rejects a signed-in jar and creates or reuses only a guest identity. A single configuration never mixes identities inside one fleet manager.

### 8.2 Project discovery and reuse

Reconciliation order within the current Prism identity:

1. deduplicated `PRISM_PROJECT_IDS` supplied explicitly;
2. IDs already recorded in `.ghostprov/fleet.json`;
3. owned, non-deleted Prism projects carrying the GhostProv worker title/marker;
4. when `GP_PROJECT_REUSE=eligible-owned`, session-owned writable projects proven empty;
5. newly created managed projects until `desired` is reached.

Non-empty unmarked projects are never modified automatically. They are used only when explicitly listed in `PRISM_PROJECT_IDS`.

Managed project identity:

- title: `GhostProv Worker <stable-index>`;
- locally persisted UUID and creation state;
- tracked project marker containing protocol version, stable worker index, and creation timestamp;
- no credentials or account tokens in the marker.

### 8.3 Idempotent automatic creation

For every missing worker, the fleet manager:

1. reserves a stable worker index under an exclusive local fleet lock;
2. atomically persists a `creating` intent and project UUID;
3. refetches project inventory to avoid duplicate creation after a crash;
4. creates the project only when the UUID/title is absent;
5. atomically records the returned project ID;
6. initializes the Prism Yjs root/settings schema when absent;
7. writes the GhostProv worker marker and text bootstrap assets through Yjs without ZIP;
8. provisions resource/Y-Sweet credentials and launches the sidecar;
9. registers capacity only after matching build/readiness proof.

Creation failures affect only the missing worker. Healthy existing runtimes remain routable, and reconciliation retries with bounded backoff. GhostProv never deletes newly created or reused projects during automatic scale-down; it stops local ownership and leaves them available for later reuse.

The live proof established four projects under one guest identity. Larger guest fleets remain governed by the same `GP_MAX_PROJECTS`, provisioning concurrency, backoff, and runtime-capacity controls; deterministic tests cover 1, 4, 8, 16, and 32 without claiming an unmeasured Prism account quota.

### 8.4 Dynamic registry and routing

`SandboxRegistry` becomes a mutable, concurrency-safe runtime registry with explicit `add`, `drain`, `removeStopped`, and snapshot operations. Router decisions always consume a point-in-time iterable and remain O(number of healthy runtimes). Project bootstrap runs outside the routing lock.

Heartbeats retain independent ten-second schedules with deterministic per-project phase offsets, preventing a large fleet from generating one synchronized burst.

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
| Signed-in sidecar not ready in `auto` | Three launch attempts, then v1 fallback before commitment |
| Anonymous sidecar not ready | Three launch attempts, mark worker unavailable; never use v1/ZIP |
| Y-Sweet disconnect before commitment | Reconnect/retransmit or reroute once |
| Y-Sweet disconnect after commitment | Stay pinned; resume frames; quarantine if terminal remains unproven |
| Sidecar crash with queued/uncommitted request | Replacement may reclaim from host Awareness state |
| Sidecar crash after committed ACK | Consult journal; never blind replay |
| Failure before `FETCH_STARTING` | Safe local failure or one pre-commit reroute according to state |
| Failure at/after `FETCH_STARTING` | Treat as ambiguous without matching terminal; never automatic retry |
| Proxy connection reset | Apply the same fetch boundary and reduce advertised capacity |
| Token expiry | Drain, refresh config, replace sidecar |
| Complete sandbox loss | Mark committed in-flight requests ambiguous and quarantine |
| Stale sidecar/host generation | Reject every frame and claim; only the current host-issued leader may commit |
| Lost guest cookie jar | Drain guest workers and require recovery; never create a replacement identity silently |
| Oversized frame/body | Reject with typed validation error before upstream call |
| Host disconnect | Cancel queued/uncommitted work; preserve committed terminal recovery |

## 11. Migration and fallback

The rollout is opt-in:

```text
GP_TRANSPORT=v1          existing file/ZIP transport
GP_TRANSPORT=v2          Awareness sidecar only; fail closed after commitment
GP_TRANSPORT=auto        prefer v2; signed-in workers may use v1 only before commitment
```

Automatic project creation is authorized only when `GP_AUTO_CREATE_PROJECTS=1`; v2's standard managed deployment enables it. Explicit project IDs and discovered managed projects are always reused before creation.

`GP_PRISM_SESSION=anonymous` requires `GP_TRANSPORT=v2`; `auto` resolves to V2-only for a guest identity. Configuration validation rejects `anonymous + v1`. The public HTTP/WebSocket API remains available while at least one V2 guest worker is ready; otherwise readiness fails without touching a different Prism identity.

Rollout order:

1. deterministic in-memory protocol and guest-session tests;
2. one signed-in test project in `v2` mode;
3. one signed-in production project in `auto` mode;
4. one isolated guest project in V2-only mode;
5. a four-project canary for the selected identity mode, never sharing identities inside one manager;
6. default `auto` after sustained metrics pass;
7. retain v1 recovery for signed-in fleets for at least one release cycle.

There is no dual execution of the same request.

## 12. Test strategy

### Unit and property tests

- frame encode/decode round trips;
- segmentation across every byte boundary;
- gzip integrity and decompression bounds;
- sequence/epoch wrap and stale-frame rejection;
- stale leader-generation and build/schema rejection;
- ACK window advancement;
- 16 KiB frame and 32-frame/512 KiB window enforcement;
- credit enforcement;
- weighted-fair scheduling without stream starvation;
- duplicate and conflicting frame handling;
- terminal digest validation;
- append-only group-commit journal state transitions;
- `FETCH_STARTING` ambiguity and no-retry enforcement;
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
- guest bootstrap persists and reuses one anonymous identity without browser storage access;
- guest configuration never invokes ZIP or v1;
- losing a guest jar does not create a replacement identity silently;
- project inventory reuses existing managed IDs before creating anything;
- partial inventories create exactly the missing workers;
- repeated reconciliation is idempotent after failures at every creation checkpoint;
- dynamic fake fleets of 1, 4, 8, 16, and 32 runtimes route without fixed-size assumptions;
- runtime addition/removal never pauses healthy routing;
- automatic scale-down drains but never deletes cloud projects.

### Live gates

- 4 MiB segmented bidirectional integrity;
- 128 active direct Responses on one sandbox;
- 256 accepted with 128 active and zero errors;
- 512 active across four synchronized sandboxes;
- repeated lifecycle launch/replacement cycles;
- sidecar crash with queued and committed cases;
- token rotation during idle and active periods;
- anonymous session bootstrap, four-project inventory, V2 sidecar readiness, and restart reuse;
- HTTP SSE, Codex WebSocket continuation, and named lanes;
- GPT Image generation and multipart editing;
- production port 8787 health before and after every candidate run.

Live fleet testing remains capped at the four projects already authorized and proven in this phase. Support for larger fleets is verified deterministically now; additional live projects require a later explicit load-test decision, not a code change.

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
- zero ZIP/export calls during V2 bootstrap, steady state, recovery, and anonymous operation;
- fleet reconciliation does not hold the router hot-path lock;
- routing remains below the 2 ms p95 budget at the configured safety ceiling.

Lifecycle:

- first-attempt sidecar launch target at least 95% over a 100-cycle soak;
- three-attempt launch success 100% in the same soak;
- token replacement produces a new ready instance before traffic switches;
- Crixet PID 1 and its supervised Deno child remain untouched.
- restart reconciliation reuses the same project fleet without duplicate creation;
- a partial fleet becomes available incrementally instead of waiting for every project;
- no automatic workflow deletes a Prism project.
- a restarted anonymous fleet reuses the same cookie jar and project IDs.

## 14. Explicit non-goals

- Replacing or restarting Crixet's supervised Deno server.
- Exposing a new public sandbox port.
- Depending on sandbox internet package installation.
- Replacing the host API/router with the sidecar.
- Realtime audio support in this phase.
- Removing v1 recovery before v2 has completed its canary period.
- Treating guest-only internal Prism endpoints as a guaranteed public API.
- Scraping or importing credentials from Chrome.

## 15. Conclusion

The prototype proves that the Awareness sidecar is materially more scalable than v1: 128 active calls per sandbox and 512 across four sandboxes, compared with 30 and 120 respectively.

The implementation is not limited to four sandboxes. Four is the default live-proven fleet; desired capacity, project discovery, idempotent creation, and the dynamic runtime registry allow larger fleets without changing transport or routing code.

The supported backend-owned guest flow can remove manual sign-in for an isolated V2 fleet, but it does not change the transport safety model. Guest capability probes, durable identity storage, and fail-closed V2-only behavior contain the risk of Prism's guest UI restrictions changing.

The production design must not copy the prototype's growing result array. Its bounded acknowledged frame protocol, host-issued leader fence, strict `FETCH_STARTING` boundary, and append-only local claim journal preserve GhostProv's strongest property: no lane is released, rerouted, or retried after an unproven committed request. Signed-in `auto` fleets alone retain v1 as a pre-commit migration fallback.
