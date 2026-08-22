# GhostProv Browserless Multi-Sandbox Router Design

Date: 2026-08-22
Branch: `br/multi-sandbox-router`

## Objective

Turn `localhost:8787` into a browserless control-plane router over multiple independent Prism projects. Each project owns one sandbox runtime with the same GhostProv daemon and up to 30 physical lanes. Four healthy projects therefore advertise up to 120 simultaneous relay lanes while preserving the existing OpenAI-compatible HTTP, SSE, Codex WebSocket, image, terminal-recovery, and `store:false` behaviors.

The implementation must be maintainable, strongly typed, resource-aware, and failure-isolated. It must remove Playwright from the runtime and dependency graph after direct HTTP/WebSocket parity is proven. Development and live verification must not stop, restart, or mutate the existing GhostProv process on port 8787.

## Proven Browserless Capabilities

The supplied Prism traces and direct Node probes established the browserless contract:

- Direct authenticated `POST /api/y` returned HTTP 200 and a Y-Sweet credential envelope.
- A direct Node `y-websocket` connection synchronized successfully using that credential.
- Direct Node requests to sandbox `heartbeat` and `wait-for-sync` returned HTTP 200 when supplied both the authenticated account cookie jar and sandbox token.
- A sandbox token without the account cookie jar returned HTTP 401. Account cookies are therefore an explicit control-plane dependency, not an incidental browser detail.
- The current Prism client bundle exposes the complete provisioning sequence and exact JSON body shapes documented below.

These results prove that Playwright is not required for Prism transport, Yjs synchronization, sandbox heartbeats, or readiness. Playwright currently supplies an authenticated cookie context and token refresh behavior; the replacement must provide those responsibilities directly.

## Scope

### Included

- One independent sandbox runtime per configured Prism project/document.
- A shared account cookie jar for all projects under the same Prism account.
- Direct HTTP provisioning, credential synchronization, heartbeat, gateway, and ZIP calls.
- Direct Node Yjs WebSocket connections per project.
- Automatic deployment and verification of the same `gp_daemon.py` build in every project.
- Dynamic global capacity equal to the sum of healthy runtime capacity.
- Intelligent least-work routing with latency, load, health, and memory signals.
- Hardware-aware RAM and CPU admission control.
- Exact 10-second heartbeat cadence per syncing or ready sandbox.
- Failure isolation, draining, recovery, terminal proof, and lane quarantine.
- Aggregate and per-sandbox monitoring.
- Backward-compatible single-project configuration.
- Removal of Playwright source integration and package dependency after parity tests pass.

### Excluded

- Multiple sandboxes sharing one Prism project/document. Shared Yjs job files would permit duplicate daemon execution without a separate namespace protocol.
- Bypassing account-wide upstream service limits.
- Splitting one model response across multiple sandboxes.
- Retrying a request on another sandbox after publication.
- Interactive Prism login. Browserless GhostProv consumes an externally provisioned authenticated cookie jar and reports `auth_required` when it expires.
- Realtime/WebRTC work deferred in the previous scope.

## Capacity Model

Each healthy daemon advertises its current physical slot capacity. The router's hard instantaneous lane ceiling is:

```text
sum(runtime.availablePhysicalCapacity for every healthy runtime)
```

With four 30-slot projects, the physical ceiling is 120 active requests. No static reserve is withheld. Capacity falls immediately when a runtime is unhealthy, draining, or has quarantined lanes.

The physical ceiling is not the only admission constraint. Every request must also acquire a byte reservation and, when needed, a CPU-transform permit. Actual active work is therefore:

```text
min(healthy physical lanes, work fitting the live memory/CPU budget)
```

Four projects multiply throughput and remove queueing under concurrent load. They do not reduce the model-generation duration of one sequential response. A single sequential Codex task normally occupies one lane; reaching high concurrency requires multiple tasks, callers, named WebSocket lanes, or image fan-out.

## Module Architecture

```text
src/prism/
  account-session.ts       authenticated cookie jar and direct account HTTP
  provisioning-client.ts   backend/resource/Yjs credential lifecycle
  protocol.ts              validated Prism request/response schemas

src/sandbox/
  runtime.ts               one project's lifecycle state machine
  registry.ts              owns and reconciles all runtimes
  gateway.ts               project-scoped direct sandbox HTTP/ZIP client
  ydoc.ts                  project-scoped Node Yjs connection
  daemon-controller.ts     deploys and verifies the daemon build
  slot-pool.ts             project-scoped physical lane ownership
  types.ts                 opaque IDs, lifecycle states, snapshots

src/router/
  router.ts                global admission, queueing, and dispatch
  policy.ts                pure predicted-completion routing policy
  resource-governor.ts     hardware detection and byte/CPU reservations
  workload.ts              request classification and memory estimates
  types.ts                 route candidates, decisions, leases, metrics

src/relay/
  slot-runner.ts           one request bound to one SandboxRuntime
  terminal-recovery.ts     terminal proof, replay, and quarantine

src/server/
  app.ts                   HTTP composition root
  websocket.ts             Codex WebSocket composition
```

Existing compatibility helpers may remain temporarily behind adapters during migration. Final active code must not import Playwright or depend on module-level project, token, Yjs, gateway, ZIP coordinator, slot pool, daemon readiness, or folder state.

## Type and Abstraction Rules

### Opaque identifiers

Use branded values so project, sandbox, session, request, and slot identifiers cannot be interchanged accidentally:

```ts
type Brand<T, Name extends string> = T & { readonly __brand: Name };
type ProjectId = Brand<string, "ProjectId">;
type SandboxId = Brand<string, "SandboxId">;
type SandboxSessionId = Brand<string, "SandboxSessionId">;
type RelayRequestId = Brand<string, "RelayRequestId">;
type SlotId = Brand<number, "SlotId">;
```

Credential types are opaque wrappers. They expose explicit header/query serialization but not implicit string conversion or JSON serialization. Logs and snapshots contain credential presence, source, and expiry only.

### Discriminated lifecycle state

`SandboxRuntimeState` is a discriminated union rather than a mutable object with unrelated booleans:

```text
configured
provisioning
syncing_credentials
syncing_workspace
deploying_daemon
ready
draining
recovering
auth_required
failed
stopped
```

Each state carries only fields valid in that state. Transitions happen through one reducer/state machine and emit typed lifecycle events.

### Dependency injection

Core units consume narrow interfaces:

- `AccountTransport`
- `SandboxTransport`
- `YDocConnectionFactory`
- `DaemonController`
- `Clock`
- `RandomSource`
- `MachineProbe`
- `MetricsSink`

Production composition uses Bun/Node implementations. Tests use deterministic clocks, transports, machine profiles, and randomness without production test hooks.

### Pure policy

Routing and resource calculations are pure functions over immutable snapshots. They do not perform network calls or mutate runtime state. This makes capacity, fairness, and failure behavior exhaustively testable.

## Account Authentication and Cookie Lifecycle

One `PrismAccountSession` serves every project under the same account.

Configuration order:

1. `PRISM_COOKIE_FILE` containing a persisted cookie jar — recommended.
2. `PRISM_COOKIE` containing an initial Cookie header — compatibility/bootstrap.

The session:

- Applies matching cookies to direct Prism requests.
- Consumes every `Set-Cookie` response and updates the jar atomically.
- Hot-reloads the cookie file on a newer modification time.
- Serializes cookie-jar persistence through one writer.
- Never writes credentials into `.env`, debug responses, normal logs, or metrics.
- Enters `auth_required` after definitive account authentication failure.
- Does not attempt interactive browser login or silently reuse stale credentials.

The same cookie jar is reused for all configured projects, while every sandbox/Yjs/resource credential remains project/runtime scoped.

## Exact Prism Provisioning Sequence

For each configured `ProjectId`, `PrismProvisioningClient` performs:

1. Acquire a sandbox:

   ```http
   POST /api/backend/1/new
   Cookie: <account jar>
   ```

   Parse `url`, `token`, optional `sandbox_id`, and optional `sandbox_session_id`.

2. Mint the project resource credential:

   ```http
   POST /api/projects/{projectId}/sandbox/resources-token
   Cookie: <account jar>
   Content-Type: application/json

   {"sandbox_session_id": "<session-or-null>"}
   ```

3. Push resource credentials into the new sandbox:

   ```http
   POST {sandboxUrl}/resources-token
   X-Crixet-Sandbox-Token: <sandbox token>
   Cookie: <account jar>
   Content-Type: application/json

   {
     "token": "<resource access token>",
     "resourceBaseUrl": "<normalized resource base URL>",
     "projectId": "<project ID>"
   }
   ```

4. Mint a current Y-Sweet credential:

   ```http
   POST /api/y
   Cookie: <account jar>
   Content-Type: application/json

   {
     "docId": "<project ID>",
     "requestContext": {
       "source": "initial-bootstrap",
       "bootstrapAttempt": 1,
       "previouslyConnected": false,
       "sandboxUrl": "<sandbox URL>",
       "sandboxId": "<optional sandbox ID>",
       "sandboxSessionId": "<optional session ID>",
       "maxAttempts": 5,
       "requestSeriesId": "<UUID>"
     }
   }
   ```

5. Push the normalized Y-Sweet response envelope into the sandbox:

   ```http
   POST {sandboxUrl}/token
   X-Crixet-Sandbox-Token: <sandbox token>
   Cookie: <account jar>
   Content-Type: application/json

   <normalized /api/y response>
   ```

6. Poll `{sandboxUrl}/wait-for-sync` every 250 milliseconds for at most 30 seconds. Readiness requires the current Y-Sweet provider capability and:

   ```text
   status == "synced"
   tokens.hasCurrentYSweetToken == true
   tokens.hasSyncedYSweetProvider == true
   ```

   Legacy readiness responses without explicit capability fields remain supported only when their documented token flags prove synchronization.

7. Establish the project-scoped Node Yjs WebSocket connection.

8. Deploy/verify daemon assets and wait for matching `gp_status.json` build/capacity.

9. Enter `ready` and become eligible for routing.

Resource credentials refresh five minutes before reported expiry. Y-Sweet credentials refresh on provider close/auth failure or lifecycle resynchronization. A sandbox confirmed expired is drained and reprovisioned; another runtime continues serving traffic.

## Ten-Second Heartbeat Invariant

Every `syncing_credentials`, `syncing_workspace`, `deploying_daemon`, `ready`, or `draining` runtime owns an independent heartbeat schedule.

- Period: exactly 10,000 milliseconds measured with a monotonic clock.
- Initial phase: deterministically staggered by project ID so multiple projects do not fire simultaneously.
- Subsequent deadline: previous scheduled deadline plus 10,000 milliseconds, avoiding timer drift.
- Maximum permitted successful-heartbeat gap while the process is runnable: 10 seconds plus transport timeout, never a multiplied interval.
- Requests include the project runtime's current cookie jar and sandbox token.
- Only one heartbeat is in flight per runtime.
- A delayed tick sends immediately; it does not enqueue overlapping catch-up heartbeats.
- Heartbeat 401 triggers account-auth handling; expired-session statuses trigger runtime reprovisioning; transient failures apply bounded retries without blocking other runtimes.
- Heartbeat timers start during sandbox credential synchronization and stop only after runtime shutdown.

Tests use a fake monotonic clock to advance several minutes and assert one independent heartbeat per runtime at every 10-second deadline, including slow responses and lifecycle transitions.

## Project-Scoped SandboxRuntime

`SandboxRuntime` owns all state that is currently global:

- Project ID and lifecycle state.
- Sandbox/session identity and credential expiry.
- Direct sandbox transport.
- One Yjs document/provider and filename index.
- One ZIP poll coordinator with at most one archive fetch/decompression in flight.
- One slot pool sized from the daemon's advertised capacity.
- Segment-folder ID and per-lane artifact state.
- Daemon readiness/build metadata.
- Heartbeat scheduler.
- ACK/completion/failure EWMAs by workload class.
- Quarantined lanes and terminal recovery tasks.

No runtime may read another runtime's doc, tokens, files, slots, or metrics through ambient globals.

## Daemon Deployment

Every independent project receives identical daemon artifacts by content hash:

- `sandbox/gp_daemon.py`
- launcher `.latexmkrc`
- any required runner/bootstrap files

`DaemonController.ensureReady()`:

1. Compares project Yjs content and live daemon status to the expected build/hash.
2. Writes only changed assets.
3. Triggers the minimum render/bootstrap action.
4. Waits for a matching status envelope and advertised protocols.
5. Rejects capacity from stale or mismatched daemon builds.

The current v6.3.1 bounded daemon protocol remains sufficient because every project is isolated. A daemon change is required only if live bootstrap evidence reveals a project-local lifecycle gap.

## Workload Classification and Memory Estimation

`WorkloadDescriptor` is created before dispatch:

```text
text-small
text-large
image-generation
image-edit
```

It records encoded request bytes, expected response reserve, streaming mode, image bytes, transform needs, and model. Initial conservative reservations are refined by bounded rolling observations.

Large HTTP bodies acquire a reservation from declared `Content-Length` before the server buffers them. Chunked bodies acquire incrementally and apply backpressure. WebSocket bodies reserve immediately after frame parsing. No request may enter the router after consuming unaccounted large memory.

## Adaptive Resource Governor

Hardware is detected through cross-platform Node/Bun APIs:

- Total physical RAM.
- Currently free RAM.
- Process RSS, heap, and external memory.
- Available logical processors.

Default sizing:

```text
systemReserve = max(4 GiB, totalRAM * 0.20)
processCeiling = min(
  totalRAM * 0.60,
  startupRSS + max(0, startupFreeRAM - systemReserve)
)
pause when freeRAM < max(2 GiB, totalRAM * 0.10)
resume when freeRAM > max(3 GiB, totalRAM * 0.15)
cpuTransformWorkers = clamp(2, 8, floor(logicalProcessors / 2))
```

Explicit environment overrides exist for deterministic deployments, but auto-tuning is the default.

The governor owns two independent resources:

- Byte reservations for live request/response/copy estimates.
- CPU permits for compression, ZIP decompression, and large base64 transforms.

It also enforces:

- One active ZIP fetch/decompression per runtime.
- Bounded SSE queues with downstream backpressure.
- Existing process-wide response-graph cache budget.
- Admission pause/resume hysteresis rather than oscillating at one threshold.
- No fixed image concurrency count; images consume their estimated byte/CPU weight.

On the measured 31.93 GiB / 12-thread host with 19.19 GiB initially free, the derived process ceiling is approximately 13 GiB. All 120 normal text lanes fit comfortably; maximum-size image concurrency is determined from live reservations rather than a hard-coded count.

## Intelligent Routing Policy

Queued work is not permanently assigned to a runtime. The router chooses a target only when both a physical lane and resource reservation can be acquired.

Candidate filtering requires:

- `ready` lifecycle state.
- Fresh heartbeat.
- Valid credential horizon.
- At least one non-quarantined lane.
- Request class supported by the runtime.

For every candidate, a pure policy predicts completion time from:

- Active and reserved weighted work.
- Usable capacity and queue depth.
- Workload-class ACK and completion EWMAs.
- Payload size.
- Recent failure and ZIP error rates.
- Quarantined-lane count.
- Runtime and global memory pressure.
- Token refresh or draining penalty.

The runtime with the lowest predicted completion time wins. Equal scores use seeded randomized tie-breaking to prevent a herd on project zero. With four projects, evaluating all candidates is cheaper and more accurate than sampling two.

The global queue uses per-connection/client deficit round robin with aging so one producer cannot monopolize all 120 lanes while old requests starve. Capacity is work-conserving: an idle eligible lane is never withheld solely for fairness.

## Request Affinity and Failure Semantics

### Before publication

Provisioning, auth, sync, lane-acquisition, or job-write failures before authoritative publication release the tentative target and may reroute once to another healthy runtime.

### After publication

Once a job is published:

- It remains bound to its original runtime and physical lane.
- It is never duplicated on another sandbox.
- The lane is released only after matching ACK, request identity, and terminal proof.
- Missing incremental data is replayed from the authoritative terminal transcript.
- An unproven lane remains quarantined and reduces advertised capacity until recovery.

### WebSocket affinity

An active response stays on its selected runtime. `store:false` continuations can be reconstructed and routed anew after the previous terminal is recorded. Persisted-state continuations retain conservative runtime affinity unless a proven stateless reconstruction permits migration.

### Runtime failure

A stale heartbeat or unrecoverable auth/sync error changes the runtime to `draining` immediately, preventing new assignments. Published jobs continue recovery on that runtime. Other projects remain routable. Reprovisioning creates a new sandbox session for the same project and revalidates the daemon before restoring capacity.

## HTTP and WebSocket Compatibility

`localhost:8787` remains the only client endpoint. Existing paths and event shapes stay stable:

- HTTP `/v1/responses`
- SSE streaming
- Codex Responses WebSocket mode
- Images compatibility endpoints
- File routes that remain live-proven

The server composition root injects `SandboxRouter` as the relay starter. Image `n` fan-out submits independent workload descriptors so children can run on different projects. Streaming handles retain their router/runtime lease through terminal settlement.

Legacy `PRISM_DOC_ID` maps to a one-entry project list. Multi-project deployments use `PRISM_PROJECT_IDS` or a typed JSON configuration file. Project IDs are unique and validated before startup.

## Monitoring and Diagnostics

Aggregate `/health` reports service readiness and effective capacity without credential material.

A dedicated sandbox status endpoint reports one sanitized snapshot per project:

- Project/runtime ID.
- Lifecycle state.
- Daemon build and protocols.
- Physical, active, available, and quarantined lanes.
- Queue depth and weighted work.
- Last heartbeat age/status.
- Credential expiry horizon/presence.
- ACK/completion EWMAs by workload class.
- ZIP and request failure counters.
- Memory reservations.

Metrics include route decisions, queue wait, publish duration, ACK duration, terminal duration, failover-before-publication, quarantine duration, heartbeat gaps, token refreshes, and runtime reprovisions.

## Playwright Removal

Removal occurs only after direct parity tests and live proof:

- Remove server auto-launch and bridge debug/token routes.
- Replace gateway/ydoc/file/render token lookups with runtime-scoped clients.
- Delete active imports of `src/bridge/playwright.ts` and browser-evaluation code.
- Remove Playwright from dependencies and Bun lockfile.
- Replace bridge tests with account/provisioning/runtime tests.
- Assert through source scanning that production modules do not import Playwright.

No hidden browser fallback remains. Missing/expired cookies produce an explicit aggregate `auth_required` state.

## Testing Strategy

### Unit tests

- Branded type/schema validation.
- Cookie matching, `Set-Cookie` updates, atomic persistence, and hot reload.
- Exact provisioning URL, headers, and JSON body shapes.
- Lifecycle transition legality and stale-generation suppression.
- Ten-second independent heartbeat schedules over fake time.
- Slow/failed heartbeat behavior without overlapping calls.
- Router candidate filtering, predicted completion, deterministic tie-breaking, and fairness.
- Dynamic capacity across zero through four 30-slot runtimes.
- 8/16/32/64 GiB machine-profile calculations.
- Byte reservations before body buffering and hysteresis under pressure.
- Workload estimates for large text, images, and mixed traffic.
- Pre-publication failover versus post-publication pinning.
- Terminal proof, replay, quarantine, and release.
- Per-runtime ZIP coalescing and one-in-flight invariant.
- Credential redaction and absence from snapshots/logs.
- No production Playwright imports.

### Integration tests

A deterministic fake Prism service models:

- Account cookies and rotation.
- Backend acquisition.
- Project resource credentials.
- Y-Sweet credential mint/push.
- Workspace synchronization states.
- Heartbeats and sandbox expiry.
- Four daemon runtimes with 30 lanes each.
- Token refresh, 401, 409, 429, 5xx, and delayed responses.

Tests submit mixed HTTP, WebSocket, large text, and image workloads and prove:

- No runtime exceeds 30 lanes.
- Aggregate active work reaches 120 when memory permits.
- Routing prefers lower predicted work.
- All response order/stream contracts remain valid.
- No duplicate upstream execution occurs.
- One failed project does not reduce healthy-project correctness.
- Every published lane settles or remains visibly quarantined.

### Live proof

Live tests run from the isolated worktree on a different local port and newly provisioned Prism sandboxes. The existing port 8787 process remains untouched.

The ramp is:

```text
16 → 32 → 64 → 96 → 120 concurrent requests
```

For each stage record per-project distribution, peak active lanes, queue wait, publish/ACK/terminal latency, memory RSS/free RAM, heartbeat gaps, failures, retries, and quarantines.

Acceptance requires:

- Four independent projects reach `ready` with the expected daemon build.
- Every runtime receives heartbeats at 10-second cadence throughout load.
- Browserless token/resource/Yjs/readiness flow succeeds.
- 120 requests can be admitted when memory and upstream service limits permit.
- Any lower discovered account-wide upstream ceiling is measured and enforced through adaptive backpressure rather than overload.
- Zero duplicate executions.
- Zero lane oversubscription.
- Zero early releases.
- Every response has terminal proof or an explicitly retained quarantine.
- Router and transport overhead do not regress the single-project baseline.

## Rollout

1. Implement and verify all new units in the isolated worktree.
2. Start a browserless candidate server on a non-8787 port.
3. Provision new Prism sandbox sessions for test projects.
4. Run functional and load proof without routing production traffic.
5. Remove Playwright only after direct parity is proven.
6. Run the full existing and new test suites, daemon tests, typecheck, audit, and live ramp.
7. Commit and push the feature branch for review.
8. Merge/restart the production router only after explicit user direction.

## Completion Criteria

- Browserless account, provisioning, Yjs, sandbox, heartbeat, and readiness flows are implemented.
- Separate project-scoped runtimes have no ambient shared mutable state.
- Every runtime independently deploys and verifies the same daemon.
- Ten-second per-sandbox heartbeats are proven with fake-clock and live measurements.
- Dynamic capacity and memory/CPU admission work across machine sizes.
- Intelligent least-work routing and failure isolation are proven.
- Existing HTTP/SSE/WebSocket/images behavior remains compatible.
- Playwright is absent from production code and dependencies.
- Full local, fake-service, daemon, and live ramp verification is green.
- The existing port 8787 process was never stopped during development.
