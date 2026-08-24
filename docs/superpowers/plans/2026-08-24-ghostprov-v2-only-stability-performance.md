# GhostProv V2-Only Stability, Throughput, and Recovery Plan

**Date:** 2026-08-24

**Status:** Implementation plan only; no production changes in this document

**Target:** V2 is the only GhostProv transport. V1 is removed completely.

**Rollback target:** an immutable, last-known-good V2 revision—not V1.

## 1. Objective

Build a V2-only GhostProv that:

1. never proactively throttles OpenAI models;
2. sends work until the provider returns its own limit;
3. preserves provider HTTP and streamed errors accurately;
4. safely uses every healthy sandbox in the existing Prism projects;
5. cannot execute one logical request twice during bootstrap, recovery, or rollback;
6. bounds every local queue, buffer, journal, and retry loop;
7. isolates slow clients and slow requests;
8. materially reduces the current large-request V2 commit latency;
9. operates indefinitely on Windows and Linux hosts with Linux Prism sidecars;
10. never creates or deletes a Prism project during recovery or rollout.

## 2. Non-negotiable constraints

- V1 source, runtime selection, configuration, tests, scripts, and active documentation are removed.
- `GP_TRANSPORT`, `auto`, and V1 fallback no longer exist.
- V2 remains the only production protocol family.
- The current V2 wire revision is frozen as **V2-R1**.
- The new fenced wire revision is **V2-R2**.
- Rollback always means R2 -> immutable R1.
- A provider `429` is observation only. It must not block, sleep, retry, reroute committed work, open a recovery circuit, or replace a sandbox.
- Local memory, queue, per-sandbox credit, transport, socket, and disk limits remain. They protect GhostProv rather than police OpenAI.
- No direct/blob upload, `/import-archive`, project-file reference transport, transient Yjs payload files, prompt rewriting, or other unproven Prism feature is part of this plan.
- No mutable in-place sidecar bundle replacement.
- No committed request migrates between revisions or sandbox incarnations.
- No implementation or live verification may kill, replace, bind, or interfere with port `18787` without an explicit user-controlled handoff. Candidate listeners use another port.

## 3. Baseline evidence

The load log established the following baseline:

| Metric | Baseline |
|---|---:|
| Stable ready sandboxes | 7 |
| Unavailable/recovering sandboxes | 1 |
| Fleet capacity | 68-112 |
| Maximum observed active | 22 |
| Sol cold-gate active ceiling | 16 |
| Later-load router queue | p50 1.99 s / p95 5.25 s / max 9.76 s |
| V2 commit ACK | p50 about 1.06 s / p95 about 2.42 s |
| First event | p50 about 2.59 s / p95 about 6.95 s |
| Successful total | p50 about 6.9 s / p95 about 19.4 s |
| 3-3.5 MiB commit | about 6-10.6 s |
| Host CPU | p50 about 17% / p95 about 29% / max about 41% |
| Host RSS | p50 about 326 MiB / p95 about 432 MiB / max about 530 MiB |
| Event-loop maximum | 1.217 s |
| Logger drops/errors | 0 / 0 |
| Real WebSocket socket backpressure | 0 observed |

The present slowdown is primarily:

1. the artificial host-wide model gate;
2. the 128 KiB stop-and-wait V2 request flight;
3. repeated request serialization/copying and retained input representations;
4. full-history WebSocket continuation materialization;
5. occasional transport and event-loop stalls.

## 4. Target architecture

```text
HTTP / WebSocket ingress
        |
        v
bounded body reader + one transferable byte lease
        |
        v
bounded preparation workers
  canonical bytes + full SHA-256 + optional gzip(level 1)
        |
        v
fair V2-only router
        |
        v
V2-R1 or V2-R2 runtime selected explicitly by project
        |
        v
fenced host <-> exact sidecar boot
        |
        v
bounded reliable data lane + independent reliable control lane
        |
        v
verified body -> durable execution state -> upstream headers
        |
        v
proxy:18080/v1/responses
        |
        v
incremental transcript hash/spool -> bounded per-request client delivery
```

## 5. Release strategy

The work is deliberately separated so rollback and diagnosis remain possible.

### Revision model

- **R1:** frozen current V2 schema and sidecar.
- **R2:** new fenced V2 schema.
- The host initially understands both R1 and R2.
- Existing projects default to R1 until selected for migration.
- Sidecar assets live at immutable build/schema-addressed paths.
- Bootstrap explicitly selects a revision; it never guesses or silently falls back.
- Build/schema mismatch fails before upstream execution.

### Project rollout

Use only already configured Prism projects:

```text
1 project -> 2 projects -> 4 projects -> remaining fleet
```

For every project:

1. stop new routing;
2. wait for committed work to finish within a bound;
3. if clean drain is impossible, mark uncertain work ambiguous and do not retry it;
4. fence the old boot;
5. bootstrap the selected immutable V2 revision in the same project;
6. prove gateway, transport, engine, and execution readiness;
7. restore routing.

Rollback reverses the selector to R1, rolls the sidecar back first, proves readiness, and only then rolls the host binary back if necessary.

## 6. Phase 0 - Freeze V2-R1 and build rollback rails

### Work

- Record a V2-R1 release tag and immutable build/schema hashes.
- Capture the current deterministic test corpus and load benchmark corpus.
- Version sidecar asset paths rather than overwriting `.ghostprov-v2/sidecar.bundle.js` in place.
- Add explicit per-project V2 revision selection.
- Make journals revision-compatible or isolate journal directories by revision.
- Add a rollback drill test before any wire change.

### Proof gate G0

- R1 behavior, fault results, and performance reproduce the baseline.
- R1 bootstrap can be selected explicitly for every existing project.
- Interrupted bootstrap cannot leave two routeable boots.
- R1 -> R1 restart and rollback drill succeed without V1.

### Expected gain

No direct speed gain. This phase makes every later change reversible without retaining V1.

## 7. Phase 1 - Remove V1 completely in an isolated change

This phase contains no V2 wire, routing, rate-limit, scheduling, or concurrency behavior change.

### Delete V1-only production code

- `sandbox/gp_daemon.py`
- `sandbox/runner.py`
- `sandbox/ask.py`
- `src/sandbox/managed-v1-runtime.ts`
- `src/sandbox/daemon-controller.ts`
- `src/sandbox/slot-pool.ts`
- `src/relay/slot-runner.ts`
- `src/relay/segments.ts`
- `src/relay/scheduler.ts`
- `src/relay/terminal-rescue.ts`
- `src/relay/unlatch-recovery.ts`
- `src/prism/zip-coordinator.ts`
- `src/v2/transport-selector.ts`

### Prune shared modules rather than deleting them

- Keep `SandboxYDoc`; remove V1-only file/ZIP assumptions only when no V2 bootstrap path uses them.
- Keep `SandboxGateway`; remove ZIP extraction, ZIP coordinator, and V1 `readFiles` behavior while preserving heartbeat, sync, token, render/bootstrap, logs, and same-session rotation.
- Move `describeSandboxFailure` out of the legacy `SandboxRuntime` module, then delete the V1 lifecycle class.
- Keep shared SSE parsing used by V2; remove `gp_*` disk-envelope handling and `TerminalBackedSseExtractor` if no remaining user exists.
- Remove V1 daemon-envelope normalization from HTTP errors.
- Preserve `multi_agent_version: "v1" | "v2"` model metadata where it refers to Codex client behavior; it is unrelated to GhostProv transport V1.

### Remove V1 configuration and composition

- Remove `TransportMode`.
- Remove `GP_TRANSPORT`.
- Remove `v1`, `auto`, `needsV1`, and `TransportSelectingRuntime` branches.
- Always load V2 sidecar assets.
- Always create `V2SandboxRuntime` inside `SupervisedSandboxRuntime`.
- Remove V1-only slot, ZIP, render-poll, unlatch, daemon, and segment settings.
- Hardcode observability transport family to `v2` while preserving the selected V2 revision.

### Delete or rewrite V1 tests/scripts

- Delete V1 daemon, segments, slot runner, daemon controller, terminal rescue, unlatch, scheduler, and transport selector suites.
- Rewrite `tests/support/fake-prism.ts` and multi-sandbox integration around the existing fake V2 runtime.
- Remove the Python daemon test command.
- Delete V1 daemon/runner inspection, synchronization, launcher, and `.latexmkrc` diagnostic scripts.
- Remove active README/environment documentation for V1 and auto fallback.
- Historical plans may be removed because Git history is the archive; active docs must describe V2 only.

### Preserve valuable V1 invariants as V2 tests

- data before commit;
- immutable request identity;
- full digest/size/count validation;
- generation fencing before any ACK;
- terminal-proof quarantine;
- bounded recovery and cleanup;
- engine health distinct from gateway health.

### Proof gate G1

- Static search finds no GhostProv V1 transport construction or configuration.
- No Python runtime is required by production or tests.
- Every remaining test passes.
- Typecheck passes.
- V2-R1 canaries reproduce baseline behavior and remain rollback-capable.
- V2 latency and throughput remain within 5% of the frozen R1 baseline.

### Expected gain

- Roughly 3,700 lines of V1 implementation and at least 5,000 lines including direct tests are removed, before scripts/docs cleanup.
- No meaningful request-speed improvement is promised because production already selected V2.
- Reliability and maintenance improve by eliminating unsafe fallback, detached Python daemons, ZIP polling, permanent V1 YDoc payload growth, and cross-transport session rotation.

## 8. Phase 2 - Harden shared YDoc and host admission on R1

### Y-Sweet connection correctness

- Replace the incorrect `connectionClose` listener with the real `connection-close` event.
- Correct the same issue in project inspection.
- Track continuous connected/disconnected/auth-failed state.
- Remint Y-Sweet credentials after classified auth failure.
- Ensure a closed provider cannot accept apparently successful local-only writes.
- Test against the real installed provider event names rather than matching fakes.

### Atomic ingress-to-execution memory lease

- Replace nested incoming-body and execution leases with one transferable/resizable lease.
- Grow unknown-length reservations as bytes arrive rather than reserving the entire request ceiling.
- Reject a request whose minimum reservation can never fit.
- Bound queued jobs and queued bytes.
- Carry one absolute deadline through body read, preparation, routing, and transport admission.
- Add governor close/reject-all and include waiters in idle accounting.
- Detect cgroup/container memory limits on Linux and retain correct Windows behavior.

### Immediate host deadlock fixes

- For `stream:false`, drain/discard V2 events concurrently while awaiting the final object.
- Add a later R2 `final-only` delivery mode.
- Give each HTTP connection/client and WebSocket session a stable router identity.
- Correct V2 active/reserved double counting.
- Correct global governor capacity being repeated as each runtime's independent capacity.
- Supervise and replace failed compression workers; add job deadlines.

### Proof gate G2

- Real provider disconnect causes connection state transition and credential remint.
- Offline writes cannot report success indefinitely.
- Concurrent ingress cannot deadlock while waiting for a second lease.
- Impossible reservations fail immediately.
- A non-stream response larger than 512 KiB completes.
- R1 throughput does not regress more than 5%.

### Expected gain

- Removes proven deadlock modes.
- Reduces coarse memory reservations for missing `Content-Length` requests.
- No large median latency promise; admission p95 should remain below 10 ms while memory is available.

## 9. Phase 3 - Harden V2-R1 lifecycle before migration

### Work

- Bound sidecar and host drain/close operations.
- Stop routing before drain begins.
- Connect transport before replaying recovered terminal spools.
- Repair/truncate a torn journal tail before appending.
- Add a local single-process/boot ownership lease.
- Ensure cleanup runs in `finally` even after startup, replay, or provider failure.
- Surface structured, credential-scrubbed startup stages and error codes.
- Add an engine-progress check separate from the out-of-band Awareness pong.
- Make liveness and recovery loops generation-owned and abortable.

### Proof gate G3

- Saturated admission cannot prevent bounded shutdown.
- A recovered maximum-size spool cannot deadlock startup before peer connection.
- Torn journal repair is deterministic.
- Repeated bootstrap attempts cannot leave two local sidecars executing.
- Stuck engine with working transport pong is detected.

### Expected gain

Primarily reliability. It establishes safe R1/R2 rollout and may eliminate some persistent recovery failures, but no recovery-success percentage is promised until the classified `49d3c661...` failure is reproduced.

## 10. Phase 4 - Introduce V2-R2 fencing, dispatch, and exact provider errors

### R2 protocol binding

Carry and validate:

- project ID;
- sandbox session ID;
- leader generation;
- bootstrap launch nonce;
- exact host instance ID;
- exact target sidecar boot/instance ID;
- build hash;
- schema hash;
- protocol revision.

The sidecar validates the complete fence before any ACK/output and again immediately before upstream fetch.

### Request execution states

Use explicit states:

```text
received
body_verified
claimed
fetch_permitted
fetch_starting
upstream_headers
streaming
terminal_success | terminal_error | cancelled
acknowledged
```

- Only pre-fetch work is safely replayable.
- `fetch_starting` or later is never automatically retried.
- Known provider errors and cancellation are durable, non-ambiguous terminal outcomes.
- Terminal acknowledgement cleans the corresponding journal/spool state.

### Dispatch architecture

- Replace the global sidecar accept chain with bounded per-request ordered chains.
- Create an independent reliable control lane for cancel, terminal ACK, credential rotation, drain, and health.
- Advance ACK only after safe bounded ingestion, not merely cursor acceptance.
- Emit an explicit reject/error if dispatch fails; never swallow it.
- Store a committed request body durably until the fetch boundary, or retain a fenced host replay copy until the sidecar proves that boundary.

### Exact provider error contract

Carry a bounded, credential-scrubbed provider envelope containing:

- HTTP status;
- `{message,type,param,code}`;
- selected `Retry-After` and rate-limit headers;
- upstream request ID;
- safe original HTTP JSON or SSE error payload;
- model;
- project;
- host/runtime/sidecar identity;
- stable organization fingerprint when present.

Add an upstream-headers promise/prelude:

- initial non-2xx returns the actual HTTP error before GhostProv writes `200`;
- a later `event:error` is forwarded exactly once inside the already-open SSE stream;
- WebSocket reports the original provider semantics rather than synthetic status 500.

### Proof gate G4

- Two same-project/same-generation sidecars observe a request; exactly one emits ACK and exactly one counted upstream fetch occurs.
- A stale sidecar emits nothing.
- Handler failure is not falsely ACKed.
- Cancel/control bypasses a capacity-waiting request.
- Crash tests cover every execution state.
- HTTP 429 and 200-SSE `event:error` preserve client-visible semantics.
- R1 and R2 can coexist in separate existing projects and roll back cleanly.

### Expected gain

Correctness rather than raw speed. Per-request dispatch removes sandbox-wide head-of-line blocking under contention and is required before safe ungating.

## 11. Phase 5 - Make memory, transcript, scheduler, and egress genuinely bounded

### Serialize and compress once

- Build one prepared request representation before sandbox reservation:
  - canonical bytes;
  - full SHA-256;
  - decoded/encoded lengths;
  - chosen encoding;
  - lazy frame source.
- Reuse it for sizing, hashing, compression, and framing.
- Use gzip level 1 only when it reduces the number of V2 flights.
- Transfer compression-buffer ownership instead of copying it twice.
- Drop router body, serialized strings, compressed bytes, and prepared frames as soon as their safety stage permits.

### One stage-aware byte allocator

Account for:

- incoming HTTP/WS bytes;
- parsed/canonical request;
- compression;
- scheduler/window frames;
- sidecar assembly;
- active upstream body;
- transcript/spool;
- per-request client output;
- WebSocket send queue.

Lease bytes move between stages instead of being independently double-reserved.

### Incremental transcripts

- Incrementally hash event bytes.
- Maintain event/byte counters without concatenating the transcript.
- Retain only the final response object in memory.
- Stream terminal spools to disk with quota and atomic fsync/rename.
- Replay spools incrementally.
- Check event count/assembly completeness before terminal materialization.

### Scheduler and assembler

- Replace `Array.shift`, repeated JSON sizing, full queue scans, and per-frame stream scans with deques/head indices and O(1) counters.
- Use bounded byte queues.
- Maintain per-stream ordering barriers so terminal never overtakes its own data.
- Preallocate/index request parts and validate canonical offsets in O(1).
- Decode base64 once.
- Add incomplete-request TTL and host-incarnation cleanup.

### Downstream isolation

- Give each request a byte-bounded output queue.
- Protocol ingestion writes into that request's queue without awaiting another client's socket.
- A request exceeding its hard output bound is cancelled alone.
- HTTP output is pull-driven and awaits Node socket drain.
- WebSocket send is awaitable, byte-bounded, fair, and aborts work on send failure.
- Configure WebSocket maximum payload, queue bytes, ping/pong, and idle expiry.

### Long-run state

- Compact journals.
- Bound acknowledged tombstone retention.
- Repair tails and remove orphan spools.
- Enforce disk quota.
- Clean diagnostics, assembler, ambiguity, listener, and request maps on every terminal outcome.

### Proof gate G5

- A slow client beside 31 healthy streams does not delay healthy first-event/terminal p95 by more than 10%.
- A 64 MiB non-stream and stream response remains within configured memory/disk bounds.
- A gzip bomb fails within bounds.
- Journal/maps/spools remain bounded during a 24-hour accelerated soak.
- Request preparation performs one canonical serialization, one digest, and at most one compression.
- No queue is unbounded by both jobs and bytes.

### Expected gain

- Large-request preparation CPU/copy work should fall roughly 30-50%; benchmark required.
- Long-lived retained input memory after commitment should fall at least 50% for large requests, with a target above 75% once all frame/body references are released.
- Transcript memory changes from O(total response bytes per peer) to O(configured buffer + active final object), giving over 90% RAM reduction for very large responses.
- Event-loop improvement is expected but not promised until profiling proves the source.

## 12. Phase 6 - Remove the model/provider rate gate

This phase occurs only after exact errors, fencing, bounded memory, and slow-consumer isolation exist.

### Remove

- `ModelRateLimitGates` and `FleetRateLimitGate` construction.
- Cold, open, cooldown, and probe admission states.
- Estimated token preflight.
- Provider-rate leases.
- Gate-driven requeueing.
- Locally generated provider-style 429s.
- Rejection of queued work after one sandbox observes 429.
- Rate-limit gate configuration and tests.

### Keep as observation only

- Parse provider HTTP/SSE rate information.
- Emit per-project/incarnation/sidecar/model observations.
- Record safe organization fingerprint and provider usage fields.
- Never let observations affect traffic, retries, recovery, or project selection.

### Proof gate G6

- More than 16 concurrent Sol requests route without model-gate delay.
- No GhostProv-originated provider 429 exists.
- A provider 429 produces zero internal sleep, retry, admission delay, sandbox replacement, or recovery-circuit transition.
- Router queue p95 remains below 100 ms whenever local fleet capacity is available.

### Expected gain

At the measured offered load and absent an immediate provider limit:

- remove about 1.99 seconds from saturated-phase queue p50;
- remove up to about 5.25 seconds from queue p95;
- reduce accepted-to-first-event latency by roughly 1-2 seconds at p50 and 3-5 seconds at p95 during the gate-saturated phase;
- remove the artificial 109 requests/minute host plateau;
- expose 56 initial local active slots across seven ready sidecars instead of 16, and up to 112 at the current per-sandbox maximum.

Those slot counts are local headroom, not a promise of proportional upstream throughput. OpenAI TPM and real transport saturation remain external/measured limits.

## 13. Phase 7 - Apply the proven V1 segmentation principles inside V2 Awareness

No V1 files, ZIP polling, render transport, tracked Yjs payload entries, or reference/blob channel is reintroduced.

### Retained principles

- bounded preparation before scarce execution admission;
- immutable request identity;
- data before commit;
- commit repeats/proves the manifest;
- original decoded size, encoded size, encoding, part count, generation, target boot, and full SHA-256 all match before fetch;
- terminal-proof quarantine remains bounded and generation-scoped.

### Physical transport

- Keep 8 KiB physical request frames initially.
- Keep one immutable flight at a time; no speculative sliding/multi-flight rollout.
- Use the existing 512 KiB maximum V2 window.
- Benchmark fixed flight byte budgets in isolation:

```text
128 KiB baseline -> 256 KiB -> 512 KiB
```

- Keep the current 32-frame flight cap initially. Thirty-two 8 KiB data frames correspond to about a 256 KiB raw V1-style logical unit and approximately 340-400 KiB after base64/JSON framing, within the current 512 KiB window.
- Publish `request_commit` only after all request data and make it repeat/prove the immutable manifest.
- Release acknowledged frame storage immediately.
- Reserve reliable control capacity so cancel/drain/terminal ACK cannot starve.

### Proof gate G7

For every candidate flight size, test:

- RTT 50/300/1000 ms;
- delayed ACK;
- disconnect/reconnect;
- duplicate state;
- dropped/reordered updates in the deterministic provider;
- token rotation;
- sidecar restart;
- 128 KiB, 512 KiB, 3.35 MiB, 16 MiB, and 64 MiB requests;
- exact request digest and exact final transcript;
- bounded retransmission and memory;
- cancellation/control latency.

Then repeat on a live existing Prism project. Ship only the largest fixed size that passes every proof gate and demonstrates repeatable improvement.

### Expected gain

- Current useful payload per 128 KiB encoded flight is roughly 88-96 KiB.
- A proven 32-frame coalesced flight carries roughly 256 KiB raw request data.
- Expected ACK-cycle reduction for large requests is approximately 2-3x.
- For requests at or above 3 MiB, the measured p50/p95 of about 6.21/10.59 seconds has a conservative candidate target of about 2.0-3.5/3.8-6.5 seconds if the 512 KiB candidate passes.
- Small requests below one current flight should improve little (typically 0-15%); this phase targets large prompts.

These are engineering ranges, not release claims. The live benchmark is authoritative.

## 14. Phase 8 - Router fairness and controlled local concurrency growth

### Work

- Use stable per-client/session identities.
- Use bounded byte-based deficit round-robin across clients and small/large workload classes.
- Skip temporarily blocked head jobs instead of stopping the full drain.
- Try additional distinct V2 runtimes for proven pre-fetch failures until the absolute deadline.
- Route using measured RTT, commit backlog, bytes/sec, active streaming work, and actual admission credits.
- Remove active/reserved double counting.
- Treat global host memory as one fleet budget rather than repeating it per runtime.
- Make WebSocket session concurrency align with proven fleet capacity rather than an unrelated fixed 16.

### Capacity rollout

Keep the local per-sandbox maximum at 16 through all earlier phases. Then test:

```text
16 -> 32 -> 64
```

No value is promised. The highest stable proven value becomes the default.

### Proof gate G8

- Load 10 -> 20 -> 40 -> 80/100 agents.
- No provider gate appears.
- No duplicate fetch.
- No local queue or memory bound is exceeded.
- p95 first-event and terminal latency do not collapse as concurrency rises.
- One large client cannot starve small clients.
- CPU, RSS, event loop, scheduler depth, retransmission, journal, disk, and WebSocket sessions remain within declared budgets.

### Expected gain

- The first accepted target is 32 simultaneous Sol streams, a 2x improvement over the removed host gate.
- Seven healthy sidecars at the present initial limit expose 56 active slots; reaching that level would provide 3.5x the former gate's local concurrency.
- Reaching 112 local streams is conditional and not promised.
- Sustainable requests/minute will be determined by actual upstream duration, OpenAI TPM, Y-Sweet RTT, and the selected proven flight size.

At the captured average stream duration, acceptance targets are:

| Active streams | Minimum load-test target |
|---:|---:|
| 24 | 140 requests/minute |
| 32 | 175 requests/minute |

Higher stages are exploratory and are waived when the original provider 429 is observed; GhostProv must forward that provider limit rather than hiding it.

## 15. Phase 9 - Recovery, observability, rollout, and final R1 retirement

### Layered health

Maintain independent states for:

1. Prism gateway/token health;
2. Y-Sweet connection health;
3. sidecar transport health;
4. engine/dispatch/journal progress;
5. provider response health.

- Refresh authentication before replacing a sandbox.
- Provider 429/4xx/5xx does not imply sandbox death unless a separate gateway/engine failure is proven.
- Recovery always reuses the same Prism project.
- Fresh sandbox session and leader generation must fence the old boot.
- A candidate becomes routeable only after engine progress is proven.
- Preserve infinite recovery intent with bounded exponential backoff and Prism allocation cooldown.

### Observability corrections

- Actual eligible candidate count rather than registry size.
- Per-attempt and total recovery duration as separate fields.
- Sidecar CPU delta, event-loop, gzip/hash/fsync, scheduler, spool, journal, and reconnect metrics.
- Host GC/array-buffer and stage timings where available.
- Rate observations include safe project/model/boot/org correlation but never control traffic.
- Logging remains bounded; large fleet samples do not flood the console sink.

### Rollout completion

- Migrate R2 per existing-project cohort.
- Demonstrate R2 -> R1 rollback during the soak.
- Retain R1 parser/bundle through at least one complete fleet soak.
- Remove R1 compatibility only in a later V2-only cleanup release after rollback is no longer operationally required.

### Proof gate G9

- Crash at claimed, before-fetch, fetch-started, mid-stream, terminal-before-ACK, and drain.
- Only pre-fetch work is retried.
- Post-fetch work is never duplicated.
- Terminal replays exactly once.
- Token rotation and Y-Sweet reconnect succeed.
- `49d3c661...` either recovers or exposes a precise external blocking stage; it never loops as an opaque timeout.
- Twenty-four-hour soak shows bounded maps, queues, journals, temp files, RSS, and WebSocket sessions.
- Windows and Linux host suites pass; Linux sidecar suites pass.

## 16. Expected aggregate outcome

### High-confidence gains

| Area | Expected result |
|---|---|
| Provider queue | GhostProv model-gate delay removed entirely |
| Saturated-phase queue p50 | About 1.99 s removed |
| Saturated-phase queue p95 | Up to about 5.25 s removed |
| Minimum accepted concurrency target | 32 concurrent Sol streams |
| Large-request ACK cycles | Roughly 2-3x fewer if the 512 KiB fixed flight passes |
| V1 maintenance surface | About 3,700 implementation lines and at least 5,000 lines including direct tests removed, plus scripts/docs |
| Duplicate-execution safety | Exactly one targeted/elected sidecar may fetch |
| Slow-client blast radius | One request rather than the entire sandbox |
| Provider errors | Original safe HTTP/SSE/WS semantics, no retry wrapper |
| Long-run journal/maps | Bounded rather than process-lifetime growth |

### Benchmark-dependent gains

| Area | Conservative target |
|---|---|
| Mixed-load first-event p95 | Reduce from about 6.95 s toward <=4.5 s when OpenAI is not limiting |
| >=3 MiB commit p50/p95 | Reduce from about 6.21/10.59 s toward about 2.0-3.5/3.8-6.5 s |
| Large-request preparation | 30-50% less CPU/copy work |
| Retained input memory after commit | At least 50% lower; target >75% for large requests |
| Very large response RAM | >90% lower through bounded spool/hash instead of full transcript copies |
| Local concurrency | 32 proven first; 56 and 112 are conditional |
| Event-loop stalls | Expected improvement after copy removal; no numeric promise until profiling proves cause |

### Gains that are intentionally not promised

- No claim that throughput scales linearly with sandbox count.
- No claim that OpenAI TPM increases with more Prism sandboxes.
- No claim that every sandbox belongs to a separate organization.
- No 32/64 per-sandbox default without proof.
- No direct/blob/reference transport.
- No automatic provider retry or cooldown.

## 17. Verification matrix

Every release phase must pass applicable rows:

| Dimension | Values |
|---|---|
| Host OS | Windows, Linux |
| Sidecar OS | Linux/Deno |
| Protocol | R1, R2, mixed fleet during migration |
| Payload | 128 KiB, 512 KiB, 3.35 MiB, 16 MiB, 64 MiB |
| Response | small text, long reasoning, large tool output, image-sized event, failed/incomplete/error |
| Concurrency | 1, 4, 16, 32, 56, 64, 80/100 agents |
| RTT | 50, 300, 1000 ms |
| Fault | loss, duplicate, reorder, disconnect, expired credential, stale boot, journal tear, process crash |
| Client | fast HTTP, slow HTTP, fast WS, slow WS, disconnect, non-stream |
| Recovery | same sandbox reconnect, fresh sandbox same project, allocation cooldown, rollback |

## 18. Definition of done

GhostProv V2-only is complete when:

- V1 transport code/config/scripts/tests/docs are absent from the active tree.
- No provider rate observation can affect admission, routing, retry, recovery, or response rewriting.
- Exact safe provider errors reach HTTP/SSE/WS clients.
- Exactly one fenced sidecar can execute each request.
- Every queue and state store is bounded by count and bytes or disk quota.
- Slow clients are isolated.
- Large-request transport meets the selected live benchmark without fault regressions.
- The fleet supports at least 32 concurrent Sol streams under the acceptance corpus.
- Same-project recovery and R2 -> R1 rollback are demonstrated.
- A full soak is flat in journals, maps, temp files, RSS, and session counts.
- Full deterministic, integration, live-canary, Windows, Linux, and sidecar verification passes.
