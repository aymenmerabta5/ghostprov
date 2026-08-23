# GhostProv V2 Transport Throughput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove 16-agent streaming stalls by batching small SSE events, bounding sidecar output pressure, and routing away from congested sandboxes.

**Architecture:** A per-request `ResponseBatcher` packs ordered small events into `response_batch` frames below 14 KiB, while large events retain existing segmentation. An asynchronous bounded sidecar sink propagates backpressure to the already-awaited upstream reader. Congestion hysteresis suppresses credits and host admission without changing committed-request pinning or the 32-frame/512 KiB reliable window.

**Tech Stack:** Bun 1.4.0, TypeScript 7.0.2, Deno, Yjs Awareness, Bun test `--isolate`.

## Global Constraints

- Execute inline on `master`; no branch, worktree, or subagents.
- Never stop or restart the live 18787 process.
- Preserve 16 KiB frames, 32-frame/512 KiB windows, terminal proof, replay, fairness, and exact-once behavior.
- Batch target <=14 KiB, <=32 events, <=5 ms; scheduler hard bound 1 MiB/1,024 frames.
- Four admitted-but-uncommitted requests maximum per sandbox; committed upstream capacity remains 128.
- No payload content enters logs.

---

### Task 1: Response Batch Protocol

**Files:** `src/v2/protocol/shared.ts`, `src/v2/protocol/validate.ts`, `tests/v2-response-batch.test.ts`, `tests/v2-protocol.test.ts`

**Produces:** `ResponseBatchFrame` with `requestId`, `firstEventIndex`, and ordered `{event,dataBase64Url}` items.

- [ ] Write tests accepting a valid batch, rejecting >32 items, non-contiguous indexes, unsafe event names, and frames over 16 KiB.
- [ ] Run `bun test --isolate tests/v2-response-batch.test.ts tests/v2-protocol.test.ts` and confirm RED.
- [ ] Add `response_batch` to `SidecarFrame`, schema descriptor, and strict validator; preserve all hard constants.
- [ ] Verify tests/typecheck and commit `feat: add v2 response batch frames`.

### Task 2: Per-Request Batcher and Exact Expansion

**Files:** create `src/v2/sidecar/response-batcher.ts`; modify `src/v2/sidecar/engine.ts`, `src/v2/transport/session.ts`; tests `v2-response-batcher.test.ts`, `v2-transport-session.test.ts`, `v2-transcript.test.ts`.

**Produces:** `ResponseBatcher.push(eventIndex,event)`, `flush(requestId)`, `cancel(requestId)`, and host expansion through existing ordered event assembly.

- [ ] Write RED tests for 14 KiB, 32 events, 5 ms, terminal/error/cancel/drain flush, and large-event fallback.
- [ ] Implement one batch per request; calculate actual serialized reliable-frame size before adding an item.
- [ ] Flush a request batch before its terminal/error; expand at host into the same `completedEvents` map.
- [ ] Prove terminal transcript/digest equality batching on/off and commit `feat: batch v2 response events`.

### Task 3: Bounded Async Sidecar Backpressure

**Files:** create `src/v2/sidecar/output-sink.ts`; modify `sandbox/v2/awareness-peer.ts`, `src/v2/sidecar/engine.ts`, `sandbox/v2/main.ts`; tests `v2-output-sink.test.ts`, `v2-sidecar-engine.test.ts`, `v2-deno-awareness-peer.test.ts`.

**Produces:** `enqueue(frame,priority,stream): Promise<void>` and `flush(stream): Promise<void>`.

- [ ] Write RED tests for high 256-frame/256 KiB pause, low 64/64 KiB resume, hard 1,024/1 MiB bound, FIFO waiter wake, cancellation, and control bypass.
- [ ] Make `SidecarEngine` emission async; await batching/output from `DirectResponsesClient.onEvent`.
- [ ] Wake producers after ACK/pump drains below low watermark; never allocate beyond hard bounds.
- [ ] Verify fault/replay/large-payload suites and commit `feat: bound v2 response backpressure`.

### Task 4: Congestion Hysteresis and Admission

**Files:** create `src/v2/transport/congestion.ts`; modify `sandbox/v2/awareness-peer.ts`, `src/v2/transport/session.ts`, `src/v2/runtime.ts`, `src/router/policy.ts`, `src/config/multi-sandbox.ts`; tests `v2-congestion.test.ts`, `v2-transport-session.test.ts`, `router-policy.test.ts`.

**Produces:** `TransportCongestion.update(snapshot): boolean`, soft credit suppression, and `maxUncommitted=4`.

- [ ] Write RED tests for enter thresholds (128 frames, 256 KiB, 2 s age), two-sample clear thresholds (32, 64 KiB, 500 ms), no oscillation, and max four uncommitted.
- [ ] Advertise zero credits while congested; preserve active requests and control traffic.
- [ ] Limit host admissions using `min(remoteCredits, 4-uncommitted)` and expose zero availability while congested.
- [ ] Add a transport-pressure routing penalty without rerouting committed work.
- [ ] Verify dynamic-fleet/router/concurrency suites and commit `feat: avoid congested v2 sandboxes`.

### Task 5: Throughput Diagnostics, Gates, and Rollout

**Files:** modify diagnostics types/observer/sampler, `.env.example`, root ignored `.env`, README; create `tests/v2-batching-throughput.test.ts`.

- [ ] Add batch counts/bytes/events, producer wait, congestion transitions/duration, credit suppression, and validation failures using scalar counters only.
- [ ] Write a deterministic 16-stream before/after benchmark asserting >=4x logical-event throughput, scheduler <=1 MiB, zero terminal loss, and no extra content logging.
- [ ] Set `GP_V2_BATCHING=1` locally but keep documented default `0` until live acceptance.
- [ ] Run focused tests, `bun test --isolate`, typecheck, daemon tests, audit, and V2 smoke.
- [ ] Confirm live PID unchanged, commit `test: gate v2 batching throughput`, fast-forward push master, then request one manual restart and diagnostic rerun.

## Completion Evidence

- Exact test counts and all verification commands exit 0.
- Deterministic throughput ratio >=4.0; scheduler memory <=1 MiB; zero terminal/protocol failures.
- Core windows remain 32 frames/512 KiB and physical frame limit remains 16 KiB.
- Live 18787 PID/start time remains unchanged until operator restart.
