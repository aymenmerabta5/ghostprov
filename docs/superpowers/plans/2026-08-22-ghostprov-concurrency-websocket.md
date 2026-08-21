# GhostProv Concurrency and Codex WebSocket Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a concurrency-first Responses API mirror with real HTTP streaming, Codex-compatible WebSockets, bounded `store:false` continuation memory, efficient slot polling, negotiated daemon v6.3 transport, and a portable Playwright bridge.

**Architecture:** One global scheduler holds capacity through terminal response completion for both HTTP and WebSocket callers. A dedicated WebSocket session layer provides FIFO lanes and structurally shared response graphs, while the existing slot pipeline remains the upstream execution engine. Gateway zip reads are coalesced and large jobs prefer the daemon's advertised immutable v6.3 commit protocol.

**Tech Stack:** TypeScript 5.6, Bun 1.3, Hono 4, `ws` 8, Yjs, Playwright 1.62, Vitest 2.1, Python 3 daemon fixture.

## Global Constraints

- Preserve client `store:false`; never silently change it to `store:true`.
- Codex-visible lineage uses `previous_response_id`; `previous_session_id` is internal-only.
- WebSocket server events are direct Responses event objects.
- Same-lane requests are FIFO; different lanes run concurrently.
- HTTP and WebSocket callers share one bounded global response scheduler.
- Do not stage or overwrite the user's modified `sandbox/gp_daemon.py`.
- Prefer `segmented-v6.3-embedded-commit` only when advertised in `gp_status.json`; retain v6.2 fallback.
- Every production behavior begins with a failing regression test.
- Do not change token/debug-route security behavior in this work.

---

### Task 1: Correct Incremental SSE Parsing

**Files:**
- Create: `tests/sse.test.ts`
- Modify: `src/relay/sse.ts`

**Interfaces:**
- Consumes: daemon progress JSON `{streaming,total,start,tail}`.
- Produces: `SseDeltaExtractor.feed(raw: string): SseFrame[]` with exactly-once complete-frame delivery and explicit `SseProtocolGapError`.

- [ ] **Step 1: Write failing parser tests**

```ts
it("emits a frame split at every character boundary", () => {
  for (let cut = 1; cut < FRAME.length; cut++) {
    const ex = new SseDeltaExtractor();
    expect(ex.feed(progress(FRAME.slice(0, cut), cut))).toEqual([]);
    expect(ex.feed(progress(FRAME, FRAME.length)).map(x => x.event)).toEqual(["response.output_text.delta"]);
  }
});

it("parses CRLF and non-BMP text exactly once", () => {
  const frame = "event: response.output_text.delta\r\ndata: {\"delta\":\"🚀\"}\r\n\r\n";
  const ex = new SseDeltaExtractor();
  expect(ex.feed(codePointProgress(frame))).toHaveLength(1);
  expect(ex.feed(codePointProgress(frame))).toEqual([]);
});
```

- [ ] **Step 2: Run `bun x vitest run tests/sse.test.ts` and confirm split-frame/CRLF failures**
- [ ] **Step 3: Replace pending/consumed logic with a complete-frame cursor, code-point slicing helpers, CRLF normalization, and explicit window-gap errors**
- [ ] **Step 4: Run focused and existing mirror tests**
- [ ] **Step 5: Commit `fix: preserve split SSE frames`**

### Task 2: Stream Fetch Responses Through Node HTTP

**Files:**
- Create: `src/http/adapter.ts`
- Create: `tests/http-streaming.test.ts`
- Modify: `src/server.ts`

**Interfaces:**
- Produces: `writeFetchResponse(res: ServerResponse, response: Response): Promise<void>`.

- [ ] **Step 1: Write a real-server failing timing test**

```ts
it("delivers the first SSE chunk before stream completion", async () => {
  const firstAt = deferred<number>();
  const doneAt = deferred<number>();
  const body = await requestTimedStream(serverUrl, firstAt, doneAt);
  expect(await firstAt.promise).toBeLessThan(await doneAt.promise);
  expect(body).toContain("response.completed");
});
```

- [ ] **Step 2: Run the focused test and verify the current `arrayBuffer()` adapter fails timing**
- [ ] **Step 3: Implement chunked backpressure-aware piping with abort/close cleanup; retain buffered handling only for bodyless responses**
- [ ] **Step 4: Replace the inline adapter in `src/server.ts` and rerun focused tests**
- [ ] **Step 5: Commit `fix: stream HTTP responses incrementally`**

### Task 3: Global End-to-End Response Scheduler

**Files:**
- Create: `src/relay/scheduler.ts`
- Create: `tests/scheduler.test.ts`
- Modify: `src/config.ts`
- Modify: `src/server.ts`
- Modify: `src/utils/queue.ts`

**Interfaces:**
- Produces: `responseScheduler.run<T>(operation, {signal?}): Promise<T>` and `scheduledRelay(body, {signal?}): Promise<RunHandle>`.

- [ ] **Step 1: Write failing tests for FIFO admission, capacity, queued cancellation, and lease retention until `handle.final` settles**
- [ ] **Step 2: Confirm current `enqueue(() => runRelay())` releases streaming capacity too early**
- [ ] **Step 3: Implement a FIFO scheduler with one settlement path per lease and counters for active/queued/completed/failed/cancelled**
- [ ] **Step 4: Route HTTP and future WebSocket work through `scheduledRelay`; derive capacity from `min(GP_SLOTS, PQUEUE_CONCURRENCY when set)`**
- [ ] **Step 5: Run scheduler, mirror, and typecheck tests**
- [ ] **Step 6: Commit `feat: share relay capacity across transports`**

### Task 4: Connection-Local Stateless Response Graph

**Files:**
- Create: `src/ws/response-graph.ts`
- Create: `tests/response-graph.test.ts`

**Interfaces:**
- Produces: `ResponseGraph.resolve(request): ResolvedRelayRequest`, `record(request, final): void`, `evict(id): void`, and `clear(): void`.

- [ ] **Step 1: Write failing tests for incremental continuation, cross-lane forks, unknown IDs, instruction non-inheritance, encrypted reasoning retention/redaction, structural sharing, and byte/count eviction**

```ts
const resolved = graph.resolve({
  type: "response.create", store: false, previous_response_id: "resp_1",
  input: [{type:"function_call_output", call_id:"call_1", output:"42"}],
});
expect(resolved.relayBody.previous_response_id).toBeUndefined();
expect(resolved.relayBody.store).toBe(false);
expect(resolved.relayBody.stream).toBe(true);
expect(resolved.relayBody.input).toEqual([...priorInput, ...priorOutput, ...newInput]);
```

- [ ] **Step 2: Verify failures because no response graph exists**
- [ ] **Step 3: Implement immutable parent-linked nodes, lazy flattening, process/connection byte budgets, and outbound encrypted-content redaction**
- [ ] **Step 4: Run focused tests and typecheck**
- [ ] **Step 5: Commit `feat: cache stateless websocket response chains`**

### Task 5: Codex-Compatible WebSocket Sessions

**Files:**
- Create: `src/ws/session.ts`
- Create: `src/ws/server.ts`
- Create: `tests/websocket.test.ts`
- Modify: `src/server.ts`

**Interfaces:**
- Consumes: injected `scheduledRelay` and `ResponseGraph`.
- Produces: `attachResponsesWebSocket(server, dependencies): WebSocketServer`.

- [ ] **Step 1: Write real `ws` integration tests for direct event objects, implicit streaming, named `stream_id`, default lane omission, same-lane FIFO, cross-lane overlap, 16 active requests, 32 named lanes, malformed input isolation, `background` rejection, tool-call continuation, forks, and disconnect cleanup**
- [ ] **Step 2: Run the tests and confirm current `{type, parsed}` envelopes and missing lane rules fail**
- [ ] **Step 3: Implement `ResponsesWebSocketSession` with per-lane promise tails, connection semaphore, outbound buffered-amount queue, direct event parsing, stable errors, and graph recording**
- [ ] **Step 4: Implement local `generate:false` warm-up responses and unknown-ID behavior**
- [ ] **Step 5: Replace `attachWss` in `src/server.ts` and run WebSocket plus mirror tests**
- [ ] **Step 6: Commit `feat: implement Codex Responses websocket mode`**

### Task 6: Coalesced Zip Polling and Daemon v6.3 Client

**Files:**
- Create: `src/prism/zip-coordinator.ts`
- Create: `tests/zip-coordinator.test.ts`
- Create: `tests/segments.test.ts`
- Modify: `src/prism/gateway.ts`
- Modify: `src/relay/segments.ts`
- Modify: `src/relay/slots.ts`

**Interfaces:**
- Produces: `ZipPollCoordinator.read(names): Promise<Map<string, Uint8Array>>`, `supportsJobProtocol(name): boolean`, and `planV63Job(slot, text): PlannedV63Job`.

- [ ] **Step 1: Write failing coalescing tests proving eight simultaneous filenames cause one archive fetch and failures reject only current waiters**
- [ ] **Step 2: Write failing v6.3 tests for parts `1..K-1` before part zero, `GP63` header validation, maximum part size, gzip/raw plans, and v6.2 fallback**
- [ ] **Step 3: Implement the 10 ms zip batcher and route status, response, and disk-verification reads through it**
- [ ] **Step 4: Cache advertised daemon capabilities from `gp_status.json`**
- [ ] **Step 5: Implement capability-negotiated immutable v6.3 publishing and cleanup without modifying `sandbox/gp_daemon.py`**
- [ ] **Step 6: Run focused tests, slot-related tests, and typecheck**
- [ ] **Step 7: Commit `feat: batch slot polling and publish v6.3 jobs`**

### Task 7: Portable and Safe Playwright Bridge

**Files:**
- Create: `src/bridge/config.ts`
- Create: `tests/bridge-config.test.ts`
- Modify: `src/bridge/playwright.ts`
- Modify: `src/prism/ydoc.ts`
- Modify: `tests/bridge.test.ts`

**Interfaces:**
- Produces: `resolveBridgeConfig(env, repoRoot): BridgeConfig` and existing bridge functions backed by that configuration.

- [ ] **Step 1: Set bridge-test environment to temporary directories before module import and write failing tests proving no live `.env`/token path is touched**
- [ ] **Step 2: Write failing tests for configured doc IDs, shared persistent headful/headless profile, absence of `--no-sandbox`, and no global Chrome termination**
- [ ] **Step 3: Implement pure bridge configuration and replace hard-coded repository, user, and project paths**
- [ ] **Step 4: Use `launchPersistentContext` for both headful/headless launches, remove `taskkill`, and close only owned contexts on retry**
- [ ] **Step 5: Use configured `PRISM_DOC_ID` for every `/api/y` and project-access request; repair Yjs live minting through the active bridge page**
- [ ] **Step 6: Run bridge tests, confirm the real token file remains byte-identical, and run typecheck**
- [ ] **Step 7: Commit `fix: make the playwright bridge portable and isolated`**

### Task 8: Active-Architecture Coverage, Documentation, and Final Verification

**Files:**
- Modify: `README.md`
- Modify: `package.json`
- Modify: existing tests as needed for active runtime coverage
- Create: `scripts/live_verify.ts`

**Interfaces:**
- Produces package scripts `typecheck`, `test:unit`, `test:integration`, and opt-in `verify:live`.

- [ ] **Step 1: Add failing assertions that every README-listed test file exists and documented defaults equal runtime config**
- [ ] **Step 2: Rewrite README around scheduler, WebSocket graph, zip batching, active slot daemon, v6.3 negotiation, and actual test commands**
- [ ] **Step 3: Label legacy relay/proxy modules compatibility-only and remove legacy tests as evidence for active server behavior**
- [ ] **Step 4: Add an opt-in live verifier for HTTP first-event timing, Codex-style tool continuation, forks, and eight concurrent lanes; do not run it without explicit live authorization**
- [ ] **Step 5: Run `bun x vitest run`, `bun x tsc --noEmit`, Python AST parsing, `bun audit --production`, and `git diff --check`**
- [ ] **Step 6: Confirm `sandbox/gp_daemon.py` remains unstaged and preserve the user's diff**
- [ ] **Step 7: Invoke branch-finishing workflow and present integration options**

## Self-Review

- Spec coverage: every architecture, protocol, concurrency, bridge, testing, and documentation requirement maps to a task above.
- Placeholders: the plan contains no deferred implementation steps; daemon hardening remains separately owned and is not required for local correctness.
- Type consistency: `scheduledRelay` returns the existing `RunHandle`; `ResponseGraph.resolve` produces the body consumed by it; `ResponsesWebSocketSession` owns lanes and calls both.
- Execution mode: inline execution was explicitly selected; no subagents will be used.
