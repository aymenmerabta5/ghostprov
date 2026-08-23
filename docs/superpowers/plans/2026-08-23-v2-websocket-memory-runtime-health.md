# V2 WebSocket Memory and Runtime Health Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate quadratic retained WebSocket continuation memory and prevent normal client/upstream outcomes from permanently poisoning sandbox routing health.

**Architecture:** `ResponseGraph` will retain only byte-budgeted response deltas and materialize one iterative request-scoped history when Codex continues a response. A new V2 runtime-health module will classify outcomes and maintain a time-decaying failure score consumed through the existing router snapshot interface.

**Tech Stack:** Bun 1.4, TypeScript 7, Bun test with `--isolate`, Node-compatible WebSocket runtime, existing `Clock` abstraction.

## Global Constraints

- Work directly on `master` because the user explicitly selected direct inline execution.
- Do not stop, restart, or replace the live router on `127.0.0.1:18787`.
- Introduce no dependency and preserve all public HTTP, SSE, WebSocket, Images, and model behavior.
- Preserve `store:false` and `previous_response_id` compatibility with Codex.
- Keep `RoutableSandboxSnapshot.recentFailureRate` as a finite value in `[0, 1]`.
- Use test-first red→green cycles and run `bun test --isolate` for final verification.

---

### Task 1: Remove Retained Flattened WebSocket Histories

**Files:**
- Modify: `tests/response-graph.test.ts`
- Modify: `src/ws/response-graph.ts`

**Interfaces:**
- Consumes: existing `ResponseGraph.resolve`, `ResponseGraph.record`, `ResponseGraph.clear`, and `ResponseGraph.stats`.
- Produces: the same public interfaces with iterative, request-scoped history materialization and no retained flattened histories.

- [ ] **Step 1: Write the failing memory-amplification regression**

Append this behavior test inside the existing `ResponseGraph` suite:

```ts
it("retains only byte-accounted deltas across a deep continuation chain", () => {
  const MiB = 1024 * 1024;
  Bun.gc(true);
  const before = process.memoryUsage().heapUsed;
  const shared = new SharedByteBudget(256 * MiB);
  const graph = new ResponseGraph({
    maxNodes: 256,
    maxBytes: 256 * MiB,
    sharedBudget: shared,
  });
  let previousResponseId: string | undefined;

  for (let index = 0; index < 80; index++) {
    const resolved = graph.resolve(request([user(
      `${index}:` + "x".repeat(32 * 1024),
    )], previousResponseId ? { previous_response_id: previousResponseId } : {}));
    previousResponseId = `resp_${index}`;
    graph.record(resolved, {
      id: previousResponseId,
      model: "gpt-5.6-sol",
      output: [],
    });
  }

  Bun.gc(true);
  const retainedHeap = Math.max(0, process.memoryUsage().heapUsed - before);
  expect(graph.stats().bytes).toBeLessThan(3 * MiB);
  expect(retainedHeap).toBeLessThan(25 * MiB);
  graph.clear();
  expect(shared.used).toBe(0);
});
```

- [ ] **Step 2: Run the memory regression and verify RED**

Run: `bun test --isolate tests/response-graph.test.ts`

Expected: FAIL because the existing graph retains roughly 100 MiB of uncharged flattened histories.

- [ ] **Step 3: Replace recursive cached materialization with one iterative expansion**

In `src/ws/response-graph.ts`, delete `flattened?: unknown[]` from `ResponseNode`. Reuse the materialized array instead of spreading it into a second array:

```ts
const history = this.materialize(parent);
history.push(...clone(inputDelta));
relayBody.input = history;
```

Replace `materialize` with:

```ts
private materialize(node: ResponseNode): unknown[] {
  const lineage: ResponseNode[] = [];
  let current: ResponseNode | undefined = node;
  while (current) {
    lineage.push(current);
    if (lineage.length > this.maxNodes) {
      throw new Error("Response graph lineage exceeds its node limit");
    }
    if (!current.parentId) break;
    const parent = this.nodes.get(current.parentId);
    if (!parent) throw new PreviousResponseNotFoundError(current.parentId);
    current = parent;
  }

  const history: unknown[] = [];
  for (let index = lineage.length - 1; index >= 0; index--) {
    const entry = lineage[index]!;
    history.push(...clone(entry.input), ...clone(entry.output));
  }
  return history;
}
```

- [ ] **Step 4: Verify GREEN and existing graph semantics**

Run: `bun test --isolate tests/response-graph.test.ts tests/websocket.test.ts`

Expected: all tests pass, including the memory gate, continuation reconstruction, forks, LRU eviction, encrypted reasoning, and WebSocket continuation.

- [ ] **Step 5: Re-run the isolated reproduction**

Run an 80-node, 32-KiB continuation chain with `Bun.gc(true)` and report accounted bytes, heap growth, and RSS growth.

Expected: accounted bytes remain approximately 2.5 MiB and additional heap is below 25 MiB.

- [ ] **Step 6: Commit Task 1**

```powershell
git add -- src/ws/response-graph.ts tests/response-graph.test.ts
git commit -m "fix: bound websocket continuation memory"
```

---

### Task 2: Add Outcome-Aware Time-Decaying Runtime Health

**Files:**
- Create: `src/v2/runtime-health.ts`
- Create: `tests/v2-runtime-health.test.ts`

**Interfaces:**
- Consumes: `Clock` and `SYSTEM_CLOCK` from `src/core/ports.ts` plus structural error fields.
- Produces: `classifyRuntimeError(error): "neutral" | "unhealthy"` and `DecayingRuntimeFailureScore` with `record(outcome)` and `value()`.

- [ ] **Step 1: Write failing classifier and decay tests**

Create `tests/v2-runtime-health.test.ts` with a manual clock and assertions equivalent to:

```ts
expect(classifyRuntimeError(new DOMException("aborted", "AbortError"))).toBe("neutral");
expect(classifyRuntimeError({ committed: true, code: "cancelled", ambiguous: true })).toBe("neutral");
expect(classifyRuntimeError({ committed: true, code: "missing_required_parameter", ambiguous: false })).toBe("neutral");
expect(classifyRuntimeError({ committed: false, code: "capacity_changed" })).toBe("neutral");
expect(classifyRuntimeError({ committed: true, code: "internal", ambiguous: true })).toBe("unhealthy");
expect(classifyRuntimeError(new Error("channel failed"))).toBe("unhealthy");

const score = new DecayingRuntimeFailureScore({ clock });
score.record("unhealthy");
expect(score.value()).toBeCloseTo(0.20);
clock.value = 60_000;
expect(score.value()).toBeCloseTo(0.10);
score.record("unhealthy");
expect(score.value()).toBeCloseTo(0.28);
score.record("healthy");
expect(score.value()).toBeCloseTo(0.224);

const cancellations = new DecayingRuntimeFailureScore({ clock });
for (let index = 0; index < 100; index++) {
  cancellations.record(classifyRuntimeError({
    committed: true,
    code: "cancelled",
    ambiguous: true,
  }));
}
expect(cancellations.value()).toBe(0);
```

- [ ] **Step 2: Run the new test and verify RED**

Run: `bun test --isolate tests/v2-runtime-health.test.ts`

Expected: FAIL because `src/v2/runtime-health.ts` does not exist.

- [ ] **Step 3: Implement classification and bounded decay**

Create `src/v2/runtime-health.ts` with:

```ts
import { SYSTEM_CLOCK, type Clock } from "../core/ports";

export type RuntimeHealthOutcome = "healthy" | "neutral" | "unhealthy";

const NEUTRAL_CODES = new Set([
  "cancelled",
  "capacity_changed",
  "draining",
  "request_too_large",
]);

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === "object"
    ? value as Record<string, unknown>
    : {};
}

export function classifyRuntimeError(error: unknown): Exclude<RuntimeHealthOutcome, "healthy"> {
  const value = record(error);
  if (value.name === "AbortError") return "neutral";
  if (typeof value.code === "string" && NEUTRAL_CODES.has(value.code)) return "neutral";
  if (value.committed === true && value.ambiguous === false) return "neutral";
  return "unhealthy";
}

export class DecayingRuntimeFailureScore {
  readonly #clock: Clock;
  readonly #weight: number;
  readonly #halfLifeMs: number;
  #score = 0;
  #updatedAtMs: number;

  constructor(options: { clock?: Clock; weight?: number; halfLifeMs?: number } = {}) {
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#weight = options.weight ?? 0.20;
    this.#halfLifeMs = options.halfLifeMs ?? 60_000;
    if (!Number.isFinite(this.#weight) || this.#weight <= 0 || this.#weight > 1) {
      throw new RangeError("runtime health weight must be in (0, 1]");
    }
    if (!Number.isFinite(this.#halfLifeMs) || this.#halfLifeMs <= 0) {
      throw new RangeError("runtime health half-life must be positive");
    }
    this.#updatedAtMs = this.#now();
  }

  record(outcome: RuntimeHealthOutcome): void {
    this.#advance();
    if (outcome === "unhealthy") {
      this.#score += this.#weight * (1 - this.#score);
    } else if (outcome === "healthy") {
      this.#score *= 1 - this.#weight;
    }
    this.#score = Math.max(0, Math.min(1, this.#score));
  }

  value(): number {
    this.#advance();
    return this.#score;
  }

  #advance(): void {
    const now = this.#now();
    if (now < this.#updatedAtMs) throw new RangeError("runtime health clock moved backwards");
    this.#score *= Math.pow(0.5, (now - this.#updatedAtMs) / this.#halfLifeMs);
    this.#updatedAtMs = now;
  }

  #now(): number {
    const value = this.#clock.now();
    if (!Number.isFinite(value)) throw new RangeError("runtime health clock must be finite");
    return value;
  }
}
```

- [ ] **Step 4: Verify GREEN**

Run: `bun test --isolate tests/v2-runtime-health.test.ts`

Expected: all classifier, accumulation, healing, time-decay, validation, and 100-cancellation tests pass.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- src/v2/runtime-health.ts tests/v2-runtime-health.test.ts
git commit -m "feat: add decaying v2 runtime health score"
```

---

### Task 3: Integrate Health Scoring into V2 Runtime and Routing Inputs

**Files:**
- Modify: `src/v2/runtime.ts`
- Modify: `tests/v2-runtime.test.ts`
- Modify: `README.md`
- Modify: `tests/documentation.test.ts`

**Interfaces:**
- Consumes: `classifyRuntimeError` and `DecayingRuntimeFailureScore` from Task 2.
- Produces: truthful `RoutableSandboxSnapshot.recentFailureRate` values without changing the router policy API.

- [ ] **Step 1: Write failing V2 runtime integration tests**

Extend `tests/v2-runtime.test.ts` to reject real `RunHandle.final` promises with `V2CommittedError` values and prove:

```ts
expect(runtime.snapshot().recentFailureRate).toBe(0); // after cancellation
expect(runtime.snapshot().recentFailureRate).toBe(0); // after definitive upstream 400
expect(runtime.snapshot().recentFailureRate).toBeCloseTo(0.20); // ambiguous transport error
await clock.advanceBy(60_000);
expect(runtime.snapshot().recentFailureRate).toBeCloseTo(0.10); // idle recovery
```

Then resolve one request successfully and expect the score to fall to approximately `0.08` after the healthy sample.

- [ ] **Step 2: Run the runtime tests and verify RED**

Run: `bun test --isolate tests/v2-runtime.test.ts`

Expected: FAIL because the existing lifetime counter reports cancellation and definitive errors as failures and never time-decays.

- [ ] **Step 3: Replace lifetime request counters with the health component**

In `src/v2/runtime.ts`:

```ts
import {
  DecayingRuntimeFailureScore,
  classifyRuntimeError,
} from "./runtime-health";
```

Initialize one scorer with the runtime clock, remove `#attempts` and `#failures`, and update outcomes:

```ts
this.#runtimeHealth.record("healthy");
this.#runtimeHealth.record(classifyRuntimeError(error));
this.#runtimeHealth.record("unhealthy"); // transient heartbeat/credential/resource faults
```

Expose the current decayed score:

```ts
recentFailureRate: this.#runtimeHealth.value(),
```

Every rejection must still propagate unchanged. Completion EWMA behavior remains unchanged.

- [ ] **Step 4: Verify runtime integration and routing policy**

Run: `bun test --isolate tests/v2-runtime-health.test.ts tests/v2-runtime.test.ts tests/router-policy.test.ts`

Expected: cancellation and definitive errors remain neutral, ambiguous faults penalize temporarily, idle time heals, and routing policy tests remain green.

- [ ] **Step 5: Document the bounded continuation and health behavior**

Add concise README failure/concurrency bullets explaining:

```md
- WebSocket `store:false` continuation caches retain only byte-budgeted deltas; full histories are materialized per request and never cached quadratically.
- Client cancellations and definitive upstream errors are routing-neutral; only transport-attributable failures feed a sixty-second decaying sandbox health score.
```

Add documentation assertions for both statements in `tests/documentation.test.ts`.

- [ ] **Step 6: Run focused tests and typecheck**

Run:

```powershell
bun run typecheck
bun test --isolate tests/response-graph.test.ts tests/websocket.test.ts tests/v2-runtime-health.test.ts tests/v2-runtime.test.ts tests/router-policy.test.ts tests/documentation.test.ts
```

Expected: zero type errors and all focused tests pass.

- [ ] **Step 7: Commit Task 3**

```powershell
git add -- src/v2/runtime.ts tests/v2-runtime.test.ts README.md tests/documentation.test.ts
git commit -m "fix: keep client outcomes out of sandbox health"
```

---

### Task 4: Full Verification and Delivery

**Files:**
- Verify: all tracked source, test, documentation, and lock files.

**Interfaces:**
- Consumes: completed Tasks 1–3.
- Produces: a clean, pushed `master` with evidence that the live router remained untouched.

- [ ] **Step 1: Run complete static, dependency, and daemon gates**

```powershell
bun run typecheck
bun run test:daemon
bun audit
```

Expected: all commands exit zero and audit reports no vulnerabilities.

- [ ] **Step 2: Run the complete isolated suite**

Run: `bun test --isolate`

Expected: every test passes with zero failures.

- [ ] **Step 3: Run protected HTTP/WebSocket smoke**

```powershell
$env:GP_SMOKE_PROTECTED_ROOT='http://127.0.0.1:18787'
bun run verify:v2-smoke
```

Expected: HTTP and WebSocket complete on an ephemeral candidate while the protected PID on port 18787 remains unchanged.

- [ ] **Step 4: Review and push**

Verify `git diff --check`, clean tracked status, commit range, and unchanged port ownership. Push `master`, fetch `origin/master`, and verify its SHA equals `HEAD`.
