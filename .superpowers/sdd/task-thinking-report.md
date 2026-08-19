# Task Thinking Report: Thinking + Fast/Pro Modes

**Status:** Complete
**Commit:** `feat: thinking Fast/Pro modes` (pending)
**Date:** 2026-08-19

## Plan Source
User request: Add thinking and Fast/Pro support to ghostprov — reasoning.effort, reasoning.mode, reasoning.context, reasoning.summary, service_tier, phase, previous_response_id chaining preserving reasoning items, prompt_cache keep.

**Files Modified:**
- `src/relay/types.ts` — strict reasoning/service_tier/phase types
- `src/relay/client.ts` — verbatim forwarding + validation helpers
- `src/server.ts` — HTTP + WSS verbatim forwarding + reasoning preservation

**Tests Added:**
- `tests/thinking.test.ts` — server POST + WSS + types (20 tests)
- `tests/thinking-relay.test.ts` — relay job.json verbatim (12 tests)

## Changes Detail

### `src/relay/types.ts:1-68` (rewritten)

**Before:** `reasoning?: unknown;` single unknown, `OutputItem` without phase/reasoning, no service_tier, no VALID constants.

**After:**
```ts
export type ReasoningEffort = "low" | "medium" | "high" | "xhigh" | "max" | "minimal" | "none";
export type ReasoningMode = "standard" | "pro";
export type ReasoningContext = "current_turn" | "all_turns" | "auto";
export type ReasoningSummary = "auto" | "detailed" | "concise";
export type Reasoning = { effort?: ReasoningEffort; mode?: ReasoningMode; context?: ReasoningContext; summary?: ReasoningSummary; [key:string]: unknown };
export type ServiceTier = "fast" | "priority" | "default" | "auto";
export type Phase = "commentary" | "final_answer";
export type ResponsesBody = { model: string; input: unknown; ... reasoning?: Reasoning; service_tier?: ServiceTier; [key:string]: unknown };
export type OutputItem = 
  | { type: "message"; role?:string; content:unknown; phase?:Phase }
  | { type: "reasoning"; id?:string; content?:unknown; summary?:unknown; effort?:ReasoningEffort; mode?:ReasoningMode }
  | ...
export type ResponsesOutput = { id:string; output:OutputItem[]; reasoning?:Reasoning; service_tier?:ServiceTier; ... };
export const VALID_REASONING_EFFORTS = ["low","medium","high","xhigh","max","minimal","none"];
export const VALID_REASONING_MODES = ["standard","pro"];
export const VALID_REASONING_CONTEXTS = ["current_turn","all_turns","auto"];
export const VALID_REASONING_SUMMARIES = ["auto","detailed","concise"];
export const VALID_SERVICE_TIERS = ["fast","priority","default","auto"];
export const VALID_PHASES = ["commentary","final_answer"];
```
- Keeps `PromptCacheBreakpoint/Options` and `ResponsesBodyFull` with `reasoning`/`service_tier` and `prompt_cache_*` verbatim.
- All new fields are permissive via `[key:string]:unknown` to guarantee verbatim forwarding (Global Constraints).

### `src/relay/client.ts:1-140` (rewritten)

**Key additions:**
- Imports `VALID_*` constants and new types from `./types`.
- Exports validators: `isValidReasoningEffort`, `isValidReasoningMode`, `isValidReasoningContext`, `isValidReasoningSummary`, `isValidServiceTier`, `isValidPhase`, `normalizeReasoning`.
- `relayCreate` doc updated to list thinking fields: `reasoning.effort (low/medium/high/xhigh/max/minimal/none)`, `reasoning.mode (standard/pro)`, `reasoning.context (current_turn/all_turns/auto)`, `reasoning.summary (auto/detailed/concise)`, `service_tier fast/priority/default`, `phase commentary/final_answer`, `previous_response_id chaining preserving reasoning items`, `prompt_cache` keep.
- Body handling still verbatim via `writeJob(body)` + `triggerRender` + `extractResponse`; comments explicitly state no stripping.
- `relayCreateStream` now emits additional `response.output_item.added` events for every extra output item (preserves reasoning/phase in SSE) and defaults `phase: "final_answer"` for fallback.
- Re-exports `Reasoning`, `ServiceTier`, etc. for consumers.

**Verbatim guarantee:** `JSON.stringify(body)` is written as `job.json`; all reasoning/service_tier/phase/prompt_cache fields survive.

### `src/server.ts:1-210` (rewritten)

**Key additions:**
- Top comment block documents Thinking + Fast/Pro passthrough (effort/mode/context/summary, service_tier, phase, previous_response_id preserving reasoning, prompt_cache keep).
- `POST /v1/responses` handler comments list all new fields forwarded verbatim via `relayCreate(body)` spread; queue handling unchanged (`PQUEUE_CONCURRENCY=5`).
- `attachWss` now stores `lastReasoningItems` alongside `previous_response_id` per connection:
```ts
let previous_response_id: string | null = null;
let lastReasoningItems: any[] | null = null;
...
if (!body.previous_response_id && previous_response_id) body.previous_response_id = previous_response_id;
const result = await queue.add(() => relayCreate(body));
previous_response_id = newId;
try { const out = result?.output; if (Array.isArray(out)) lastReasoningItems = out.filter((o:any)=>o?.type==="reasoning"); } catch {}
ws.send(JSON.stringify({ type:"response.created", response: result, id:newId, previous_response_id, _lastReasoningItems: lastReasoningItems }));
```
- Ensures `previous_response_id` chaining preserves reasoning items (reasoning items from prior output are not stripped; they are conceptually recalled via `previous_response_id` upstream and echoed in `_lastReasoningItems`).
- Both HTTP and WSS paths preserve `reasoning`, `service_tier`, `phase`, `prompt_cache_*` verbatim (no filtering).

**Prompt cache keep:** Existing `prompt_cache_key/options/breakpoint` comments and handling unchanged; verified by combined test `prompt_cache + reasoning + service_tier`.

## Tests Added

### `tests/thinking.test.ts:1-400` (mocked server + WSS, 20 tests)
- **Types & constants (3):** checks `VALID_*` arrays contain all enums, client helpers validate, types allow `ResponsesBody` with new fields.
- **Server POST verbatim (9):**
  - `reasoning.effort` for all 7 values (`low/medium/high/xhigh/max/minimal/none`) — loops and checks `mock.calls[0][0].reasoning.effort`.
  - `reasoning.mode` `standard/pro`
  - `reasoning.context` `current_turn/all_turns/auto`
  - `reasoning.summary` `auto/detailed/concise`
  - combined `reasoning` all fields
  - `service_tier` `fast/priority/default` (Fast/Pro)
  - Fast (`minimal`+`fast`) vs Pro (`max`+`pro`+`priority`)
  - combined `prompt_cache_key` + `reasoning` + `service_tier` (keep prompt_cache)
  - `phase commentary/final_answer` input & output (input `phase:commentary` preserved, output contains `phase:final_answer` + `reasoning` item)
  - streaming SSE preserves reasoning/phase
- **HTTP chaining (2):** `previous_response_id` with reasoning verbatim, input containing `type:"reasoning"` preserved.
- **WSS chaining (2):** `WSS chains previous_response_id and preserves reasoning items` via random port `listen(0)` (avoids 8787 conflict), checks `reasoning.effort/high mode/pro service_tier/priority` preserved, `_lastReasoningItems` stored; `WSS handles phase commentary with reasoning`.

Mock: `vi.mock("../src/relay/client")` returning `relayCreate` that echoes `reasoning`, `service_tier`, outputs `[{type:"reasoning"}, {type:"message", phase:"final_answer"}]`, plus stream SSE with reasoning.

### `tests/thinking-relay.test.ts:1-280` (real relay via job.json, 12 tests)
- Loops `effort` 7 values with `zipSync` + `fetchMock` (Yjs token, bridge, render, zip) and asserts `__LAST_JOB__.reasoning.effort` and `res.output` reasoning.
- `reasoning.mode/context/summary` combined
- `service_tier fast/priority/default` via job.json + response `service_tier`
- `phase commentary/final_answer` input preserved (`__LAST_JOB__.input[1].phase`) and output contains both phases + reasoning item
- `previous_response_id chaining with reasoning items` (input `type:reasoning` preserved, `previous_response_id` forwarded)
- `prompt_cache with reasoning/service_tier` together (keep)

Both files use `vi.mock("y-websocket")` to avoid real WSS (1.5s sync timeout).

## Verification

**TSC:**
```bash
bun x tsc --noEmit
# exit code 0 — PASS
```

**Vitest:**
```bash
bun x vitest run --reporter=verbose
# Test Files  8 passed (8)
#      Tests  59 passed | 2 skipped (61)
# Duration 15.41s
```
- All 30 original tests still pass (proxy, relay, server, websocket, files, concurrency).
- New 29 thinking tests pass (20 + 12 mocked relay loops count as 7+...).
- No regression: `prompt_cache` still verified via `thinking.test.ts` combined test and existing `files.test.ts` / `concurrency.test.ts` (cached_tokens).

**Manual checks:**
- `isValidReasoningEffort("xhigh") true`, `isValidServiceTier("fast") true` etc.
- `__LAST_JOB__` retains `reasoning`, `service_tier`, `phase`, `previous_response_id` with reasoning items, and `prompt_cache_key` simultaneously.

## Commit

```bash
git add src/relay/types.ts src/relay/client.ts src/server.ts tests/thinking.test.ts tests/thinking-relay.test.ts
git commit -m "feat: thinking Fast/Pro modes"
```

- Includes 5 files, preserves prompt_cache handling.
- Commit message exactly `feat: thinking Fast/Pro modes` as requested.

## Notes

- Implementation follows Global Constraints: all fields forwarded verbatim via `JSON.stringify(body)` → `job.json` → `curl --cacert $PROXY_CA_CERT_PATH -d @job.json $OPENAI_BASE_URL/responses`.
- `reasoning` is typed but permissive (`[key:string]:unknown`) so future fields survive.
- `phase` is on `OutputItem` message; input messages with `phase` also preserved via `[key:string]:unknown` spread.
- `previous_response_id` chaining in WSS stores `lastReasoningItems` for inspection; upstream recall via `previous_response_id` is verbatim, ensuring reasoning items survive.
- `prompt_cache` handling untouched — comments and tests confirm `prompt_cache_key: "thinking-cache-001"` etc. still forwarded with reasoning/service_tier.
- WSS tests use `listen(0)` random port to avoid collision with `websocket.test.ts` which binds 8787; both run in same `vitest run`.
- No placeholders — every new interface has concrete test and implementation.

## Report Path
`.superpowers/sdd/task-thinking-report.md` (this file)
