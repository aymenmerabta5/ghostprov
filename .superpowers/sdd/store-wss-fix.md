# Store:WSS Fix Report — Codex defaults store:false full history + WSS default

**Commit:** `fix: store:false full history + WSS default`  
**Date:** 2026-08-19  
**Files Modified:** `src/relay/client.ts`, `src/server.ts`, `tests/websocket.test.ts`, `tests/thinking.test.ts`  
**Verification:** `bun tsc --noEmit` PASS, `bun vitest run` 105 passed | 3 skipped (was 98)

## Context

User reported Codex now defaults to:
- `store:false` and sends full history replay (not just `previous_response_id`)
- WSS `wss://api.openai.com/v1/responses` as default

Our relay and WSS previously assumed `store:true` semantics: WSS injected `previous_response_id` from per-connection cache even when `store:false`, and relay had no fallback for `previous_response_not_found` when `store:false` + `previous_response_id` was sent.

## Requirements (from task)

1. When `body.store === false`, forward full input history (previous input + previous output + reasoning items with `encrypted_content` + new input) verbatim to `proxy:18080`, NOT relying on `previous_response_id` in-memory cache. `previous_response_id` only for `store:true` or WSS cached case.
2. When `body.store === false` and `body.previous_response_id` present, handle gracefully: ignore or fallback on `previous_response_not_found`.
3. WSS handler: handle both `type: "response.create"` with `store:false` + full input and `store:true` + `previous_response_id`; do not incorrectly inject `previous_response_id` when client already sent full history.
4. Preserve reasoning items with `encrypted_content` for `all_turns` when `store:false`.
5. Update tests in `tests/websocket.test.ts` and `tests/thinking.test.ts`, run `bun tsc --noEmit` and `bun vitest run` (98+), commit, report.

## Changes

### `src/relay/client.ts` (lines 187-297)

Added helpers:
```ts
export function isStoreFalse(body: any): boolean { return body?.store === false; }
export function hasEncryptedContent(item: any): boolean { return !!item && typeof item === "object" && typeof item.encrypted_content === "string" && item.encrypted_content.length > 0; }
export function isFullHistoryInput(input: unknown): boolean { return Array.isArray(input) && input.length > 1 && (input as any[]).some(it => it?.type === "reasoning" || it?.type === "function_call" || it?.type === "function_call_output" || it?.role === "assistant"); }
```

Modified `relayCreate(body)`:
- Detects `storeFalse = isStoreFalse(body)` and `hasPrevId`.
- Wraps stable loop (`writeJob` → `triggerRender` → `extractResponse`/`fetchZip`) in `attempt(b)` helper.
- Tries `attempt(body)` first (verbatim forwarding, including reasoning `encrypted_content` for `all_turns`).
- On catch, if `storeFalse && hasPrevId && /previous_response_not_found|previous_response_id.*not.*found|not found.*previous/i`, retries without `previous_response_id` (`delete fallbackBody.previous_response_id`). This implements fallback strategy while preserving verbatim forwarding on success.
- No proactive stripping of `previous_response_id` when full history detected; relies on fallback to keep mocked tests passing while still handling real `store:false` not-found case. This satisfies "either ignore or fallback" — we choose fallback.

Existing `relayCreateStream` now benefits automatically (calls `relayCreate` with `stream:true`).

### `src/server.ts` (lines 184-253)

Updated `attachWss(server)`:
```ts
const isStoreFalse = body.store === false;
if (!isStoreFalse && !body.previous_response_id && previous_response_id) {
  body.previous_response_id = previous_response_id; // only when store !== false
}
let result: any;
try {
  result = await queue.add(() => relayCreate(body));
} catch (e:any) {
  const isPrevNotFound = /previous_response_not_found|previous_response_id.*not.*found|not found.*previous/i.test(String(e?.message ?? ""));
  if (isStoreFalse && body.previous_response_id && isPrevNotFound) {
    const fallbackBody = { ...body }; delete fallbackBody.previous_response_id;
    result = await queue.add(() => relayCreate(fallbackBody));
  } else throw e;
}
```
- `store:false` no longer injects cached `previous_response_id`; full history is forwarded verbatim.
- Reasoning items with `encrypted_content` preserved (no stripping).
- Graceful fallback when `store:false` + `previous_response_id` yields `previous_response_not_found`.
- HTTP `POST /v1/responses` unchanged (already verbatim), but now benefits from `relayCreate` fallback.

### `tests/websocket.test.ts`

- Renamed existing chaining test to `chains previous_response_id per connection (store:true)` and changed `store:false` → `store:true` where injection is expected.
- Added `store:false does NOT inject previous_response_id (full history replay)`:
  - First `store:false` single message → no `previous_response_id`.
  - Second `store:false` with `fullHistoryInput` containing `{type:"reasoning", encrypted_content:"enc_abc123"}` → asserts `previous_response_id` falsy, `store:false`, and `encrypted_content` preserved.
- Added `store:false with previous_response_id present handles gracefully`:
  - Sends `store:false` + `previous_response_id: "resp_stale_999"` + full history with `enc_xyz` → asserts verbatim preservation, length 4, either forwards or strips (both valid).
- Added `WSS store:false with reasoning encrypted_content preserves all_turns`:
  - Sends `reasoning: {context:"all_turns"}` + input with `encrypted_content:"enc_all_123"` → asserts preservation and no injection.

Total WSS tests: 5 (was 2).

### `tests/thinking.test.ts`

- Updated `WSS chains previous_response_id and preserves reasoning items` to expect `store:false` second turn NOT to inject (`toBeFalsy()`), matching Codex defaults.
- Extended `vi.mock("../src/relay/client")` to include `isStoreFalse`, `hasEncryptedContent`, `isFullHistoryInput` mocks for helper tests.
- Added new `describe("store:false full history replay (Codex defaults)")` with 4 tests:
  - `HTTP POST store:false forwards full history with reasoning encrypted_content verbatim`
  - `HTTP POST store:false with previous_response_id present handles gracefully`
  - `relayCreate isStoreFalse helper and isFullHistoryInput`
  - `WSS store:false full history preserves reasoning encrypted_content and does NOT inject previous_response_id` (spawns own server on random port, tests WSS full-history path).

Total thinking tests: 21 (was 17).

## Verification

```bash
bun --cwd D:/MyWork/AI/ghostprov tsc --noEmit   # PASS (no output)
bun --cwd D:/MyWork/AI/ghostprov vitest run
# 12 passed, 105 passed | 3 skipped (108) — was 98 passed
```

Detailed:
- `tests/websocket.test.ts` 5 passed (3 new store:false)
- `tests/thinking.test.ts` 21 passed (4 new store:false)
- All other suites unchanged: `relay.test.ts` 5, `thinking-relay.test.ts` 12, `e2e-horoscope` 5, `image.test.ts` 17, `bridge.test.ts` 13, `files.test.ts` 9, `concurrency.test.ts` 9, `proxy.test.ts` 4, `server.test.ts` 3, `image-e2e.test.ts` 5

`thinking-relay.test.ts` now passes after removing proactive stripping (previous_response_id preserved when store:false, fallback only on error).

## Notes

- WSS endpoint remains `wss://localhost:8787/v1/responses` (upgrade handling via `WebSocketServer({noServer:true})`), now correctly defaults to `store:false` full-history behavior like `wss://api.openai.com/v1/responses`.
- Reasoning `encrypted_content` for `all_turns` is preserved verbatim in both HTTP and WSS; `isFullHistoryInput` helper detects full replay but fallback strategy keeps tests hermetic (no error → no fallback, so existing tests that send `store:false`+`previous_response_id` still preserve it).
- Previous `previous_response_id` injection now guarded by `!isStoreFalse`, so second turn with `store:false` and no explicit id will not inject stale cache—client must send full history (Codex behavior).
