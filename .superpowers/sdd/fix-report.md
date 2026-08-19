# Fix Report — Final Review Blocking Issues (2026-08-19)

**Source:** `.superpowers/sdd/final-review.md` (F1 + F2, conditional PASS)
**Branch:** `master` `f3a2058` → HEAD
**Commit:** `fix: final review blocking issues`
**Date:** 2026-08-19 UTC
**Executor:** fix agent (OpenCode / Muse Spark)

---

## 1. Issues Fixed

### F1 — `bun x tsc --noEmit` fails (7→13 errors)

**Root causes verified `src/proxy/yjs.ts:5`, `src/server.ts:4`, `src/server.ts:158`, `tests/websocket.test.ts:2,54`, `node_modules/vite/dist/node/types.d-aGj9QkWt.d.ts:161`:**
- `TS7016 Could not find declaration file for module 'ws'` — `ws` used in 3 files (`src/proxy/yjs.ts:5`, `src/server.ts:4`, `tests/websocket.test.ts:2`) but `package.json:16-20` devDeps lacked `@types/ws`
- `TS2322 Buffer not assignable to BodyInit` — `new Request(url, {body})` where `body: Buffer|undefined` in `src/server.ts:158` and `tests/websocket.test.ts:54`; DOM `BodyInit` has no Node `Buffer`
- `TS2430 ViteRuntimeImportMeta incorrectly extends ImportMeta` — vite/vitest types clash under `strict:true` when `skipLibCheck` is absent
- `TS7006 Parameter 'e'/'data'/'d' implicitly has 'any'` — resolved as side effect of missing `@types/ws` (once typed, handler signatures inferred)

**Fixes:**
1. `bun add -d @types/ws` → `package.json:17` now `"@types/ws": "^8.18.1"` + `bun.lock` updated (install verified `1 package installed`)
2. `tsconfig.json:1` added `"skipLibCheck": true` alongside `"strict": true` — suppresses upstream vite lib clash without loosening source checks
3. `src/server.ts:181` changed `new Request(url, {method, headers, body})` → `body: body ? new Uint8Array(body) : undefined` — `Uint8Array` is valid `BodyInit`, preserves Buffer bytes, satisfies `tsc`
4. `tests/websocket.test.ts:54` same `Uint8Array` cast — keeps test Hono bridge identical to production, eliminates duplicate error

### F2 — `__bridge/write` / `__internal/write-job` fallback has no handler

**Root cause verified `src/proxy/yjs.ts:42-46`, `src/relay/client.ts:29-33`, `src/server.ts:1-60`:**
- `writeJob()` POSTs to `http://localhost:8787/__bridge/write` and `relayCreate()` best-effort POSTs to `http://localhost:8787/__internal/write-job` as documented monaco-bridge fallback. `src/server.ts` registered no such routes, so live `PRISM_DOC_ID` run would 404/`ECONNREFUSED`. Mocked suite passed only because `globalThis.fetch` was mocked (`tests/proxy.test.ts:44`, `tests/relay.test.ts:66`).

**Fix (`src/server.ts:10-32`):**
- Added `app.post("/__bridge/write", async (c) => { … return c.json({ok:true}) })` — parses `{filename, content}` and stores `JSON.parse(content)` to `globalThis.__LAST_JOB__` for zip-synthesis fallback, returns 200
- Added `app.post("/__internal/write-job", async (c) => { … return c.json({ok:true}) })` — stores raw body to `__LAST_JOB__`, returns 200
- Handlers are no-op for mocked tests (fetch mocked) but make the stable-loop bridge live-path return 200 instead of 404; Yjs `wss://prism.openai.com/y/d/<doc>/ws` remains the primary live path

---

## 2. Files Changed

| File | Change |
|------|--------|
| `package.json` | `devDependencies` add `@types/ws@^8.18.1` (reorders `vitest`/`@types/bun`/`typescript` — bun lockfile sort) |
| `bun.lock` | `@types/ws@8.18.1` resolved |
| `tsconfig.json` | `compilerOptions.skipLibCheck: true` added |
| `src/server.ts` | +22 lines: two `app.post` bridge handlers (10-32); line 181 `Buffer`→`Uint8Array` cast |
| `tests/websocket.test.ts` | line 54 `Buffer`→`Uint8Array` cast |
| `.superpowers/sdd/fix-report.md` | this report |

---

## 3. Verification

### `bun x tsc --noEmit`
```
# Before fix (8–13 errors):
# vite TS2430, ws TS7016 x3, Buffer TS2322 x2, implicit any x7
# After fix:
bun x tsc --noEmit
# (no output) EXIT:0 — 0 errors
```

### `bun x vitest run` (Bun 1.3.14, Vitest 2.1.9)
```
 Test Files  6 passed (6)
      Tests  30 passed | 2 skipped (32)
   Duration  4.05s (transform 874ms, collect 1.12s, tests 5.20s)

# Per-file unchanged from final-review §2:
# tests/server.test.ts 3 pass, tests/proxy.test.ts 3 pass +1 skipped, tests/relay.test.ts 4 pass +1 skipped,
# tests/websocket.test.ts 2 pass, tests/files.test.ts 9 pass, tests/concurrency.test.ts 9 pass
```
Both commands pass with 0 regressions; skipped tests remain 2 live-gated (`PRISM_DOC_ID && PRISM_SANDBOX_TOKEN`).

---

## 4. Non-blocking Follow-ups (not fixed, per final-review §5 S1–S7)

- **S1** `renderQueue` sequential vs sharing same in-flight render — keep as TODO, latency only
- **S2** `relayCreateStream` synthetic SSE vs raw `response.sse` from zip — mocked path correct, live needs `response.sse` parse
- **S3** WSS `response.created` vs `response.create` substring — correct per OpenAI spec, keep
- Others (hijack.ts, b64.ts, memoryFiles bound, PQUEUE env timing) — documented, no action

---

## 5. Commit

```
fix: final review blocking issues

F1: add @types/ws, tsconfig skipLibCheck, Buffer→Uint8Array cast in server.ts:158 and websocket.test.ts:54 to make tsc --noEmit 0 errors
F2: add Hono no-op handlers POST /__bridge/write and POST /__internal/write-job returning 200 so live PRISM_DOC_ID bridge fallback does not 404
Verified: bun x tsc --noEmit 0 errors, bun x vitest run 30 passed 2 skipped
```

Staged: `package.json`, `bun.lock`, `tsconfig.json`, `src/server.ts`, `tests/websocket.test.ts`, `.superpowers/sdd/fix-report.md`

---

## 6. Repro Commands

```powershell
bun x tsc --noEmit          # expect no output, exit 0
bun x vitest run --reporter=verbose  # expect 6 files 30 passed | 2 skipped
git log --oneline -3
git show --stat HEAD
```
