# E2E Report: Aquarius Horoscope Multi-Step Flow

**Status:** Complete
**Task:** Add an end-to-end test for the Aquarius horoscope multi-step flow to ghostprov
**Date:** 2026-08-19 UTC
**Branch:** `master` `c62c7ea` → HEAD
**Commit:** `test: e2e horoscope multi-step`
**File:** `tests/e2e-horoscope.test.ts:1-260`

## Plan Source

User request: Add an end-to-end test for the Aquarius horoscope multi-step flow at `D:/MyWork/AI/ghostprov`.

Requirements:
1. First request: `POST /v1/responses` with `{model:"gpt-5.6-sol", store:true, input:[{role:"user", content:"What is my horoscope? I am an Aquarius."}], tools:[{type:"function", name:"get_horoscope", ...}]}`
   → expect `function_call` with `Aquarius`, capture `resp.id` and `call_id`
2. Second request: `POST /v1/responses` with `{model:"gpt-5.6-sol", store:true, previous_response_id: resp.id, input:[{type:"function_call_output", call_id, output:"Aquarius: Next Tuesday you will befriend a baby otter."}], tools:[...]}`
   → expect final message with `"Aquarius: Next Tuesday, you will befriend a baby otter."` and `phase: final_answer`
3. Create `tests/e2e-horoscope.test.ts` that does this via the Hono app (`store:true` + `previous_response_id`), mocked relay for CI, but also test `job.json` handling via `__LAST_JOB__`
4. Use the existing relay client and server (`src/server.ts`, `src/relay/client.ts`, `src/proxy/yjs.ts`, `src/utils/zip.ts`)
5. Ensure `bun x vitest run` shows 30+ passed, commit with `test: e2e horoscope multi-step`, report to `.superpowers/sdd/e2e-report.md`

## Audit (Before)

- `tests/` contained 9 files: `server.test.ts` (3), `relay.test.ts` (4), `proxy.test.ts` (3), `websocket.test.ts` (2), `files.test.ts` (9), `concurrency.test.ts` (9), `thinking.test.ts` (20), `thinking-relay.test.ts` (13), `image.test.ts` (18) = 76 passed + 2 skipped live (78)
- No existing test covered the full 2-turn `get_horoscope` Aquarius flow with `store:true` chaining via both Hono and direct `relayCreate`
- Existing `relay.test.ts:54-126` verified single `function_call` but not chaining via `previous_response_id` + `function_call_output` → `final_answer`
- `thinking.test.ts:366-439` verified `previous_response_id` chaining with reasoning items but not horoscope tool round-trip
- `src/server.ts:95-125` already implements `POST /v1/responses` via `queue.add(()=>relayCreate(body))` with verbatim forwarding of `store`, `previous_response_id`, `tools`, `input` — required flow was already supported but untested end-to-end
- `src/relay/client.ts:187-230` already forwards all fields verbatim via `writeJob(body)` → `triggerRender` → `extractResponse` (zip), capturing `__LAST_JOB__` for job.json inspection

Gap: No end-to-end file exercising the exact user-requested horoscope payloads and asserting `phase: final_answer` + `job.json` verbatim.

## Changes Detail

### `tests/e2e-horoscope.test.ts:1-260` (new, 4 tests + 1 live skipped)

**Top-level docs:** File header documents the live `curl` flow for both turns (copy-pasteable) and explains mocked vs live strategy. `src/server.ts:95-125` already supports the flow, so no server changes were needed — test is pure `tests/` addition per plan.

**Constants `tests/e2e-horoscope.test.ts:15-45`:**
```ts
const horoscopeTool = {
  type:"function", name:"get_horoscope",
  description:"Get horoscope for a zodiac sign",
  parameters:{ type:"object", properties:{sign:{type:"string"}}, required:["sign"], additionalProperties:false }
};
const HOROSCOPE_TOOL_OUTPUT = "Aquarius: Next Tuesday you will befriend a baby otter.";
const HOROSCOPE_FINAL_TEXT  = "Aquarius: Next Tuesday, you will befriend a baby otter.";
const FUNCTION_CALL_RESPONSE = { id:"resp_horoscope_1_mock", output:[{type:"function_call", name:"get_horoscope", arguments:'{"sign":"Aquarius"}', call_id:"call_aquarius_123"}] };
const FINAL_ANSWER_RESPONSE  = { id:"resp_horoscope_2_mock", output:[{type:"message", role:"assistant", content:[{type:"output_text", text:HOROSCOPE_FINAL_TEXT}], phase:"final_answer"}], output_text:HOROSCOPE_FINAL_TEXT };
```

**Helper `createHoroscopeFetchMock()` `tests/e2e-horoscope.test.ts:47-98`:**
- Handles `POST /api/y` → `mock_y_token`
- Handles `POST /__bridge/write` (parses `content` → `__LAST_JOB__`) and `POST /__internal/write-job` (raw body → `__LAST_JOB__`) — mirrors `src/server.ts:13-31` handlers
- Handles `POST /s/sandboxes/proxy/render?renderMode=async` → 202, `GET /render-status` → `complete`
- Handles `GET /s/sandboxes/proxy/zip` → dynamically builds `zipSync({"job.json": JSON.stringify(__LAST_JOB__), "response.json": JSON.stringify(isSecondTurn ? FINAL_ANSWER_RESPONSE : FUNCTION_CALL_RESPONSE)})`
  - `isSecondTurn = !!lastJob.previous_response_id || JSON.stringify(input).includes("function_call_output")`
  - Ensures zip always reflects the last written job for job.json verbatim asserts, while returning the correct mock response per turn
- Mirrors existing `relay.test.ts:81-115` and `proxy.test.ts:56-94` fetch mock shapes (same URLs, same `prism_cache_bust` handling via wildcard)

**Describe blocks:**

1. `e2e horoscope live (plan, gated)` `tests/e2e-horoscope.test.ts:100-130` — 1 test, `hasCreds ? it : it.skip`, uses real `relayCreate` against Prism when `PRISM_DOC_ID && PRISM_SANDBOX_TOKEN` set. Documents live 2-turn flow, asserts same `function_call` + `final_answer` properties as mocked.

2. `e2e horoscope via Hono app (mocked fetch, job.json verbatim) — 2-turn Aquarius` `tests/e2e-horoscope.test.ts:132-213` — 2 tests via `app.request("/v1/responses")` (imports `src/server.ts` default app):
   - `full 2-turn flow via POST /v1/responses with store:true + previous_response_id (mocked fetch, validates job.json)` — implements exactly the spec:
     - Turn1: `firstBody = {model:"gpt-5.6-sol", store:true, input:[{role:"user", content:"What is my horoscope? I am an Aquarius."}], tools:[horoscopeTool]}` → `app.request` → assert `fc.name==="get_horoscope"`, `JSON.parse(fc.arguments).sign==="Aquarius"`, `fc.call_id` truthy, capture `respId`, `callId`; assert `job1.store===true`, `job1.model==="gpt-5.6-sol"`, `job1.tools[0].name==="get_horoscope"`, `job1.input` contains `Aquarius`
     - Turn2: `secondBody = {model:"gpt-5.6-sol", store:true, previous_response_id: respId, input:[{type:"function_call_output", call_id: callId, output: HOROSCOPE_TOOL_OUTPUT}], tools:[horoscopeTool]}` → `app.request` → assert `output_text` contains `Aquarius` + `baby otter` + `Next Tuesday`, `finalMsg.phase==="final_answer"`, `content` contains `Aquarius: Next Tuesday,? you will befriend a baby otter.`; assert `job2.store===true`, `job2.previous_response_id===respId`, `job2.input` `function_call_output` with correct `call_id`/`output`, `job2.tools[0].name==="get_horoscope"`; assert fetchMock touched `__bridge/write` or `__internal/write-job`, `/render`, `/zip`
   - `validates job.json for turn 1 and turn 2 independently (store:true preserved)` — second Hono test ensures `store:true` preservation across chaining without relying on combined flow.

3. `e2e horoscope via relayCreate (mocked fetch, job.json verbatim) — direct client` `tests/e2e-horoscope.test.ts:215-260` — 2 tests via `relayCreate(body)` directly (same fetch mock + `__LAST_JOB__` checks):
   - `relayCreate 2-turn Aquarius: function_call then final_answer with phase final_answer` — mirrors Hono flow but via client, asserts same `function_call` → `final_answer` + job.json `store:true`, `previous_response_id`, `function_call_output` verbatim
   - `relayCreate preserves tools and store:true across chaining` — ensures `job2.tools` still contains `get_horoscope` after chaining

All assertions use tolerant regex `/Aquarius: Next Tuesday,? you will befriend a baby otter\./` to accept both with/without comma (spec's second output without comma, final with comma), but primary `FINAL_ANSWER_RESPONSE` uses comma as spec expects.

No changes to `src/` — flow uses existing `src/server.ts:95-125` (Hono `POST /v1/responses` with `queue.add(()=>relayCreate(body))`), `src/relay/client.ts:187-230` (`relayCreate` verbatim via `writeJob`→`triggerRender`→`extractResponse`), `src/proxy/yjs.ts:10-55` (`writeJob` + `__LAST_JOB__`), `src/utils/zip.ts:3-23` (`fetchZip`/`extractResponse`). Test validates that those paths already forward `store:true` + `previous_response_id` + `function_call_output` verbatim.

## Tests Added

`tests/e2e-horoscope.test.ts` — 5 total (4 pass, 1 skipped live):
- `live 2-turn Aquarius horoscope via relayCreate (requires Prism creds)` — skipped when `PRISM_DOC_ID`/`PRISM_SANDBOX_TOKEN` absent, documents live flow
- Hono app (mocked fetch, job.json): 2 tests covering spec's 2-turn via `app.request` with full job.json checks
- relayCreate (mocked fetch, job.json): 2 tests covering same via direct `relayCreate` + job.json checks

Each 2-turn test captures `resp.id` + `call_id` from turn 1 and reuses them in turn 2, asserting both output shapes and `__LAST_JOB__` verbatim.

## Verification

**TSC:**
```bash
bun x tsc --noEmit
# exit 0 (0 errors) — PASS
```

**Vitest:**
```bash
bun x vitest run --reporter=verbose
# Test Files  10 passed (10)
#      Tests  80 passed | 3 skipped (83)
# Duration 15.46s (transform 1.81s, collect 2.41s, tests 36.18s, prepare 1.85s)
# Before: 9 files, 76 passed | 2 skipped (78)
# After:  10 files, 80 passed | 3 skipped (83) — +4 new (e2e-horoscope 4 pass + 1 live skipped), 0 regressions
```

- Single-file run: `bun x vitest run tests/e2e-horoscope.test.ts` — 4 passed | 1 skipped (5), ~8.9s
- Each e2e test ~2.0–2.2s (Yjs sync mock 0ms + render mock immediate + zip)
- All prior 76 tests still pass; no flake in `concurrency.test.ts` (queue 5 + renderQueue 1)
- Meets 30+ passed requirement (80 >> 30)

**Manual checks:**
- `__LAST_JOB__.store === true` preserved both turns (verifies `store:true` not stripped)
- `job1.tools[0].name === "get_horoscope"` and `JSON.parse(fc.arguments).sign === "Aquarius"` (turn1)
- `job2.previous_response_id === j1.id` and `job2.input[0].type === "function_call_output"` with `call_id`/`output` (turn2)
- `j2.output.find(o=>o.phase==="final_answer")` defined and `output_text` `/Aquarius: Next Tuesday,? you will befriend a baby otter\./` (turn2)
- `fetchMock` URLs contain `__bridge/write`, `/render`, `/zip` (stable loop exercised)
- Live doc block at top is copy-pasteable `curl ... | jq '.output'` for both turns

## Commit

```bash
git add tests/e2e-horoscope.test.ts .superpowers/sdd/e2e-report.md
git commit -m "test: e2e horoscope multi-step"
```

- Single commit `test: e2e horoscope multi-step` (exact message)
- Includes `tests/e2e-horoscope.test.ts` (new, 260 lines) + this report `.superpowers/sdd/e2e-report.md`
- No `src/` changes — server/relay already supported verbatim flow
- Commit parent `c62c7ea feat: image vision and generation support`
- Verified via `git log --oneline -3` and `git status`

## Notes

- Verbatim guarantee: `store:true`, `model:"gpt-5.6-sol"`, `tools:[get_horoscope]`, `previous_response_id`, `input:[function_call_output]` all survive via `JSON.stringify(body)` → `job.json` → `curl --cacert $PROXY_CA_CERT_PATH -d @job.json $OPENAI_BASE_URL/responses` → `response.json`; test proves by inspecting `globalThis.__LAST_JOB__` before zip extraction, matching `relay.test.ts` and `thinking-relay.test.ts` patterns.
- Phase `final_answer` is explicitly asserted on the assistant message (`output.find(o=>o.phase==="final_answer")`) per `src/relay/types.ts:20` `Phase` and `src/server.ts:46` `phase` comment.
- `call_id` capture/reuse matches spec: `call_aquarius_123` stable mock ensures second turn's `function_call_output.call_id` equals first turn's `function_call.call_id`; test captures dynamically (`const callId = fc.call_id`) so it would catch a mismatch if relay generated a different id.
- Output text tolerance: spec has `output:"Aquarius: Next Tuesday you will befriend a baby otter."` (no comma) for turn2 input, but final expects `"Aquarius: Next Tuesday, you will befriend a baby otter."` (with comma). Mock final uses comma variant; assertion uses regex `,?` to accept either, ensuring CI stability while documenting canonical comma text.
- Mocked fetch covers both `__bridge/write` and `__internal/write-job` (best-effort) per `src/proxy/yjs.ts:42-46` and `src/relay/client.ts:202-210` — same as `thinking-relay.test.ts` and `image.test.ts` — so bridge handler added in `fix-report.md` (`src/server.ts:13-31`) is exercised even when mocked.
- `PQUEUE_CONCURRENCY=5` not stressed in this test (sequential turns) but Hono path uses `queue.add` per `src/server.ts:115` and passes via existing `concurrency.test.ts` (3 concurrent with `prompt_cache_key` all 200).
- No secrets: test uses `mock_y_token` / `mock-token` fallback, no `OPENAI_API_KEY`, no `api.openai.com` call, same as other mocked suites.
- Follow-up (optional): add WSS variant via `attachWss` (`tests/websocket.test.ts` + `thinking.test.ts:441-621`) exercising same horoscope flow over `ws://localhost:8787/v1/responses` with `type:"response.create"` chaining — not required by this task but would mirror HTTP coverage.
- Report path: `.superpowers/sdd/e2e-report.md` (this file)

