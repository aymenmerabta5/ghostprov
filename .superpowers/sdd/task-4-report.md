# Task 4 Report — Responses SSE to Anthropic SSE

## Status

Implemented on `br/anthropic-claude-code-compat` with no HTTP, pull-loop, or cancellation wiring changes (reserved for Task 5).

## Implementation

- Added `AnthropicStreamTranslator.start()`, `.push(frame)`, `.finish(final)`, and `.fail(error)` as a pure bounded state machine.
- Added canonical Anthropic event types and `anthropicSseFrame(event)` formatting with named `event:`/`data:` pairs and no `[DONE]` sentinel.
- Emits `message_start`, contiguous indexed thinking/text/tool blocks, one final `message_delta`, and `message_stop` exactly once.
- Streams text and reasoning summaries, emits the bounded `ghostprov-r1.` reasoning signature immediately before thinking block stop, and preserves incremental function JSON chunk boundaries after completion/refusal safety checks.
- Recovers missing added/done events lazily and reconciles against `responseToAnthropic` using bounded counters and SHA-256 prefix fingerprints, appending only authoritative missing suffixes.
- Defers tool exposure until the authoritative final can suppress incomplete, malformed, content-filtered, or refused calls.
- Reuses Task 3 stop, usage, prewarm, refusal, terminal validation, tool-argument, and reasoning-envelope helpers.
- Stop filtering retains at most `maxStopSequenceLength - 1` ordinary-text characters, spans adjacent ordinary text/delta boundaries, resets at reasoning/tool boundaries, and drains upstream after a match.
- Unknown additive events are ignored. Failed, malformed, or upstream error events become canonical redacted Anthropic error events.
- Retained pending state is bounded by the existing output/relay/item limits; the translator never retains upstream SSE frames or the full transcript.

## Files

- `src/anthropic/stream.ts`
- `src/anthropic/stream-final.ts`
- `src/anthropic/stream-types.ts`
- `tests/anthropic-stream.test.ts`
- `README.md`

## TDD Evidence

RED:

```text
bun test --isolate tests/anthropic-stream.test.ts
0 pass, 1 fail, 1 error
Cannot find module '../src/anthropic/stream'
```

Additional regressions were each observed failing before their fixes: relay-error redaction, streamed incomplete/refused tools, malformed terminal tool JSON, aggregate pending bounds, incomplete reasoning, authoritative prefix divergence, preserved function JSON chunks, and missing terminal argument suffixes.

GREEN:

```text
bun test --isolate tests/anthropic-stream.test.ts tests/anthropic-response.test.ts
24 pass, 0 fail, 116 expect() calls

bun run typecheck
exit 0

git diff --check
exit 0
```

The requested one-time full-suite run completed with `712 pass`, `4 fail`, and `1 error`. All four failures were unrelated five-second timeout/contention failures in existing filesystem/session tests; the run also recorded a Windows `EPERM` manifest rename. Each exact failure passed immediately in isolation:

```text
FleetDecommissionService exact-digest test: pass (369 ms)
ProjectFleetManager excess-runtime drain test: pass (352 ms)
RotatingJsonlSink rotation test: pass (325 ms)
PrismSessionManager anonymous-conflict test: pass (39 ms)
```

No production process, listener, or port was inspected or changed.

## Self-Review

- Verified canonical ordering/indexing, terminal idempotence, additive-event tolerance, lazy recovery, and no duplicate fallback content.
- Verified refusal/content-filter/incomplete tool suppression before tool exposure.
- Verified malformed/divergent upstream data fails closed without payload, encrypted reasoning, or tool-argument leakage.
- Verified stop filtering across delta and adjacent-text boundaries, plus reset at non-text boundaries.
- Verified 10,000 tiny deltas do not accumulate a transcript.
- Split wire types/formatting and authoritative final projection from the state-machine module.

## Authoritative References

- OpenAI Responses streaming: <https://developers.openai.com/api/docs/guides/streaming-responses>
- OpenAI Responses create/event reference: <https://developers.openai.com/api/reference/resources/responses/methods/create>
- Anthropic Messages streaming: <https://platform.claude.com/docs/en/build-with-claude/streaming>

## Concern

The repository-wide parallel suite remains subject to unrelated Windows filesystem/timer contention as recorded above; all Task 4 tests, typecheck, diff checks, and each isolated failed full-suite case pass.

---

## Dual Shared-Context Review Remediation — 2026-08-23

### Status

All requirements in `.superpowers/sdd/task-4-findings.md` are implemented within pure stream/final helpers. No HTTP route, pull-loop, process, listener, or port code was touched.

### Fixes

1. Enforced required `sequence_number` beginning at 1 and exact increments across every event, including ignored additive events.
2. Added strict output/item/part identity and lifecycle tracking: canonical output/content/summary order, ID↔index↔type stability, alias stability, duplicate/gap/reverse rejection, no deltas after done, duplicate terminal rejection, and final identity validation.
3. Added configurable production stream limits for events, output blocks, identities, cumulative UTF-8/category bytes, and cumulative pending chunks.
4. Added numeric-only `snapshot()` history/retained counters and terminal cleanup of blocks, maps, hashes, Unicode suffixes, and pending buffers on success or failure.
5. Separated raw `reasoning_text` lifecycle from visible reasoning summaries; raw reasoning is shape/sequence validated, byte-accounted, and never emitted or reconciled as thinking.
6. Enforced canonical visible block ordering with one open block, including later-output buffering until lower output items are complete and completed-tool safety before chunk exposure.
7. Replaced UTF-16 stop buffering/slicing with a bounded Unicode-code-point filter, including split-surrogate handling and code-point stop-length validation.
8. Removed eager empty text block creation, so a one-code-point stop at offset zero terminates cleanly with no divergent block.
9. Redacted all non-validation and upstream 4xx/5xx messages while preserving only trusted local 400/413 validation text.
10. Allowed current terminal `usage:null` for ordinary responses and emit bounded estimated input/output usage; zero-token prewarm still requires authoritative cache usage.
11. Split protocol, Unicode stop filtering, limits, cumulative budget, block state, and test fixtures into focused modules.
12. Removed only the two downloaded review docs created under `.tmp`:
    - `.tmp/task4-review-openai-streaming.html`
    - `.tmp/task4-review-openai-responses-create.html`

### Strict TDD Evidence

Each defect was captured failing before its production fix. Representative RED evidence:

```text
bun test --isolate tests/anthropic-stream-protocol.test.ts
0 pass, 6 fail

bun test --isolate tests/anthropic-stream-bounds.test.ts
0 pass, 1 fail, 1 error
Export named 'DEFAULT_ANTHROPIC_STREAM_LIMITS' not found

bun test --isolate tests/anthropic-stream-safety.test.ts
3 pass, 3 fail
split astral emitted a lone surrogate; retained suffix was 2 UTF-16 units; 5xx secret leaked

bun test --isolate tests/anthropic-response-usage.test.ts -t "null usage|prewarm"
6 pass, 2 fail
usage:null rejected as malformed final
```

Additional individual RED→GREEN cycles covered conflicting function aliases, final ID/type collisions, malformed raw reasoning completion, 429 redaction, code-point stop limits, and partial/final lifecycle ordering.

### Final Verification

```text
All Anthropic focused tests:
195 pass, 0 fail

bun run typecheck
exit 0

bun test --isolate --max-concurrency=4
741 pass, 0 fail, 4335 expect() calls
119 files, 35.04s

git diff --check
exit 0
```

The full suite used `--max-concurrency=4` (down from Bun's default 20) to avoid the previously documented Windows filesystem/timer contention. The final full gate is completely green.

### Final Concerns

None.

---

## Final Lifecycle and Accounting Closure — 2026-08-24

### Status

All requirements in `.superpowers/sdd/task-4-findings-2.md` are implemented in pure stream/final helpers. No HTTP route, pull loop, process, listener, or port code changed.

### Closure Changes

1. Sequence numbers now anchor on the first nonnegative safe integer (including 0 or 1) and require exact `+1` progression thereafter.
2. Every accepted `frame.data` is UTF-8 byte-counted before JSON parsing under a new cumulative wire-byte limit, including unknown, done, terminal, failed, and error frames.
3. Category and total output accounting now counts only unseen suffixes from done events and authoritative final fallback, including final-only text, reasoning, and function JSON; snapshots expose wire/category totals.
4. Authoritative final validation now requires every observed output item to remain present with the same index, ID, and type while still allowing additional final-only items.
5. Every `push`, `finish`, or `fail` after terminal success/error is an idempotent `[]`; no post-terminal error event can be emitted.
6. `fail()` now redacts every externally supplied error message, including typed 400/413 errors, while retaining only the safe Anthropic error category.
7. Known current events validate required fields even during prewarm suppression; `response.function_call_arguments.done` requires item/output identity, name, and arguments.
8. Lone surrogate data is rejected before final/fallback text, thinking, refusal, or function JSON can be emitted.
9. The production block cap is now 4,096. Block lookup uses identity/position maps, canonical arrivals append directly, exceptional earlier insertion uses binary positioning, and lower-output readiness is tracked with a monotonic completed prefix.
10. Removed only the two docs downloaded for this review:
    - `.tmp/task4-findings2-openai-streaming.html`
    - `.tmp/task4-findings2-openai-responses-create.html`

### Strict TDD Evidence

Representative RED runs observed before implementation:

```text
Sequence anchor/idempotence: 0 pass, 2 fail
External 400/413 fail redaction: 0 pass, 1 fail
Wire/done/final accounting: 0 pass, 4 fail
Disappearing final item + prewarm shape validation: 0 pass, 2 fail
Practical production block cap: 0 pass, 1 fail
Lone-surrogate fallback: 0 pass, 1 fail
Blank canonical tool accounting: 0 pass, 1 fail
```

Every regression was rerun GREEN immediately after its focused fix.

### Final Verification

```text
All Anthropic focused tests:
202 pass, 0 fail, 1160 expect() calls

bun run typecheck
exit 0

bun test --isolate
748 pass, 0 fail, 4376 expect() calls
119 files, 33.15s

bun test --isolate --max-concurrency=4
748 pass, 0 fail, 4376 expect() calls
119 files, 45.90s

git diff --check
exit 0
```

### Final Concerns

None.
---

## Final Protocol Closure — 2026-08-24

### Status

All requirements in `.superpowers/sdd/task-4-findings-3.md` are implemented within pure stream/final protocol helpers. No routes, pull loops, processes, listeners, or ports changed.

### Protocol Fixes

1. `response.output_item.done` message text now charges only unseen suffix bytes to text and total output budgets before reconciliation/emission.
2. Authoritative final validation now requires every observed visible content part and reasoning-summary part to remain present with the same item/index/type; additional final-only parts remain valid.
3. Known lifecycle events validate documented response/error envelopes. Output-item added/done status and known message/reasoning/function/content fields remain mandatory even during prewarm suppression.
4. Streamed reasoning summaries use bounded stateful Unicode-scalar accumulators. Split surrogate pairs combine across adjacent frames, invalid/lone surrogates fail before `thinking_delta`, and retained scalar state is cleared terminally.
5. Removed only `.tmp/task4-findings3-openai-streaming.html`, the documentation file downloaded for this review.

### Strict TDD Evidence

```text
Message output-item done suffix accounting: 0 pass, 1 fail
Disappearing observed content/summary parts: 0 pass, 1 fail
Known lifecycle envelopes + prewarm required fields: 0 pass, 3 fail
Reasoning-summary split/invalid Unicode: 0 pass, 2 fail
```

Each regression was rerun GREEN immediately after its focused implementation.

### Final Verification

```text
All Anthropic focused tests:
207 pass, 0 fail

bun run typecheck
exit 0

git diff --check
exit 0
```

Per controller instruction, no broad suite was run during this closure; the controller will run the default full suite after reviewer shutdown.

### Final Concerns

None.

---

## Final Reasoning/Event-Schema Closure — 2026-08-24

### Status

Implemented all four requested reasoning and event-schema fixes in pure stream/final helpers. No route, HTTP pull-loop, cancellation, process, listener, or port code changed.

### Closure Changes

1. Reasoning-summary UTF-16 unit counters and Unicode-scalar accumulators are now retained per `summary_index`, while each thinking block keeps an aggregate unit total. Summary parts reconcile independently against text-done, output-item-done, and authoritative-final data in summary order.
2. `ResponsesStreamProtocol.validateItem` now traverses known message content and reasoning summary arrays, validating `output_text`, `refusal`, and `summary_text` members even when zero-token prewarm suppresses translation. Known output-text arrays include current required `annotations` and `logprobs` members.
3. Output-item status checks are item-kind-specific: reasoning status may be omitted, but a present status must match the added/done lifecycle; message and function-call statuses remain required and lifecycle-correct.
4. Current `error` events accept `code: null` or a nonempty string and continue rejecting missing or empty codes.
5. Removed only the official-docs download created for this closure: `.tmp/task4-reasoning-event-schema-docs-20260824.md`.

### Strict TDD Evidence

The initial focused regressions failed for the six requested behavior groups:

```text
bun test --isolate tests/anthropic-stream-protocol.test.ts tests/anthropic-stream-safety.test.ts
25 pass, 6 fail, 104 expect() calls

Failures:
- nullable error event code
- optional reasoning item status
- recursive prewarm item validation
- independent multi-part summary reconciliation
- per-summary Unicode scalar accumulation
- per-summary authoritative-final boundary reconciliation
```

The current required `output_text.logprobs` regression was also observed RED independently:

```text
bun test --isolate tests/anthropic-stream-protocol.test.ts -t "requires known output_text members"
0 pass, 1 fail, 1 expect() call
```

Focused GREEN after implementation/refactor:

```text
bun test --isolate tests/anthropic-stream-protocol.test.ts tests/anthropic-stream-safety.test.ts
32 pass, 0 fail, 117 expect() calls
```

### Final Verification

```text
All Anthropic tests:
213 pass, 0 fail, 1208 expect() calls
12 files

bun run typecheck
exit 0

git diff --check
exit 0
```

Per controller instruction, no broad suite was run during this closure.

### Self-Review

- Confirmed per-summary state retains only bounded counters and at most one pending surrogate per summary index; terminal success/error clears it.
- Confirmed aggregate reasoning byte accounting counts only unseen suffixes and does not duplicate text-done, output-item-done, or final reconciliation.
- Confirmed known nested payload validation runs before prewarm suppression and errors remain canonical/redacted.
- Confirmed message/function terminal status safety and unknown additive event tolerance remain intact.

### Final Concerns

None.
