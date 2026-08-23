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
