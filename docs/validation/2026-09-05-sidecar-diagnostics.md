# R10 sandbox diagnostics — 2026-09-05

R10 integrates bounded sandbox logs, authenticated log downloads, and the existing persistent supervisor. The user explicitly authorized R10 and then requested publication of the current changes before further investigation. The full eight-project recovery/concurrency gate has **not passed**; its outcomes are recorded below. Production port 18787 and the user's model configuration were not changed.

## Release identity and behavior

```text
revision: r10
protocolRevision: r2
transportProfile: bulk-v1
buildHash: ce9f48171863bdaa0adcd1a6344ec175502dd1ef9d1b9fae9e4a512a1474c32a
bundleSha256: a808969a1e89db0ec9636915a667613246c17fe5829b877aab0e8f192adc9776
bundleBytes: 600744
```

R1-R9 bundle bytes and identities remain frozen. R10 is the default for new process configuration; an explicit existing R9 setting remains an R9 selection. Both direct and LLM supervisor bootstrap modes remain available. Direct execution still uses the exact `Hiii` prompt. R10 requires the persistent-supervisor `relay` mode.

Sidecar edits must be rebuilt before live use. The R10 checker builds twice, verifies deterministic bytes and the registered identity, and refuses to overwrite a different immutable artifact. The live R10 verifiers run that check before reading credentials or creating projects. R8/R9 deterministic checks and the pinned Deno 2.9.5 checks remain part of the combined release gate.

The upstream client is an instrumentation adapter around the unchanged HTTP/SSE implementation. It preserves request normalization, terminal objects and permits, continuation IDs, and provider-error/retry behavior. The Awareness data transport is shared with the prior releases. A type-only public engine interface avoids duplicating its implementation; rebuilding confirmed identical R8, R9, and R10 bundle bytes after that refactor.

## Bounded logs and retrieval

Each execution workspace retains six files: two 128 KiB segments for the supervisor, two for the current child, and two for the previous child. Each segment holds at most 1,024 records; each record is capped at 2 KiB. The previous child's files are archived only after exact retained-child exit proof. Retained file descriptors, create-only opens, and directory/collision validation keep rotation scoped to the intended files. Logging failures do not fail model execution.

Records contain fixed phase/error categories, process IDs, numeric request ordinals, processing and journal durations, upstream header timing, first text/tool/argument readiness, event gaps, delivery durations, token counts, transport counters, and optional resource measurements. Prompts, generated text, tool arguments, headers, cookies, credentials, URLs, and client identifiers are excluded. Unavailable resource metrics remain null.

Summaries account for all events. Routine event records are sampled after the first four, then every 32nd. First text/tool, tool readiness, gaps of at least 250 ms, delivery delays of at least 50 ms, and terminal/failure records are retained. This bounds diagnostic writes without delaying delivery or discarding model output.

The authenticated endpoint is `GET /v1/diagnostics/sandboxes/<project-id>/logs/<file>`. Only the six fixed filenames are accepted. Readback uses Prism's file API, validates the exact execution session and path, decodes canonical `body_b64`, enforces byte/count bounds, and validates each complete log record. It makes no model request. Two simultaneous downloads are allowed, each charged to a 4 MiB governor reservation until delivery, cancellation, or failure. Stale runtime results fail closed.

## Passing integrated live tests

The one-project test on 18888 completed **11/11 requests**: native text, a function call and stored follow-up, Anthropic streaming, approximately 40 KiB input, four concurrent requests, and native/Anthropic requests after normal child recovery. It retained the original supervisor, recovered one child in **35.213 seconds including failure detection**, and captured an invalid-config failure through the authenticated log endpoint. The invalid configuration exists only in the verifier; the shipped sidecar has no synthetic failure hook.

That run used one direct bootstrap and zero LLM-bootstrap endpoint calls. Project and credential-copy cleanup succeeded. Its small-request first text was 1.428–3.487 seconds, with complete tool arguments at 1.582 seconds. These samples are not a matched speedup benchmark. Local evidence: `one-8w7yLm`.

A separate live control-loss test deliberately dropped one outgoing fresh challenge update and one incoming authenticated reply. Both recovered in **1,819.956 / 1,825.577 ms**, retaining the same supervisor and child. It made zero model requests, used one direct bootstrap, and cleaned up its dedicated project. Local evidence: `one-saepHn`.

The host reannounces only the same pending fresh challenge, at most three times, within the original connection and deadline. A bounded outer Awareness marker triggers the frozen peer's exact-command reply cache; the credential rotation executes once. Ordinary lifecycle commands and model requests are not repeated. Three regression tests failed before the fix and passed afterward; a real Yjs test explains why an identical state renewal alone is insufficient.

## Sustained streaming measurement

Three real `gpt-5.6-sol` requests each produced 8,416 visible characters and 1,682 text-delta events. One dedicated sandbox served native Responses HTTP, Responses WebSocket, and Anthropic HTTP, with the same model and low reasoning setting. All completed successfully; the project and new credential copy were deleted. Local evidence: `batch-on-e8Ds88`.

| Measurement | Responses HTTP | Responses WebSocket | Anthropic HTTP |
| --- | ---: | ---: | ---: |
| Client first text | 7.273 s | 5.740 s | 2.237 s |
| Sandbox upstream first text | 6.587 s | 5.201 s | 1.754 s |
| Total client duration | 14.396 s | 16.295 s | 9.762 s |
| Upstream execution duration | 13.267 s | 15.512 s | 9.243 s |
| Visible delivery bursts | 29 | 44 | 31 |
| Pause between bursts, median | 240.722 ms | 238.043 ms | 243.564 ms |
| Pause between bursts, p95 | 379.708 ms | 336.580 ms | 364.671 ms |
| Measured relay RTT | 216.239 ms | 221.769 ms | 219.927 ms |
| Host event to client, median | 1.239 ms | 1.575 ms | Not directly comparable after translation |
| Host event to client, p95 | 3.541 ms | 11.174 ms | Not directly comparable after translation |
| Cumulative sidecar event-delivery callbacks | 42.990 ms | 41.666 ms | 26.691 ms |

Visible pauses count gaps greater than 1 ms. Most token deltas arrive together, so an unconditional per-delta gap percentile near zero hides the burstiness. Host forwarding uses matching provider event sequence numbers on the same host clock. Host and sandbox wall clocks are not subtracted.

The frozen R8, R9, and R10 publication pacers have identical bytes, with SHA-256 `caf0b619fefea9840cd42bbb52d444fc51ae469b4b7392e99ac9ecc46f0f31de`. All use 25 ms default publication pacing and 25 ms response batching. No newly introduced 50 ms per-token limiter was found. The measured burst cadence is consistent with the existing ACK-clocked flight and its approximately 220 ms round trip. Most first-text waiting in these runs already occurred upstream. No matched historical R8 live latency comparison was available.

A separate short follow-up took 3.487 seconds at the client despite a 1.213-second upstream fetch-to-terminal interval. Its sandbox log showed a 1.882-second unacknowledged flight, six queued frames, and three retransmissions. This identifies relay delivery as the location of that outlier, without establishing the physical cause of the missed deliveries. No speculative streaming-timer change was made.

## Fleet attempts and remaining validation

| Integrated R10 fleet attempt | Actual outcome |
| --- | --- |
| `recovery-oEtTTF` | 24 requests completed; 15/16 forced reconnects passed. One synchronized control connection's fresh challenge timed out. This preceded the bounded challenge repair. All eight projects were deleted. |
| `recovery-yexnGt` | One startup Awareness connection never opened or synchronized before its deadline. Recovery replaced that runtime; all eight then became ready and completed 16 requests. The verifier stopped because the original-generation prerequisite for its reconnect proof was no longer met. All eight projects were deleted. |

Neither attempt passed the full eight-project child-recovery, concurrency-through-32, and final stability sequence. The latest attempt did not reach the controlled-loss or simultaneous 24-socket phases. No 130-minute natural reconnect soak started or passed. The user requested publication with these limits rather than another live run.

The earlier isolated prototype passed 9/9 profiling requests with failure readback, and a separate 15/15 request run with three normal child recoveries. Those prototype results remain useful development evidence, but are not substituted for integrated R10 fleet proof. The earlier intermittent missing-child-readiness failure remains unclassified.

## Astra model discovery and routing

The publication also adds the explicitly requested `gpt-6-astra` to both `/v1/models` catalog formats and the Anthropic adapter's native model set. Native Responses already forwarded the exact model ID. A failing regression test demonstrated that Messages previously treated Astra as unknown and used the configured fallback, even while its public response echoed the requested model. Stable and beta tool-result turns now retain Astra through upstream translation. Existing default model choices and Claude tier aliases are unchanged.

The Codex catalog advertises the [documented Astra reasoning efforts and context ceiling](https://developers.openai.com/api/docs/models/gpt-6-astra): `low`, `medium`, `high`, `xhigh`, `max`, and 1,050,000 tokens. It uses standard Responses and retains GhostProv's existing initial 272,000-token client context budget. This final catalog/adapter addition is verified offline; the earlier R10 live run's `model` field records the requested model and is not proof of Astra-specific Messages routing.

## Offline validation and scope

The full R10 diagnostics/reconnect suite passed **2,335 tests across 178 files**, zero failures, in 130.25 seconds, including capacity and 16/64 MiB opt-ins. Both TypeScript checks passed. Deterministic R8/R9/R10 and pinned Deno checks passed. Deno upstream coverage passed 1 test; journal coverage passed 13, including 64 MiB, with 2 Windows platform skips.

After the final Astra catalog/adapter addition, **147 focused tests across five files** passed with zero failures. These covered HTTP composition, stable/beta Messages, documentation, R10 release behavior, and supervisor reconnect. Both TypeScript checks and the explicit deterministic R10 check passed again. The full 2,335-test run above predates that model addition; no additional live run was started for publication.

The original and R10 clients run the same 31 upstream tests. The original and R10 engines run the same 47 engine tests, and both supervisors run the same 17 retained-child lifecycle tests. New tests cover retention across 32 child replacements, descriptor safety, strict file decoding, authenticated retrieval, memory ownership, stale sources, cancellation, deadlines, exact release identities, and missed reconnect notifications.

Upstream `stream:true` / `store:true`, exact caller continuation IDs, shared tenant scope, public terminal/provenance barriers, and Codex's rate-limit waiting behavior remain unchanged. Native request model IDs are preserved. The production log tail examined for the model question already contained `gpt-6-astra` as the requested model; no GhostProv Astra override was found. Earlier diagnostics explicitly selecting Astra were test inputs, not production configuration changes.

See [the sanitized R10 evidence](../../benchmarks/r10-diagnostics-2026-09-05.json) and [the reconnect investigation](2026-09-05-r9-reconnect.md). Raw logs and credential-bearing test setup remain outside Git in ignored local diagnostic directories.
