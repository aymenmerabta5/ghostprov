# Direct supervisor bootstrap and tool latency validation

Prism direct execution starts the existing R9 supervisor without an LLM bootstrap. A separate, bounded change to completion receipts removes a measured host scheduling wait from immediate tool-result follow-ups. The original LLM bootstrap remains selectable.

These results were collected on 2026-09-05 with Bun 1.4.0 on Windows, the pinned Deno 2.9.5 checks, `gpt-5.6-sol`, dedicated Prism projects, and candidate port `18888`. The existing service on `18787` was not probed, restarted, reconfigured, or replaced.

[Portable measurements](../../benchmarks/prism-direct-exec-2026-09-05.json) include per-request timing summaries, numeric token counts, comparison medians, successful and failed fleet outcomes, and hashes of the original local reports. Complete event timelines remain in the local reports produced by the verifier; credentials and local session artifacts are excluded from Git.

## Behavior and controls

`GP_V2_SUPERVISOR_BOOTSTRAP_MODE=direct` is the default. It makes one authenticated `codex/codex_exec_async` submission with `codex_path: "/bin/sh"`, the exact prompt `Hiii`, and the service-required model label `gpt-5.6-sol`. The shell executes the staged installer. The model label is request metadata; there is no model inference in this bootstrap path. Ordinary user requests still invoke the requested model.

The host seals bootstrap credentials to a private execution-session identity. It validates the returned session, workspace, execution mode, model label, and bounded job identity, then waits for signed supervisor readiness. Only afterward does it remove its temporary `exec` file, installer, and encrypted credential. A colliding user `exec` file causes a closed failure and is preserved. Ambiguous submission never triggers automatic resubmission or LLM fallback.

`GP_V2_SUPERVISOR_BOOTSTRAP_MODE=llm` retains the original two-turn Codex bootstrap. Its original implementation and clients are preserved. Both modes use the same immutable R9 supervisor, retained child process, authenticated lifecycle commands, and exact identity checks. Mode selection applies at process startup.

`GP_V2_TERMINAL_RECEIPT_COALESCING=1` is also the default. It holds an otherwise standalone, verified completion receipt for at most 10 ms so an immediate next request can share its immutable reliable flight. `0` restores eager receipts. It uses the existing bounded receipt registry and one abortable wait. Queued work and draining flush it; close and recovery cancel the wait. Cumulative receive ACKs, upstream execution fences, final transcript proof, provenance, and continuation commit barriers keep their existing semantics.

## Bootstrap measurements

The comparison uses one clean, successful single-project run for each mode. The first failed LLM trial is excluded from the speed calculation and documented below.

| Measurement | Original LLM mode | Direct mode | Observed reduction |
| --- | ---: | ---: | ---: |
| Supervisor bootstrap | 20,284 ms | 2,881 ms | 85.8% |
| Complete one-project startup | 36,415 ms | 17,163 ms | 52.9% |
| LLM bootstrap starts | 2 | 0 | Two model starts removed |

The clean LLM run passed 10 requests and one recovery within the original supervisor. The direct single-project run passed 39 requests, one recovery, and bounded concurrency bursts. Both deleted their dedicated projects.

In the final eight-project run, the individual supervisor bootstrap times ranged from 2,534 to 2,887 ms. Full fleet startup took 66,753 ms with the existing bounded cold-start concurrency. Single-project and complete fleet startup measure different work and should not be compared as equivalent operations.

## Tool-result follow-ups

Both comparison variants used direct bootstrap, event batching enabled, low reasoning effort, identical tool schemas, and real provider inference. Each variant made 27 requests across HTTP Responses, WebSocket Responses, and Anthropic Messages: three text requests plus twelve tool requests and their twelve follow-ups. Each surface had two repetitions with 0/1,024-character argument payloads and 128-byte/32-KiB local file slices as tool results.

The table starts timing when the client submits the tool result. It measures first resumed text, not initial tool emission.

| Surface | Follow-ups per variant | First text, eager receipts | First text, coalesced receipts | Reduction | Host scheduling, eager | Host scheduling, coalesced |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTP Responses | 4 | 1,540.0 ms | 1,472.8 ms | 4.4% | 195.85 ms | 0.16 ms |
| WebSocket Responses | 4 | 1,707.4 ms | 1,584.6 ms | 7.2% | 215.26 ms | 0.17 ms |
| Anthropic Messages | 4 | 1,512.0 ms | 1,344.4 ms | 11.1% | 207.12 ms | 0.29 ms |
| All surfaces | 12 | 1,570.7 ms | 1,403.6 ms | 10.6% | 198.75 ms | 0.16 ms |

Across those twelve follow-ups per variant, median completion changed from 1,873.3 to 1,744.2 ms, a 6.9% reduction. The directly attributable change is removal of roughly one 200 ms scheduling wait for eligible immediate follow-ups. Inference and cache state vary between runs; some individual requests and surface-specific completion metrics did not improve. These samples do not establish statistical significance or a universal latency reduction.

Native relay-to-client forwarding was already typically a few milliseconds. Anthropic tool blocks remain gated on authoritative terminal/refusal validation. Disabling event batching did not give consistent gains and was not adopted.

The verifier records relative timestamps for every observed relay/client event, event counts, text/argument character counts, reported numeric token usage, provider timing diagnostics, and delivery pauses. Stream deltas can contain multiple tokens; these observations are not internal per-token generation timestamps. No prompts, generated text, tool arguments, provider response IDs, client IDs, or credentials are retained in the trace report.

## Reliability and concurrency

The final extended run completed **151/151 requests and 24/24 recoveries within the original supervisors**: three successive child recoveries on each of eight sandboxes. It made eight direct bootstrap submissions and zero LLM bootstrap endpoint calls. It recorded no recovery failures or retired supervisor generations and deleted all eight projects.

| Concurrent client burst | Completed | Whole burst | p50 | p95 | Requests/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1,672 ms | 1,672 ms | 1,672 ms | 0.598 |
| 8 | 8 | 2,589 ms | 1,925 ms | 2,589 ms | 3.090 |
| 16 | 16 | 2,500 ms | 2,044 ms | 2,500 ms | 6.400 |
| 32 | 32 | 2,769 ms | 2,123 ms | 2,734 ms | 11.555 |

The earlier direct/eager-receipt fleet completed its 32-request burst in 2,728 ms at 11.729 requests/s; an independent coalesced-receipt repeat took 2,740 ms at 11.680 requests/s. These runs show similar burst throughput, not a measured throughput gain. Concurrency refers to submitted client requests, not unrestricted simultaneous upstream execution. The existing fleet, admission, memory, and per-runtime ceilings remain authoritative.

Additional checks passed:

- The instrumented eight-project repeat completed 115/115 requests and 8/8 recoveries within the original supervisors, with no captured recovery failures or replacements.
- A controlled fault suppressed four completion-receipt publications before stopping the sidecar. The same supervisor recovered its child, continuation completed, and no provider request was re-executed.
- A deterministic recovery test carried a still-unpublished deferred receipt across suspension/adoption and verified cancellation of the old timer without another `request_start`.
- A full host restart in one project completed four provider requests, two cold starts, and a persisted continuation across the restart.
- The shipped trace command completed all 27 requests across the three client surfaces with one direct bootstrap, zero LLM bootstrap attempts, and confirmed project deletion.
- The trace verifier records its dedicated project before attempting creation, attempts exact-identity cleanup after an ambiguous create, and fails cleanup unless deletion is confirmed. Offline tests cover foreign identities, missing projects, failed lookups, and unconfirmed deletion.

## Failed attempts and limits

An earlier optimized eight-sandbox run failed the strict recovery gate when one worker entered a new supervisor generation. Its original coarse diagnostics did not capture the underlying cause. That failure is not conclusively attributed to Prism connectivity or to receipt coalescing, and it is not claimed as fixed by a known root-cause patch. Recovery timeouts and correctness assertions were not weakened. Bounded phase/cause instrumentation was added, followed by the passing lost-receipt fault test, the 115-request repeat, and the three-cycle 151-request run. The failed outcome is retained as `earlierFailedCoalescedFleet` in the portable measurements.

The first LLM comparison also failed its strict generation check after a startup retry. Its inflated 179,661 ms full-startup duration is excluded from the speed table. Only the fresh passing LLM comparison is used there; the failed outcome remains in the measurements as `earlierFailedLlmSingle`.

These are finite lab canaries with short synthetic prompts and controlled local file tools. They validate the current observed Prism execution interface and the tested failure boundaries. They do not guarantee arbitrary external-tool latency, future Prism compatibility, or indefinite fleet reliability.

## Reproduction and offline validation

Run only with authorization for dedicated live projects and provider calls. The verifiers read credentials through the existing configured cookie-file path, copy them into an isolated directory, bind `18888`, and remove their copies during cleanup.

```powershell
$env:GP_LIVE_TOOL_LATENCY_TRACE = "1"
$env:GP_LIVE_ALLOW_TEST_PROJECT_MUTATION = "1"
$env:PORT = "18888"
$env:GP_TRACE_BATCHING = "1"
$env:GP_V2_TERMINAL_RECEIPT_COALESCING = "1" # use 0 for the eager-receipt comparison
bun run verify:tool-latency
```

```powershell
$env:GP_LIVE_R9_SUPERVISOR_RECOVERY = "1"
$env:GP_LIVE_ALLOW_TEST_PROJECT_MUTATION = "1"
$env:GP_LIVE_PROJECT_COUNT = "8"
$env:GP_LIVE_MODEL = "gpt-5.6-sol"
$env:PORT = "18888"
$env:GP_LIVE_TIMEOUT_MS = "300000"
$env:GP_V2_SUPERVISOR_BOOTSTRAP_MODE = "direct"
$env:GP_V2_TERMINAL_RECEIPT_COALESCING = "1"
$env:GP_LIVE_BENCHMARK_CONCURRENCY = "1"
$env:GP_LIVE_RECOVERY_CYCLES = "3"
bun scripts/live_r9_supervisor_recovery.ts
```

The final offline suite includes the capacity soak, 16-MiB performance, and exact 64-MiB integrity opt-ins. Runtime and script TypeScript checks, deterministic R8/R9 rebuilds, pinned Deno checks, upstream retry checks, and the explicit Deno 64-MiB spool-boundary check are part of release validation. Two POSIX-only Deno tests cannot execute on this Windows host.

```powershell
$env:GP_RUN_V2_CAPACITY_SOAK = "1"
$env:GP_V2_PERF_LARGE = "1"
$env:GP_V2_PERF_64M = "1"
bun --no-env-file test --isolate
bun run typecheck
bun run typecheck:scripts
$env:GP_DENO_LARGE = "1"
bun run check:v2-sidecar
```

Final offline replay: **2,196 passed, 0 failed across 173 files** in 146.16 seconds, with all three opt-ins enabled. Runtime and script TypeScript checks passed. The sidecar check with `GP_DENO_LARGE=1` passed the deterministic R8/R9 identity checks, both pinned Deno bundle checks, the upstream retry test, and 13 journal tests including the 64-MiB boundary; the two POSIX-only journal tests were ignored on Windows.

All nine frozen R1-R9 bundles retain their original hashes. R9 SHA-256 remains `032ce82a6104ab513c8efa7a8dd9a7d67fefb8ca41ee73cf47c9f4399d90012d`. The original LLM implementation, upstream `stream:true`/`store:true` normalization, explicit continuation IDs, shared authenticated scope, terminal barriers, and no-replay policy are preserved.
