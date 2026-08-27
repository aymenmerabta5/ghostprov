# V2 continuous performance decisions

This append-only log records why an experiment was kept or rejected. A broad
benchmark win is insufficient: exactness, tiny-request latency, ACK-round
distribution, concurrency, fault recovery, window limits, and the full suite
must also remain green.

## 2026-08-27 - Phase 1 - dense R2 request framing - KEEP

Candidate: `0a5658a`  
Accepted baseline: `4251fb2`

The host R2 request part increased from 11,264 to 11,900 bytes while retaining
at least 256 bytes of headroom under the 16 KiB physical frame cap at worst
metadata widths.

| Scenario | Previous p95 | Candidate p95 | Result |
|---|---:|---:|---:|
| 128 KiB x40, 50 ms RTT | 2.0954 s | 2.0955 s | stable |
| 512 KiB x40, 400 ms RTT | 25.043 s | 22.796 s | 8.97% faster |
| mixed 128/512 KiB x24 | 12.619 s | 11.986 s | 5.02% faster |
| 1 MiB x4, 1,000 ms RTT | 10.698 s | 8.196 s | 23.39% faster |
| 13 MiB x1, 400 ms RTT | 9.173 s | 8.772 s | 4.37% faster |
| 64 MiB x1, 400 ms RTT | 42.204 s | 40.202 s | 4.74% faster |

No regression gate failed; the 16/64 MiB and full 1,621-test suites passed.

## 2026-08-27 - Phase 2 - profile-aware medium concurrency - KEEP

Candidate: `06bc065`  
Accepted baseline: `a08e7a2`

R5 admits four exact requests through 2 MiB because their aggregate retained
body bytes remain below one legal 13 MiB bulk body. Requests above 2 MiB still
consume four of six units, preventing two true bulk transfers. Standard/R4
rollback retains its original conservative accounting.

| Scenario | Previous p95 | Candidate p95 | Result |
|---|---:|---:|---:|
| 2 MiB x16, 400 ms RTT | 30.702 s | 22.559 s | 26.52% faster |
| total modeled duration | 32.704 s | 24.561 s | 24.90% faster |
| 128 KiB x40 | 2.095499 s | 2.095499 s | unchanged |
| 512 KiB x40 | 22.795819 s | 22.795819 s | unchanged |
| mixed 128/512 KiB x24 | 11.986228 s | 11.986227 s | unchanged |

The first non-profile-aware attempt made the existing standard low-bandwidth
test regress to 51.4 seconds and was discarded. The corrected implementation
passed 1,624 tests plus both opt-in large gates.

## 2026-08-27 - Phase 3 - six-body prefetch with four start-risk slots - REJECT

Candidate was intentionally not committed. Baseline remains `a08e7a2`.

The broad matrix showed 17.70% faster 512 KiB x40 and 19.13% faster mixed p95,
while a separate gate kept at most four R2 `request_start` risks. Deeper tests
nevertheless proved that body prefetch changed the latency-critical scheduling
contract:

| Gate | Required | Candidate | Result |
|---|---:|---:|---|
| mixed tiny p95 delta | <=0.01 ms | 731.13 ms | reject |
| tiny ACK-round distribution | exact match | changed | reject |
| medium admission invariant | four active | five active | reject |

The entire experiment was rolled back rather than weakening any test.

## Next investigation

Target host-side O(n) request-state scans performed for every Awareness
publication. The next experiment must preserve wire bytes, flight membership,
ACK rounds, and all modeled timings exactly; only measured host CPU/wall cost
may improve.

## 2026-08-27 - Phase 3 repair - protected six-body R5 pipeline - KEEP

Candidate: `06ea865`
Accepted baseline: `6f0734f`

The rejected attempt was repaired rather than abandoned. R5 can now transfer
six medium bodies into a distinct prefetch layer, while a separate prepared
start queue holds the irreversible `request_start` boundary to four. Snapshot
and router accounting expose the two body-only reservations separately instead
of pretending they are active execution slots.

Latency-critical requests through 256 KiB always take the next pending and
frame-scheduler position. Their admission also returns body depth to four until
the last such request leaves the transport. This recovered exact mixed-load
behavior after the first repair iteration still showed a repeatable 1.65%
mixed p95 slowdown.

| Scenario | Phase 2 p95 | Repaired p95 | Result |
|---|---:|---:|---:|
| 128 KiB x40, 50 ms RTT | 2.095499 s | 2.095499 s | exact within 0.0002 ms |
| 128 KiB x24, 400 ms RTT | 10.219169 s | 10.219170 s | exact within 0.0006 ms |
| 512 KiB x1, 400 ms RTT | 1.029817 s | 1.029817 s | unchanged |
| 512 KiB x40, 400 ms RTT | 22.795819 s | 18.760867 s | 17.70% faster |
| mixed 128/512 KiB x24 | 11.986227 s | 11.986227 s | unchanged |
| 2 MiB x16, 400 ms RTT | 22.558793 s | 21.920255 s | 2.83% faster |
| 13 MiB x1, 400 ms RTT | 8.772264 s | 8.772264 s | unchanged |
| 1 MiB x4, 1,000 ms RTT commit | 8.195500 s | 8.195500 s | unchanged; total varied one RTT |

The final three-repetition comparator reported KEEP with no regressions. The
full suite passed 1,627 tests; focused fault, cancellation, recovery, exact
tiny ACK-round, standard rollback, and mixed-load tests passed. Both opt-in
16/64 MiB exact-integrity gates, reproducible R5 release check, Deno bundle
check, and real Deno journal suite also passed.

## Next investigation after Phase 3 repair

Proceed with the previously identified host CPU experiment: replace repeated
O(n) request-state scans on Awareness publication with mutation-maintained
counters. Accept it only if every wire byte, flight, ACK round, modeled timing,
and safety result remains identical while measured host wall cost improves.

## 2026-08-27 - Phase 4 - constant-time host transport accounting - KEEP

Candidate: `755def4`
Accepted baseline: `524e31b`

Committed, admitted-uncommitted, and unadmitted request totals are now updated
only at their authoritative state transitions. `snapshot()`, normal Awareness
publication metadata, and host transport diagnostics read those counters in
O(1) instead of repeatedly materializing and filtering the request map.
Recovery keeps its rare explicit execution-risk scan so no safety boundary was
approximated.

Seven warmed samples of 250,000 snapshots each produced these median costs:

| Queued requests | Previous | Candidate | Result |
|---:|---:|---:|---:|
| 0 | 194.3 ns | 127.1 ns | 34.6% faster |
| 32 | 608.3 ns | 126.7 ns | 79.2% faster |
| 128 | 1,866.9 ns | 121.4 ns | 93.5% faster |

The new 128-request scaling test failed against the prior implementation and
passes only when loaded snapshot cost stays within three times the idle cost.
The full modeled matrix remained stable with exact request/terminal proofs,
tiny ACK rounds, wire bytes, windows, and concurrency. The full suite passed
1,628 tests, and both opt-in 16/64 MiB integrity gates plus release/Deno checks
passed.

## Next investigation after Phase 4

The remaining normal-publication O(n) work is per-request diagnostic peak
fan-out, which also clones one full transport diagnostic object per live
request per publication. Investigate a shared live view that freezes one exact
copy only when a request leaves the transport; reject it if request-local
diagnostic lifetime semantics or observer isolation changes.

## 2026-08-27 - Phase 5 - lazy isolated diagnostic snapshots - KEEP

Candidate: `f893b51`
Accepted baseline: `8c85a5a`

Normal host publication now installs one versioned transport-diagnostic
snapshot. Each request lazily clones that snapshot only on observation and
freezes one isolated final copy when it exits. A request created after a prior
publication does not inherit that older sample. Recovered requests freeze the
old generation and follow the replacement only after its next publication.

Using 128 queued requests, a no-op Awareness channel, 2,000 warmups, and seven
samples of 10,000 urgent publications produced:

| Metric | Previous | Candidate | Result |
|---|---:|---:|---:|
| median host publication | 6.932 us | 1.134 us | 83.6% faster |
| eager diagnostic clones per publication | 128 | 0 | eliminated |

The regression test observed all 128 eager clones before the change and now
requires zero. Additional tests prove lazy refresh, stable identity between
publications, cross-request mutation isolation, freeze-on-exit, and exact
replacement-generation following. The modeled transport matrix remained
stable. The full suite passed 1,631 tests; the 16/64 MiB gates and release/Deno
checks passed.

## Next investigation after Phase 5

Measure the unadmitted request queue under 128-way churn. Admitted and committed
states currently remain in the same array, so every later admission and final
removal can scan or splice through unrelated live work. First try removing a
state from that queue at its admission boundary, preserving priority and
failover ordering exactly; keep it only with a material measured gain.

## 2026-08-27 - Phase 6 - unadmitted queue restructuring - REJECT

Candidate was intentionally not committed. Baseline remains `8c85a5a`.

The first attempt removed admitted states early but only moved work into array
splices. The repaired attempt used insertion-ordered sets with a separate tiny
priority subset, preserving FIFO and transfer-cost bypass behavior. Neither
attempt produced a material gain at the real bounded 128-agent workload:

| 128-agent metric | Phase 5 | Best candidate | Result |
|---|---:|---:|---:|
| measured commit p95 | 1,419.5 ms | 1,421.1 ms | no gain |
| measured total p95 | 1,664.8 ms | 1,669.2 ms | no gain |
| modeled commit p95 | 6,653.38 ms | 6,653.38 ms | exact |
| ACK rounds | 2-3 | 2-3 | exact |

At an artificial 8,192-request cancellation depth the set version improved
43.40 ms to 37.34 ms, but that exceeds the router's bounded real workload and
does not justify extra production complexity. Both attempts were removed;
Phases 1 through 5 remain intact.

## Next investigation after Phase 6

Stop speculative queue tuning. The remaining local costs are already below
network RTT at supported queue depths. The next useful evidence is a fresh
post-restart live R5 trace comparing commit, first-event, total, CPU, event-loop,
and router-capacity timeouts against these deterministic baselines. Do not
restart or touch the protected port as part of offline research.

## 2026-08-27 - false-capacity remediation below 16 agents - KEEP

Production evidence from the fixed PID 17232 snapshot showed that this was not
CPU, memory, WebSocket, or configured-agent saturation: 109 of 2,774 accepted
requests ended in `router_capacity_timeout` (3.93%), while timeout context had
only 11 active requests at p50. The median timeout nevertheless saw seven of
eight runtimes availability-zero and four capacity-zero even though most still
reported lifecycle `ready`. Requests above 2 MiB timed out at 13.03%; their
successful commit p95 was 14.325 seconds.

The remediation was kept as four separately committed phases:

| Phase | Commit | Evidence | Decision |
|---|---|---|---|
| Sanitized bottleneck telemetry and repro | `80210a2` | 3 x 2.9 MiB, 600 ms RTT, 400 KiB/s single-link baseline: 27.942 s p50, 41.913 s p95 | KEEP |
| Smooth R5 observed-size admission | `24cdacd` | >2-3 MiB costs 2 of 6 transfer units; >3 MiB remains cost 4; R4/standard unchanged | KEEP |
| Accelerated proven-stall confirmation | `54a65ac` | reliable recovery 20 s -> 12 s; suspect window 10 s -> 2 s; one sample still causes zero relaunches | KEEP |
| Progress-aware use of the fixed deadline | `b74e423` | fresh data ACK/exact prepared proof retains >2-8 MiB R5 work after 15 s; no progress retries; absolute 30 s unchanged | KEEP |

The deterministic eight-runtime observed-load result is:

| Scenario | Result |
|---|---:|
| 14 simultaneous incompressible 2.9 MiB requests | 14 exact requests and terminals |
| commit p50 / p95 / max | 25.725 / 27.234 / 27.234 s |
| modeled 30-second capacity timeouts | 0 / 14 |
| commits after the old destructive 15-second cutoff | 12 / 14 |
| ten-wave explicit soak | 140 / 140 exact, 0 timeouts, 0 retransmissions |
| mixed 128 KiB tiny p95, standard / R5 | 2,382.126 / 2,382.124 ms |
| mixed tiny delta and ACK rounds | -0.00245 ms; 2 vs 2 exact |

No-progress, stale-progress, late-observer, caller-cancellation, 25 ms proof
fence, execution-risk, standard rollback, and R5-small paths have explicit
tests. Only newly cumulatively acknowledged `request_data` frames and one exact
`request_prepared` proof count as progress; malformed and duplicate prepared
frames do not. Observer exceptions are isolated after authoritative transport
state changes.

Verification on the integrated candidate: the research comparator was stable;
the full suite passed 1,640 tests with 3 explicit large/soak opt-ins skipped and
0 failures; the ten-wave opt-in soak passed separately; the explicit 16 MiB and
64 MiB gates both passed; the frozen R5 release reproduced byte-for-byte; Deno
bundle checking passed; and the real Deno journal suite passed 12 with 3
platform/large opt-ins ignored and 0 failures.

This proves zero false-capacity failures only inside the stated deterministic
envelope. It does not claim that an external sandbox, network, OS, or provider
can never fail. The physical test also proves why that distinction matters:
seven usable 400 KiB/s runtimes keep this 14-request workload below 30 seconds,
while six produce 39.043 s p95 and four produce 51.378 s p95. Those are true
capacity shortfalls, not router bugs, and cannot be promised away without more
usable links, a longer deadline, or a different wire architecture.
