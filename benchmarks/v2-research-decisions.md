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
