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
