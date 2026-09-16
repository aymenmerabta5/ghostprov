# Silent-lane admission recovery — 15 September 2026

## Incident

PID26728 loaded the repaired R10 build `fe8bbf0d...`, but still produced a real transport-admission503 under load. The frozen log contains3,901 complete records from19:30:41.570 through19:54:29.846UTC. Its SHA256 is `a029a0dc27c2452daf540141b6fd793c3c7c542463313434bef328ed31921c8a`.

Trace `30ee2a03-e9d1-4452-85f1-1b32fee442d7` was a901,823-byte root with191 items and no previous provider ID. Memory admission took0.861ms. The configured60-second budget produced two30-second attempts; both reached368,900/453,720 compressed bytes, exactly31×11,900-byte parts after request_open. The remaining approximately115KB framed tail did not finish before the deadline.

Six outer runtimes remained ready, but three five-second samples reported zero routable workers. A21KB explicit-ID continuation also took75.343seconds tocommit. This was not explained by repeatedly sending a full history or exhausting host memory. Some response queues were pressured, while other flights stalled with little queued output. The log does not conclusively distinguish missed delivery from blocked processing or return ACKs.

Worker8 later reported a control-channel1006 and recovered through an existing-credential reconnect in2.787seconds. Ordinary connect was a no-op while the provider's local connected flag remained true. That exposed a concrete recovery gap for silent lanes.

## Corrections

- Shared system/router admission default is now **180seconds**, retaining60-second automatic attempts, finite payload scaling, explicit overrides and the original absolute deadline. The local `GP_ROUTER_QUEUE_TIMEOUT_MS` setting was changed60000→180000 for the next launch, with all other file bytes verified unchanged.
- Exact-instance control-probe timeout or authoritative retained data-flight stall can request an explicit repair of only the affected Awareness lane.
- Repair reuses the provider, credential, document, client identity, immutable reliable frames, pending requests and provider lineage. It owns a bounded10-second window inside existing recovery deadlines.
- Availability requires a fresh exact-instance pong. Data recovery additionally waits for a validated ACK to remove an original retained frame; control pongs alone cannot certify data progress.
- Publication during a delayed reconnect is recoverable and retains pending state. The installed provider's retired socket open/message/error/close callbacks cannot alter or falsely synchronize its replacement.
- Actual physical reconnect promises remain part of local-cleanup ownership even when an abortable public waiter has ended.
- Authentication failures, newer failure versions and failed bounded repair retain the established credential-rotation/recovery fallback.
- Diagnostics now distinguish data/control receipt ages and validated revisions. Missing/older fields remain null.
- Held cleanup started/succeeded/deferred events now reach production logging through an exhaustive mapper and strict schemas. Deferred reasons use a fixed enum.

The successful repair does not remint credentials, replace the child, clear a reliable window, or re-execute provider work.

## Provider continuation

`store:true` and `previous_response_id` remain the provider continuation contract. A new turn is not tied to the sandbox that completed its parent.

A real router regression completes the901KB root on A, then sends the unchanged21KB one-item delta and exact provider ID to B while A remains busy, transfer-constrained, recovering or under Awareness repair. Parent provenance remains A; the new response records B. Each request executes once. An already executing request still retains its original ownership until terminal/cancellation proof.

## Before/after proof

The same actual-component fixtures fail on the retained pre-change source and pass on the candidate:

- Baseline: **0 passed /3 failed**.
- Candidate: **3 passed /0 failed**,76 assertions.

The fixtures use real V2SandboxRuntime, V2TransportSession, YSweet/Split channels and the mutable R10 peer/engine. Provisioning, bootstrap, heartbeat and network are synthetic. They retain the901,823-byte root at its first-flight boundary, an already-committed256-event response stream with1KiB deltas, and a21,052-byte explicit-ID delta.

Automatic control and data repairs each finish all three requests with exactly three simulated upstream executions, one provision/bootstrap, no credential remint or child replacement, and no reconnect of the healthy lane. A100ms handshake delay exceeds the test publication/RTO intervals and still preserves the flight. The automatic tests invoke the real runtime trigger; they do not manually call reconnect.

Short test thresholds make these composition cases fast. Separate manual-clock tests cover normal30-second challenge timeout and the exact10-second repair budget.

The exact-size router fixture reproduces the old60-second503. At180seconds it preserves the same partially uploaded request through modeled repair and fresh progress until completion at76seconds. Permanently stalled work still ends at exactly180seconds; caller cancellation remains immediate, and execution-risk work is never rerouted.

Independent review reproduced and then verified stale errors and stale Y-sync packets using the installed WebsocketProvider, plus native ws event-handler accessors without network. Current-socket errors still propagate. Delayed abort-ignoring reconnects retain the cleanup witness until their actual promise settles.

## Validation

- Main and scripts TypeScript checks passed.
- R8/R9/R10 deterministic and pinned Deno checks passed; no sidecar source/wire changes or new R10 identity were needed.
- Router/provenance, actual runtime/transport composition, strict logging and independent recovery reviews passed.
- Final full isolated suite with the14-agent soak and16/64MiB flags enabled: **3,105 passed, zero failed, zero skipped**,153,267 assertions across210files in156.41seconds. The40-agent and mixed-context R10 cases also ran.
- All536captured executable/source/configuration files matched their pre-run SHA256; all15prior release artifacts remained byte-identical. Final `git diff --check` passed.

The first full run passed3,103tests and found one positive logging fixture supplying legacy replacement fields to new cleanup events. The fixture now uses the correct exact fields, preserving strict rejection and all older coverage. No runtime validation or test timeout was loosened.

Evidence is under `E:/Temp/ghostprov-admission-recovery-26728-20260915`: router-budget-evidence.txt, silent-lane-recovery-results.md, baseline/candidate-silent-lane-regression.txt, independent socket/witness regressions, and full-suite logs. Source log analysis is under `E:/Temp/ghostprov-log-review-26728-20260915`.

## Operating boundary

Port18787 was not stopped, restarted, probed or taken over. No live Prism/provider calls, project mutations, live credential use, commit or push were performed. The only local private configuration edit was the explicitly requested non-secret admission setting; credentials and other values were not exposed or altered.

These results establish zero admission failures in the bounded repaired fixtures, not a claim of zero failures for every possible live outage. A future process must load the changes and report180000ms at startup. Live verification of the same workload remains necessary before declaring the production symptom eliminated. Permanent outages, client cancellation and missing authoritative proof still respect bounded failure and no-replay safeguards.
