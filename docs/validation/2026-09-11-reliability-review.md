# Reliability review: startup, transport, and recovery

Status: implementation and isolated validation passed. The existing 18787 process remains on its old code; a normal user-controlled restart is required to activate the fixes and prepared legacy migration.

## Scope and operating boundary

The user authorized a full reliability review, fixes, and experiments. The existing production listener on port 18787 was not stopped, restarted, rebound, or taken over. Candidate listeners used 18888; live experiments used disposable projects and copied private authentication state. No commit or push was requested or performed.

The starting checkout was `master` at `8ddf253ad2dd53de2e0e19a20a3c3e7e08fba39d` with pre-existing recovery changes. Their baseline diff was saved under `.ghostprov/reliability-review-20260911/baseline.patch` before new edits.

## Incident evidence

The restarted process 30504 had six healthy workers while workers 8 and 9 repeatedly failed with `bootstrap_cleanup_unproven`, reason `bootstrap`. This phase means the supervisor transport synchronized, but the host did not accept the exact prior supervisor's authenticated status. It does not establish a general internet outage. The complete-line snapshot at 21:46:23.245Z contained 280 events, 25 accepted requests, 24 completions, and one committed cancellation.

The preceding process 27056 also had sidecar probe failures while direct sandbox heartbeats remained fresh. Workers 3 and 4 entered recovery and later returned; the fleet changed from seven to five to seven healthy workers. These events identify the failing sidecar path, but logs alone do not establish the precise cause of every outage.

Guarded diagnostic executions later confirmed that both blocked journals refer to absent old execution workspaces. Original journals were unchanged and diagnostic launchers were restored. Evidence: `.ghostprov/reliability-review-20260911/existing-retirement-9HT8yX/report.json` (worker 8) and `existing-retirement-q9qoGo/report.json` (worker 9).

## Reproduced defects and changes

- A caller cancellation immediately after signed remote shutdown could prevent durable local `cleanup_proven` recording. The journal would continue to describe a submitted supervisor that had stopped. Local proof persistence now has independent bounded ownership.
- An exact retained authenticated shutdown was excluded from reconnect and could not be used as cleanup proof. Journal-authorized terminal recovery is now separate from live readiness and never submits another shutdown or launch.
- Once the supervisor exits and clears ephemeral Awareness, its terminal can disappear before the host saves cleanup proof. A disposable live reproduction confirmed this failure. The updated R10 publishes one bounded signed retirement receipt after child cleanup, peer closure, exact PID retirement, and ownership release; a separate connection verifies persistence. A publication lease prevents older publishers from overwriting successors.
- An ordinary sidecar control update overwrote the only pong. A deterministic real host/peer reproduction timed out while connected and with a fresh sidecar peer. The R10 peer retains one exact challenge across later control publications, and ping-bearing control updates continue ACK/credit processing.
- An idle sidecar might produce no later control update after an isolated lost pong. Host probes now make at most two additional frame-free publications of the same challenge inside the original five-second deadline. Late pongs cannot bypass that deadline. Provider requests are never replayed by this mechanism.
- Runtime monitor and reconnect could initiate competing probes. They now share the exact transport's active probe. The fixed recovery deadline also has an independent watchdog, and late credential rotations cannot reconnect Awareness after shutdown.
- A pending worker startup could prevent other workers' recovery events from reaching the serialized fleet reconciler. Startup ownership, recovery-event buffering, and provisioning permits are bounded and decoupled from that wait without replaying startup.
- A transient manifest read failure could silently drop a successful-recovery reset and falsely combine failures from separate episodes. Evidence gaps now invalidate replacement qualification. Repeated waits on one pending runtime stop also share one bounded observation.
- Legacy live inspection found the exact original supervisor still in sleeping process state after its retirement marker and released ownership were verified. This establishes delayed process exit, not an indefinite timer leak. Updated R10 entrypoints exit their own process only after full successful cleanup; failed child cleanup retains the exact resource and ownership for bounded, single-owner retries.

An immutable private migration ledger supports at most two exact legacy project IDs. It binds account, managed worker/index, archived R10 identity, sealed authority, and journal digests. Busy or stale grants do not block ordinary reconciliation. The ordinary hold budget remains four; the two granted IDs can use two additional slots, with a hard total of six. Consumed grants never transfer to successors. All healthy-majority, outage-circuit, no-active/quarantined-work, and continuation-preservation gates remain.

The original R1-R9 assets and original R10 artifact are preserved. The intermediate c109 build is also an immutable cleanup-only archive. The selected content-addressed R10 build is `9d2aa8b7aecefcad429dff4a1f7fb0707b95dec9abe0beac4fb6d77ad70e5b7b`, schema `51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f`, bundle SHA-256 `21c83c16d210b3208a1cb097df2363f4690fac705ae6d1ccc71e143805854121`, 612077 bytes. Prior cleanup resolves only exact registered identities.

Request-path review and tests confirmed `stream:true`/`store:true`, exact explicit continuation IDs, one composition-owned tenant scope, bounded ownership/backpressure, and authoritative terminal/provenance/index ordering. No upstream WebSocket, socket affinity, Conversations API, or local transcript cache was introduced.

## Final validation

| Gate | Result |
| --- | --- |
| Full isolated suite | 2706 passed, 3 conditional stress tests skipped, 0 failed; 196 files |
| Explicit stress suite | 24 passed, including all three conditional tests: ten 14-agent waves and exact 16/64 MiB integrity |
| Main and scripts typechecks | Passed |
| R8/R9/R10 deterministic and pinned Deno 2.9.5 R10 checks | Passed |
| Frozen prior artifacts and final diff checks | Passed |
| Lost local shutdown commit, then normal restart | Passed: three submissions, three authenticated shutdowns, independent receipt validation, project deleted |
| Current R10 process retirement preflight | Passed: exact authority checks and two process scans, project deleted |
| Eight-worker mixed workload/outage/stability run | Passed: 34/34 correlated unique execution identities and provider terminals |

Primary receipts:

- `.ghostprov/reliability-review-20260911/final-validation.log`.
- `.ghostprov/reliability-review-20260911/retired-repro-UK4l28/report.json`.
- `.ghostprov/reliability-review-20260911/retirement-preflight-TDGy6K/report.json`.
- `.ghostprov/live-outage-recovery/proof-4vpMDF/report.json`.

The final live run included eight valid input requests from 1 KiB through 256 KiB, dropped initial pongs, eight in-flight requests during each 30/60/90-second loss of data/control/heartbeat delivery, public Responses and Anthropic Messages, and 120 one-second stability samples at 8/8 afterward. All 34 requests completed; every supervisor and sidecar remained its original instance through the faults, and each request had one observed `request_start` publication. Cleanup had eight authenticated supervisor shutdowns, zero bootstrap journals, and eight confirmed project deletions.

These are bounded experiments, not an indefinite-uptime claim. Faults intentionally removed usable capacity while delivery was blocked. The request ledger proves observed host execution identities and correlated provider terminals, not independently instrumented internal provider fetch counts.

Earlier failed experiments remain recorded: the original missing-proof reproduction, an intermediate publication deadlock caught with the real lease implementation, transient startup/synchronization failures, the initial lost-pong test, and a missing Anthropic version header in the verifier. The header was corrected in the verifier; protocol validation was not weakened. Every disposable project created during this task was confirmed deleted.

## Prepared activation

`C:/Users/Aymen/.ghostprov/fresh-v2/fleet.json.bootstrap-migration.json` was prepared and read back with exactly workers 8 and 9, two grants, and 901 bytes. SHA-256: `7d6d922ab1e66696ae4470e878791c2b3b3781c9ee4bb84fa4d53668244b3ed6`. The existing manifest still had its original twelve records and four holds after preparation; no production fleet mutation was performed.

Let the current router complete a normal graceful shutdown, then run the usual `bun dev`. The updated process will load the fixed R10 build and exact-ID migration ledger. It may create and verify successors for those two legacy workers only when existing safety gates permit, retaining old projects and journals. The migration fixture restores 6 ready/4 held/2 blocked to 8 ready using exactly two successors, without deleting or retiring old continuation state, and remains idempotent across restart.

Hosted CI and a production restart were not performed. No commit or push was performed.
