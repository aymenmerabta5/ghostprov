# Bootstrap lifecycle hardening

## Incident evidence

The production log `ghostprov-v2.current.11552.jsonl` initially showed worker 2
in persistent `bootstrap_ownership_conflict`, and worker 7 subsequently entered
`bootstrap_cleanup_unproven`. The two workers had different recovery states.

- Worker 2 (`fb552a7f-787b-43fc-98f0-6e77c098ee78`) retained an exact registered
  generation-189 installer and matching encrypted bootstrap material, with no
  local recovery journal. Its sidecar barrier remained at generation 188.
  Retained run 21828 recorded shutdown with zero active requests and no provider
  commitment on this project; its generation-189 bootstrap was interrupted.
  After confirming that prior host had exited and the current worker had zero
  active/quarantined/ambiguous work, only those two proven stale files were
  tombstoned. The production recovery loop returned the original project to
  ready without restarting the listener.
- Worker 7 (`0d1de39d-37cd-4608-8da6-f24876b4ab5c`) retained a submitted local
  journal for generation 163, while the remote supervisor/sidecar static state
  remained at generation 162 and all expected transient launch files were
  absent. A journal-lease-protected attempt to reconnect to the exact generation
  163 supervisor timed out in the bootstrap phase. No launch was replayed, no
  journal was cleared, and no shutdown proof was fabricated. This worker is
  still unresolved. The observed mismatch alone does not prove what happened
  to the original execution.

Production was 7 ready / 1 unavailable at the final observation,
`2026-09-11T18:33:12.759Z`.
The protected production listener was not restarted, so the source changes
below are not yet loaded into that process.

## Changes and regression evidence

An offline loss-of-staged-state reproduction previously reached submission
after synchronization had replaced the staged document. It now rejects before
submission and permits the following generation to recover its proven
unsubmitted attempt.

- Recheck exact staged files after synchronization and immediately before the
  execution boundary; verify cleanup files again before discarding authority.
- Retain one bounded private recovery receipt through successful startup and
  the supervisor's complete lifetime. Persist signed cleanup proof before
  removing it; preserve it after ambiguous shutdown or failed cleanup.
- Permit only journal-authorized host recovery to authenticate a retained
  supervisor sequence, then require a fresh challenge before lifecycle work.
  Ordinary new hosts cannot adopt prior command state. No prior lifecycle
  command is replayed.
- Include fixed `bootstrap_reason` categories in sanitized recovery events and
  diagnostics, including missing receipts, file/release/generation conflicts,
  and the failed supervisor handshake phase.
- Track supervisor cleanup separately from bounded runtime stop waiters.
  Integrated live validation exposed account/session resources closing while
  supervisor cleanup was still pending. A registry bounded to
  `min(256, 2 * maxProjects + provisionConcurrency)` now fences late registration
  and joins shutdown/close before account teardown, within the existing overall
  deadline. Exact owners release their entries only after close finishes.

No frozen sidecar assets, upstream request policy, continuation contracts,
provider retry rules, fleet project-replacement rules, or traffic scheduling
were changed.

## Guarded live validation

All validation projects were separately owned disposable projects; candidate
configuration used port 18888. No provider inference requests were made.

- `.ghostprov/bootstrap-repair/proof/run-tQ0z9j/report.json`: injected a
  pre-submission synchronization failure and cleanup failure, recovered the
  durable prepared record with 2 controllers and exactly 1 submission, reached
  readiness, retained the active receipt, and completed signed supervisor
  shutdown and journal cleanup before system close returned. Project deletion
  was verified.
- `.ghostprov/bootstrap-repair/proof/retained-gowh8G/report.json`: closed the
  first host after a signed command while retaining its receipt, recovered the
  exact existing supervisor with a fresh host, obtained signed shutdown, and
  launched its successor. Exactly 2 submissions; 8 signed liveness checks over
  120 seconds; final signed shutdown, journal cleanup, and project deletion
  verified.
- Earlier integrated attempts reached readiness but exposed premature account
  teardown and failed cleanup verification. They are retained as failed proof,
  not counted as successes. One separate attempt failed account transport
  before creating a project. An initial retained-host verifier used an invalid
  empty stop-nonce list and was corrected without weakening protocol checks.

The two-minute disposable proof is not a production soak and does not establish
that the unavailable production worker has recovered. The original ambiguous
worker-7 record still requires authoritative cleanup evidence.

## Offline validation

Final isolated suite: **2,525 passed, 3 skipped, 0 failed** across 190 files
(109.52 seconds). The skips are the opt-in modeled 14-agent soak, 16 MiB
transport performance case, and 64 MiB request-integrity case.
Focused lifecycle/recovery/protocol tests and main/scripts typechecks passed.
The R8/R9/R10 deterministic release checks passed with unchanged bundle hashes.
The final full-suite result is recorded in
`.ghostprov/bootstrap-repair/current-offline-tests-verified.log`.
Pinned Deno checks and provider/Anthropic traffic probes were not rerun because
the changes are host lifecycle code; no hosted CI or publication was performed.
