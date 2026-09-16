# Shutdown cleanup incident, 2026-09-13

## Observed failure

The saved agent command ran `bun run shutdown:pc` correctly and returned exit code
1 at 02:34:48 UTC. GhostProv reported `Fleet shutdown was incomplete`, and the
command correctly left Windows running.

The five retained PID-34356 log prefixes contain 40,131 structured events across
275,290,855 bytes. They establish the following sequence:

- Worker 6 lost heartbeat at 01:13:03 UTC and subsequently failed 33 recovery
  attempts with `bootstrap_cleanup_unproven`, in the `deploy`/`bootstrap` phase.
  Recovery had been failing for about 81 minutes before shutdown.
- Worker 1 had an earlier cleanup failure and remained stopped after replacement
  began at 19:41:33 UTC on September 12.
- The final fleet sample reported six ready runtimes, two unavailable runtimes,
  four held projects, and three pending retirements. Worker 6 replacement had
  reached the bootstrap hold limit.
- The last request completed at 02:33:55.971 UTC. System shutdown started at
  02:33:56.111 UTC with zero active and zero queued requests.

Evidence is in `ghostprov-v2.20260913T021428480Z.34356.3.jsonl` at lines 6199,
6254, and 9609; `ghostprov-v2.current.34356.jsonl` at lines 1222 and 1241-1245;
and `ghostprov-v2.20260912T201826522Z.34356.1.jsonl` at line 7065, under
`C:\Users\Aymen\.ghostprov\fresh-v2\logs`.

The logs do not expose the aggregate's inner shutdown exceptions or establish
why the old supervisor stopped supplying authenticated bootstrap proof. The
code defects below are independently reproducible; they are not proof that all
retained remote state from this incident can now be cleaned.

## Changes

- System composition publishes one absolute shutdown deadline before aborting
  startup. Runtime and supervising-runtime cleanup use that remaining budget
  instead of ordinary recovery's 15-second and 30-second wait limits. An
  established earlier cleanup deadline is preserved, and a genuine cleanup
  rejection still fails shutdown.
- An interrupted recovery can make one bounded cleanup-only attempt with a
  fresh signal and the exact journal-authorized supervisor. This path cannot
  submit a successor, replay a provider request, or treat an ordinary failed
  recovery as permission to retry. Unproven cleanup retains the journal.
- Cleanup obligations compare complete supervisor bindings by value, so a
  freshly loaded durable receipt can discharge the exact obligation.
- The fleet error includes bounded failed-project details and remaining runtime
  counts rather than hiding everything behind the aggregate's generic message.

No upstream request policy, continuation behavior, provider retry rules, fleet
replacement rules, or immutable sidecar releases were changed.

## Validation and limits

- Final isolated full suite: **2,897 passed, 3 skipped, 0 failed**, across 202
  files in 101.39 seconds. The skipped cases are the opt-in 14-agent soak,
  16 MiB transport performance case, and 64 MiB request-integrity case.
- Final standalone runtime/supervising-runtime tests: **137 passed**. Final
  bootstrap-recovery tests: **29 passed**. Earlier focused fleet-manager and
  system checks passed 52 and 23 tests respectively.
- `bun run typecheck`, `bun run typecheck:scripts`, and `git diff --check`
  passed. Independent review found no remaining actionable issues.

The successful full run used unchanged tests and time limits, with `TEMP` and
`TMP` set only for that process to `.ghostprov/shutdown-fix-test-temp`. Earlier
runs under the default `E:\Temp` failed with rename `EPERM` errors and repeated
file-operation/hook timeouts; the first broad run also overlapped the final
source edit and was not valid final-tree evidence. Failed/cascading test
processes were stopped only after matching their exact offline test command
and process identity. No production process was stopped.

The final full-suite output is retained locally in
`.ghostprov/shutdown-fix-final-workspace-temp.log`. The failing earlier outputs
remain separate; they are not counted as passing validation.

No live cleanup, service restart, provider request, Prism project mutation, or
Windows shutdown was performed. Production bootstrap journals and credentials
were not read or changed. The new code must be loaded by a future process
before it can affect production shutdown; it does not retroactively repair
unreachable supervisors or certify the old failed shutdown.
