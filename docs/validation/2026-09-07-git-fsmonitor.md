# R10 Git fsmonitor bootstrap validation

The integrated `GP_V2_SUPERVISOR_BOOTSTRAP_MODE=git-fsmonitor` mode passed the same one-project live lifecycle proof as its experimental predecessor. The production default remains `direct`; this change does not reconfigure or restart an existing service.

## Implementation

- R10 relay bootstrap only; configuration rejects prior-release defaults and per-project overrides when this mode is selected.
- The existing supervisor controller seals the exact private execution session, stages the existing R10 launcher, and waits for authenticated supervisor readiness.
- A temporary Git hook takes the sandbox's Git configuration lock, preserves its original configuration, restores it before launch, and releases the lock. Foreign configuration or lock ownership fails closed.
- One launch claim prevents a second execution, including after ambiguous outcomes or an unsuccessful child. The hook expires after 90 seconds; host staging and receipt acquisition are bounded to 75 seconds.
- Repeated lock conflicts remove only their fresh, unlaunched execution workspaces. A workspace that may contain a supervisor is removed only after signed shutdown.
- Recovery, upstream request normalization, continuation, admission, and response transport continue through the existing R10 implementation. No fallback launcher or upstream WebSocket transport was added.

The Python hook is byte-identical to the tested prototype and pinned by SHA-256:

`86ce17d9629b428dbf8c197f36513e6ee771b6fcae680e9abe10c43c49cd8652`

R1-R9 assets remain untouched. R10 remains the existing release:

- Build: `ce9f48171863bdaa0adcd1a6344ec175502dd1ef9d1b9fae9e4a512a1474c32a`
- Bundle: `a808969a1e89db0ec9636915a667613246c17fe5829b877aab0e8f192adc9776`

## Integrated live result

The verifier selected the real configuration mode. Its gateway wrapper only observed calls; it did not substitute an experimental launcher. The fresh disposable project used candidate port 18888. No operation targeted production port 18787.

| Check | Result |
| --- | --- |
| Supervisor bootstrap triggers | Exactly 1 |
| Configuration restoration and lock release before launch | Passed |
| Native Responses requests | 6 completed |
| Anthropic Messages requests | 2 completed |
| Concurrent mixed wave | 2 Responses + 2 Messages completed |
| Forced reconnect with first challenge dropped | Authenticated restoration in 1,845.493 ms |
| Forced reconnect with first reply dropped | Authenticated restoration in 1,792.294 ms |
| Exact signed sidecar stop and recovery | Ready capacity restored in 33,924 ms |
| Supervisor identity during recovery | Unchanged |
| Sidecar launches | 2 total: initial launch and one deliberate recovery |
| Explicit pre-recovery provider response ID | Accepted on the post-recovery request |
| Stability after recovery | 120 seconds of ready capacity, connected transport, and no quarantine |
| LLM / direct-exec / Render launch attempts | 0 / 0 / 0 |
| Signed shutdown and private workspace removal | Passed |
| Disposable project deletion | Verified |
| Candidate cookie copies and port 18888 after completion | Removed / no listener |

The live verifier exited 0. Its content-free report is retained in `benchmarks/git-fsmonitor-2026-09-07.json`. Local detailed evidence is under `.ghostprov/git-fsmonitor/r10-jedrac/`.

## Offline checks

- `bun --no-env-file test --isolate`: 2,387 passed, 3 skipped, 0 failed across 181 files; exit 0.
- `bun run typecheck` and `bun run typecheck:scripts`: passed.
- `bun run check:git-fsmonitor-hook`: 18 checks passed, including real Linux Git, Python, and `setsid` execution; exact-once launch under 32 concurrent hooks; configuration preservation; lock conflicts; expiry; cancellation marker; symlink rejection; and truthful shutdown proof.
- R8, R9, and R10 deterministic release checks: passed.
- Pinned Deno R10 check: passed.
- Working diff and whitespace checks: passed.

The new unit tests also exercise the actual supervisor controller: selected-mode startup, retained-host recovery, concurrent readiness callers, no fallback or replay, failed readiness, cancellation during authentication, colliding project files, and refusal to delete an ambiguously running workspace.

## Scope and unrun gates

This is one live sandbox with a four-request concurrent wave and a two-minute stability window. It is not an eight-project fleet proof or a long natural-reconnect soak. The three skipped offline cases are the existing opt-in modeled 14-agent waves and 16/64 MiB transport performance/integrity tests; no new tests were skipped.

## Repeat the checks

```powershell
bun --no-env-file test --isolate tests/v2-supervisor-git-fsmonitor.test.ts tests/multi-sandbox-config.test.ts
bun run check:git-fsmonitor-hook
```

The Linux hook verifier uses the installed `Ubuntu-24.04` WSL distribution on Windows, or local Python 3 and Git on Linux.

The following requires explicit live authorization, uses configured Prism credentials, creates and deletes one disposable project, and makes eight small model requests:

```powershell
$env:GP_LIVE_GIT_FSMONITOR = '1'
$env:GP_LIVE_ALLOW_TEST_PROJECT_MUTATION = '1'
$env:PORT = '18888'
bun run verify:git-fsmonitor
```
