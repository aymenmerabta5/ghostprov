# R10 Git clean-filter bootstrap validation

The fifth bootstrap mode is `GP_V2_SUPERVISOR_BOOTSTRAP_MODE=git-clean-filter`. It uses a temporary clean filter during the Git snapshot preceding one harmless Render request, then starts the existing frozen R10 supervisor. The default remains `direct`; `llm`, `git-fsmonitor`, and `git-ssh-signing` remain selectable.

Status: **two independent integrated production runs passed every lifecycle, adapter, and cleanup gate**, following two successful research R10 runs. The final full repository suite and all enabled opt-ins passed. The default remains `direct`; the new method is explicitly selected.

## Mechanism and ownership

The filter matches exactly one nonce-named TeX document and accepts at most 8 KiB of expected input. It restores owned Git configuration and releases its lock before launching, passes the original document bytes unchanged, seals the expected launcher, and makes one durable launch claim. Cancelled, expired, malformed, foreign, and ambiguous states fail closed. A launch receipt alone does not prove readiness.

The host reserves one persistent project owner record before staging a probe document. Another runtime cannot add further probes while that ownership is unresolved. Failures before changing global configuration remove only provably unlaunched private staging and exact owned probe records. Ambiguous work retains ownership until authenticated cleanup or retirement. Signed shutdown removes the exact probe, ownership record, and private execution workspace, then verifies workspace absence.

The mode requires R10 relay bootstrap throughout the fleet, Python, Git, Linux sealed-memory support, and Render. It does not need an executable `.latexmkrc` bootstrap payload, root, signing tools, or model-assisted bootstrap. An absent `.latexmkrc` or GhostProv's exact inactive file is accepted; custom `.latexmkrc`, `.gitattributes`, and `.gitignore` files are rejected before staging. Git/filesystem failures remain shared dependencies with the third and fourth methods.

No HTTP, Anthropic, WebSocket, routing, continuation, or upstream request-policy implementation changed. R1-R9 bundles and all release identities remain frozen. R10 retains build `ce9f48171863bdaa0adcd1a6344ec175502dd1ef9d1b9fae9e4a512a1474c32a` and bundle `a808969a1e89db0ec9636915a667613246c17fe5829b877aab0e8f192adc9776`.

Source pins use LF-normalized UTF-8 bytes, matching the staged Linux files.

| Verified source | SHA-256 |
| --- | --- |
| Python clean filter | `cbaf273695287f5a34f5cac9fb9e359be0d2a4e8ffbb671cd145dd40aec5b555` |
| Finite marker verifier | `49a40722a002349e61c858ae03e907de43e195684fba9066736ca8192d4033b9` |

## Offline proof

- 44 real Linux Git/process checks passed as UID/EUID 65534 under Git 2.43.0. These include byte preservation, empty/binary/8 KiB input, 32 racing callbacks with one launch, configuration/inode preservation, cancellation, expiry, deadlines, changed identities, symlinks/hard links, foreign state, sealed launch bytes, lost receipts, ambiguous submission, and an unsuccessful child whose submission must not be mistaken for readiness.
- 46 focused production controller/configuration/guard tests passed. They exercise the selected fifth mode, retained supervisor recovery, cancellation and failed readiness, exact signed cleanup, no replay, source/receipt validation, R10-only configuration, bounded ownership across 32 later bootstrap attempts, and failed cleanup synchronization.
- Existing-method and adapter baseline: 152 passed, zero failures, covering direct/LLM/fsmonitor/SSH supervision, Responses HTTP, Anthropic continuation, WebSocket, shared routing, and upstream policy.
- Final full suite: 2,435 passed, zero failures/skips across 185 files in 197.13 seconds. The 16 MiB, 64 MiB, and ten modeled 14-agent-wave opt-ins were enabled in the same run.
- Both TypeScript projects, R8/R9/R10 deterministic checks, and pinned Deno 2.9.5 R10 checking passed. Frozen release and runtime adapter/policy files remain unchanged.

## Live research results

All runs used dedicated disposable projects. The two R10 runs used candidate port 18888, made one small model request each, and never accessed the protected 18787 listener.

| Run | Result |
| --- | --- |
| Marker `clean-marker-8eAXNJ` | Unprivileged UID/EUID 5000 launch; committed/worktree bytes verified; one Render; zero model requests; project and cookie copy deleted |
| R10 `clean-r10-OMBWcD` | Signed startup in 40.050 s; ordinary/lost-challenge/lost-reply reconnects in 850/1879/3130 ms; same-supervisor child recovery in 30.644 s; model request in 1.881 s; 120-second readiness; signed shutdown and all cleanup passed |
| R10 `clean-r10-MttIHc` | Hardened host ownership path; signed startup in 44.238 s; reconnects in 809/1890/1803 ms; same-supervisor recovery in 36.109 s; model request in 1.894 s; 120-second readiness; signed shutdown and all cleanup passed |

The first research run's exact host sources were retained before the persistent probe-owner safeguard was added. The second run includes that safeguard. The Python callback and frozen R10 assets are identical in both. Each had one supervisor bootstrap, two child launches including deliberate recovery, one Render, and no alternate launch attempt. Both exited 0 and verified project/cookie deletion.

These are individual samples, not performance distributions or proof of a speedup.

## Integrated confirmation

Both runs used `scripts/live_git_clean_filter.ts` with the actual registered production mode. The verifier observed calls and receipt identities; it did not replace the launcher. Each completed one native Responses request and one Anthropic Messages request, three reconnect cases, child recovery under the same supervisor, two minutes of readiness, signed shutdown, and explicit probe/owner/workspace/project/credential cleanup. Each had one supervisor bootstrap, one Render, two child launches including deliberate recovery, and zero alternate launch attempts. Both exited 0 with no bootstrap or cleanup errors.

| Integrated run | Startup | Ordinary / lost-challenge / lost-reply reconnect | Child recovery | Responses / Messages | Exit |
| --- | --- | --- | --- | --- | --- |
| `r10-rwsKnN` | 43.233 s | 804 / 1859 / 1881 ms | 37.375 s | 1.969 / 2.140 s | 0 |
| `r10-DTlz1p` | 43.261 s | 892 / 1863 / 1828 ms | 39.422 s | 2.483 / 2.539 s | 0 |

Startup includes account/project provisioning. These are individual observations. The [sanitized evidence](../../benchmarks/git-clean-filter-2026-09-08.json) records source identities and gate results. Candidate credential, identity, and lock files were independently checked absent, and candidate port 18888 had no listener after completion. Port 18787 was never probed or manipulated.

## Regression failures retained and corrected

Two full-suite attempts exposed timing assumptions in existing test setup. The queued-cancellation test sometimes aborted during asynchronous preparation; preparation must remain accounted until it settles. It now establishes the actual queued state before aborting, while retaining the immediate zero-queue assertion afterward. The system model-override test sometimes exceeded its implicit five-second setup budget while persisting its local manifest; startup now sits inside cleanup's `try/finally` with the same bounded 30-second budget as neighboring system-composition tests. Both tests then passed 20 consecutive repetitions each.

A later run exposed the missing-worker test racing a 100 ms ready/grace deadline instead of observing the intended missing-module exit. Its ready/grace budgets are now 1,000 ms; all original exit, no-child, and zero-forced-stop assertions remain. It passed 20 consecutive repetitions. Separate readiness-timeout tests retain their original deadlines. Production transport, host-supervisor, and adapter implementations were not changed for these test corrections, and no workload or performance threshold was weakened.

Failed logs remain under `.tmp/fifth-method/full-tests-final-attempt-01.log`, `full-tests-final-attempt-02.log`, and the targeted reproduction logs. The final full run passed all 2,435 tests.

## Evidence limits

Single-project lifecycle tests and short observation windows do not establish multi-project fleet capacity, long-duration availability, or compatibility with future Prism Git/Render changes. The callback assumes the same trusted isolated sandbox filesystem as the existing bootstrap methods. Unproven ownership or cleanup requires quarantine or retirement.
