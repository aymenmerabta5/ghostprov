# R10 Git textconv bootstrap validation

The sixth explicit bootstrap mode is `GP_V2_SUPERVISOR_BOOTSTRAP_MODE=git-textconv`. It invokes a temporary display converter during `git show HEAD`, after snapshot commit, then starts the unchanged R10 supervisor. The default remains `direct`; all five earlier choices remain available.

Status: **two consecutive integrated production runs passed every lifecycle, adapter, and cleanup gate**, following two successful research R10 runs. The final combined suite passed all 2,472 tests with no failures or skips. One earlier connection-setup timeout is retained below; it was not reclassified as a pass.

## Mechanism and shared guarantees

Git's [textconv interface](https://git-scm.com/docs/gitattributes) passes a filename to a configured program and uses its stdout for display. The local proof verified that indexing/commit alone does not invoke this callback; the subsequent `git show HEAD` does. This is a distinct callback from filesystem monitoring, SSH signing, and clean filtering. It shares their Git/filesystem infrastructure and additionally requires the Render snapshot/display path.

The input adapter verifies its own source hash and the shared executor's exact bytes before loading it. It accepts an absolute, expected worktree or Git temporary-blob path, requires an owned regular single-link file of at most 8 KiB, rejects symlinks/FIFOs/directories and changed input, and seals the bytes before passing them to the existing content executor. It never opens a FIFO in blocking mode. Cachetextconv is explicitly disabled.

The host reuses the tested `GitCleanFilterBootstrap` lifecycle through a narrow deployment gateway. It changes only the exact attributes file, driver configuration, and plan configuration digest, and stages one pinned input adapter. Canonical plan validation rejects duplicates, altered identities, extra fields, and format drift. All unrelated bytes, headers, signals, and operations pass through. Duplicate or incomplete staging cannot arm the callback.

The fifth and sixth methods intentionally share one persistent probe-owner record and the same durable launch claim, configuration restoration, sealed launcher, authenticated R10 readiness, and signed cleanup. A failed or ambiguous attempt never switches callbacks or replays. R10 relay is required throughout the fleet. No HTTP, Anthropic, WebSocket, router, continuation, upstream request-policy, or frozen sidecar implementation changed for the sixth method.

The mode needs no root, model-assisted bootstrap, direct-exec API, or executable `.latexmkrc` payload. As with the fifth method, only an absent or exact inactive `.latexmkrc` is accepted; custom `.gitattributes` and `.gitignore` files fail closed.

| Asset | SHA-256 of LF-normalized UTF-8 |
| --- | --- |
| Shared Python executor | `cbaf273695287f5a34f5cac9fb9e359be0d2a4e8ffbb671cd145dd40aec5b555` |
| Textconv input adapter | `176dd2f86f1a4a4e1350023a9070db1a705222d3db02d68c565209468bc78d68` |
| Frozen R10 bundle | `a808969a1e89db0ec9636915a667613246c17fe5829b877aab0e8f192adc9776` |

R10 retains build identity `ce9f48171863bdaa0adcd1a6344ec175502dd1ef9d1b9fae9e4a512a1474c32a`; R1-R9 bundles and identities remain unchanged.

## Offline proof

- 30 real Linux textconv/file-input checks passed as UID/EUID 65534 under Git 2.43.0. They cover the actual show trigger, stored/displayed bytes, empty/binary/8 KiB inputs, 32 callbacks with one launch, repeat display, bounded FIFO rejection, symlinks/hard links, wrong input, malformed arguments and source hashes, foreign configuration/locks, cancellation, and expiry.
- The production verifier also reran the shared executor's 44 real Linux Git/process checks; all passed.
- 59 production controller/deployment/configuration/guard tests passed, including actual mode selection, exact signed cleanup, no replay, source/plan integrity, bounded ownership, preservation of unrelated staging, and rejection of incomplete/duplicate arming.
- Final combined suite: 2,472 passed, zero failures/skips across 188 files in 129.17 seconds. The 16 MiB, 64 MiB, and ten modeled 14-agent-wave opt-ins were all enabled.
- Both TypeScript projects, deterministic R8/R9/R10 checks, and pinned Deno 2.9.5 R10 checking passed. The existing runtime adapters, shared request policy, host supervisor, and frozen sidecar implementation remain unchanged.

## Live research

Every run used a new disposable project. R10 runs used candidate port 18888 and one small model request each. All projects and private credential copies were deleted and verified. No test accessed port 18787.

| Run | Result |
| --- | --- |
| Marker `textconv-marker-lClgJg` | UID/EUID 5000; one textconv configuration/input-adapter write and one Render; expected blob/worktree and config cleanup verified; zero model calls; project/cookie cleanup passed |
| R10 `textconv-r10-vnh8jB` | Startup 43.085 s; reconnects 802/1848/1813 ms; same-supervisor recovery 32.375 s; model request 1.608 s; 120-second readiness; signed shutdown/workspace/project/cookie cleanup passed; exit 0 |
| R10 `textconv-r10-QyqnbF` | Startup 42.441 s; reconnects 848/1842/1811 ms; same-supervisor recovery 31.867 s; model request 2.044 s; 120-second readiness; signed shutdown/workspace/project/cookie cleanup passed; exit 0 |

Reconnects were ordinary, dropped-challenge, and dropped-reply cases. Each run used one supervisor bootstrap and two sidecar launches including deliberate recovery, one Render, and no alternate callback. Startup includes account/project provisioning; these are individual observations, not a performance distribution.

## Integrated confirmation

Both completed runs used `scripts/live_git_textconv.ts` and the actual registered mode, with one Responses and one Anthropic Messages request per run, all lifecycle/fault checks, and explicit probe/owner/workspace/project/credential cleanup. The verifier observed production calls and rejected the clean-filter and other alternative callbacks; it did not substitute the launcher. Each had one supervisor bootstrap, one Render, one input-adapter/configuration write, and two sidecar launches including deliberate recovery.

| Integrated run | Startup | Ordinary / lost-challenge / lost-reply reconnect | Child recovery | Responses / Messages | Exit |
| --- | --- | --- | --- | --- | --- |
| `r10-PX0a0r` | 42.588 s | 817 / 1825 / 1897 ms | 35.888 s | 1.806 / 2.174 s | 0 |
| `r10-JDMYFz` | 40.385 s | 811 / 1782 / 1802 ms | 31.371 s | 1.791 / 2.100 s | 0 |

Both observed 120 seconds of ready capacity without quarantine or ambiguity, then proved signed shutdown and all cleanup. All created projects and private credential copies, including the failed attempt below, were deleted. Candidate port 18888 was free afterward. Source identities and content-free results are retained in the [sanitized evidence](../../benchmarks/git-textconv-2026-09-08.json).

## Evidence limits

The first integrated attempt, `r10-rGATkG`, is retained as a failed run. Bootstrap and the ordinary reconnect passed, but the second reconnect did not synchronize within the existing 15-second deadline. No challenge-loss injection had occurred, and no model request was made. Its exact connection-setup cause was not established. Signed shutdown and private-workspace removal were unproved, so no PID or ambiguous workspace cleanup was forced. The dedicated project was retired/deleted and credential cleanup was verified. The code and deadline were unchanged for subsequent fresh-project checks; this failure is not counted as a passing gate.

Short single-project runs do not establish multi-project capacity, long-duration availability, or compatibility with future Prism Git/Render changes. The callback assumes a trusted isolated sandbox filesystem, as the existing methods do. Unknown ownership or cleanup requires quarantine or retirement. Shared Git/Render dependencies must not be described as independent outage protection.
