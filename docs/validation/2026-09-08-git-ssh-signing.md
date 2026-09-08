# R10 Git SSH-signing bootstrap validation

The fourth bootstrap mode is `GP_V2_SUPERVISOR_BOOTSTRAP_MODE=git-ssh-signing`. It uses temporary Git SSH-signing callbacks and Deno WebCrypto to start the existing frozen R10 supervisor. The default remains `direct`; `llm` and `git-fsmonitor` remain available. No HTTP, Anthropic, WebSocket, router, continuation, or upstream request-policy implementation changes are required.

Status: **two independent integrated production runs passed every lifecycle and cleanup gate**, following successful research and existing-method control runs. All repository tests and offline acceptance gates passed. The mode remains an explicit selection; the existing default and adapters are unchanged.

## Mechanism and scope

The prepare callback restores the original Git configuration before generating a disposable Ed25519 key. The signing callback produces and verifies a genuine SSHSIG signature, removes its keys, freezes launcher bytes in sealed memory, and claims one launch. A submission receipt does not establish readiness: the existing authenticated supervisor protocol remains authoritative.

The signer source is hash-verified and sealed. Signing and verification receive bounded stdin frames and have no Deno filesystem, network, environment, or subprocess permissions. Key generation may write only its two private key paths. The candidate requires the sandbox's existing Python, Deno, Git, and Linux sealed-memory support, but neither root nor `ssh-keygen`. OpenSSH is used for independent offline verification only.

An ambiguous launch is never resubmitted or switched to another method. Cleanup requires signed supervisor shutdown and a valid restored-config receipt. It re-enters Prism filesystem mode before deleting only the private execution workspace, then verifies absence. Foreign configuration, locks, and files without ownership proof are preserved.

The Git-signing and filesystem-monitor callbacks share Prism's Git initialization and filesystem APIs. They are distinct launch callbacks, not independent protection against failure of those shared facilities.

## Immutable identities

| Asset | SHA-256 |
| --- | --- |
| Git signing hook | `55c887e041f792f881ab2e6c14142e1da218a20cc45d1265188070cddc2d2a22` |
| SSHSIG signer | `492d2fd30686527cef50e65e864ccfc22e299f45ffdbf8e1e8d2c669b5547eff` |
| Frozen R10 bundle | `a808969a1e89db0ec9636915a667613246c17fe5829b877aab0e8f192adc9776` |

R10 retains build identity `ce9f48171863bdaa0adcd1a6344ec175502dd1ef9d1b9fae9e4a512a1474c32a`. R1-R9 artifacts and identities are unchanged.

## Offline checks

- Production Linux acceptance: **51 passed, 0 failed**, including real signed Git commits, a 32-way callback race with one launch, config restoration, cancellation, expiry, interrupted preparation, malformed plans, identity mismatches, symlinks/hard links, foreign state, and lost receipts.
- Production SSHSIG interoperability: **43 passed, 0 failed**, including independent OpenSSH verification, both hash algorithms used by SSHSIG, bounded stdin transport, malformed frames, wrong messages/keys/principals, and oversized inputs.
- Targeted bootstrap/configuration/live-guard checks: **40 passed, 0 failed**.
- Existing-method and client-adapter baseline: **188 passed, 0 failed** across direct/LLM/fsmonitor supervision, Responses HTTP, Anthropic continuation, WebSocket, and shared router integration.
- Full repository suite: **2,407 passed, 0 failed**, across 183 files in 151.86 seconds; the three opt-in checks were initially skipped.
- The remaining three opt-ins then passed in 21.45 seconds: 16 MiB integrity, 64 MiB integrity, and ten modeled waves of 14 agents. All 2,410 distinct repository tests therefore passed across the full and opt-in runs.
- Both TypeScript projects, R8/R9/R10 deterministic bundle checks, the pinned Deno 2.9.5 R10 check, and `git diff --check` passed after integration.

Offline Git and crypto checks used Linux Git 2.43.0 and Deno 2.9.5 under WSL. They are not live provider or fleet proof.

## Live results before production integration

All runs used a dedicated disposable project, candidate port 18888 where a listener was needed, and no more than one small model request per run. No operation accessed or modified the protected 18787 listener.

| Run | Result |
| --- | --- |
| WebCrypto marker `ssh-webcrypto-nuqgf1` | Valid signed commit and independent verification; one launcher; config/lock/key cleanup; zero model requests; project and credential copy deleted |
| R10 research `webcrypto-r10-NFhGfi` | Signed startup, three authenticated reconnects, exact child recovery under the same supervisor, one completed model response, 120 seconds of idle readiness, signed shutdown, and workspace/project/credential cleanup all passed |
| Existing fsmonitor control `fsmonitor-control-O6bt5C` | The same full sequence passed using the shipped third method |

The three reconnect cases were an ordinary reconnect, a dropped fresh challenge, and a dropped fresh reply. The fourth-method run measured 866/1814/1823 ms, retained the original supervisor, and recovered its child in 31.660 seconds including failure detection. Startup took 39.421 seconds and the one small request completed in 1.562 seconds. These are individual samples, not performance distributions or a measured speedup over another method.

## Integrated production confirmation

Both runs used `scripts/live_git_ssh_signing.ts` and the actual registered `git-ssh-signing` mode. The verifier observed the production calls; it did not substitute another launcher. Each passed signed readiness, an ordinary reconnect, a dropped challenge, a dropped reply, same-supervisor child recovery, one completed model response, 120 seconds of idle readiness, signed shutdown, and verified removal of the private workspace, project, and credential copy. Both made one supervisor bootstrap, two child launches including recovery, and zero attempts to invoke LLM, direct-exec, filesystem-monitor, or Render launch paths.

| Integrated run | Startup | Ordinary / lost-challenge / lost-reply reconnect | Child recovery | Small request | Exit |
| --- | --- | --- | --- | --- | --- |
| `r10-TmKhED` | 42.985 s | 845 / 1850 / 1929 ms | 30.889 s | 2.946 s | 0 |
| `r10-2UTMWT` | 41.594 s | 856 / 1843 / 1874 ms | 30.621 s | 1.789 s | 0 |

No bootstrap errors or cleanup failures occurred in either integrated run. The [sanitized evidence](../../benchmarks/git-ssh-signing-2026-09-08.json) includes the source identities and exact gate results.

## Failures retained in the evidence

- The initial OpenSSH candidate passed local tests but failed live because `/usr/bin/ssh-keygen` was absent. A bounded capability check found UID/EUID 5000, no `sudo` or `doas`, and no configured path to root. Its filesystem search was limited.
- The first WebCrypto integration failed 9 of 51 real-Git checks: Deno refused file-data reads from `/proc/self/fd` without unrestricted permission. Bounded stdin transport fixed that failure while retaining permission restrictions.
- R10 research run `webcrypto-r10-hx7jdH` passed runtime, reconnect, recovery, request, stability, and signed-shutdown checks but failed workspace cleanup. The omitted transition back to Prism filesystem mode was reproduced offline and corrected. Later live workspace deletion passed.
- `webcrypto-r10-noPnSM` timed out awaiting a fresh reconnect reply after four challenge publications. `webcrypto-r10-meT7cr` passed reconnects and signed cleanup but failed same-supervisor child recovery before replacement launch. These remain failed runs; their precise causes were not established.
- The first existing-method comparison, `fsmonitor-control-dIfYbK`, failed on account transport before creating a project or invoking a bootstrap. Subsequent public connectivity checks succeeded. This demonstrates a transport failure independent of the fourth method but does not diagnose every earlier failure.

Failed runs are not counted as passing gates. All created research/control projects and their credential copies were deleted. No failed or ambiguous provider request was replayed.

## Remaining evidence limits

The live observations cover individual disposable projects and short idle windows. They do not establish multi-project fleet capacity, long-duration availability, or stability across future Prism Git/filesystem changes. The callback assumes the same trusted isolated sandbox filesystem as the existing bootstrap methods; it does not defend against arbitrary code already running as the same sandbox user. Unproven cleanup or ownership loss requires quarantine or retirement.

The production mode was exercised directly by `scripts/live_git_ssh_signing.ts`; its launcher was not replaced by a research callback. Final sanitized results are retained in `benchmarks/git-ssh-signing-2026-09-08.json`.
