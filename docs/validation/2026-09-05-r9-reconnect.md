# Supervisor reconnect investigation — 2026-09-05

The host fixes support the frozen R9 supervisor and the new R10 logging release. They address a reproduced reconnect handshake defect and a reproduced missed-message case. Connection-establishment failures, model generation time, and relay streaming cadence are reported separately.

## Verified defects and changes

After a supervisor WebSocket closed, the old host waited for the original sequence-zero `ready` value. The running supervisor had already replaced that Awareness value with its latest command reply. The host could wait for its full 90-second budget even after transport synchronization.

The host now sends a fresh authenticated credential-rotation challenge supported by the existing supervisor protocol. An old signed reply can reconcile whether a command sequence was consumed, but only the exact new challenge reply establishes fresh readiness. An unresolved lifecycle command is never replayed. Newly minted credentials are available before reauthentication, and cancellation, connection generations, shutdown, and absolute deadlines fence late proof.

Initial bootstrap can survive transient transport errors within its original deadline. It still requires synchronization and a signed ready on the current connection, without resubmitting a bootstrap job.

The first integrated R10 fleet test exposed a second gap: one fresh challenge received no authenticated reply within the 15-second test deadline although the socket opened and synchronized. The other 15 forced reconnects passed. Deterministic tests reproduced missing challenge and missing reply notifications. Real Yjs tests also confirmed that renewing an identical Awareness value does not notify `change` listeners.

A pending fresh challenge can now be reannounced up to three times within its original connection and deadline. The signed command stays identical. A bounded numeric marker outside the authenticated command causes the existing peer to revisit its exact-command reply cache. Tests prove that credential rotation is executed once. Ordinary lifecycle commands and provider requests are not repeated.

Two related runtime fixes remain included:

- If credential reconnect completes while recovery is waiting, recheck exact sidecar liveness and reliable-window progress before restarting a healthy child.
- Preserve supervisor-bootstrap readiness failures through deployment-error wrappers so the existing bounded failed-session policy receives the correct evidence. Generic deployment errors remain distinct.

Allowlisted `reconnect_phase`, `supervisor_phase`, and `V2SupervisorError` fields distinguish credential minting, supervisor proof, host transport, and sidecar credential rotation without logging credentials or payloads.

## Measured live proof

| Check | Result |
| --- | --- |
| R9 standard one-project canary | 43/43 requests; same-supervisor child recovery; concurrency through 16 |
| Two forced R9 supervisor reconnects in that canary | 784.000 / 779.434 ms |
| Controlled child recovery in that canary | 31.879 s including failure detection |
| Later R9 fleet, supervisor-only reconnect stage | 16/16 reconnects passed |
| Additional simultaneous closure of all 24 owned supervisor/data/control sockets | 8/8 regained usable capacity in 2.050–3.338 s; original children and supervisors retained |
| Complete later R9 fleet run | Failed afterward during a restarted child's missing readiness; not a passed fleet gate |
| Initial integrated R10 one-project test | 11/11 requests, normal child recovery, and invalid-config fatal-log readback passed |
| Initial integrated R10 fleet | 24 requests passed; 15/16 forced reconnects passed; stopped at the remaining control timeout |
| Deliberately dropped challenge and reply after the host fix | Both recovered in 1.820 / 1.826 s with the original supervisor and child |

The controlled loss proof made zero model requests, used one direct bootstrap, and verified project and credential-copy cleanup. The frozen R10 bundle did not change for this host-only repair. The subsequent complete R10 fleet result belongs in the [R10 validation report](2026-09-05-sidecar-diagnostics.md).

These are component timings and diagnostic samples. They are not a matched before/after benchmark of the user's production conversations. In particular, a faster control reconnect is not a claim that the provider generates tokens faster.

## Connectivity evidence

Temporary clients independently reproduced connection establishment problems before model execution. Node recorded TCP timeouts on 4/10 Prism checks and 7/7 GitHub checks, generally around 21 seconds, after successful DNS resolution. Bun independently reproduced similar failures. On the actual Y-Sweet path, two Bun opening sockets failed around 21 seconds; the SDK's next attempts synchronized in 615/625 ms, after the original 15-second connection budget had already expired.

The user then reset the router. The supplied production startup pastes changed from 154.404 seconds with eight retries and six heartbeat failures to 37.853 seconds with all eight workers ready on their first attempts. Both production runs used the same eight projects and R9/direct/relay settings. Fresh public checks afterward passed 6/6 in 122–267 ms. This is connectivity evidence, not a code speedup or proof of a particular faulty router/ISP/edge component.

A later fixed production snapshot, from 13:31:21Z to 14:45:47Z, contained 74 accepted requests, 72 completions, one cancellation, and one `sandbox_recovery` failure. Three post-startup recoveries took 33.456, 36.575, and 97.297 seconds. All eight workers were usable at the final snapshot. These records do not justify attributing every failure to internet connectivity.

## Streaming findings

The frozen R8, R9, and R10 bundles have the same 25 ms publication pacer and 25 ms response batching defaults. The extracted publication-pacer bytes match exactly. No new 50 ms per-token limiter was found.

Three sustained `gpt-5.6-sol` streams produced identical visible text lengths. HTTP Responses, WebSocket Responses, and Anthropic delivery all showed approximately 240 ms pauses between visible bursts, alongside measured relay RTTs of 216–222 ms. Host-received events reached HTTP/WebSocket clients in 1.239/1.575 ms median. The longer waits before first text already existed in the sandbox's upstream stream. See the R10 report for complete measurements and definitions.

An independent short follow-up finished upstream in approximately 1.213 seconds but reached the client at 3.487 seconds. The sandbox log showed an unacknowledged flight aged 1.882 seconds, six queued frames, and three retransmissions. This locates that outlier in relay delivery. It does not identify the physical cause of the missed deliveries. Host and sandbox wall clocks differ; only valid within-process durations and same-host forwarding timestamps were compared.

## Verification and limits

The final offline suite after the control-message fix passed **2,335 tests across 178 files**, with zero failures, in 130.25 seconds. Capacity and 16/64 MiB opt-ins were enabled. Both TypeScript checks passed. Deterministic R8/R9/R10 and pinned Deno checks passed; Deno upstream coverage passed 1 test and journal coverage passed 13 tests, including 64 MiB, with 2 Windows platform skips.

The installed Codex 0.153.1 client previously passed its isolated rate-limit retry verifier with two sampling attempts and an observed 264 ms wait. No provider-rate-limit retry policy was changed. Codex keeps its automatic waiting behavior.

All live work used dedicated projects and candidate 18888. Production 18787 was not accessed or modified. Every failed fleet attempt remains failed in the [sanitized evidence](../../benchmarks/r9-reconnect-2026-09-05.json). No 130-minute natural reconnect soak started or passed. The earlier missing-child-readiness failure remains unclassified; synthetic failure capture and later passing tests must not be relabelled as its diagnosis.

Upstream `stream:true` / `store:true`, exact caller continuation IDs, tenant scope, public terminal/provenance barriers, and the no-ambiguous-provider-replay policy remain unchanged. R1-R9 bundle bytes and identities are frozen. The direct prompt is still `Hiii`, and the original LLM bootstrap remains selectable.
