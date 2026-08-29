# GhostProv Agent Guide

## Authority and scope

This file is the repository-wide engineering policy. Follow it for every change unless a more specific `AGENTS.md` below the working directory adds compatible constraints. When historical documentation, tests, or comments describe `store:false` upstream continuation, this file contains the final policy and wins.

Work directly in the requested branch. Do not commit, push, rewrite history, or broaden the task unless the user explicitly asks. Preserve unrelated working-tree changes.

## Project purpose

GhostProv is a Bun/TypeScript, browserless, OpenAI-compatible router over isolated Prism sandbox projects. It serves both Codex-style Responses clients and the Anthropic/Claude adapter through the same bounded fleet, admission, transport, and observability layers.

The primary product goal is high concurrency for mixed Codex and Claude workloads without duplicate provider execution, unbounded host memory, or cross-sandbox state corruption. Optimize shared contracts and resource accounting first; do not add provider- or lane-specific behavior to shared primitives.

## Final upstream Responses policy

- Every request GhostProv sends to an upstream Responses endpoint must explicitly contain `stream: true` and `store: true`.
- `store:true` is intentional. Persisted provider-side response state and the provider's approximately 30-day retention window are accepted product behavior.
- Do not add a privacy shim, local-history emulation, or `store:false` fallback that contradicts this policy.
- Preserve a caller's `previous_response_id` exactly when present. Do not remove, replace, synthesize, reinterpret, or couple it to a transport lane.
- Normalize only at the upstream request boundary. Use a narrow, idempotent, copy-on-write normalizer: return the original object when both required fields are already `true`; otherwise shallow-copy and overwrite only `stream` and `store`.
- Keep client-facing streaming semantics separate from upstream streaming. An HTTP client may request a non-streaming final response while the internal provider request still uses `stream:true`.
- Apply the same upstream policy to native Responses traffic and requests translated from Anthropic Messages.
- Do not introduce the Conversations API. Responses plus `previous_response_id` is the continuation contract.
- Do not introduce native upstream WebSockets or socket affinity. Existing client-facing WebSocket support does not authorize a new upstream WebSocket transport.

## Continuation and shared-lane contracts

- `previous_response_id` expresses provider lineage; lane IDs express local ordering. They are independent.
- Construct one opaque `AuthenticatedTenantScope` in system composition and pass that exact object through the app, Anthropic routes, WebSocket listener/session/graph, and router. Never derive scope authority from request data, headers, API-key text, client IDs, model names, or serialized values.
- The router owns a content-free, tenant-scoped provenance registry used only as a routing hint. Reserve worst-case commit capacity before provider execution so concurrent terminals are bounded. After checking immutable binding conflicts, commit a completed provider response ID to its exact project/incarnation and evict only the oldest routing hints needed for that record; metadata pressure must not become a global request quota. Unknown or expired IDs route normally through the provider. Missing scope, scope conflicts, and binding conflicts fail closed.
- Generated WebSocket responses are provider-persisted and never enter a local history cache. Forward the current delta and exact provider `previous_response_id`. Local graph state is allowed only for explicit `generate:false` synthetic warmup aliases and must remain connection-local, scope-bound, byte/count bounded, and free of generated output.
- The Anthropic adapter owns one app-scoped index shared by stable and beta routes. Native Responses HTTP owns a separate app-scoped index for clients that resend exact full input arrays. Each retains only keyed rolling digests, provider response IDs, counts, and bounded lifecycle metadata; never raw messages, media, reasoning, tools, output, canonical JSON, or scope secrets. Reuse only a longest unique item-boundary prefix whose settings and lossless replay projection match exactly. An explicit caller `previous_response_id` always bypasses native inference.
- Shared types should describe only facts all lanes need, such as the normalized upstream body, immutable request identity, provider response identity, opaque authorization scope, and bounded lifecycle state.
- Keep lane-specific caches, translation, scheduling, and response shaping outside shared helpers.
- Provider response IDs and continuation metadata must stay correlated to the exact request that produced them.
- Do not infer continuation lineage from timing, socket identity, model name, or client connection.
- Authoritative terminal proof, router provenance, and every eligible prefix-index commit must complete before a public JSON/SSE/WebSocket terminal is exposed.

## Execution safety and retries

- Never replay a request after committed ACK, `request_start`, `FETCH_STARTING`, a potentially successful upstream write, or any other ambiguous execution-risk signal.
- Safe rerouting is allowed only before commitment, inside the original admission deadline, and when the implementation can prove no upstream execution began.
- A timeout, disconnect, cancellation race, or malformed terminal after a request may have been sent is ambiguous, not retry authorization.
- Preserve immutable request bodies across all admissible pre-commit attempts. A retry must not observe or construct a mutated body.
- Definitive provider errors are returned once with credential-sensitive details scrubbed. The sole continuation exception is an auto-inferred HTTP prefix hit—Anthropic translated history or native Responses full history—that receives a definitive HTTP-origin 400/404 `previous_response_not_found` before any relay event or public commitment: compare-delete that exact generation and retry the immutable original full history once. Never apply this fallback to an explicit caller or WebSocket provider ID, generic failures, SSE-origin failures, ambiguous work, or any request that emitted a relay/public event.
- GhostProv does not add provider-rate-limit sleeps, probes, synthetic cooldowns, or cross-sandbox retries.
- Quarantine ambiguous work and reduce capacity rather than risk duplicate execution.
- Recovery may replace a runtime for the same registered Prism project and worker index; it must not silently create, adopt, or delete projects.

## Release and bootstrap invariants

- V2 R8 is the authorized minimal sidecar successor for persisted continuation. Keep R1-R7 checked-in bundles and hashes byte-for-byte frozen. R8 may only remove the legacy post-`FETCH_STARTING` `invalid_json` replay and defensively require/normalize upstream `stream:true` plus `store:true`; do not add upstream WebSockets or unrelated behavior.
- Once generated and verified, the R8 release revision, protocol revision, schema hash, build hash, and bundle hash are frozen identities.
- Generated prior-sidecar matching must enumerate exact registered entrypoint paths constructed from each release's revision, `schemaHash`, and `buildHash`. Escape regex literals for Perl and anchor with `\z`.
- Never accept arbitrary 64-hex placeholders, cross-paired hashes, path prefixes or suffixes, trailing newlines, or unregistered revisions as an immutable sidecar identity.
- Bootstrap may retire a prior process only after exact PID/start-tick, process-group/session, nonce, owner record, static authority, and immutable entrypoint proof.
- Keep checked-in sidecar artifacts offline and content-addressed. Do not add runtime network imports.

## Bounded-resource rules

- Every queue, map, cache, journal, spool, parser, request body, response event, waiter set, and diagnostic buffer must have an explicit count, byte, and/or time bound appropriate to its lifetime.
- Reserve memory before buffering. Keep permits attached to data until delivery, eviction, cancellation, or terminal cleanup.
- Preserve end-to-end backpressure. Do not replace a bounded pause with unbounded buffering or optimistic ACKs.
- Avoid quadratic history materialization and unconditional deep clones. Prefer immutable inputs, copy-on-write normalization, exact-sized buffers, and bounded scratch space.
- Release permits and remove indexes on every success, failure, timeout, abort, disconnect, and recovery path.
- Keep the existing global resource-governor accounting and per-runtime capacity ceilings authoritative.
- Observability must be bounded and must never log prompts, responses, tool arguments, request bodies, headers, cookies, tokens, URLs, or client identifiers.

## Code conventions

- Runtime: Bun 1.4.0. Language: strict TypeScript/ES modules targeting ES2022.
- Prefer small, typed modules and discriminated unions. Use `readonly` contracts for immutable data.
- Validate untrusted objects before narrowing them. Preserve exact field omission versus explicit `null` where the external protocol distinguishes them.
- Use Web Platform types already supported by Bun unless an existing local abstraction is required for deterministic tests.
- Keep pure policy helpers free of I/O, clocks, globals, environment reads, and network behavior.
- Reuse established error, redaction, deadline, resource-governor, and stream helpers instead of creating parallel mechanisms.
- Do not weaken strict validation just to accept a new test fixture.
- Avoid unrelated formatting or generated-file churn.

## Repository map

- `src/relay/`: shared relay contracts and upstream request policy.
- `src/router/`: bounded admission, selection, commitment, safe pre-commit rerouting, and scoped provider-response provenance.
- `src/v2/`: Awareness transport, bootstrap, immutable sidecar releases, journals, and runtime integration.
- `src/anthropic/`: Anthropic Messages validation, translation, app-scoped digest continuation, streaming, and response shaping.
- `src/ws/`: client-facing Responses WebSocket sessions, provider-persisted generated turns, synthetic warmup aliases, and local ordering.
- `src/http/`, `src/server/`: authenticated HTTP surfaces and client-facing stream/final response behavior.
- `src/observability/`: allowlisted, credential-scrubbed, bounded telemetry.
- `tests/`: isolated offline unit/integration coverage.
- `scripts/`: guarded build, maintenance, and explicitly authorized live verification tools.

## Test workflow

Use the smallest deterministic offline checks first, then broaden only as needed:

```text
bun test --isolate tests/<focused-file>.test.ts
bun run typecheck
bun test --isolate
```

Additional rules:

- Tests must not use the network, live Prism sandboxes, provider APIs, real credentials, or user secret files unless the user explicitly authorizes a guarded live task.
- Automated tests may bind only an OS-assigned port (`listen(0)`) or port `18888` when a fixed port is unavoidable.
- Never bind, probe, proxy through, stop, restart, signal, or otherwise manipulate the protected listener on port `18787` while running tests.
- Use deterministic fakes for clocks, random sources, sockets, gateways, and provider events.
- Test both acceptance and rejection boundaries: exact registered values, malformed values, omitted fields, duplicates, cross-pairing, prefixes/suffixes, and strict end-of-string behavior.
- A skipped or guard-disabled live test is not live proof. Do not describe offline fixtures as provider or Prism validation.
- Do not run `bunx` commands that would download packages during an offline task. Use only already-installed/pinned tools.

## Live-operation safety

Live operations are opt-in and are not implied by a request to implement or test code.

- Do not read cookies, API keys, token files, credential environment variables, or other secrets without explicit user authorization for that exact live operation.
- Do not create, adopt, delete, or mutate Prism projects without the corresponding explicit live guard and dedicated project IDs.
- Port `18787` is a protected existing service. Never stop, restart, kill, signal, reconfigure, replace, or rebind it. Never run supervisor lifecycle commands against it.
- If a separately authorized verifier needs a listener, use an OS-assigned port or `18888`; it must not alter the protected service.
- Do not use live sandboxes or paid provider calls for ordinary unit, integration, typecheck, or compile validation.
- Keep credentials out of command lines, diffs, test output, logs, and summaries.

## Prohibited scope for the active persisted-continuation release

Unless a later task explicitly authorizes them, do not add:

- V2 R9, mutations to frozen R1-R7 assets, or any R8 behavior beyond the explicitly authorized minimal stored-request/exact-once delta.
- Native upstream WebSocket transport or header-capable WebSocket probes.
- Socket affinity or transport-coupled continuation state.
- Conversations API integration.
- Lane-specific continuation behavior in shared policy modules.
- Unsafe post-send retry or replay.
- Unbounded local transcript/history retention.

## Completion checklist

Before reporting a change complete:

1. Inspect the working diff and preserve unrelated edits.
2. Confirm upstream bodies explicitly normalize to `stream:true` and `store:true` before WebSocket graph handling and before V2 canonical bytes/digests, without losing `previous_response_id` or any unrelated field.
3. Confirm the system shares one opaque authenticated scope across HTTP, WebSocket, Anthropic, and router provenance; no serialized request or client-controlled field can create one.
4. Confirm public terminals wait for authoritative final proof, router provenance, and every eligible prefix-index commit.
5. Confirm no retry path exists after an ambiguous execution boundary; stale auto-inferred HTTP prefix fallback remains pre-event, HTTP-definitive, exact-generation compare-delete guarded, full-history, and single-attempt, and never applies to explicit caller or WebSocket IDs.
6. Confirm all new state, reservations, and buffering are bounded and cleaned up on every terminal, rejection, timeout, abort, disconnect, and recovery path.
7. Confirm no R9, upstream WebSocket, socket affinity, or Conversations API scope leaked in, and that R8 differs from R7 only by the authorized stored-request/exact-once sidecar delta.
8. Run focused offline tests and `bun run typecheck`.
9. Re-inspect `git diff --check` and the final file list.
10. Report tests actually run and any validation intentionally not run.
