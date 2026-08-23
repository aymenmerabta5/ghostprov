# GhostProv Anthropic and Claude Code Compatibility Design

Date: 2026-08-23

## Objective

Expose a production-grade Anthropic Messages API and the existing OpenAI Responses API from the same GhostProv listener. Claude Code must run against GhostProv by setting `ANTHROPIC_BASE_URL` to that listener and a non-empty dummy `ANTHROPIC_API_KEY`, including streamed text, extended-thinking continuity, client tool calls and results, images, prompt-cache markers, token counting, cancellation, and Anthropic-shaped errors.

The OpenAI surface, router fairness, sandbox transports, terminal proof, backpressure, image routes, and observability redaction remain unchanged. Development and live verification must never stop, restart, or bind port `18787`; the implementation is exercised on another port.

## Evidence and Protocol Baseline

The compatibility target is the wire contract used by the local Claude Code source at `D:\MyWork\claude-code` (rebuilt 2.1.87 with `@anthropic-ai/sdk` 0.81.0) and the installed Claude Code executable used for final live verification.

The SDK sends Claude Code traffic to:

- `POST /v1/messages?beta=true`
- `POST /v1/messages/count_tokens?beta=true`

Stable SDK callers use the same paths without `?beta=true`. The beta SDK removes `betas` from the JSON body and serializes it into the `anthropic-beta` header. It also sends `anthropic-version`, `x-api-key` or bearer authorization, Claude Code session headers, and normal user-agent headers.

Claude Code's main request includes `model`, `messages`, `system`, `tools`, `tool_choice`, `metadata`, `max_tokens`, `thinking`, and optional `temperature`, `context_management`, `output_config`, `speed`, and prompt-cache markers. Its raw-stream consumer requires the standard Anthropic event sequence and consumes text, thinking, signature, and incremental tool JSON deltas.

The selected OpenAI Responses stateless pattern is documented by the official OpenAI API reference: use `store:false`, request `reasoning.encrypted_content`, and pass returned reasoning items back beside tool outputs. GhostProv already preserves arbitrary Responses fields and routes every body through the same terminal-proven relay.

Official references:

- Anthropic Messages: <https://platform.claude.com/docs/en/api/messages>
- Anthropic streaming: <https://platform.claude.com/docs/en/api/messages-streaming>
- Anthropic errors: <https://platform.claude.com/docs/en/api/errors>
- OpenAI Responses create: <https://developers.openai.com/api/reference/resources/responses/methods/create>
- OpenAI function calling: <https://developers.openai.com/api/docs/guides/function-calling>

## Considered Approaches

### 1. Same-process protocol adapter at the HTTP edge — selected

Register Anthropic routes in the existing Hono app. Pure request translators build OpenAI Responses bodies, the existing router executes them, and pure final/stream translators emit Anthropic objects and SSE.

Benefits:

- Both APIs share one port and one capacity pool.
- Existing cancellation, memory admission, sandbox selection, terminal replay, and backpressure remain authoritative.
- Translation can be tested without Prism or a live model.
- No Anthropic-specific behavior enters the sandbox transport protocols.

Tradeoff: fields with no OpenAI analogue require explicit compatibility semantics rather than verbatim forwarding.

### 2. Add arbitrary upstream paths to both sandbox transports — rejected

This could forward OpenAI's `/responses/input_tokens` and future native endpoints, but it would change V1, V2, daemon, sidecar, terminal-proof, and retry protocols merely to implement token counting. Claude Code already tolerates estimated token counts, and no other required Messages behavior needs a second upstream path.

### 3. Run a standalone Anthropic proxy beside GhostProv — rejected

This would simplify translation but violate the same-port requirement, create a second concurrency and cancellation boundary, and duplicate configuration. A forwarding server is allowed only as a disposable verification harness on a non-`18787` port; production remains integrated.

## Architecture

### Model mapping

Anthropic model names are accepted without requiring Claude Code to know GhostProv model IDs:

- names containing `opus` map to `gpt-5.6-sol`;
- names containing `sonnet` map to `gpt-5.6-terra`;
- names containing `haiku` map to `gpt-5.6-luna`;
- native GhostProv/OpenAI model IDs pass through unchanged;
- unknown Anthropic-looking model names use the configured default, `gpt-5.6-sol`.

`GP_ANTHROPIC_OPUS_MODEL`, `GP_ANTHROPIC_SONNET_MODEL`, `GP_ANTHROPIC_HAIKU_MODEL`, and `GP_ANTHROPIC_DEFAULT_MODEL` override the defaults. The Anthropic response echoes the requested model so Claude Code retains its own capability and display behavior.

### Focused modules

- `src/anthropic/types.ts`: permissive wire types for Messages, content blocks, events, and usage.
- `src/anthropic/request.ts`: validation, model selection, content/tool conversion, structured-output conversion, and Responses body construction.
- `src/anthropic/reasoning.ts`: versioned opaque encoding and strict decoding of OpenAI reasoning items inside Anthropic thinking signatures.
- `src/anthropic/response.ts`: non-streaming output conversion and a stateful Responses-SSE to Anthropic-SSE translator.
- `src/anthropic/token-count.ts`: deterministic conservative input-token estimate for messages, system blocks, tools, images, and documents.
- `src/anthropic/errors.ts`: Anthropic error type/status mapping and request IDs.
- `src/anthropic/routes.ts`: Hono route registration, body parsing, cancellation wiring, streaming, and keepalives.

`src/server/app.ts` only injects the existing `router.startRelay` dependency and options. The router and every sandbox runtime continue to receive ordinary Responses bodies.

## Request Mapping

Every Messages generation becomes a Responses request with:

- mapped `model`;
- `store:false`;
- `include:["reasoning.encrypted_content"]`;
- translated `input` items;
- top-level Anthropic `system` text joined into Responses `instructions`;
- `max_tokens` mapped to `max_output_tokens`;
- client tools mapped to Responses function tools;
- Anthropic tool choice mapped to `auto`, `required`, `none`, or a named function choice;
- `disable_parallel_tool_use` mapped to `parallel_tool_calls:false`;
- adaptive/enabled thinking and `output_config.effort` mapped to Responses reasoning effort with a summary request;
- disabled thinking mapped to `reasoning.effort:"none"`;
- `speed:"fast"` mapped to `service_tier:"fast"`;
- JSON-schema output format mapped to Responses `text.format`;
- compatible `temperature`, `top_p`, and metadata forwarded when present.

Unsupported beta hints such as context-edit policies, Anthropic server tools, `top_k`, and exact thinking token budgets are ignored rather than rejected when they do not affect Claude Code's client-tool loop. Unknown client content blocks or tool definitions receive a clear `invalid_request_error`; they are never silently stringified into misleading prompts.

### Message content

- User text becomes `input_text`.
- Assistant text becomes a completed assistant message with `output_text`.
- Base64 and URL images become `input_image` data/HTTP URLs.
- Plain-text and PDF documents become `input_text` or `input_file`.
- Assistant `tool_use` becomes a Responses `function_call` with the same Anthropic ID as `call_id`.
- User `tool_result` becomes `function_call_output`; text, image, and document result content remains structured where Responses accepts it.
- Prompt-cache annotations are accepted and removed from the wire conversion. Exact repeated prefixes still receive the upstream's normal automatic prompt caching.

## Reasoning and Tool-Loop Continuity

Tool loops are stateless and must not depend on an in-memory response cache. Each upstream request asks for encrypted reasoning. A completed OpenAI reasoning item is encoded into a versioned `ghostprov-r1.` base64url envelope and returned as an Anthropic thinking block signature. The visible Anthropic thinking text contains the available OpenAI reasoning summary.

When Claude Code sends that thinking block back on the next turn, the adapter validates and decodes only the supported reasoning shape and restores the exact Responses reasoning item before the related function call and tool output. Native Anthropic signatures or malformed GhostProv envelopes are ignored; visible assistant text and tool history remain usable. The encrypted payload is never logged.

Tool-use IDs are the upstream `call_id`, so Claude Code's `tool_result.tool_use_id` round-trips directly into `function_call_output.call_id`. This supports sequential and parallel tool calls without server-side affinity.

## Response and Streaming Mapping

### Non-streaming

The final Anthropic object contains:

- `id`, `type:"message"`, `role:"assistant"`, and the requested `model`;
- ordered `thinking`, `text`, and `tool_use` content blocks;
- `stop_reason` mapped to `tool_use`, `max_tokens`, `stop_sequence`, or `end_turn`;
- `stop_sequence:null` unless a proven sequence is available;
- Anthropic usage derived from Responses usage, including cached-input fields when available.

### Streaming

The translator emits valid `event:` and `data:` pairs in this order:

1. `message_start` with empty content, null stop values, and estimated input usage.
2. For each ordered output block, `content_block_start`, zero or more `content_block_delta` events, and `content_block_stop`.
3. `message_delta` with final stop reason and cumulative output usage.
4. `message_stop`.

Mappings include:

- `response.output_text.delta` -> `text_delta`;
- `response.reasoning_summary_text.delta` and compatible reasoning deltas -> `thinking_delta`;
- `response.function_call_arguments.delta` -> `input_json_delta`;
- completed reasoning encrypted content -> `signature_delta` before its block stops;
- completed function calls -> `tool_use` blocks;
- `response.completed` -> final usage/stop events.

The state machine tolerates event variants that omit `response.output_item.added` by opening blocks lazily from the first delta or final output. It emits every Anthropic block index once, never duplicates terminal events, and falls back to the authoritative final Responses object when an incremental event was absent.

SSE comments provide keepalives and are not exposed as data events. Client disconnects abort the same router request. Backpressure remains the existing `ReadableStream` plus Node response-adapter behavior.

## Token Counting

`POST /v1/messages/count_tokens` accepts the same input/system/tool shapes and returns `{input_tokens:number}`. Because GhostProv's sandbox relay exposes `/responses` only, the first release uses a deterministic conservative estimator based on normalized text, JSON tool schemas, media dimensions/bytes, and fixed per-item overhead. It is bounded, side-effect free, and close enough for Claude Code's context decisions; Claude Code already uses a rough local fallback when a provider cannot count exactly.

The implementation keeps the estimator behind one interface so a future transport-safe `/responses/input_tokens` operation can replace it without changing routes.

## Headers, Authentication, and Errors

For local compatibility, the adapter requires no secret by default, matching existing GhostProv routes. Claude Code still needs a non-empty `ANTHROPIC_API_KEY` so its SDK initializes; any value is accepted unless `GP_API_KEY` is configured by a future shared authentication layer.

`anthropic-version` is accepted, and both stable and `?beta=true` paths work. Unknown beta names are tolerated. Responses include a `request-id` header and propagate it into Anthropic error envelopes where applicable.

Before streaming, failures use Anthropic's shape:

```json
{"type":"error","error":{"type":"invalid_request_error","message":"..."},"request_id":"req_..."}
```

Status mapping preserves 400 validation, 401 authentication if configured, 404 route/model errors, 413 request size, 429 rate/capacity, 500 internal, 503 unavailable, and 529 overloaded semantics. After streaming begins, errors are emitted as Anthropic `error` SSE events and the stream closes.

## Testing and Live Verification

Test-first coverage includes:

- model aliases and environment overrides;
- system, text, assistant history, image, document, tool-use, and tool-result request conversion;
- thinking effort and encrypted-signature round trips;
- tool choice, parallel-tool control, JSON schema, speed, metadata, and cache-marker tolerance;
- deterministic token counts and malformed-body errors;
- non-streaming text/reasoning/tool output and usage conversion;
- exact streaming event order for text, thinking, one tool, parallel tools, lazy/missing incremental events, upstream errors, and cancellation;
- stable and `?beta=true` route behavior plus Anthropic headers and error envelopes;
- unchanged OpenAI Responses, WebSocket, image, router, V1, and V2 suites.

Live verification uses a disposable forwarding harness on a port other than `18787`. The harness uses the production Hono app and Anthropic adapter, but its `RouterLike` forwards translated Responses requests to the already-running `http://127.0.0.1:18787/v1/responses`. Therefore it exercises a real model and the complete adapter while neither restarting nor competing with the protected GhostProv process.

The installed Claude Code executable runs in `--bare --print` mode against the disposable port with a dummy key. Verification must prove:

1. a streamed text-only answer completes;
2. Claude Code invokes a real local tool, receives the result, and completes a second model turn;
3. a prompt that permits independent work can complete parallel tool calls;
4. the tool loop preserves reasoning continuity without API validation errors;
5. direct stable/beta non-streaming and token-count requests return valid Anthropic JSON;
6. malformed requests return Anthropic errors;
7. the harness is stopped without touching PID/listener `18787`.

## Completion Criteria

- OpenAI and Anthropic routes coexist in one app and share one router.
- Claude Code 2.1.87 request shapes and the installed Claude Code executable are supported.
- Streaming text, thinking, client tool calls/results, parallel tools, images/documents, token counting, cancellation, and errors have automated coverage.
- Encrypted reasoning and tool IDs survive stateless multi-turn tool loops.
- Tiered default model mapping is implemented and documented.
- A real Claude Code tool-use run succeeds through a non-`18787` verification listener backed by the protected live GhostProv instance.
- Port `18787` is never stopped, restarted, rebound, or killed.
- Full tests, daemon tests, typecheck, production audit, and `git diff --check` pass.

