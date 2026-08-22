# GhostProv Images Compatibility Design

Date: 2026-08-22

## Objective

Add a production-grade OpenAI Images compatibility surface to GhostProv without changing the upstream contract. The upstream available to the Prism sandbox accepts only `POST /responses`; it rewrites native image, audio, and Chat Completions paths to that handler. GhostProv must therefore translate supported native-shaped image requests into Responses image-generation tool calls locally and translate their results back into Images API-shaped responses.

This work also hardens upstream error propagation and makes segmented v6.3 transport the default for large request bodies. It preserves the existing global concurrency scheduler, terminal-proof lane lifecycle, Codex WebSocket behavior, and the live port-8787 process.

## Evidence and Scope

Live probes against `http://localhost:8787/v1` established the following:

- A Responses request using `gpt-5.6-sol` with an `image_generation` tool using `gpt-image-2` completed successfully and returned `image_generation_call.result` as base64 PNG data.
- Reference-image editing using an `input_image` data URL and `action: "edit"` completed successfully.
- Mask editing using `input_image_mask.image_url` completed successfully.
- The Responses image tool rejects `tools[0].n`; native `n` support therefore requires bounded fan-out.
- `reasoning: { effort: "max", mode: "pro", summary: "concise" }` completed successfully and was preserved by the response.
- `gpt-audio-1.5` is rejected because that model is not supported by the Responses API.
- A valid WAV `input_audio` content part is rejected with `Audio input is not available.`
- Project-file upload currently fails at the Prism boundary with `Missing file id`; image edits must use data URLs rather than unproven project file IDs.

Implemented scope:

- `POST /v1/images/generations` for non-streaming `gpt-image-2` generation.
- `POST /v1/images/edits` for non-streaming multipart reference editing, including multiple reference images and an optional PNG mask.
- Native `n` values from 1 through 10 through concurrent, globally scheduled Responses calls.
- GPT Image response fields: base64 image data, creation time, background, output format, quality, size, revised prompt when present, and aggregated image-tool usage.
- Clear OpenAI-shaped validation and upstream errors with appropriate HTTP status codes.
- Existing Responses reasoning fields remain verbatim and receive explicit regression coverage.
- Large payloads use the existing segmented v6.3 job transport by default unless explicitly disabled.

Deferred scope:

- Realtime, WebRTC, VAD, live translation, and live voice remain deferred by user direction.
- Transcription, recorded-audio translation, and speech generation are reported as unsupported because the only available upstream handler rejects the required audio capabilities.
- Image streaming is rejected explicitly in this compatibility release. The underlying Responses SSE event model differs from Images API SSE, and it requires a separate, independently verified design.
- Image variations are not added. Current GPT Image editing subsumes the requested workflow, while the native variation endpoint is a DALL-E 2 surface.
- URL image responses are not emulated. GPT Image models return base64 data; manufacturing temporary URLs would add storage and lifecycle semantics that the upstream does not provide.

## Considered Approaches

### 1. Local Images-to-Responses adapters — selected

GhostProv parses and validates the native request, builds one Responses image-tool body per requested image, executes those bodies through the existing global scheduler, and aggregates the results.

Benefits:

- Uses the only live-proven upstream contract.
- Shares the existing 30-lane scheduler, terminal replay, and release guarantees.
- Requires no daemon endpoint routing or native multipart support upstream.
- Keeps compatibility logic isolated and unit-testable.

Tradeoff: multipart files become base64 data URLs, increasing request size. The adapter applies explicit size limits and relies on segmented v6.3 transport above 512 KiB.

### 2. Generic native-path forwarding — rejected

This cannot work because arbitrary upstream paths are rewritten to the Responses handler. The observed Responses-style validation errors from nonexistent and native paths prove that path forwarding cannot expose native image or audio APIs.

### 3. New daemon-native endpoint and sidecar protocol — rejected for this scope

A daemon-side native router would still call the same Responses-only upstream. A sidecar binary transport could reduce host-to-sandbox JSON expansion, but it would substantially expand protocol complexity without unlocking a new upstream capability. The existing segmented transport is adequate for the bounded image-edit payload supported here.

## Architecture

### Compatibility module

Create a focused image compatibility module with pure boundaries:

- Parse and validate JSON generation requests.
- Parse and validate multipart edit requests.
- Convert image and mask files to validated data URLs.
- Build Responses bodies without mutating caller input.
- Extract exactly one completed image-generation result per relay response.
- Aggregate results and usage into an Images API-shaped response.
- Produce typed API errors that carry HTTP status, error type, parameter, and code.

The module receives a relay starter dependency. Tests can therefore use deterministic fake relays without starting Playwright, Yjs, the daemon, or an HTTP server.

### HTTP integration

`src/server.ts` remains the composition root. It registers the two routes and injects `startScheduledRelay`, ensuring every generated image consumes one normal scheduler lease until its authoritative final response settles.

The main server request collector enforces a configurable maximum request-body size before buffering an entire multipart edit. This prevents unbounded memory growth. The default edit-input budget is 40 MiB total, below the daemon's 64 MiB assembled-job ceiling after base64 and JSON expansion. A client that exceeds the configured limit receives HTTP 413 before relay dispatch.

### Error normalization

Introduce shared HTTP error normalization instead of relying on `instanceof RelayError`. The slot path throws `SlotError`, so the current code incorrectly converts upstream HTTP 400 failures into HTTP 500 `internal` responses.

The normalizer will:

- Recognize any relay error with a supported `kind` property.
- Parse daemon errors shaped as `{code:"http_400", message:"<upstream JSON>"}`.
- Preserve the upstream OpenAI error object when valid.
- Map upstream 4xx to the corresponding HTTP status.
- Map exhaustion/rate limiting to 429, missing bridge to 503, timeout to 504, and transport/internal failures to 502 or 500 as appropriate.
- Never expose private daemon protocol fields.

Both `/v1/responses` and Images compatibility routes use the same normalizer.

## Request Mapping

### Generations

Input fields:

- Required: `prompt`.
- Supported: `model`, `n`, `quality`, `size`, `background`, `output_format`, `output_compression`, `moderation`, `user`, and `response_format` when absent or `b64_json`.
- Rejected: `stream: true`, `response_format: "url"`, DALL-E-only fields, unsupported model names, transparent background for `gpt-image-2`, invalid enum/range values, and unknown body shapes that would make compatibility ambiguous.

Each fan-out body uses:

- Responses model `gpt-5.6-sol` by default, configurable through `GP_IMAGE_RESPONSES_MODEL`.
- `store: false`.
- The prompt as Responses input.
- One forced `image_generation` tool with `model: "gpt-image-2"` and mapped image options.
- Optional reasoning/service-tier configuration only through server configuration, not invented from native image fields.

### Edits

Input fields:

- Required: `prompt` and one or more `image` files.
- Optional: `mask`, `model`, `n`, `quality`, `size`, `background`, `output_format`, `output_compression`, `input_fidelity`, `user`, and non-streaming controls.
- Up to 16 PNG, JPEG, or WebP reference images.
- Optional PNG mask up to 4 MiB.

The adapter verifies declared content type, filename extension, and magic bytes. It converts files to data URLs and builds one user message containing the prompt followed by `input_image` parts. The tool uses `action: "edit"`; an optional mask is mapped to `input_image_mask.image_url`.

## Concurrency and Failure Semantics

Native `n` is implemented as concurrent fan-out because the Responses image tool rejects `n`. All child operations enter the existing FIFO global scheduler; no second independent capacity pool is introduced.

For one request:

- All `n` child bodies are submitted together for maximum image throughput.
- Global scheduler capacity remains the hard process-wide ceiling across HTTP Responses, Codex WebSockets, image generations, and image edits.
- Results are returned in submission order, independent of completion order.
- The handler waits for all submitted operations to settle, preventing orphaned promise failures.
- If any child fails, the whole native request fails with the first deterministic error after all already-submitted children settle. Scheduler leases remain held until each child's proven terminal.
- Downstream aborts remove queued children through the scheduler signal. Already-published upstream work follows the existing terminal-proof quarantine rules and is never released early.

## Transport and Daemon

No new daemon endpoint is required. The daemon continues to accept only Responses jobs.

Segmented transport becomes enabled by default for bodies above 512 KiB when the daemon advertises a supported v6 protocol. `GP_SEGMENT=0` remains an explicit emergency opt-out. Gzip remains enabled by default, although image base64 is expected to compress poorly; segmentation still prevents monolithic Yjs truncation.

The adapter's default 40 MiB raw edit-input budget keeps the assembled JSON request below the daemon's default 64 MiB job limit with safety margin. This limit is configurable but must not silently exceed the daemon's configured maximum.

## Testing

Test-first coverage will include:

- Generation request validation and exact Responses mapping.
- Edit multipart parsing, file ordering, data URLs, mask mapping, and magic-byte validation.
- Rejection of audio routes/capabilities, streaming, URL response format, unsupported models, invalid sizes, transparent `gpt-image-2` backgrounds, and oversized requests.
- `n` fan-out starts concurrently, preserves result order, shares the injected scheduler, and aggregates usage.
- Partial failure waits for all submitted work and returns a normalized error.
- Responses upstream 400 errors preserve status and OpenAI error JSON instead of becoming HTTP 500 internal errors.
- Reasoning effort/mode/context/summary and service tier remain verbatim.
- Segment transport defaults on for large bodies and retains the explicit opt-out.
- Existing SSE, terminal rescue, WebSocket, daemon, and concurrency suites remain green.

Verification commands:

```text
bun test --isolate
bun run test:daemon
bun run typecheck
bun audit --production
git diff --check
```

Live verification uses the existing port-8787 process without restarting it while this Codex session depends on it. Source changes are verified with unit/integration tests; the new routes become live after the user's next safe GhostProv restart. Direct `/v1/responses` live probes remain available to verify the underlying image and reasoning capabilities before that restart.

## Completion Criteria

- Both image compatibility endpoints are implemented and documented.
- Supported generation, reference edit, and mask edit mappings have automated coverage.
- Native `n` uses bounded global concurrency and preserves ordering.
- Upstream 4xx errors no longer surface as HTTP 500 internal errors.
- Reasoning modes remain fully preserved.
- Audio limitations are explicit and never simulated.
- No unproven lane-release behavior is reintroduced.
- All verification commands pass.
- Changes are committed and pushed directly to `origin/master`.
