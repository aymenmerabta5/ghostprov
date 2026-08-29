# GhostProv Images Compatibility Implementation Plan

> **Historical and superseded continuation policy (2026-08-28):** This document preserves the design record as written. Any upstream `store:false`, stateless replay, or local generated-history requirement below is superseded by the active `stream:true`/`store:true` persisted-continuation policy in `README.md` and `AGENTS.md`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add non-streaming OpenAI Images generation/edit compatibility over GhostProv's Responses-only relay, with concurrent `n` fan-out, accurate upstream errors, safe large-body transport, and explicit unsupported audio behavior.

**Architecture:** Pure image request parsers and translators build one Responses image-tool body per output image. A batch executor runs those bodies through the existing global scheduler and aggregates results in Images API shape. Shared HTTP error normalization handles both Responses and image routes, while the existing daemon remains Responses-only.

**Tech Stack:** Bun 1.4.0, TypeScript 7.0.2, Hono 4.6, Bun test with `--isolate`, existing Responses relay and segmented v6.3 daemon protocol.

## Global Constraints

- Do not restart or stop the live GhostProv process on port 8787 while this Codex session depends on it.
- Do not close the existing Chrome/Prism window.
- Use no subagents.
- Keep one FIFO global scheduler across HTTP Responses, Codex WebSockets, image generations, and image edits.
- Preserve terminal-proof lane release and authoritative SSE replay behavior.
- Support only live-proven `gpt-image-2` and `gpt-image-2-2026-04-21` image models in the compatibility routes.
- Return base64 image data; do not emulate temporary URLs.
- Explicitly reject image streaming in this release.
- Explicitly report audio capabilities as unavailable; never simulate them.
- Default total raw edit input is 40 MiB and remains configurable.
- Use Bun 1.4.0, `bun test --isolate`, and TypeScript 7.0.2.

---

### Task 1: Normalize API and relay errors

**Files:**
- Create: `src/http/errors.ts`
- Modify: `src/server.ts`
- Test: `tests/http-errors.test.ts`

**Interfaces:**
- Produces: `ApiError`, `apiErrorResponse(error: unknown): Response`, and `apiErrorEvent(error: unknown): {type:"error"; error:{...}}`.
- Consumes: relay errors carrying a string `kind` property and daemon upstream messages containing `{code:"http_<status>",message:"<JSON>"}`.

- [ ] **Step 1: Write failing tests for local validation, slot upstream 400, exhaustion, and missing bridge**

```ts
import { describe, expect, it } from "bun:test";
import { ApiError, apiErrorResponse } from "../src/http/errors";

describe("apiErrorResponse", () => {
  it("preserves ApiError status and OpenAI shape", async () => {
    const response = apiErrorResponse(new ApiError(400, "prompt is required", {
      param: "prompt", code: "missing_required_parameter",
    }));
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ error: {
      message: "prompt is required", type: "invalid_request_error",
      param: "prompt", code: "missing_required_parameter",
    }});
  });

  it("unwraps daemon http_400 errors from SlotError-shaped values", async () => {
    const inner = JSON.stringify({ error: { message: "Unknown parameter", type: "invalid_request_error", param: "tools[0].n", code: "unknown_parameter" }});
    const outer = JSON.stringify({ code: "http_400", message: inner });
    const response = apiErrorResponse(Object.assign(new Error(`upstream failed: ${outer}`), { kind: "upstream" }));
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ error: JSON.parse(inner).error });
  });

  it("maps exhausted to 429 and no-bridge to 503", () => {
    expect(apiErrorResponse(Object.assign(new Error("full"), { kind: "exhausted" })).status).toBe(429);
    expect(apiErrorResponse(Object.assign(new Error("missing"), { kind: "no-bridge" })).status).toBe(503);
  });
});
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `bun test --isolate tests/http-errors.test.ts`

Expected: FAIL because `src/http/errors.ts` does not exist.

- [ ] **Step 3: Implement typed errors and nested upstream parsing**

```ts
export interface ApiErrorOptions {
  type?: string;
  param?: string | null;
  code?: string | null;
}

export class ApiError extends Error {
  constructor(readonly status: number, message: string, readonly options: ApiErrorOptions = {}) {
    super(message);
    this.name = "ApiError";
  }
}

export function apiErrorResponse(error: unknown): Response {
  const normalized = normalizeApiError(error);
  return Response.json({ error: normalized.error }, { status: normalized.status });
}
```

Implement `normalizeApiError` to parse the outer daemon JSON, then its inner OpenAI JSON, and apply the exact status mapping from the design.

- [ ] **Step 4: Replace `/v1/responses` non-streaming `instanceof RelayError` handling**

```ts
} catch (error) {
  return apiErrorResponse(error);
}
```

For streaming errors, emit the normalized OpenAI error object without attempting to change the already-sent HTTP status.

- [ ] **Step 5: Run focused and existing HTTP tests**

Run: `bun test --isolate tests/http-errors.test.ts tests/http-streaming.test.ts tests/websocket.test.ts`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```text
git add src/http/errors.ts src/server.ts tests/http-errors.test.ts
git commit -m "fix: preserve upstream OpenAI errors"
```

---

### Task 2: Parse and translate image generation requests

**Files:**
- Create: `src/images/compat.ts`
- Modify: `src/relay/types.ts`
- Test: `tests/images-compat.test.ts`

**Interfaces:**
- Produces: `parseGenerationRequest(value: unknown, options: ImageCompatOptions): ParsedImageRequest`.
- Produces: `buildGenerationBodies(request: ParsedImageRequest): Record<string, unknown>[]`.
- Produces: `ImageCompatOptions { responsesModel: string; maxEditInputBytes: number }`.
- Extends: `ImageGenerationTool` with `action`, `input_fidelity`, and `input_image_mask`.

- [ ] **Step 1: Write failing generation mapping and validation tests**

```ts
it("fans native n out into independent Responses bodies", () => {
  const parsed = parseGenerationRequest({
    prompt: "a red circle", model: "gpt-image-2", n: 3,
    quality: "low", size: "1024x1024", output_format: "png",
    background: "opaque", response_format: "b64_json",
  }, OPTIONS);
  const bodies = buildGenerationBodies(parsed);
  expect(bodies).toHaveLength(3);
  expect(bodies[0]).toMatchObject({
    model: "gpt-5.6-sol", store: false, input: "a red circle",
    tool_choice: { type: "image_generation" },
    tools: [{ type: "image_generation", model: "gpt-image-2", quality: "low", size: "1024x1024", output_format: "png", background: "opaque" }],
  });
  expect((bodies[0].tools as any[])[0]).not.toHaveProperty("n");
});

it.each([
  [{ prompt: "x", stream: true }, "stream"],
  [{ prompt: "x", response_format: "url" }, "response_format"],
  [{ prompt: "x", background: "transparent" }, "background"],
  [{ prompt: "x", n: 11 }, "n"],
])("rejects unsupported generation input %#", (body, param) => {
  expect(() => parseGenerationRequest(body, OPTIONS)).toThrow(expect.objectContaining({ options: expect.objectContaining({ param }) }));
});
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `bun test --isolate tests/images-compat.test.ts`

Expected: FAIL because the compatibility functions do not exist.

- [ ] **Step 3: Implement strict parsing and gpt-image-2 size validation**

```ts
const IMAGE_MODELS = new Set(["gpt-image-2", "gpt-image-2-2026-04-21"]);
const OUTPUT_FORMATS = new Set(["png", "jpeg", "webp"]);
const QUALITIES = new Set(["low", "medium", "high", "auto"]);

function parseCount(value: unknown): number {
  const count = value == null ? 1 : Number(value);
  if (!Number.isInteger(count) || count < 1 || count > 10) {
    throw invalid("n must be an integer between 1 and 10", "n", "invalid_value");
  }
  return count;
}
```

Validate arbitrary `WIDTHxHEIGHT` sizes as multiples of 16, aspect ratio 1:3 through 3:1, maximum edge 3840, and maximum pixels `3840 * 2160`; also allow `auto`.

- [ ] **Step 4: Build one tool body per image without forwarding `n`**

```ts
return Array.from({ length: request.n }, () => ({
  model: request.responsesModel,
  store: false,
  input: request.prompt,
  tools: [{ type: "image_generation", model: request.model, ...request.toolOptions }],
  tool_choice: { type: "image_generation" },
}));
```

- [ ] **Step 5: Run the focused tests**

Run: `bun test --isolate tests/images-compat.test.ts`

Expected: generation tests pass.

- [ ] **Step 6: Commit**

```text
git add src/images/compat.ts src/relay/types.ts tests/images-compat.test.ts
git commit -m "feat: translate Images generation requests"
```

---

### Task 3: Parse multipart image edits and masks

**Files:**
- Modify: `src/images/compat.ts`
- Test: `tests/images-compat.test.ts`

**Interfaces:**
- Produces: `parseEditRequest(form: FormData, options: ImageCompatOptions): Promise<ParsedImageRequest>`.
- Produces: `buildEditBodies(request: ParsedImageRequest): Record<string, unknown>[]`.
- Consumes: `ApiError` from Task 1 and shared option/value parsers from Task 2.

- [ ] **Step 1: Write failing multipart mapping tests**

```ts
it("maps ordered reference images and a PNG mask to Responses edit input", async () => {
  const form = new FormData();
  form.append("prompt", "make it blue");
  form.append("image", new File([PNG], "one.png", { type: "image/png" }));
  form.append("image", new File([JPEG], "two.jpg", { type: "image/jpeg" }));
  form.append("mask", new File([PNG], "mask.png", { type: "image/png" }));
  form.append("input_fidelity", "high");
  const parsed = await parseEditRequest(form, OPTIONS);
  const [body] = buildEditBodies(parsed);
  expect((body.input as any[])[0].content.map((part: any) => part.type)).toEqual([
    "input_text", "input_image", "input_image",
  ]);
  expect((body.tools as any[])[0]).toMatchObject({
    action: "edit", input_fidelity: "high",
    input_image_mask: { image_url: expect.stringMatching(/^data:image\/png;base64,/) },
  });
});
```

Add cases for missing prompt/image, more than 16 images, unsupported MIME/magic bytes, mask not PNG, mask over 4 MiB, and total images over `maxEditInputBytes`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `bun test --isolate tests/images-compat.test.ts`

Expected: edit tests fail because the edit parser is absent.

- [ ] **Step 3: Implement signature-aware file conversion**

```ts
async function fileToImage(file: Blob & { name?: string }, total: ByteBudget): Promise<InputImage> {
  const bytes = new Uint8Array(await file.arrayBuffer());
  const mime = detectImageMime(bytes, file.type, file.name ?? "image");
  total.add(bytes.byteLength);
  return { type: "input_image", image_url: `data:${mime};base64,${Buffer.from(bytes).toString("base64")}`, detail: "auto" };
}
```

Require PNG signature `89 50 4e 47`, JPEG `ff d8 ff`, or WebP `RIFF....WEBP`. Validate mask independently as PNG and do not include it as a normal reference image.

- [ ] **Step 4: Build edit bodies using shared immutable content**

```ts
const input = [{ role: "user", content: [
  { type: "input_text", text: request.prompt },
  ...request.images,
] }];
return Array.from({ length: request.n }, () => ({
  model: request.responsesModel, store: false, input,
  tools: [{ type: "image_generation", model: request.model, action: "edit", ...request.toolOptions }],
  tool_choice: { type: "image_generation" },
}));
```

- [ ] **Step 5: Run focused tests**

Run: `bun test --isolate tests/images-compat.test.ts`

Expected: all generation and edit parsing tests pass.

- [ ] **Step 6: Commit**

```text
git add src/images/compat.ts tests/images-compat.test.ts
git commit -m "feat: translate multipart image edits"
```

---

### Task 4: Execute concurrent image batches and aggregate responses

**Files:**
- Create: `src/images/service.ts`
- Test: `tests/images-service.test.ts`

**Interfaces:**
- Consumes: `RelayStarter`-compatible `(body, signal?) => Promise<RunHandle>`.
- Produces: `executeImageBatch(bodies, startRelay, signal): Promise<ImagesResponse>`.
- Produces: deterministic result ordering and summed image-tool usage.

- [ ] **Step 1: Write failing concurrency, ordering, usage, and partial-failure tests**

```ts
it("starts all images concurrently but returns submission order", async () => {
  const gates = [deferred(), deferred(), deferred()];
  const started: number[] = [];
  const pending = executeImageBatch([{ index: 0 }, { index: 1 }, { index: 2 }], async body => {
    const index = body.index as number;
    started.push(index);
    return handle(gates[index].promise);
  });
  await Promise.resolve();
  expect(started).toEqual([0, 1, 2]);
  gates[2].resolve(imageFinal("C"));
  gates[0].resolve(imageFinal("A"));
  gates[1].resolve(imageFinal("B"));
  expect((await pending).data.map(image => image.b64_json)).toEqual(["A", "B", "C"]);
});

it("waits for all submitted children before rejecting", async () => {
  const slow = deferred();
  let slowSettled = false;
  const pending = executeImageBatch([{ index: 0 }, { index: 1 }], async body =>
    handle(body.index === 0 ? Promise.reject(upstream400()) : slow.promise.finally(() => { slowSettled = true; })),
  );
  await Promise.resolve();
  expect(slowSettled).toBe(false);
  slow.resolve(imageFinal("B"));
  await expect(pending).rejects.toBeDefined();
  expect(slowSettled).toBe(true);
});
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `bun test --isolate tests/images-service.test.ts`

Expected: FAIL because the service does not exist.

- [ ] **Step 3: Implement all-settled execution and strict result extraction**

```ts
const settled = await Promise.allSettled(bodies.map(async body => {
  const handle = await startRelay(body, signal);
  return handle.final;
}));
const rejected = settled.find((result): result is PromiseRejectedResult => result.status === "rejected");
if (rejected) throw rejected.reason;
const finals = settled.map(result => (result as PromiseFulfilledResult<any>).value);
```

Require exactly one completed `image_generation_call` with a non-empty `result` per final response. Sum `tool_usage.image_gen` numeric fields and nested token details without mutating upstream objects.

- [ ] **Step 4: Run focused tests**

Run: `bun test --isolate tests/images-service.test.ts`

Expected: all service tests pass.

- [ ] **Step 5: Commit**

```text
git add src/images/service.ts tests/images-service.test.ts
git commit -m "feat: execute concurrent image batches"
```

---

### Task 5: Register HTTP routes and enforce edit-body bounds

**Files:**
- Create: `src/http/body.ts`
- Modify: `src/config.ts`
- Modify: `src/server.ts`
- Test: `tests/images-http.test.ts`
- Test: `tests/http-body.test.ts`

**Interfaces:**
- Produces: `collectRequestBody(chunks: AsyncIterable<Uint8Array>, maxBytes: number): Promise<Buffer>`.
- Registers: `POST /v1/images/generations`, `POST /v1/images/edits`, explicit unsupported image variation/audio routes.
- Consumes: parsers from `src/images/compat.ts`, executor from `src/images/service.ts`, and `apiErrorResponse`.

- [ ] **Step 1: Write failing Hono route tests with an injected relay starter**

```ts
it("returns Images API JSON for a generation", async () => {
  const app = createApp({ startRelay: async () => handle(Promise.resolve(imageFinal("PNGDATA"))) });
  const response = await app.request("/v1/images/generations", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ prompt: "a circle", model: "gpt-image-2" }),
  });
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({ data: [{ b64_json: "PNGDATA" }] });
});

it("returns 501 for audio routes", async () => {
  const app = createApp({ startRelay: failIfCalled });
  expect((await app.request("/v1/audio/speech", { method: "POST" })).status).toBe(501);
});
```

Refactor the exported app construction to `createApp(dependencies?)`, while retaining `export const app = createApp()` for existing imports.

- [ ] **Step 2: Write a failing streaming body-budget test**

```ts
it("stops collecting as soon as the configured limit is exceeded", async () => {
  async function* chunks() { yield new Uint8Array(4); yield new Uint8Array(4); }
  await expect(collectRequestBody(chunks(), 6)).rejects.toMatchObject({ status: 413 });
});
```

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/images-http.test.ts tests/http-body.test.ts`

Expected: FAIL because app injection, routes, and bounded collection are absent.

- [ ] **Step 4: Implement config and route composition**

```ts
const imageOptions = {
  responsesModel: config.imageResponsesModel,
  maxEditInputBytes: config.imageEditMaxInputBytes,
};

app.post("/v1/images/generations", async c => {
  try {
    const parsed = parseGenerationRequest(await c.req.json(), imageOptions);
    return c.json(await executeImageBatch(buildGenerationBodies(parsed), deps.startRelay, c.req.raw.signal));
  } catch (error) {
    return apiErrorResponse(error);
  }
});
```

The edit route calls `await c.req.formData()`, `parseEditRequest`, `buildEditBodies`, and the same executor. Unsupported audio/variation routes throw `ApiError(501, ..., {type:"unsupported_feature", code:"unsupported_feature"})`.

- [ ] **Step 5: Use bounded collection only for multipart image edits**

```ts
const bodyLimit = requestPath(req.url) === "/v1/images/edits"
  ? config.imageEditHttpMaxBodyBytes
  : Number.POSITIVE_INFINITY;
body = await collectRequestBody(req, bodyLimit);
```

Reject an oversized declared `content-length` before reading, and use `collectRequestBody` for chunked requests.

- [ ] **Step 6: Run route, body, and existing integration tests**

Run: `bun test --isolate tests/images-http.test.ts tests/http-body.test.ts tests/http-streaming.test.ts tests/files.test.ts tests/websocket.test.ts`

Expected: all tests pass.

- [ ] **Step 7: Commit**

```text
git add src/http/body.ts src/config.ts src/server.ts tests/images-http.test.ts tests/http-body.test.ts
git commit -m "feat: expose Images compatibility routes"
```

---

### Task 6: Default large jobs to segmented v6.3 and lock reasoning passthrough

**Files:**
- Modify: `src/relay/segments.ts`
- Modify: `src/relay/slots.ts`
- Modify: `tests/segments.test.ts`
- Modify: `tests/bridge.test.ts`

**Interfaces:**
- Produces: `isSegmentTransportEnabled(value?: string): boolean`.
- Preserves: verbatim reasoning and service-tier fields.

- [ ] **Step 1: Write failing default/opt-out tests**

```ts
it("enables segmented transport by default with an explicit zero opt-out", () => {
  expect(isSegmentTransportEnabled(undefined)).toBe(true);
  expect(isSegmentTransportEnabled("1")).toBe(true);
  expect(isSegmentTransportEnabled("0")).toBe(false);
});
```

Add a bridge body assertion for `{reasoning:{effort:"max",mode:"pro",context:"all_turns",summary:"concise"},service_tier:"fast"}`.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `bun test --isolate tests/segments.test.ts tests/bridge.test.ts`

Expected: FAIL because the segment enablement helper is absent.

- [ ] **Step 3: Implement the default-on helper and use it in the slot path**

```ts
export function isSegmentTransportEnabled(value = process.env.GP_SEGMENT): boolean {
  return value !== "0";
}
```

Replace `process.env.GP_SEGMENT === "1"` with `isSegmentTransportEnabled()` and update the comment to state that `GP_SEGMENT=0` is the opt-out.

- [ ] **Step 4: Run focused tests**

Run: `bun test --isolate tests/segments.test.ts tests/bridge.test.ts`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```text
git add src/relay/segments.ts src/relay/slots.ts tests/segments.test.ts tests/bridge.test.ts
git commit -m "feat: default large jobs to segmented transport"
```

---

### Task 7: Document, live-probe through port 8787, and verify the repository

**Files:**
- Modify: `README.md`
- Modify: `scripts/live_verify.ts`
- Test: `tests/documentation.test.ts`

**Interfaces:**
- Documents exact supported and unsupported API surfaces.
- Adds optional live image verification that can call the existing `/v1/responses` process without restarting it.

- [ ] **Step 1: Write failing documentation assertions**

```ts
expect(readme).toContain("POST /v1/images/generations");
expect(readme).toContain("POST /v1/images/edits");
expect(readme).toContain("Audio input is not available through the configured Responses-only upstream");
expect(readme).toContain("GP_SEGMENT=0");
```

- [ ] **Step 2: Run documentation test and verify RED**

Run: `bun test --isolate tests/documentation.test.ts`

Expected: FAIL because the new surface is undocumented.

- [ ] **Step 3: Document curl examples, limits, concurrency, and capability boundaries**

```text
curl.exe http://127.0.0.1:8787/v1/images/generations ^
  -H "Content-Type: application/json" ^
  -d "{\"model\":\"gpt-image-2\",\"prompt\":\"a red circle\",\"quality\":\"low\"}"

curl.exe http://127.0.0.1:8787/v1/images/edits ^
  -F "model=gpt-image-2" -F "prompt=make it blue" -F "image=@input.png"
```

State that native `n` fans out through the shared scheduler, image streaming and URL responses are rejected, and audio endpoints return 501 because live upstream probes reject both the audio model and audio input.

- [ ] **Step 4: Add an opt-in live image probe**

Keep the default live verifier inexpensive. When `GP_LIVE_IMAGE=1`, import the compatibility parser/builder, execute one low-quality request through `http://localhost:8787/v1/responses`, and assert a non-empty `image_generation_call.result` without printing base64.

- [ ] **Step 5: Run complete verification**

Run:

```text
bun test --isolate
bun run test:daemon
bun run typecheck
bun audit --production
git diff --check
```

Expected:

- Bun tests: zero failures.
- Daemon tests: zero failures.
- TypeScript: no diagnostics.
- Production audit: no vulnerabilities.
- Diff check: no whitespace errors.

- [ ] **Step 6: Run the live verifier without restarting port 8787**

Run: `bun run verify:live`

Expected: health, text Responses, SSE, and WebSocket checks pass against the existing process. Do not expect the newly registered native routes to exist until the next safe process restart.

Run one adapter-level live image check using the new builder and the existing `/v1/responses` endpoint. Assert only status, result type, and non-empty base64 length.

- [ ] **Step 7: Commit and push**

```text
git add README.md scripts/live_verify.ts tests/documentation.test.ts
git commit -m "docs: document Images compatibility"
git push origin master
```

- [ ] **Step 8: Confirm remote state and shut down only after success**

Run `git status --short --branch` and `git log -1 --oneline --decorate`; require a clean worktree and `master` aligned with `origin/master`.

After every completion criterion is satisfied and the push is confirmed, issue the explicitly authorized Windows shutdown command. Do not stop port 8787 separately before shutdown.
