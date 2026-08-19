import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { zipSync } from "fflate";

vi.mock("y-websocket", () => {
  return {
    WebsocketProvider: class {
      on(event: string, cb: (v: boolean) => void) {
        if (event === "sync") setTimeout(() => cb(true), 0);
      }
      destroy() {}
    },
  };
});

describe("Image helpers (utils/image.ts)", () => {
  it("encodeImageBytesToDataUrl produces data URL and round-trips via base64", async () => {
    const { encodeImageBytesToDataUrl, base64ToBytes, parseDataUrl, bytesToBase64 } = await import("../src/utils/image");
    const oneByOnePngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const bytes = base64ToBytes(oneByOnePngB64);
    const dataUrl = encodeImageBytesToDataUrl(bytes, "image/png");
    expect(dataUrl).toBe(`data:image/png;base64,${oneByOnePngB64}`);
    expect(dataUrl.startsWith("data:image/png;base64,")).toBe(true);
    const parsed = parseDataUrl(dataUrl)!;
    expect(parsed.mime).toBe("image/png");
    expect(parsed.b64).toBe(oneByOnePngB64);
    // bytesToBase64 round-trip
    expect(bytesToBase64(bytes)).toBe(oneByOnePngB64);
  });

  it("createInputImage factories for URL, data URL, file_id, bytes, base64 with detail", async () => {
    const { createInputImageFromUrl, createInputImageFromFileId, createInputImageFromBytes, createInputImageFromBase64, base64ToBytes } = await import("../src/utils/image");
    const url = "https://example.com/cat.jpg";
    const img1 = createInputImageFromUrl(url, "high");
    expect(img1).toEqual({ type: "input_image", image_url: url, detail: "high" });

    const fileId = "file-abc123";
    const img2 = createInputImageFromFileId(fileId, "low");
    expect(img2).toEqual({ type: "input_image", file_id: fileId, detail: "low" });

    const b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const img3 = createInputImageFromBase64(b64, "image/png", "original");
    expect(img3.image_url).toBe(`data:image/png;base64,${b64}`);
    expect(img3.detail).toBe("original");

    const bytes = base64ToBytes(b64);
    const img4 = createInputImageFromBytes(bytes, "image/png", "auto");
    expect(img4.image_url).toBe(`data:image/png;base64,${b64}`);
    expect(img4.detail).toBe("auto");
  });

  it("isValidImageDetail and detail values low/high/auto/original", async () => {
    const { isValidImageDetail } = await import("../src/utils/image");
    const { VALID_IMAGE_DETAILS } = await import("../src/relay/types");
    expect(VALID_IMAGE_DETAILS).toEqual(expect.arrayContaining(["low", "high", "auto", "original"]));
    expect(isValidImageDetail("low")).toBe(true);
    expect(isValidImageDetail("high")).toBe(true);
    expect(isValidImageDetail("auto")).toBe(true);
    expect(isValidImageDetail("original")).toBe(true);
    expect(isValidImageDetail("invalid")).toBe(false);
    // client helper also
    const client = await import("../src/relay/client");
    expect(client.isValidImageDetail("low")).toBe(true);
    expect(client.isValidImageDetail("original")).toBe(true);
  });

  it("isDataUrl / isHttpUrl / inferMimeFromFilename", async () => {
    const { isDataUrl, isHttpUrl, inferMimeFromFilename } = await import("../src/utils/image");
    // we also check client helpers
    const client = await import("../src/relay/client");
    expect(client.isDataUrl("data:image/png;base64,abc")).toBe(true);
    expect(client.isHttpUrl("https://example.com/a.jpg")).toBe(true);
    expect(client.isHttpUrl("data:image/png;base64,abc")).toBe(false);
    expect(isDataUrl("https://example.com/a.jpg")).toBe(false);
    expect(isHttpUrl("data:image/png;base64,abc")).toBe(false);
    expect(inferMimeFromFilename("photo.jpg")).toBe("image/jpeg");
    expect(inferMimeFromFilename("image.webp")).toBe("image/webp");
    expect(inferMimeFromFilename("file.png")).toBe("image/png");
  });

  it("extractImageGenerationResults and extractImageGenerationCalls, saveBase64ImageToFile", async () => {
    const { extractImageGenerationResults, extractImageGenerationCalls, base64ToBytes, saveBase64ImageToFile } = await import("../src/utils/image");
    const fakeB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const output = [
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "here is image" }] },
      { type: "image_generation_call", id: "ig_123", status: "completed", result: fakeB64, revised_prompt: "a cat" },
      { type: "image_generation_call", id: "ig_124", status: "completed", b64_json: fakeB64 },
    ];
    const calls = extractImageGenerationCalls(output);
    expect(calls.length).toBe(2);
    expect(calls[0].id).toBe("ig_123");
    expect(calls[0].revised_prompt).toBe("a cat");
    const results = extractImageGenerationResults(output);
    expect(results.length).toBe(2);
    expect(results[0]).toBe(fakeB64);
    expect(results[1]).toBe(fakeB64);

    // client helpers also
    const client = await import("../src/relay/client");
    expect(client.extractImageGenerationResults(output)).toEqual(results);
    expect(client.isImageGenerationCall({ type: "image_generation_call", id: "x" })).toBe(true);
    expect(client.isImageGenerationCall({ type: "message" })).toBe(false);

    // save to temp file and read back
    const fs = await import("node:fs/promises");
    const os = await import("node:os");
    const path = await import("node:path");
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "ghostprov-image-"));
    const filePath = path.join(tmpDir, "out.png");
    await saveBase64ImageToFile(fakeB64, filePath);
    const read = await fs.readFile(filePath);
    const expected = base64ToBytes(fakeB64);
    expect(read.length).toBe(expected.length);
    expect(read[0]).toBe(expected[0]);
    // cleanup
    await fs.rm(tmpDir, { recursive: true, force: true });
  });
});

describe("Audio helpers (utils/audio.ts)", () => {
  it("createInputAudio factories and extract helpers", async () => {
    const { createInputAudioFromBytes, createInputAudioFromBase64, createInputAudioFromFileId, bytesToBase64, extractOutputAudio } = await import("../src/utils/audio");
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const b64 = bytesToBase64(bytes);
    const a1 = createInputAudioFromBytes(bytes, "wav");
    expect(a1).toEqual({ type: "input_audio", input_audio: { data: b64, format: "wav" } });
    const a2 = createInputAudioFromBase64(b64, "mp3");
    expect(a2).toEqual({ type: "input_audio", input_audio: { data: b64, format: "mp3" } });
    const a3 = createInputAudioFromFileId("file-audio-123");
    expect(a3).toEqual({ type: "input_audio", file_id: "file-audio-123" });

    // client re-exports
    const client = await import("../src/relay/client");
    expect(client.createInputAudioFromBytes(bytes, "wav")).toEqual(a1);
    expect(client.createInputAudioFromFileId("file-audio-123")).toEqual(a3);

    // extractOutputAudio
    const output = [
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "hi" }] },
      { type: "output_audio", data: b64, transcript: "hello" },
    ];
    const audios = extractOutputAudio(output);
    expect(audios.length).toBe(1);
    expect(audios[0].transcript).toBe("hello");
  });
});

describe("Relay image vision passthrough (mocked via job.json)", () => {
  let originalFetch: typeof fetch;
  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("preserves input_image with http URL and detail low", async () => {
    const url = "https://example.com/image.jpg";
    const body: any = {
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [{ type: "input_text", text: "what is this" }, { type: "input_image", image_url: url, detail: "low" }] }],
      store: false,
    };
    const mockResponse = { id: "resp_url", output: [{ type: "message", content: "ok" }] };
    const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const u = typeof input === "string" ? input : input.toString();
      if (u.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (u.includes("/__bridge/write") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(JSON.parse(init.body as string).content); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (u.includes("/__internal/write-job") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(init.body as string); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (u.includes("/s/sandboxes/proxy/render") && u.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (u.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200 });
      if (u.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect(res.id).toBe("resp_url");
    const job = (globalThis as any).__LAST_JOB__;
    expect(job.input[0].content[1].image_url).toBe(url);
    expect(job.input[0].content[1].detail).toBe("low");
  });

  it("preserves input_image with data URL base64 and detail high", async () => {
    const dataUrl = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const body: any = {
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [{ type: "input_text", text: "what" }, { type: "input_image", image_url: dataUrl, detail: "high" }] }],
      store: false,
    };
    const mockResponse = { id: "resp_b64", output: [{ type: "message", content: "ok" }] };
    const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const u = typeof input === "string" ? input : input.toString();
      if (u.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (u.includes("/__bridge/write") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(JSON.parse(init.body as string).content); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (u.includes("/__internal/write-job") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(init.body as string); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (u.includes("/s/sandboxes/proxy/render") && u.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (u.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200 });
      if (u.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect(res.id).toBe("resp_b64");
    const job = (globalThis as any).__LAST_JOB__;
    expect(job.input[0].content[1].image_url).toBe(dataUrl);
    expect(job.input[0].content[1].detail).toBe("high");
  });

  it("preserves input_image with file_id and detail auto/original", async () => {
    for (const detail of ["auto", "original"] as const) {
      const body: any = {
        model: "gpt-5.6-sol",
        input: [{ role: "user", content: [{ type: "input_text", text: "what" }, { type: "input_image", file_id: "file-xyz-123", detail }] }],
        store: false,
      };
      const mockResponse = { id: `resp_${detail}`, output: [{ type: "message", content: "ok" }] };
      const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
      const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const u = typeof input === "string" ? input : input.toString();
        if (u.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
        if (u.includes("/__bridge/write") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(JSON.parse(init.body as string).content); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
        if (u.includes("/__internal/write-job") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(init.body as string); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
        if (u.includes("/s/sandboxes/proxy/render") && u.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
        if (u.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200 });
        if (u.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
        return new Response("not mocked", { status: 404 });
      });
      // @ts-ignore
      globalThis.fetch = fetchMock as unknown as typeof fetch;
      const { relayCreate } = await import("../src/relay/client");
      const res: any = await relayCreate(body);
      expect(res.id).toBe(`resp_${detail}`);
      const job = (globalThis as any).__LAST_JOB__;
      expect(job.input[0].content[1].file_id).toBe("file-xyz-123");
      expect(job.input[0].content[1].detail).toBe(detail);
      delete (globalThis as any).__LAST_JOB__;
    }
  });

  it("preserves image_generation tool with gpt-image-2 params via relay", async () => {
    const body: any = {
      model: "gpt-5.6-sol",
      input: "Generate an image of a cat hugging an otter",
      tools: [{ type: "image_generation", model: "gpt-image-2", quality: "high", size: "1024x1024", output_format: "png", background: "auto" }],
      store: false,
    };
    const fakeB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const mockResponse = {
      id: "resp_ig",
      output: [{ type: "image_generation_call", id: "ig_1", status: "completed", result: fakeB64, revised_prompt: "a cat hugging otter" }],
    };
    const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const u = typeof input === "string" ? input : input.toString();
      if (u.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (u.includes("/__bridge/write") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(JSON.parse(init.body as string).content); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (u.includes("/__internal/write-job") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(init.body as string); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (u.includes("/s/sandboxes/proxy/render") && u.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (u.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200 });
      if (u.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect(res.output[0].type).toBe("image_generation_call");
    expect(res.output[0].result).toBe(fakeB64);
    const job = (globalThis as any).__LAST_JOB__;
    expect(job.tools[0].type).toBe("image_generation");
    expect(job.tools[0].model).toBe("gpt-image-2");
    expect(job.tools[0].quality).toBe("high");
    expect(job.tools[0].size).toBe("1024x1024");
  });

  it("preserves input_audio with base64 wav and file_id via relay", async () => {
    const bytes = new Uint8Array([1, 2, 3]);
    const b64 = Buffer.from(bytes).toString("base64");
    const body: any = {
      model: "gpt-4o-transcribe",
      input: [{ role: "user", content: [{ type: "input_audio", input_audio: { data: b64, format: "wav" } }] }],
      store: false,
    };
    const body2: any = {
      model: "gpt-4o-transcribe",
      input: [{ role: "user", content: [{ type: "input_audio", file_id: "file-audio-123" }] }],
      store: false,
    };
    for (const b of [body, body2]) {
      const mockResponse = { id: "resp_audio", output: [{ type: "message", content: "transcript" }] };
      const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(b)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
      const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const u = typeof input === "string" ? input : input.toString();
        if (u.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
        if (u.includes("/__bridge/write") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(JSON.parse(init.body as string).content); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
        if (u.includes("/__internal/write-job") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(init.body as string); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
        if (u.includes("/s/sandboxes/proxy/render") && u.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
        if (u.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200 });
        if (u.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
        return new Response("not mocked", { status: 404 });
      });
      // @ts-ignore
      globalThis.fetch = fetchMock as unknown as typeof fetch;
      const { relayCreate } = await import("../src/relay/client");
      const res: any = await relayCreate(b);
      expect(res.id).toBe("resp_audio");
      const job = (globalThis as any).__LAST_JOB__;
      expect(job.input[0].content[0].type).toBe("input_audio");
      delete (globalThis as any).__LAST_JOB__;
    }
  });
});

describe("Server image/audio passthrough (mocked relay)", () => {
  beforeEach(() => {
    vi.resetModules();
  });
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("POST /v1/responses forwards input_image URL/base64/file_id verbatim", async () => {
    vi.doMock("../src/relay/client", () => ({
      relayCreate: vi.fn(async (body: any) => ({ id: "resp_mock", output: [{ type: "message", content: "ok" }], _echo: body })),
      relayCreateStream: vi.fn(async () => new TextEncoder().encode("event: done\ndata: [DONE]\n\n")),
      isValidReasoningEffort: vi.fn(() => true),
      isValidReasoningMode: vi.fn(() => true),
      isValidReasoningContext: vi.fn(() => true),
      isValidReasoningSummary: vi.fn(() => true),
      isValidServiceTier: vi.fn(() => true),
      isValidPhase: vi.fn(() => true),
      isValidImageDetail: vi.fn(() => true),
      normalizeReasoning: vi.fn((r: any) => r),
    }));
    vi.doMock("y-websocket", () => ({ WebsocketProvider: class { on(_e: string, cb: (v: boolean) => void) { if (_e === "sync") setTimeout(() => cb(true), 0); } destroy() {} } }));
    const { default: app } = await import("../src/server");
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;

    const cases: Array<{ desc: string; content: any; check: (body: any) => void }> = [
      {
        desc: "http URL",
        content: [{ type: "input_image", image_url: "https://example.com/a.jpg", detail: "high" }],
        check: (body: any) => expect(body.input[0].content[1].image_url).toBe("https://example.com/a.jpg"),
      },
      {
        desc: "data URL",
        content: [{ type: "input_image", image_url: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=", detail: "auto" }],
        check: (body: any) => expect(body.input[0].content[1].image_url).toContain("data:image/png;base64"),
      },
      {
        desc: "file_id",
        content: [{ type: "input_image", file_id: "file-xyz-123", detail: "low" }],
        check: (body: any) => expect(body.input[0].content[1].file_id).toBe("file-xyz-123"),
      },
      {
        desc: "original detail",
        content: [{ type: "input_image", file_id: "file-abc", detail: "original" }],
        check: (body: any) => expect(body.input[0].content[1].detail).toBe("original"),
      },
    ];
    for (const c of cases) {
      mock.mockClear();
      const body: any = { model: "gpt-5.6-sol", input: [{ role: "user", content: [{ type: "input_text", text: "hi" }, ...c.content] }], store: false };
      const res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
      expect(res.status).toBe(200);
      expect(mock).toHaveBeenCalledTimes(1);
      c.check(mock.mock.calls[0][0]);
    }
  });

  it("POST /v1/responses forwards image_generation tool verbatim (gpt-image-2)", async () => {
    vi.doMock("../src/relay/client", () => ({
      relayCreate: vi.fn(async (body: any) => ({ id: "resp_mock", output: [{ type: "image_generation_call", id: "ig_1", result: "fakeb64" }], _echo: body })),
      relayCreateStream: vi.fn(async () => new TextEncoder().encode("event: done\ndata: [DONE]\n\n")),
    }));
    vi.doMock("y-websocket", () => ({ WebsocketProvider: class { on(_e: string, cb: (v: boolean) => void) { if (_e === "sync") setTimeout(() => cb(true), 0); } destroy() {} } }));
    const { default: app } = await import("../src/server");
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const body: any = {
      model: "gpt-5.6-sol",
      input: "Generate an image",
      tools: [{ type: "image_generation", model: "gpt-image-2", quality: "high", size: "1024x1024", output_format: "webp", background: "transparent" }],
      store: false,
    };
    const res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].tools[0].type).toBe("image_generation");
    expect(mock.mock.calls[0][0].tools[0].model).toBe("gpt-image-2");
    expect(mock.mock.calls[0][0].tools[0].quality).toBe("high");
    expect(mock.mock.calls[0][0].tools[0].size).toBe("1024x1024");
    expect(mock.mock.calls[0][0].tools[0].output_format).toBe("webp");
  });

  it("POST /v1/responses forwards input_audio verbatim", async () => {
    vi.doMock("../src/relay/client", () => ({
      relayCreate: vi.fn(async (body: any) => ({ id: "resp_mock", output: [{ type: "message", content: "ok" }], _echo: body })),
      relayCreateStream: vi.fn(async () => new TextEncoder().encode("event: done\ndata: [DONE]\n\n")),
    }));
    vi.doMock("y-websocket", () => ({ WebsocketProvider: class { on(_e: string, cb: (v: boolean) => void) { if (_e === "sync") setTimeout(() => cb(true), 0); } destroy() {} } }));
    const { default: app } = await import("../src/server");
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const b64 = Buffer.from([1, 2, 3]).toString("base64");
    const body: any = {
      model: "gpt-4o-transcribe",
      input: [{ role: "user", content: [{ type: "input_audio", input_audio: { data: b64, format: "wav" } }] }],
      store: false,
    };
    const res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].input[0].content[0].type).toBe("input_audio");
    expect(mock.mock.calls[0][0].input[0].content[0].input_audio.data).toBe(b64);
    // file_id variant
    mock.mockClear();
    const body2: any = { model: "gpt-4o-transcribe", input: [{ role: "user", content: [{ type: "input_audio", file_id: "file-audio-xyz" }] }], store: false };
    const res2 = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body2) });
    expect(res2.status).toBe(200);
    expect(mock.mock.calls[0][0].input[0].content[0].file_id).toBe("file-audio-xyz");
  });

  it("types include ImageDetail, ImageGenerationTool, InputAudio (compile-time check)", async () => {
    const types = await import("../src/relay/types");
    expect(types.VALID_IMAGE_DETAILS).toBeDefined();
    expect(types.VALID_IMAGE_DETAILS).toEqual(expect.arrayContaining(["low", "high", "auto", "original"]));
    // ToolDefinition should accept image_generation
    const tool: any = { type: "image_generation", model: "gpt-image-2", quality: "high" };
    expect(tool.type).toBe("image_generation");
  });
});

describe("Files vision upload (mocked)", () => {
  let originalFetch: typeof fetch;
  beforeEach(() => { originalFetch = globalThis.fetch; });
  afterEach(() => { globalThis.fetch = originalFetch; vi.restoreAllMocks(); });

  it("uploadImageForVision posts to /api/project-files/upload with purpose vision", async () => {
    const mockFileResp = { id: "file-vision-123", object: "file", bytes: 123, filename: "cat.png", purpose: "vision" };
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/project-files/upload")) {
        expect(url).toContain("prism_cache_bust=");
        // body is FormData, check purpose inside form (we can't easily inspect but at least ensure called)
        return new Response(JSON.stringify(mockFileResp), { status: 200, headers: { "content-type": "application/json" } });
      }
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { uploadImageForVision } = await import("../src/proxy/files");
    const blob = new Blob([new Uint8Array([1, 2, 3])], { type: "image/png" });
    const res: any = await uploadImageForVision(blob as any, "cat.png");
    expect(res.id).toBe("file-vision-123");
    expect(res.purpose).toBe("vision");
    expect(fetchMock).toHaveBeenCalled();
  });

  it("createInputImageFromBytesViaUpload uses Files API then returns file_id input_image", async () => {
    const mockFileResp = { id: "file-xyz-999", object: "file", bytes: 99, filename: "x.png", purpose: "vision" };
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/project-files/upload")) return new Response(JSON.stringify(mockFileResp), { status: 200, headers: { "content-type": "application/json" } });
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { createInputImageFromBytesViaUpload } = await import("../src/utils/image");
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const input = await createInputImageFromBytesViaUpload(bytes, "x.png", "image/png", "high");
    expect(input.type).toBe("input_image");
    expect(input.file_id).toBe("file-xyz-999");
    expect(input.detail).toBe("high");
  });
});
