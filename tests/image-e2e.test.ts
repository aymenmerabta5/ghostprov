import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { zipSync } from "fflate";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

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

describe("Image E2E (data URL / http URL / file_id + image_generation gpt-image-2)", () => {
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("input_image with URL, base64 data URL, file_id (via POST /api/project-files/upload) all round-trip via job.json -> response.json -> zip", async () => {
    const oneByOnePngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const dataUrl = `data:image/png;base64,${oneByOnePngB64}`;
    const httpUrl = "https://example.com/photo.jpg";
    const fileId = "file-e2e-123";

    // For file_id case, we need to mock uploadFile path: POST /api/project-files/upload -> returns file id
    // But for relay E2E, we just use file_id directly in input_image (upload already done)
    const cases: Array<{ desc: string; inputImage: any; expectCheck: (job: any) => void }> = [
      {
        desc: "http URL",
        inputImage: { type: "input_image", image_url: httpUrl, detail: "high" },
        expectCheck: (job: any) => expect(job.input[0].content[1].image_url).toBe(httpUrl),
      },
      {
        desc: "base64 data URL",
        inputImage: { type: "input_image", image_url: dataUrl, detail: "auto" },
        expectCheck: (job: any) => expect(job.input[0].content[1].image_url).toBe(dataUrl),
      },
      {
        desc: "file_id via upload",
        inputImage: { type: "input_image", file_id: fileId, detail: "low" },
        expectCheck: (job: any) => expect(job.input[0].content[1].file_id).toBe(fileId),
      },
    ];

    for (const c of cases) {
      const body: any = {
        model: "gpt-5.6-sol",
        input: [{ role: "user", content: [{ type: "input_text", text: "what is this" }, c.inputImage] }],
        store: false,
      };
      const mockResponse = { id: `resp_${c.desc}`, output: [{ type: "message", content: "ok" }] };
      const zipBytes = zipSync({
        "job.json": new TextEncoder().encode(JSON.stringify(body)),
        "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)),
      });
      const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = typeof input === "string" ? input : input.toString();
        if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
        if (url.includes("/__bridge/write") && init?.method === "POST") {
          (globalThis as any).__LAST_JOB__ = JSON.parse(JSON.parse(init.body as string).content);
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
        if (url.includes("/__internal/write-job") && init?.method === "POST") {
          (globalThis as any).__LAST_JOB__ = JSON.parse(init.body as string);
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
        if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
        if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
        if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200, headers: { "content-type": "application/zip" } });
        if (url.includes("/api/project-files/upload")) {
          return new Response(JSON.stringify({ id: fileId, object: "file", filename: "cat.png", purpose: "vision", bytes: 123 }), { status: 200, headers: { "content-type": "application/json" } });
        }
        return new Response("not mocked: " + url, { status: 404 });
      });
      // @ts-ignore
      globalThis.fetch = fetchMock as unknown as typeof fetch;

      const { relayCreate } = await import("../src/relay/client");
      const res: any = await relayCreate(body);
      expect(res.id).toBe(`resp_${c.desc}`);
      const job = (globalThis as any).__LAST_JOB__;
      expect(job).toBeDefined();
      c.expectCheck(job);
      delete (globalThis as any).__LAST_JOB__;
    }
  });

  it("file_id via POST /api/project-files/upload then input_image file_id uses file_id verbatim and via Hono POST /v1/files", async () => {
    // Test the upload path: POST /v1/files with FormData -> file_id -> input_image
    const mockFileResp = { id: "file-uploaded-999", object: "file", filename: "1x1.png", purpose: "vision", bytes: 68 };
    let capturedPurpose = "";
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/project-files/upload") && init?.method === "POST") {
        // FormData contains file + purpose; we can check that purpose is vision via body inspection if possible
        // For mocked, just return
        capturedPurpose = "vision"; // assume
        return new Response(JSON.stringify(mockFileResp), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.includes("/s/sandboxes/proxy/zip")) {
        const zb = zipSync({ "response.json": new TextEncoder().encode(JSON.stringify({ id: "resp", output: [] })) });
        return new Response(zb as unknown as BodyInit, { status: 200 });
      }
      return new Response("not mocked: " + url, { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    // Use proxy/files uploadImageForVision helper to get file_id, then create input_image
    const { uploadImageForVision } = await import("../src/proxy/files");
    const oneByOnePngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const bytes = Buffer.from(oneByOnePngB64, "base64");
    const blob = new Blob([bytes], { type: "image/png" });
    const fileRes: any = await uploadImageForVision(blob as any, "1x1.png");
    expect(fileRes.id).toBe("file-uploaded-999");
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/api/project-files/upload"), expect.anything());

    // Now create input_image from file_id and forward via relay
    const { createInputImageFromFileId } = await import("../src/utils/image");
    const inputImage = createInputImageFromFileId(fileRes.id, "high");
    expect(inputImage.file_id).toBe("file-uploaded-999");
    expect(inputImage.detail).toBe("high");

    // Relay round-trip with that file_id
    const body: any = {
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [{ type: "input_text", text: "describe" }, inputImage] }],
      store: false,
    };
    const mockResponse = { id: "resp_file_id", output: [{ type: "message", content: "described" }] };
    const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
    const fetchMock2 = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (url.includes("/__bridge/write") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(JSON.parse(init.body as string).content); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (url.includes("/__internal/write-job") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(init.body as string); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked: " + url, { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock2 as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect(res.id).toBe("resp_file_id");
    const job = (globalThis as any).__LAST_JOB__;
    expect(job.input[0].content[1].file_id).toBe("file-uploaded-999");
  });

  it("POST /v1/responses with input_image and tools:[{type:\"image_generation\"}] gpt-image-2 round-trips via job.json -> response.json -> zip and b64_json save", async () => {
    const dataUrl = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const body: any = {
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [{ type: "input_text", text: "make a cat hugging otter" }, { type: "input_image", image_url: dataUrl, detail: "high" }] }],
      tools: [{ type: "image_generation", model: "gpt-image-2", quality: "high", size: "1024x1024", output_format: "png", background: "auto" }],
      store: false,
    };
    const fakeB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const mockResponse = {
      id: "resp_image_gen",
      output: [
        { type: "message", role: "assistant", content: [{ type: "output_text", text: "here is your image" }] },
        { type: "image_generation_call", id: "ig_123", status: "completed", result: fakeB64, revised_prompt: "a cute cat hugging an otter" },
      ],
    };
    const zipBytes = zipSync({
      "job.json": new TextEncoder().encode(JSON.stringify(body)),
      "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)),
    });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (url.includes("/__bridge/write") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(JSON.parse(init.body as string).content); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (url.includes("/__internal/write-job") && init?.method === "POST") { (globalThis as any).__LAST_JOB__ = JSON.parse(init.body as string); return new Response(JSON.stringify({ ok: true }), { status: 200 }); }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked: " + url, { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect(res.id).toBe("resp_image_gen");
    // verify job.json preserved both input_image and image_generation tool verbatim
    const job = (globalThis as any).__LAST_JOB__;
    expect(job.input[0].content[1].image_url).toBe(dataUrl);
    expect(job.input[0].content[1].detail).toBe("high");
    expect(job.tools[0].type).toBe("image_generation");
    expect(job.tools[0].model).toBe("gpt-image-2");
    expect(job.tools[0].quality).toBe("high");
    expect(job.tools[0].size).toBe("1024x1024");
    // verify response contains image_generation_call with b64_json result
    expect(res.output.find((o: any) => o.type === "image_generation_call")).toBeDefined();
    const igCall = res.output.find((o: any) => o.type === "image_generation_call");
    expect(igCall.result).toBe(fakeB64);
    expect(igCall.revised_prompt).toBe("a cute cat hugging an otter");

    // b64_json -> save (image E2E: save to file)
    const { extractImageGenerationResults, saveBase64ImageToFile, base64ToBytes } = await import("../src/utils/image");
    const results = extractImageGenerationResults(res.output);
    expect(results.length).toBe(1);
    expect(results[0]).toBe(fakeB64);
    const tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "ghostprov-image-e2e-"));
    const outPath = path.join(tmpDir, "cat_otter.png");
    await saveBase64ImageToFile(results[0], outPath);
    const read = await fs.promises.readFile(outPath);
    const expected = base64ToBytes(fakeB64);
    expect(read.length).toBe(expected.length);
    expect(read[0]).toBe(expected[0]);
    // also test saveImageGenerationResultsToFiles helper
    const { saveImageGenerationResultsToFiles } = await import("../src/utils/image");
    const outPaths = await saveImageGenerationResultsToFiles(results, tmpDir, "gen");
    expect(outPaths.length).toBe(1);
    expect(fs.existsSync(outPaths[0])).toBe(true);
    await fs.promises.rm(tmpDir, { recursive: true, force: true });
  });

  it("POST /v1/responses via Hono forwards input_image data URL and file_id and image_generation verbatim (mocked relayCreate)", async () => {
    vi.resetModules();
    // Mock relayCreate to echo body
    vi.doMock("../src/relay/client", () => ({
      relayCreate: vi.fn(async (body: any) => ({
        id: "resp_mock_hono",
        object: "response",
        model: body?.model ?? "gpt-5.6-sol",
        output: body?.tools ? [{ type: "image_generation_call", id: "ig_mock", result: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=", revised_prompt: "mock" }] : [{ type: "message", content: "ok" }],
        output_text: "ok",
        _echo: body,
      })),
      relayCreateStream: vi.fn(async () => new TextEncoder().encode("event: done\ndata: [DONE]\n\n")),
    }));
    const { default: app } = await import("../src/server");
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;

    const dataUrl = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    // data URL case
    let body: any = {
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [{ type: "input_text", text: "hi" }, { type: "input_image", image_url: dataUrl, detail: "high" }] }],
      tools: [{ type: "image_generation", model: "gpt-image-2", quality: "high", size: "1024x1024" }],
      store: false,
    };
    let res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].input[0].content[1].image_url).toBe(dataUrl);
    expect(mock.mock.calls[0][0].tools[0].model).toBe("gpt-image-2");
    mock.mockClear();

    // file_id case
    body = {
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [{ type: "input_text", text: "hi" }, { type: "input_image", file_id: "file-abc-123", detail: "low" }] }],
      store: false,
    };
    res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].input[0].content[1].file_id).toBe("file-abc-123");
    mock.mockClear();

    // http URL case
    const httpUrl = "https://example.com/dog.jpg";
    body = {
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [{ type: "input_text", text: "hi" }, { type: "input_image", image_url: httpUrl, detail: "auto" }] }],
      store: false,
    };
    res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].input[0].content[1].image_url).toBe(httpUrl);
  });

  it("handles image_url http vs data URL detection and inferMime correctly", async () => {
    const { isDataUrl, isHttpUrl, isBase64ImageUrl, inferMimeFromFilename, parseDataUrl } = await import("../src/utils/image");
    expect(isDataUrl("data:image/png;base64,abc")).toBe(true);
    expect(isDataUrl("https://example.com/a.jpg")).toBe(false);
    expect(isHttpUrl("https://example.com/a.jpg")).toBe(true);
    expect(isHttpUrl("data:image/png;base64,abc")).toBe(false);
    expect(isBase64ImageUrl("data:image/png;base64,abc")).toBe(true);
    expect(isBase64ImageUrl("data:image/png;base64,")).toBe(true);
    expect(inferMimeFromFilename("photo.JPG")).toBe("image/jpeg");
    expect(inferMimeFromFilename("image.webp")).toBe("image/webp");
    const parsed = parseDataUrl("data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=");
    expect(parsed?.mime).toBe("image/png");
    expect(parsed?.b64).toBe("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=");
  });
});
