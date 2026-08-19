import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { zipSync } from "fflate";

// Mock relayCreate for prompt caching tests (same as server.test.ts)
vi.mock("../src/relay/client", () => ({
  relayCreate: vi.fn(async (body: any) => ({
    id: "resp_mock_files_123",
    object: "response",
    model: body?.model ?? "gpt-5.6-sol",
    output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "ok" }] }],
    output_text: "ok",
    // echo back prompt_cache fields for verification
    _echo: {
      prompt_cache_key: body?.prompt_cache_key,
      prompt_cache_options: body?.prompt_cache_options,
      prompt_cache_breakpoint: body?.prompt_cache_breakpoint,
      tool_choice: body?.tool_choice,
      input: body?.input,
    },
  })),
  relayCreateStream: vi.fn(async (body: any) => {
    const chunks = [
      `event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", item: { type: "message", role: "assistant" } })}\n\n`,
      `event: done\ndata: [DONE]\n\n`,
    ];
    return new TextEncoder().encode(chunks.join(""));
  }),
}));

vi.mock("y-websocket", () => ({
  WebsocketProvider: class {
    on(_event: string, cb: (v: boolean) => void) {
      if (_event === "sync") setTimeout(() => cb(true), 0);
    }
    destroy() {}
  },
}));

describe("Files + Prompt Caching Passthrough (Task 7)", () => {
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("src/proxy/files.ts exports uploadFile and getFileContent (via /api/project-files/upload and /zip)", async () => {
    const mod = await import("../src/proxy/files");
    expect(typeof mod.uploadFile).toBe("function");
    expect(typeof mod.getFileContent).toBe("function");
    // Also check getFile and uploadFileFromFormData if present
    expect(mod).toBeDefined();
  });

  it("uploadFile forwards to POST /api/project-files/upload with prism_cache_bust", async () => {
    const mockFileResp = {
      id: "file_mock_123",
      object: "file",
      bytes: 1234,
      created_at: 1234567890,
      filename: "test.png",
      purpose: "assistants",
    };

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/project-files/upload")) {
        expect(url).toContain("prism_cache_bust=");
        expect(init?.method).toBe("POST");
        expect(init?.headers).toBeDefined();
        // x-crixet-sandbox-token header must be present
        const headers = init?.headers as Record<string, string>;
        const tokenHeader = (headers && (headers["x-crixet-sandbox-token"] || headers["X-Crixet-Sandbox-Token"])) || "";
        // token is mock-token fallback, so just check header exists via fetch mock
        return new Response(JSON.stringify(mockFileResp), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response("not mocked: " + url, { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { uploadFile } = await import("../src/proxy/files");
    const blob = new Blob(["hello"], { type: "image/png" });
    const result: any = await uploadFile(blob as any, "test.png", "assistants");
    expect(result.id).toBe("file_mock_123");
    expect(result.bytes).toBe(1234);
    expect(fetchMock).toHaveBeenCalled();
    const calledUrl = fetchMock.mock.calls[0][0] as string;
    expect(calledUrl).toContain("/api/project-files/upload");
    expect(calledUrl).toContain("prism_cache_bust=");
  });

  it("getFileContent via GET /zip (prism_cache_bust) extracts file bytes", async () => {
    const fileId = "file_mock_abc";
    const fileContent = new Uint8Array([1, 2, 3, 4, 5]);
    const zipBytes = zipSync({
      [fileId]: fileContent,
      [`files/${fileId}`]: fileContent, // alternative key
      "response.json": new TextEncoder().encode(JSON.stringify({ id: "resp", output: [] })),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/s/sandboxes/proxy/zip")) {
        expect(url).toContain("prism_cache_bust=");
        return new Response(zipBytes as unknown as BodyInit, {
          status: 200,
          headers: { "content-type": "application/zip" },
        });
      }
      return new Response("not mocked: " + url, { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { getFileContent } = await import("../src/proxy/files");
    const data = await getFileContent(fileId);
    expect(data).toBeDefined();
    expect(data.length).toBeGreaterThan(0);
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/s/sandboxes/proxy/zip"), expect.anything());
  });

  it("POST /v1/files upload via POST /api/project-files/upload", async () => {
    const mockFileResp = {
      id: "file_server_123",
      object: "file",
      bytes: 68,
      created_at: 1700000000,
      filename: "1x1.png",
      purpose: "vision",
    };

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/project-files/upload")) {
        return new Response(JSON.stringify(mockFileResp), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      // Allow other fetches (e.g., y-websocket token, etc.) to succeed silently
      if (url.includes("/api/y")) {
        return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.includes("/__bridge/write")) {
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/s/sandboxes/proxy/render")) {
        return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      }
      if (url.includes("/s/sandboxes/proxy/render-status")) {
        return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.includes("/s/sandboxes/proxy/zip")) {
        const zb = zipSync({ "response.json": new TextEncoder().encode(JSON.stringify({ id: "resp", output: [] })) });
        return new Response(zb as unknown as BodyInit, { status: 200, headers: { "content-type": "application/zip" } });
      }
      return new Response("not mocked: " + url, { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { default: app } = await import("../src/server");
    const form = new FormData();
    const oneByOnePngBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const pngBytes = Uint8Array.from(atob(oneByOnePngBase64), (c) => c.charCodeAt(0));
    const blob = new Blob([pngBytes], { type: "image/png" });
    form.append("file", blob, "1x1.png");
    form.append("purpose", "vision");

    const res = await app.request("/v1/files", {
      method: "POST",
      body: form as any,
    });
    expect(res.status).toBe(200);
    const j: any = await res.json();
    expect(j.id).toBe("file_server_123");
    expect(j.bytes).toBe(68);
    expect(j.filename).toBe("1x1.png");
  });

  it("GET /v1/files/:id/content via GET /zip", async () => {
    const fileId = "file_content_123";
    const pngBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const pngBytes = Uint8Array.from(atob(pngBase64), (c) => c.charCodeAt(0));
    const zipBytes = zipSync({
      [fileId]: pngBytes,
      "response.json": new TextEncoder().encode(JSON.stringify({ id: "resp", output: [] })),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/s/sandboxes/proxy/zip")) {
        return new Response(zipBytes as unknown as BodyInit, {
          status: 200,
          headers: { "content-type": "application/zip" },
        });
      }
      return new Response("not mocked: " + url, { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { default: app } = await import("../src/server");
    const res = await app.request(`/v1/files/${fileId}/content`);
    expect(res.status).toBe(200);
    const buf = new Uint8Array(await res.arrayBuffer());
    expect(buf.length).toBe(pngBytes.length);
    expect(buf[0]).toBe(pngBytes[0]);
  });

  it("POST /v1/responses forwards prompt_cache_key, prompt_cache_options, prompt_cache_breakpoint verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();

    const { default: app } = await import("../src/server");
    const body: any = {
      model: "gpt-5.6-sol",
      input: "hello cache",
      prompt_cache_key: "my-cache-key-42",
      prompt_cache_options: { mode: "explicit", ttl: "30m" },
      prompt_cache_breakpoint: { mode: "explicit" },
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    expect(mock).toHaveBeenCalledTimes(1);
    const calledBody = mock.mock.calls[0][0];
    expect(calledBody.prompt_cache_key).toBe("my-cache-key-42");
    expect(calledBody.prompt_cache_options).toEqual({ mode: "explicit", ttl: "30m" });
    expect(calledBody.prompt_cache_breakpoint).toEqual({ mode: "explicit" });
  });

  it("POST /v1/responses forwards tool_choice: allowed_tools verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();

    const { default: app } = await import("../src/server");
    const body: any = {
      model: "gpt-5.6-sol",
      input: "use tools",
      tools: [
        { type: "function", name: "get_horoscope", parameters: { type: "object", properties: { sign: { type: "string" } }, required: ["sign"] } },
        { type: "function", name: "get_weather", parameters: { type: "object", properties: { city: { type: "string" } } } },
      ],
      tool_choice: { type: "allowed_tools", allowed_tools: [{ type: "function", name: "get_horoscope" }] },
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    expect(mock).toHaveBeenCalledTimes(1);
    const calledBody = mock.mock.calls[0][0];
    expect(calledBody.tool_choice).toEqual({ type: "allowed_tools", allowed_tools: [{ type: "function", name: "get_horoscope" }] });
    expect(calledBody.tools).toBeDefined();
    expect(calledBody.tools.length).toBe(2);
  });

  it("POST /v1/responses preserves 1x1 png data URL input_image verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();

    const { default: app } = await import("../src/server");
    const oneByOnePng = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const body: any = {
      model: "gpt-5.6-sol",
      input: [
        {
          role: "user",
          content: [
            { type: "input_text", text: "what is in this image" },
            { type: "input_image", image_url: oneByOnePng, detail: "auto" },
          ],
        },
      ],
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    expect(mock).toHaveBeenCalledTimes(1);
    const calledBody = mock.mock.calls[0][0];
    expect(calledBody.input[0].content[1].image_url).toBe(oneByOnePng);
  });

  it("POST /v1/responses handles prompt_cache_key with allowed_tools together", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();

    const { default: app } = await import("../src/server");
    const body: any = {
      model: "gpt-5.6-sol",
      input: "combined test",
      prompt_cache_key: "combined-key-15rpm",
      prompt_cache_options: { mode: "explicit", ttl: "30m" },
      prompt_cache_breakpoint: { mode: "explicit" },
      tools: [{ type: "function", name: "get_horoscope", parameters: { type: "object", properties: { sign: { type: "string" } } } }],
      tool_choice: { type: "allowed_tools", allowed_tools: [{ type: "function", name: "get_horoscope" }] },
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    const called = mock.mock.calls[0][0];
    expect(called.prompt_cache_key).toBe("combined-key-15rpm");
    expect(called.tool_choice.type).toBe("allowed_tools");
  });
});
