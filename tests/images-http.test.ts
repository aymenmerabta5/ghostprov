import { describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { registerImageRoutes } from "../src/images/routes";
import type { ImageCompatOptions } from "../src/images/compat";
import type { RunHandle } from "../src/relay/run-handle";

const OPTIONS: ImageCompatOptions = {
  responsesModel: "gpt-5.6-sol",
  maxEditInputBytes: 40 * 1024 * 1024,
};
const PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
  0x00, 0x00, 0x00, 0x0d,
]);
const JPEG = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46]);

function imageFinal(result: string) {
  return {
    status: "completed",
    created_at: 123,
    output: [{
      type: "image_generation_call",
      status: "completed",
      result,
      background: "opaque",
      output_format: "png",
      quality: "low",
      size: "1024x1024",
    }],
  };
}

function handle(final: Promise<unknown>): RunHandle {
  return {
    async *events() {},
    final,
    stats: { bytes: 0, firstByteMs: null, frames: 0 },
  };
}

function testApp(startRelay: (body: Record<string, unknown>, signal?: AbortSignal) => Promise<RunHandle>) {
  const app = new Hono();
  registerImageRoutes(app, { options: OPTIONS, startRelay });
  return app;
}

describe("Images compatibility HTTP routes", () => {
  it("returns native Images JSON while fanning n through separate relay calls", async () => {
    const bodies: Record<string, unknown>[] = [];
    const app = testApp(async body => {
      bodies.push(body);
      return handle(Promise.resolve(imageFinal(`PNG-${bodies.length}`)));
    });

    const response = await app.request("/v1/images/generations", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        prompt: "a circle",
        model: "gpt-image-2",
        n: 2,
        quality: "low",
      }),
    });

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      created: 123,
      data: [{ b64_json: "PNG-1" }, { b64_json: "PNG-2" }],
      background: "opaque",
      output_format: "png",
      quality: "low",
      size: "1024x1024",
    });
    expect(bodies).toHaveLength(2);
    expect(bodies.every(body => body.stream === true && body.store === true)).toBe(true);
    expect((bodies[0].tools as any[])[0]).not.toHaveProperty("n");
  });

  it("parses multipart edits and sends an edit tool call", async () => {
    let body: Record<string, unknown> | undefined;
    const app = testApp(async value => {
      body = value;
      return handle(Promise.resolve(imageFinal("EDITED")));
    });
    const form = new FormData();
    form.append("prompt", "make it blue");
    form.append("image", new File([PNG], "input.png", { type: "image/png" }));

    const response = await app.request("/v1/images/edits", { method: "POST", body: form });

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ data: [{ b64_json: "EDITED" }] });
    expect(body).toMatchObject({ stream: true, store: true });
    expect((body!.tools as any[])[0]).toMatchObject({
      type: "image_generation",
      model: "gpt-image-2",
      action: "edit",
    });
  });

  it("accepts the repeated image[] fields emitted by the OpenAI SDK", async () => {
    let body: Record<string, unknown> | undefined;
    const app = testApp(async value => {
      body = value;
      return handle(Promise.resolve(imageFinal("EDITED")));
    });
    const form = new FormData();
    form.append("prompt", "combine references");
    form.append("image[]", new File([PNG], "one.png", { type: "image/png" }));
    form.append("image[]", new File([JPEG], "two.jpg", { type: "image/jpeg" }));

    const response = await app.request("/v1/images/edits", { method: "POST", body: form });

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ data: [{ b64_json: "EDITED" }] });
    const content = ((body!.input as any[])[0].content as Array<Record<string, unknown>>);
    expect(content.slice(1).map(part => part.image_url)).toEqual([
      `data:image/png;base64,${Buffer.from(PNG).toString("base64")}`,
      `data:image/jpeg;base64,${Buffer.from(JPEG).toString("base64")}`,
    ]);
  });

  it("rejects mixed image and image[] fields before relay dispatch", async () => {
    let calls = 0;
    const app = testApp(async () => {
      calls++;
      return handle(Promise.resolve(imageFinal("never")));
    });
    const form = new FormData();
    form.append("prompt", "ambiguous references");
    form.append("image", new File([PNG], "direct.png", { type: "image/png" }));
    form.append("image[]", new File([JPEG], "array.jpg", { type: "image/jpeg" }));

    const response = await app.request("/v1/images/edits", { method: "POST", body: form });

    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ error: {
      type: "invalid_request_error",
      param: "image",
      code: "invalid_value",
    }});
    expect(calls).toBe(0);
  });

  it("returns OpenAI-shaped validation errors without starting the relay", async () => {
    let calls = 0;
    const app = testApp(async () => {
      calls++;
      return handle(Promise.resolve(imageFinal("never")));
    });

    const response = await app.request("/v1/images/generations", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-image-2" }),
    });

    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ error: {
      message: "Missing required parameter: 'prompt'.",
      type: "invalid_request_error",
      param: "prompt",
      code: "missing_required_parameter",
    }});
    expect(calls).toBe(0);
  });

  it("does not revive legacy daemon envelopes for unknown relay failures", async () => {
    const details = {
      code: "http_400",
      message: JSON.stringify({ error: {
        message: "Upstream rejected the image",
        type: "invalid_request_error",
        param: "prompt",
        code: "content_policy_violation",
      }}),
    };
    const app = testApp(async () => handle(Promise.reject(Object.assign(
      new Error("upstream failed"),
      { kind: "upstream", details },
    ))));

    const response = await app.request("/v1/images/generations", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt: "blocked" }),
    });

    expect(response.status).toBe(502);
    const output = await response.json();
    expect(output).toMatchObject({ error: {
      type: "upstream_error",
      code: "upstream_error",
    }});
    expect(JSON.stringify(output)).not.toContain("content_policy_violation");
  });

  it("rejects malformed JSON with the standard error shape", async () => {
    const app = testApp(async () => handle(Promise.resolve(imageFinal("never"))));
    const response = await app.request("/v1/images/generations", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{",
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ error: {
      type: "invalid_request_error",
      code: "invalid_json",
    }});
  });

  it.each([
    "/v1/images/variations",
    "/v1/audio/speech",
    "/v1/audio/transcriptions",
    "/v1/audio/translations",
  ])("returns an explicit unsupported-capability error for %s", async path => {
    const app = testApp(async () => handle(Promise.resolve(imageFinal("never"))));
    const response = await app.request(path, { method: "POST" });
    expect(response.status).toBe(501);
    expect(await response.json()).toMatchObject({ error: {
      type: "unsupported_feature",
      code: "unsupported_feature",
    }});
  });
});
