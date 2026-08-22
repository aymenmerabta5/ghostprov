import { describe, expect, it } from "bun:test";
import { ApiError } from "../src/http/errors";
import {
  buildEditBodies,
  buildGenerationBodies,
  parseEditRequest,
  parseGenerationRequest,
  type ImageCompatOptions,
} from "../src/images/compat";

const OPTIONS: ImageCompatOptions = {
  responsesModel: "gpt-5.6-sol",
  maxEditInputBytes: 40 * 1024 * 1024,
};

function expectApiError(fn: () => unknown, expected: {
  status?: number;
  param?: string;
  code?: string;
}): void {
  try {
    fn();
    throw new Error("expected ApiError");
  } catch (error) {
    expect(error).toBeInstanceOf(ApiError);
    const apiError = error as ApiError;
    if (expected.status !== undefined) expect(apiError.status).toBe(expected.status);
    if (expected.param !== undefined) expect(apiError.options.param).toBe(expected.param);
    if (expected.code !== undefined) expect(apiError.options.code).toBe(expected.code);
  }
}

async function expectAsyncApiError(fn: () => Promise<unknown>, expected: {
  status?: number;
  param?: string;
  code?: string;
}): Promise<void> {
  try {
    await fn();
    throw new Error("expected ApiError");
  } catch (error) {
    expect(error).toBeInstanceOf(ApiError);
    const apiError = error as ApiError;
    if (expected.status !== undefined) expect(apiError.status).toBe(expected.status);
    if (expected.param !== undefined) expect(apiError.options.param).toBe(expected.param);
    if (expected.code !== undefined) expect(apiError.options.code).toBe(expected.code);
  }
}

const PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
  0x00, 0x00, 0x00, 0x0d,
]);
const JPEG = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46]);
const WEBP = new Uint8Array([
  0x52, 0x49, 0x46, 0x46, 0x04, 0x00, 0x00, 0x00,
  0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38, 0x20,
]);

describe("Images API compatibility", () => {
  describe("generation requests", () => {
    it("fans native n out into independent Responses image-tool bodies", () => {
      const parsed = parseGenerationRequest({
        prompt: "a red circle",
        model: "gpt-image-2",
        n: 3,
        quality: "low",
        size: "1024x1024",
        output_format: "png",
        background: "opaque",
        moderation: "low",
        response_format: "b64_json",
        user: "user-123",
      }, OPTIONS);

      const bodies = buildGenerationBodies(parsed);

      expect(bodies).toHaveLength(3);
      for (const body of bodies) {
        expect(body).toEqual({
          model: "gpt-5.6-sol",
          store: false,
          input: "a red circle",
          user: "user-123",
          tools: [{
            type: "image_generation",
            model: "gpt-image-2",
            quality: "low",
            size: "1024x1024",
            output_format: "png",
            background: "opaque",
            moderation: "low",
          }],
          tool_choice: { type: "image_generation" },
        });
        expect((body.tools as Array<Record<string, unknown>>)[0]).not.toHaveProperty("n");
      }
      expect(bodies[0]).not.toBe(bodies[1]);
      expect(bodies[0].tools).not.toBe(bodies[1].tools);
    });

    it("defaults to gpt-image-2 and allows valid arbitrary gpt-image-2 sizes", () => {
      const parsed = parseGenerationRequest({
        prompt: "wide landscape",
        size: "1536x864",
        output_format: "webp",
        output_compression: 72,
      }, OPTIONS);

      expect(parsed).toMatchObject({
        model: "gpt-image-2",
        n: 1,
        size: "1536x864",
        outputFormat: "webp",
        outputCompression: 72,
      });
      expect(buildGenerationBodies(parsed)[0]).toMatchObject({
        tools: [{
          type: "image_generation",
          model: "gpt-image-2",
          size: "1536x864",
          output_format: "webp",
          output_compression: 72,
        }],
      });
    });

    it("accepts the pinned gpt-image-2 snapshot and null optional fields", () => {
      const parsed = parseGenerationRequest({
        prompt: "snapshot",
        model: "gpt-image-2-2026-04-21",
        n: null,
        quality: null,
        size: null,
        response_format: null,
        stream: false,
      }, OPTIONS);

      expect(parsed.model).toBe("gpt-image-2-2026-04-21");
      expect(parsed.n).toBe(1);
      expect(buildGenerationBodies(parsed)[0]).toEqual({
        model: "gpt-5.6-sol",
        store: false,
        input: "snapshot",
        tools: [{ type: "image_generation", model: "gpt-image-2-2026-04-21" }],
        tool_choice: { type: "image_generation" },
      });
    });

    it.each([
      [{}, "prompt", "missing_required_parameter"],
      [{ prompt: "" }, "prompt", "invalid_value"],
      [{ prompt: "x", model: "gpt-image-1" }, "model", "model_not_supported"],
      [{ prompt: "x", n: 0 }, "n", "invalid_value"],
      [{ prompt: "x", n: 1.5 }, "n", "invalid_value"],
      [{ prompt: "x", n: 11 }, "n", "invalid_value"],
      [{ prompt: "x", stream: true }, "stream", "unsupported_parameter"],
      [{ prompt: "x", response_format: "url" }, "response_format", "unsupported_parameter"],
      [{ prompt: "x", background: "transparent" }, "background", "unsupported_parameter"],
      [{ prompt: "x", output_format: "gif" }, "output_format", "invalid_value"],
      [{ prompt: "x", quality: "hd" }, "quality", "invalid_value"],
      [{ prompt: "x", moderation: "strict" }, "moderation", "invalid_value"],
      [{ prompt: "x", output_format: "png", output_compression: 50 }, "output_compression", "unsupported_parameter"],
      [{ prompt: "x", output_format: "jpeg", output_compression: 101 }, "output_compression", "invalid_value"],
      [{ prompt: "x", size: "1000x1000" }, "size", "invalid_value"],
      [{ prompt: "x", size: "4096x1024" }, "size", "invalid_value"],
      [{ prompt: "x", size: "3840x3840" }, "size", "invalid_value"],
      [{ prompt: "x", style: "vivid" }, "style", "unsupported_parameter"],
      [{ prompt: "x", made_up: true }, "made_up", "unknown_parameter"],
    ] as const)("rejects invalid generation request %#", (body, param, code) => {
      expectApiError(
        () => parseGenerationRequest(body, OPTIONS),
        { status: 400, param, code },
      );
    });
  });

  describe("edit requests", () => {
    it("maps ordered reference images and a PNG mask to Responses edit bodies", async () => {
      const form = new FormData();
      form.append("prompt", "make both references blue");
      form.append("model", "gpt-image-2");
      form.append("image", new File([PNG], "one.png", { type: "image/png" }));
      form.append("image", new File([JPEG], "two.jpg", { type: "image/jpeg" }));
      form.append("mask", new File([PNG], "mask.png", { type: "image/png" }));
      form.append("n", "2");
      form.append("quality", "high");
      form.append("size", "1024x1024");
      form.append("output_format", "webp");
      form.append("output_compression", "80");
      form.append("background", "opaque");
      form.append("input_fidelity", "high");
      form.append("user", "editor-1");

      const parsed = await parseEditRequest(form, OPTIONS);
      const bodies = buildEditBodies(parsed);

      expect(bodies).toHaveLength(2);
      const input = bodies[0].input as Array<{ role: string; content: Array<Record<string, unknown>> }>;
      expect(input).toHaveLength(1);
      expect(input[0].role).toBe("user");
      expect(input[0].content.map(part => part.type)).toEqual([
        "input_text",
        "input_image",
        "input_image",
      ]);
      expect(input[0].content[0]).toEqual({ type: "input_text", text: "make both references blue" });
      expect(input[0].content[1]).toMatchObject({
        type: "input_image",
        image_url: expect.stringMatching(/^data:image\/png;base64,/),
        detail: "auto",
      });
      expect(input[0].content[2]).toMatchObject({
        type: "input_image",
        image_url: expect.stringMatching(/^data:image\/jpeg;base64,/),
        detail: "auto",
      });
      expect(bodies[0]).toMatchObject({
        model: "gpt-5.6-sol",
        store: false,
        user: "editor-1",
        tools: [{
          type: "image_generation",
          model: "gpt-image-2",
          action: "edit",
          quality: "high",
          size: "1024x1024",
          output_format: "webp",
          output_compression: 80,
          background: "opaque",
          input_fidelity: "high",
          input_image_mask: {
            image_url: expect.stringMatching(/^data:image\/png;base64,/),
          },
        }],
        tool_choice: { type: "image_generation" },
      });
      expect((bodies[0].tools as Array<Record<string, unknown>>)[0]).not.toHaveProperty("n");
      expect(bodies[0]).not.toBe(bodies[1]);
      expect(bodies[0].tools).not.toBe(bodies[1].tools);
    });

    it("accepts WebP references by matching type, extension, and magic bytes", async () => {
      const form = new FormData();
      form.append("prompt", "use this reference");
      form.append("image", new File([WEBP], "reference.webp", { type: "image/webp" }));

      const [body] = buildEditBodies(await parseEditRequest(form, OPTIONS));
      const content = (body.input as any[])[0].content;
      expect(content[1].image_url).toStartWith("data:image/webp;base64,");
    });

    it("enforces the configured total raw edit-input budget before encoding", async () => {
      const form = new FormData();
      form.append("prompt", "too large");
      form.append("image", new File([PNG], "large.png", { type: "image/png" }));

      await expectAsyncApiError(
        () => parseEditRequest(form, { ...OPTIONS, maxEditInputBytes: PNG.byteLength - 1 }),
        { status: 413, param: "image", code: "request_too_large" },
      );
    });

    it("rejects more than sixteen reference images", async () => {
      const form = new FormData();
      form.append("prompt", "too many");
      for (let index = 0; index < 17; index++) {
        form.append("image", new File([PNG], `${index}.png`, { type: "image/png" }));
      }

      await expectAsyncApiError(
        () => parseEditRequest(form, OPTIONS),
        { status: 400, param: "image", code: "too_many_files" },
      );
    });

    it("rejects a mask larger than four MiB before reading it", async () => {
      const form = new FormData();
      form.append("prompt", "masked edit");
      form.append("image", new File([PNG], "image.png", { type: "image/png" }));
      form.append("mask", new File([new Uint8Array(4 * 1024 * 1024 + 1)], "mask.png", { type: "image/png" }));

      await expectAsyncApiError(
        () => parseEditRequest(form, { ...OPTIONS, maxEditInputBytes: 8 * 1024 * 1024 }),
        { status: 400, param: "mask", code: "file_too_large" },
      );
    });

    it.each([
      ["missing prompt", (form: FormData) => {
        form.append("image", new File([PNG], "image.png", { type: "image/png" }));
      }, "prompt", "missing_required_parameter"],
      ["missing image", (form: FormData) => {
        form.append("prompt", "missing image");
      }, "image", "missing_required_parameter"],
      ["mismatched image magic", (form: FormData) => {
        form.append("prompt", "bad image");
        form.append("image", new File([JPEG], "image.png", { type: "image/png" }));
      }, "image", "invalid_image"],
      ["unsupported image type", (form: FormData) => {
        form.append("prompt", "bad image");
        form.append("image", new File([new Uint8Array([0x47, 0x49, 0x46])], "image.gif", { type: "image/gif" }));
      }, "image", "unsupported_file_type"],
      ["non-PNG mask", (form: FormData) => {
        form.append("prompt", "bad mask");
        form.append("image", new File([PNG], "image.png", { type: "image/png" }));
        form.append("mask", new File([JPEG], "mask.jpg", { type: "image/jpeg" }));
      }, "mask", "unsupported_file_type"],
      ["invalid fidelity", (form: FormData) => {
        form.append("prompt", "bad fidelity");
        form.append("image", new File([PNG], "image.png", { type: "image/png" }));
        form.append("input_fidelity", "medium");
      }, "input_fidelity", "invalid_value"],
      ["streaming", (form: FormData) => {
        form.append("prompt", "stream");
        form.append("image", new File([PNG], "image.png", { type: "image/png" }));
        form.append("stream", "true");
      }, "stream", "unsupported_parameter"],
      ["unknown form field", (form: FormData) => {
        form.append("prompt", "unknown");
        form.append("image", new File([PNG], "image.png", { type: "image/png" }));
        form.append("made_up", "true");
      }, "made_up", "unknown_parameter"],
    ] as const)("rejects %s", async (_name, build, param, code) => {
      const form = new FormData();
      build(form);
      await expectAsyncApiError(
        () => parseEditRequest(form, OPTIONS),
        { status: 400, param, code },
      );
    });
  });
});
