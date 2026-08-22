import { describe, expect, it } from "bun:test";
import { ApiError } from "../src/http/errors";
import {
  buildGenerationBodies,
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
});
