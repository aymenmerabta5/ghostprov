import { describe, expect, it } from "bun:test";
import {
  MAX_WORKLOAD_JSON_ITEMS,
  WorkloadTraversalLimitError,
  classifyWorkload,
  jsonEncodedByteLength,
  jsonEncodedByteLengthAsync,
} from "../src/router/workload";

const MiB = 1024 ** 2;

describe("workload classification", () => {
  it("matches cooperative and synchronous JSON sizing across escaping boundaries", async () => {
    const value = {
      input: [
        "plain\0\b\t\n\f\r\"\\",
        "é漢😀",
        "lone-\ud800-low-\udc00",
        { finite: 1e20, nonfinite: Number.POSITIVE_INFINITY, omitted: undefined },
      ],
    };
    expect(await jsonEncodedByteLengthAsync(value)).toBe(jsonEncodedByteLength(value));
    expect(await jsonEncodedByteLengthAsync(value)).toBe(
      Buffer.byteLength(JSON.stringify(value), "utf8"),
    );
  });

  it("yields inside one large escaped string and preserves exact abort reason", async () => {
    const value = { input: "\0".repeat(1024 * 1024) };
    let yields = 0;
    expect(await jsonEncodedByteLengthAsync(value, {
      yieldTask: async () => { yields++; },
    })).toBe(Buffer.byteLength(JSON.stringify(value), "utf8"));
    expect(yields).toBeGreaterThan(1);

    const controller = new AbortController();
    const reason = new DOMException("stop sizing", "AbortError");
    await expect(jsonEncodedByteLengthAsync(value, {
      signal: controller.signal,
      yieldTask: async () => { controller.abort(reason); },
    })).rejects.toBe(reason);
  });

  it("classifies small and large streamed text with bounded reservations", () => {
    const small = classifyWorkload({ model: "gpt-5.6-sol", input: "hello", stream: true });
    const large = classifyWorkload({ model: "gpt-5.6-sol", input: "x".repeat(MiB), stream: true });
    expect(small.class).toBe("text-small");
    expect(small.reservedBytes).toBeGreaterThanOrEqual(8 * MiB);
    expect(large.class).toBe("text-large");
    expect(large.reservedBytes).toBeGreaterThan(small.reservedBytes);
  });

  it("distinguishes image generation from reference-image editing", () => {
    const generation = classifyWorkload({
      model: "gpt-5.6-sol",
      input: "draw a circle",
      tools: [{ type: "image_generation", model: "gpt-image-2" }],
    });
    const edit = classifyWorkload({
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [
        { type: "input_text", text: "edit" },
        { type: "input_image", image_url: `data:image/png;base64,${"a".repeat(MiB)}` },
      ] }],
      tools: [{ type: "image_generation", model: "gpt-image-2", action: "edit" }],
    });
    expect(generation.class).toBe("image-generation");
    expect(generation.model).toBe("gpt-image-2");
    expect(edit.class).toBe("image-edit");
    expect(edit.model).toBe("gpt-image-2");
    expect(edit.reservedBytes).toBeGreaterThan(generation.reservedBytes);
  });

  it("honors precomputed encoded byte sizes without retaining a serialized copy", () => {
    const body: Record<string, unknown> = {
      model: "gpt-5.6-sol",
      input: "already encoded",
    };
    const workload = classifyWorkload(
      body,
      { encodedBytes: 2 * MiB },
    );
    expect(workload.requestBytes).toBe(2 * MiB);
    expect(workload).not.toHaveProperty("serializedBody");
  });

  it("budgets a single 4K RGBA data-url request below the V2 body ceiling", () => {
    const rawImageBytes = 3_840 * 2_160 * 4;
    const base64Bytes = 4 * Math.ceil(rawImageBytes / 3);
    const requestBytes = base64Bytes + 4 * 1024;
    const workload = classifyWorkload({
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: [{ type: "input_image" }] }],
      stream: true,
    }, {
      encodedBytes: requestBytes,
      structuralBytes: requestBytes,
    });

    expect(requestBytes).toBeLessThan(64 * MiB);
    expect(workload).toEqual({
      class: "text-large",
      requestBytes,
      reservedBytes: requestBytes * 6 + 4 * MiB,
      stream: true,
      model: "gpt-5.6-sol",
    });
  });

  it("never trusts a shorter wire spelling than canonical structural JSON", () => {
    const body = { model: "gpt-5.6-sol", input: Array(1_000).fill(1e-6) };
    const canonicalBytes = Buffer.byteLength(JSON.stringify(body), "utf8");
    const workload = classifyWorkload(body, { encodedBytes: 4_000 });
    expect(workload.requestBytes).toBe(canonicalBytes);
    expect(() => classifyWorkload(body, { encodedBytes: Number.NaN })).toThrow(RangeError);
    expect(() => classifyWorkload(body, { encodedBytes: -1 })).toThrow(RangeError);
  });

  it("counts exact JSON UTF-8 bytes without retaining a large duplicate", () => {
    const body = {
      model: "gpt-5.6-sol",
      input: `quotes:\" slash:\\ controls:\n unicode:©🚀 lone:\ud800 ${"x".repeat(MiB)}`,
      nullable: null,
      omitted: undefined,
      values: [true, false, Number.NaN, undefined],
    };
    const workload = classifyWorkload(body);
    expect(workload.requestBytes).toBe(Buffer.byteLength(JSON.stringify(body), "utf8"));
    expect(workload).not.toHaveProperty("serializedBody");
  });

  it("rejects over-deep sizing with a stable typed request error", () => {
    let value: unknown = { leaf: true };
    for (let depth = 0; depth < 257; depth++) value = [value];

    const failure = (() => {
      try {
        jsonEncodedByteLength({ metadata: value });
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(failure).toBeInstanceOf(WorkloadTraversalLimitError);
    expect(failure).toMatchObject({
      status: 400,
      code: "invalid_json_structure",
      param: "body",
      reason: "depth",
    });
  });

  it("bounds iterative input-image scanning even with precomputed structural bytes", () => {
    let input: unknown = { type: "input_text", text: "leaf" };
    for (let depth = 0; depth < 258; depth++) input = [input];

    expect(() => classifyWorkload({
      input,
      tools: [{ type: "image_generation" }],
    }, { structuralBytes: 1 })).toThrow(WorkloadTraversalLimitError);
  });

  it("matches validation depth at top-level fields and input-item roots", () => {
    let metadata: unknown = { leaf: true };
    let inputItem: unknown = { type: "input_text", text: "leaf" };
    for (let depth = 0; depth < 256; depth++) {
      metadata = [metadata];
      inputItem = [inputItem];
    }

    expect(() => classifyWorkload({ metadata, input: "text" })).not.toThrow();
    expect(() => classifyWorkload({
      input: [inputItem],
      tools: [{ type: "image_generation", action: "edit" }],
    }, { structuralBytes: 1 })).not.toThrow();
  });

  it("caps iterative input-image scanning by visited value count", () => {
    const input = Array.from({ length: MAX_WORKLOAD_JSON_ITEMS + 1 }, () => null);
    let failure: unknown = null;
    try {
      classifyWorkload({
        input,
        tools: [{ type: "image_generation" }],
      }, { structuralBytes: 1 });
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(WorkloadTraversalLimitError);
    expect(failure).toMatchObject({ reason: "items" });
  });

  it("preserves circular JSON sizing failures without recursive overflow", () => {
    const body: Record<string, unknown> = {};
    body.self = body;
    expect(() => jsonEncodedByteLength(body)).toThrow("Converting circular structure to JSON");

    expect(() => classifyWorkload({
      input: body,
      tools: [{ type: "image_generation" }],
    }, { structuralBytes: 1 })).toThrow("Converting circular structure to JSON");
  });
});
