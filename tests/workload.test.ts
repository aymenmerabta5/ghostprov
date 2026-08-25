import { describe, expect, it } from "bun:test";
import { classifyWorkload } from "../src/router/workload";

const MiB = 1024 ** 2;

describe("workload classification", () => {
  it("classifies small and large streamed text with bounded reservations", () => {
    const small = classifyWorkload({ model: "gpt-5.6-sol", input: "hello", stream: true });
    const large = classifyWorkload({ model: "gpt-5.6-sol", input: "x".repeat(MiB), stream: true });
    expect(small.class).toBe("text-small");
    expect(small.reservedBytes).toBeGreaterThanOrEqual(8 * MiB);
    expect(large.class).toBe("text-large");
    expect(large.reservedBytes).toBeGreaterThan(small.reservedBytes);
    expect(large.cpuPermits).toBe(1);
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
    expect(edit.cpuPermits).toBe(1);
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
});
