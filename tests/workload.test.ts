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
    expect(edit.class).toBe("image-edit");
    expect(edit.reservedBytes).toBeGreaterThan(generation.reservedBytes);
    expect(edit.cpuPermits).toBe(1);
  });

  it("honors precomputed encoded byte sizes without serializing twice", () => {
    const workload = classifyWorkload(
      { model: "gpt-5.6-sol", input: "already encoded" },
      { encodedBytes: 2 * MiB, serializedBody: '{"precomputed":true}' },
    );
    expect(workload.requestBytes).toBe(2 * MiB);
    expect(workload.serializedBody).toBe('{"precomputed":true}');
  });
});
