import { describe, expect, it } from "vitest";
import { gunzipSync } from "node:zlib";
import { createHash } from "node:crypto";
import { chooseSegmentProtocol, planV63Job } from "../src/relay/segments";

function decode(plan: ReturnType<typeof planV63Job>): string {
  const ordered = [...plan.parts].sort((left, right) => left.index - right.index);
  const newline = ordered[0].text.indexOf("\n");
  const header = JSON.parse(ordered[0].text.slice("GP63 ".length, newline));
  const payload = ordered[0].text.slice(newline + 1) + ordered.slice(1).map(part => part.text).join("");
  const bytes = header.enc === "gz+b64"
    ? gunzipSync(Buffer.from(payload, "base64"))
    : Buffer.from(payload, "utf8");
  expect(bytes.length).toBe(header.size);
  expect(createHash("sha256").update(bytes).digest("hex").slice(0, 12)).toBe(header.sha12);
  return bytes.toString("utf8");
}

describe("v6.3 immutable segment planning", () => {
  it("publishes parts 1..K-1 before the embedded-header part zero", () => {
    const input = JSON.stringify({ model: "gpt-5.6-sol", input: "0123456789".repeat(80_000) });
    const plan = planV63Job("job_s3.json", input, { gzip: false, partBytes: 64 * 1024 });
    expect(plan.version).toBe(6.3);
    expect(plan.publishOrder.at(-1)?.index).toBe(0);
    expect(plan.publishOrder.slice(0, -1).map(part => part.index)).toEqual(
      Array.from({ length: plan.parts.length - 1 }, (_, index) => index + 1),
    );
    expect(plan.parts[0].text.startsWith("GP63 ")).toBe(true);
    expect(decode(plan)).toBe(input);
  });

  it("uses gzip+base64 when it reduces part count", () => {
    const input = JSON.stringify({ input: "compress-me-".repeat(100_000) });
    const plan = planV63Job("job_s1.json", input, { gzip: true, partBytes: 32 * 1024 });
    expect(plan.enc).toBe("gz+b64");
    expect(decode(plan)).toBe(input);
  });

  it("keeps every commit file under the daemon hard limit including the header", () => {
    const input = JSON.stringify({ input: "🚀".repeat(200_000) });
    const plan = planV63Job("job_s8.json", input, {
      gzip: false,
      partBytes: 256 * 1024,
      maxPartBytes: 256 * 1024,
    });
    for (const part of plan.parts) {
      expect(Buffer.byteLength(part.text, "utf8")).toBeLessThanOrEqual(256 * 1024);
    }
    expect(decode(plan)).toBe(input);
  });

  it("selects v6.3 only when the daemon advertises it", () => {
    expect(chooseSegmentProtocol(["single-v5", "segmented-v6.3-embedded-commit"])).toBe(6.3);
    expect(chooseSegmentProtocol(["single-v5", "segmented-v6.2"])).toBe(6.2);
  });
});
