import { describe, expect, it } from "bun:test";
import { gunzipSync } from "node:zlib";
import { CpuTransformPool } from "../src/router/cpu-transform-pool";

describe("CpuTransformPool", () => {
  it("runs compression away from the main event loop with bounded workers", async () => {
    const pool = new CpuTransformPool({ workers: 2 });
    const payloads = Array.from({ length: 6 }, (_, index) => new TextEncoder().encode(`payload-${index}-`.repeat(80_000)));
    const started = performance.now();
    const timer = new Promise<number>(resolve => setTimeout(() => resolve(performance.now() - started), 10));
    const compressed = Promise.all(payloads.map(payload => pool.gzip(payload)));

    const timerDelay = await timer;
    const results = await compressed;

    expect(timerDelay).toBeLessThan(50);
    expect(pool.snapshot().maxActive).toBeLessThanOrEqual(2);
    for (let index = 0; index < results.length; index++) {
      expect(new TextDecoder().decode(gunzipSync(results[index]))).toBe(new TextDecoder().decode(payloads[index]));
    }
    await pool.close();
  });
});
