import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";
import { CONFIG_DEFAULTS } from "../src/config";

const root = path.resolve(import.meta.dirname, "..");
const readme = fs.readFileSync(path.join(root, "README.md"), "utf8");
const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));

describe("project documentation", () => {
  it("lists every executable test file that exists", () => {
    const tests = fs.readdirSync(path.join(root, "tests"))
      .filter(name => name.endsWith(".test.ts"))
      .sort();
    for (const test of tests) expect(readme).toContain(`tests/${test}`);
  });

  it("documents runtime concurrency defaults from the same source", () => {
    expect(readme).toContain(`| \`GP_SLOTS\` | \`${CONFIG_DEFAULTS.slots}\``);
    expect(readme).toContain(`| \`GP_ZIP_BATCH_MS\` | \`${CONFIG_DEFAULTS.zipBatchMs}\``);
    expect(readme).toContain(`| WebSocket active responses | \`${CONFIG_DEFAULTS.wsActiveResponses}\``);
    expect(readme).toContain(`| WebSocket named lanes | \`${CONFIG_DEFAULTS.wsNamedStreams}\``);
  });

  it("provides stable verification scripts", () => {
    expect(pkg.packageManager).toBe("bun@1.4.0");
    expect(pkg.engines.bun).toBe("^1.4.0");
    expect(pkg.devDependencies.typescript).toBe("7.0.2");
    expect(pkg.devDependencies["@types/bun"]).toBe("1.4.0");
    expect(pkg.devDependencies).not.toHaveProperty("vitest");
    expect(pkg.scripts).toMatchObject({
      test: "bun test --isolate",
      typecheck: "tsc --noEmit",
      "test:unit": expect.any(String),
      "test:integration": expect.any(String),
      "verify:live": expect.any(String),
    });
    expect(fs.existsSync(path.join(root, "scripts", "live_verify.ts"))).toBe(true);
  });
});
