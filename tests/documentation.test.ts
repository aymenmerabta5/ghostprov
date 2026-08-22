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
    expect(readme).toContain(`| \`GP_IMAGE_EDIT_MAX_INPUT_BYTES\` | \`${CONFIG_DEFAULTS.imageEditMaxInputBytes}\``);
    expect(readme).toContain(`| \`GP_IMAGE_EDIT_HTTP_MAX_BODY_BYTES\` | \`${CONFIG_DEFAULTS.imageEditHttpMaxBodyBytes}\``);
  });

  it("documents the Images compatibility surface and honest capability boundaries", () => {
    expect(readme).toContain("POST /v1/images/generations");
    expect(readme).toContain("POST /v1/images/edits");
    expect(readme).toContain("Audio input is unavailable through the configured Responses-only upstream");
    expect(readme).toContain("GP_SEGMENT=0");
    expect(readme).toContain("GP_LIVE_IMAGE=1");
  });

  it("provides stable verification scripts", () => {
    const unitScript = String(pkg.scripts["test:unit"]);
    const integrationScript = String(pkg.scripts["test:integration"]);
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
      "test:daemon": "python -B -m unittest sandbox/test_gp_daemon.py",
      "verify:live": expect.any(String),
    });
    expect(unitScript.includes("tests/images-compat.test.ts")).toBe(true);
    expect(unitScript.includes("tests/images-service.test.ts")).toBe(true);
    expect(unitScript.includes("tests/http-errors.test.ts")).toBe(true);
    expect(integrationScript.includes("tests/images-http.test.ts")).toBe(true);
    expect(fs.existsSync(path.join(root, "scripts", "live_verify.ts"))).toBe(true);
  });
});
