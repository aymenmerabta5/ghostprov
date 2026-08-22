import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const readme = fs.readFileSync(path.join(root, "README.md"), "utf8");
const gitignore = fs.readFileSync(path.join(root, ".gitignore"), "utf8");
const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));

describe("project documentation", () => {
  it("lists every executable test file that exists", () => {
    const tests = fs.readdirSync(path.join(root, "tests"))
      .filter(name => name.endsWith(".test.ts"))
      .sort();
    for (const test of tests) expect(readme).toContain(`tests/${test}`);
  });

  it("documents runtime concurrency defaults from the same source", () => {
    expect(readme).toContain("| `GP_SLOTS` | `30`");
    expect(readme).toContain("| `GP_HEARTBEAT_MS` | `10000`");
    expect(readme).toContain("| `GP_ZIP_BATCH_MS` | `10`");
    expect(readme).toContain("| WebSocket active responses | `16`");
    expect(readme).toContain("| WebSocket named lanes | `32`");
    expect(readme).toContain("| `GP_IMAGE_EDIT_MAX_INPUT_BYTES` | `41943040`");
    expect(readme).toContain("| `GP_IMAGE_EDIT_HTTP_MAX_BODY_BYTES` | `44040192`");
  });

  it("documents the Images compatibility surface and honest capability boundaries", () => {
    expect(readme).toContain("POST /v1/images/generations");
    expect(readme).toContain("POST /v1/images/edits");
    expect(readme).toContain("audio");
    expect(readme).toContain("PRISM_PROJECT_IDS");
    expect(readme).toContain("PRISM_COOKIE_FILE");
    expect(readme).toContain("18787");
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
      "verify:browserless": expect.any(String),
      "verify:multi-sandbox": expect.any(String),
    });
    expect(pkg.dependencies).not.toHaveProperty("playwright");
    expect(unitScript.includes("tests/images-compat.test.ts")).toBe(true);
    expect(unitScript.includes("tests/images-service.test.ts")).toBe(true);
    expect(unitScript.includes("tests/http-errors.test.ts")).toBe(true);
    expect(integrationScript.includes("tests/images-http.test.ts")).toBe(true);
    expect(integrationScript.includes("tests/multi-sandbox-integration.test.ts")).toBe(true);
    expect(fs.existsSync(path.join(root, "scripts", "live_verify.ts"))).toBe(true);
    expect(fs.existsSync(path.join(root, "scripts", "probe_browserless_prism.ts"))).toBe(true);
    expect(fs.existsSync(path.join(root, "scripts", "live_multi_sandbox.ts"))).toBe(true);
  });

  it("keeps live multi-sandbox state out of version control", () => {
    expect(gitignore.split(/\r?\n/)).toContain(".ghostprov/");
  });
});
