import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const readme = fs.readFileSync(path.join(root, "README.md"), "utf8");
const envExample = fs.readFileSync(path.join(root, ".env.example"), "utf8");
const gitignore = fs.readFileSync(path.join(root, ".gitignore"), "utf8");
const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const liveMultiSandbox = fs.readFileSync(path.join(root, "scripts", "live_multi_sandbox.ts"), "utf8");
const liveVerify = fs.readFileSync(path.join(root, "scripts", "live_verify.ts"), "utf8");

describe("project documentation", () => {
  it("lists every executable test file that exists", () => {
    const tests = fs.readdirSync(path.join(root, "tests"))
      .filter(name => name.endsWith(".test.ts"))
      .sort();
    for (const test of tests) expect(readme).toContain(`tests/${test}`);
  });

  it("documents runtime concurrency defaults from the same source", () => {
    expect(readme).toContain("| `GP_V2_SANDBOX_CAPACITY` | `128`");
    expect(readme).toContain("| `GP_V2_INITIAL_ACTIVE_PER_SANDBOX` | `8`");
    expect(readme).toContain("| `GP_V2_MAX_ACTIVE_PER_SANDBOX` | `16`");
    expect(readme).toContain("| `GP_TARGET_CONCURRENCY` | `512`");
    expect(readme).toContain("| `GP_HEARTBEAT_MS` | `10000`");
    expect(readme).toContain("| `GP_ROUTER_QUEUE_TIMEOUT_MS` | `30000`");
    expect(readme).toContain("| `GP_V2_TRANSPORT_RESERVATION_BYTES` | `16777216`");
    expect(readme).toContain("| `GP_V2_SIDECAR_MEMORY_BUDGET_BYTES` | `536870912`");
    expect(readme).toContain("| WebSocket active responses | `16`");
    expect(readme).toContain("| WebSocket named lanes | `32`");
    expect(readme).toContain("| `GP_IMAGE_EDIT_MAX_INPUT_BYTES` | `41943040`");
    expect(readme).toContain("| `GP_IMAGE_EDIT_HTTP_MAX_BODY_BYTES` | `44040192`");
    expect(readme).toContain("ghostprovV2Data");
    expect(readme).toContain("ghostprovV2Control");
    expect(readme).toContain("128 KiB");
    expect(envExample).toContain("GP_V2_INITIAL_ACTIVE_PER_SANDBOX=8");
    expect(envExample).toContain("GP_V2_MAX_ACTIVE_PER_SANDBOX=16");
    expect(envExample).toContain("GP_ROUTER_QUEUE_TIMEOUT_MS=30000");
  });

  it("documents the Images compatibility surface and honest capability boundaries", () => {
    expect(readme).toContain("POST /v1/images/generations");
    expect(readme).toContain("POST /v1/images/edits");
    expect(readme).toContain("audio");
    expect(readme).toContain("PRISM_PROJECT_IDS");
    expect(readme).toContain("PRISM_COOKIE_FILE");
    expect(readme).toContain("18787");
  });

  it("documents same-port Claude Code setup, routes, auth, and tier mapping", () => {
    for (const value of [
      "POST /v1/messages",
      "POST /v1/messages/count_tokens",
      "POST /v1/messages?beta=true",
      "POST /v1/messages/count_tokens?beta=true",
      "ANTHROPIC_BASE_URL=http://127.0.0.1:18787",
      "ANTHROPIC_API_KEY=ghostprov-local",
      "must not end in `/v1`",
      "Opus → `gpt-5.6-sol`",
      "Sonnet → `gpt-5.6-terra`",
      "Haiku → `gpt-5.6-luna`",
      "GP_ANTHROPIC_OPUS_MODEL",
      "GP_ANTHROPIC_SONNET_MODEL",
      "GP_ANTHROPIC_HAIKU_MODEL",
      "GP_ANTHROPIC_DEFAULT_MODEL",
      "anthropic-version",
      "x-api-key",
      "Bearer",
      "bun run verify:claude-code",
      "127.0.0.1:18788",
    ]) expect(readme).toContain(value);
    expect(envExample).toContain("ANTHROPIC_BASE_URL=http://127.0.0.1:18787");
    expect(envExample).toContain("ANTHROPIC_API_KEY=ghostprov-local");
    expect(envExample).toContain("GP_CLAUDE_VERIFY_PORT=18788");
    expect(envExample).toContain("GP_CLAUDE_VERIFY_UPSTREAM=http://127.0.0.1:18787");
    expect(pkg.scripts["verify:claude-code"]).toBe("bun scripts/verify_claude_code.ts");
  });

  it("uses a separate second-terminal Claude Code flow for the verifier", () => {
    const verifierStart = readme.indexOf("bun run verify:claude-code");
    const secondTerminal = readme.indexOf("In a second terminal", verifierStart);
    const verifierBase = readme.indexOf(
      '$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:18788"',
      secondTerminal,
    );
    const verifierKey = readme.indexOf(
      '$env:ANTHROPIC_API_KEY = "ghostprov-local-verification"',
      verifierBase,
    );
    const verifierClaude = readme.indexOf("claude", verifierKey);

    expect(verifierStart).toBeGreaterThan(0);
    expect(secondTerminal).toBeGreaterThan(verifierStart);
    expect(verifierBase).toBeGreaterThan(secondTerminal);
    expect(verifierKey).toBeGreaterThan(verifierBase);
    expect(verifierClaude).toBeGreaterThan(verifierKey);
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
      "verify:v2-bootstrap": "bun scripts/probe_v2_bootstrap.ts",
      "verify:v2-single": "bun scripts/live_v2_single.ts",
      "verify:v2-fleet": "bun scripts/live_v2_fleet.ts",
      "verify:v2-smoke": "bun scripts/smoke_v2_candidate.ts",
      "decommission:v2": "bun scripts/decommission_v2_fleet.ts",
      "monitor:v2": "bun scripts/monitor_v2.ts",
      "diagnose:v2-concurrency": "bun scripts/diagnose_v2_concurrency.ts",
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
    expect(fs.existsSync(path.join(root, "scripts", "live_v2_single.ts"))).toBe(true);
    expect(fs.existsSync(path.join(root, "scripts", "live_v2_fleet.ts"))).toBe(true);
    expect(fs.existsSync(path.join(root, "scripts", "smoke_v2_candidate.ts"))).toBe(true);
  });

  it("documents privacy-safe performance tracing and its diagnosis order", () => {
    for (const value of [
      "bun run monitor:v2",
      "GP_LOG_DIR",
      "request.accepted",
      "transport.committed",
      "response.first_event",
      "fleet.sample",
      "monitor.sample",
      "admission_wait_ms",
      "commit_ack_ms",
      "post_commit_first_event_ms",
    ]) expect(readme).toContain(value);
    expect(readme).toContain("manual restart");
    expect(readme).toContain("never logs prompts");
    expect(readme).toContain("64 MiB");
    expect(envExample).toContain("GP_LOG_ROTATE_BYTES=67108864");
    expect(envExample).toContain("GP_MONITOR_ROOT=http://127.0.0.1:18787");
  });

  it("documents bounded continuations and outcome-aware sandbox health", () => {
    expect(readme).toContain("retain only byte-budgeted deltas");
    expect(readme).toContain("never cached quadratically");
    expect(readme).toContain("Client cancellations and definitive upstream errors are routing-neutral");
    expect(readme).toContain("sixty-second decaying sandbox health score");
  });

  it("documents active-flight credits and bounded completion recovery", () => {
    expect(readme).toContain("ACK-stall age starts with the active reliable flight");
    expect(readme).toContain("a fully drained transport restores credits immediately");
    expect(readme).toContain("completion estimates cap extreme model durations");
    expect(readme).toContain("neutral ten-second baseline every idle minute");
  });

  it("keeps live multi-sandbox state out of version control", () => {
    expect(gitignore.split(/\r?\n/)).toContain(".ghostprov/");
  });

  it("makes the live ramp import-safe and verifies terminal identity plus peak capacity", () => {
    expect(liveMultiSandbox).toContain("if (import.meta.main)");
    expect(liveMultiSandbox).toContain("validateProofResponse");
    expect(liveMultiSandbox).toContain("peakPhysicalActive");
    expect(liveMultiSandbox).toContain("uniqueResponseIds");
    expect(liveMultiSandbox).toContain("quarantinePeak");
  });

  it("exercises native image generation and multipart edit routes in live verification", () => {
    expect(liveVerify).toContain("/images/generations");
    expect(liveVerify).toContain("/images/edits");
    expect(liveVerify).toContain("GP_LIVE_IMAGE_EDIT");
  });
});
