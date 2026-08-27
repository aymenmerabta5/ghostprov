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
const anthropicDesign = fs.readFileSync(path.join(
  root,
  "docs",
  "superpowers",
  "specs",
  "2026-08-23-anthropic-claude-code-compatibility-design.md",
), "utf8");
const anthropicPlan = fs.readFileSync(path.join(
  root,
  "docs",
  "superpowers",
  "plans",
  "2026-08-23-anthropic-claude-code-compatibility.md",
), "utf8");

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
    expect(readme).toContain("| `GP_V2_MAX_ACTIVE_PER_SANDBOX` | `8`");
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
    expect(readme).toContain("through 256 KiB retain the exact R4 32-frame/512-KiB host flight");
    expect(readme).toContain("two clean request flights are required before acceleration resumes");
    expect(readme).toContain("six pre-commit bodies of at most 2 MiB");
    expect(readme).toContain("at most four `request_start` execution risks");
    expect(readme).toContain("four-request standard until that latency-critical request leaves");
    expect(readme).toContain("two true bulk transfers remain forbidden");
    expect(envExample).toContain("GP_V2_INITIAL_ACTIVE_PER_SANDBOX=8");
    expect(envExample).toContain("GP_V2_MAX_ACTIVE_PER_SANDBOX=8");
    expect(envExample).toContain("GP_V2_SIDECAR_REVISION=r5");
    expect(envExample).toContain("GP_V2_SIDECAR_PROJECT_REVISIONS=project-uuid-2=r1");
    expect(envExample).toContain("GP_ROUTER_QUEUE_TIMEOUT_MS=30000");
    expect(envExample).toContain("GHOSTPROV_API_KEY=GHOST_IS_KING");
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
      "ANTHROPIC_API_KEY=GHOST_IS_KING",
      "GHOSTPROV_API_KEY",
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
    expect(envExample).toContain("GHOSTPROV_API_KEY=GHOST_IS_KING");
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
      '$env:ANTHROPIC_API_KEY = "GHOST_IS_KING"',
      verifierBase,
    );
    const verifierClaude = readme.indexOf("claude", verifierKey);

    expect(verifierStart).toBeGreaterThan(0);
    expect(secondTerminal).toBeGreaterThan(verifierStart);
    expect(verifierBase).toBeGreaterThan(secondTerminal);
    expect(verifierKey).toBeGreaterThan(verifierBase);
    expect(verifierClaude).toBeGreaterThan(verifierKey);
  });

  it("documents current mid-conversation system and custom developer roles", () => {
    for (const value of [
      "claude-fable-5",
      "claude-mythos-5",
      "claude-opus-4-8",
      "claude-opus-5",
      "immediately follow a user turn",
      "final or immediately followed by an assistant turn",
      "Consecutive `system` messages are coalesced",
      "`developer` is accepted only for custom non-`claude-*` model IDs",
    ]) {
      expect(readme).toContain(value);
      expect(anthropicDesign).toContain(value);
    }
  });

  it("documents verifier aggregate, queue, and deadline defaults", () => {
    for (const value of [
      "eight concurrent forwards",
      "2 MiB per SSE event",
      "64 MiB aggregate parser-memory ceiling",
      "32 forward waiters",
      "32 incoming-body waiters",
      "300-second forward deadline",
      "1 MiB per-run output queue",
    ]) expect(readme).toContain(value);
  });

  it("tracks the installed Claude version and current cache-write wire contract", () => {
    for (const source of [anthropicDesign, anthropicPlan]) {
      expect(source).toContain("2.1.241");
      expect(source).toContain("2.1.87");
      expect(source).not.toContain("2.1.220");
    }
    expect(anthropicPlan).toContain(
      'role: "user" | "assistant" | "system" | "developer";',
    );
    expect(anthropicPlan).toContain("cache_write_tokens: 10");
    expect(anthropicPlan).toContain("cache_creation_input_tokens: 10");
    expect(anthropicPlan).not.toContain(
      "`cache_creation_input_tokens` is `null` because Responses does not distinguish a cache write",
    );
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
      "build:v2-sidecar-release": "bun scripts/build_v2_sidecar_release.ts --write",
      "check:v2-sidecar-release": "bun scripts/build_v2_sidecar_release.ts",
      "check:v2-sidecar-deno": "bunx deno@2.9.5 check --no-config --no-remote --no-npm --no-lock src/v2/sidecar/releases/r5/sidecar.bundle.js",
      "test:v2-deno-fs": "bunx deno@2.9.5 test --no-check --unstable-sloppy-imports --allow-read --allow-write --allow-env tests/deno/v2-journal-storage.deno.ts",
    });
    expect(pkg.scripts).not.toHaveProperty("test:daemon");
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
      "route.attempt_expired",
      "transport.committed",
      "response.first_event",
      "fleet.sample",
      "monitor.sample",
      "admission_wait_ms",
      "commit_ack_ms",
      "post_commit_first_event_ms",
    ]) expect(readme).toContain(value);
    expect(readme).toContain("manual restart");
    expect(readme).toContain("actually-routable runtime counts");
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
    expect(readme).toContain("a second independent sample two seconds later");
    expect(readme).toContain("one slow sample never relaunches");
    expect(readme).toContain("completion estimates cap extreme model durations");
    expect(readme).toContain("neutral ten-second baseline every idle minute");
  });

  it("documents failover at the exact R2 execution fence", () => {
    expect(readme).toContain("exact bootstrap-bound V2 sidecar");
    expect(readme).toContain("Admission-credit ownership does not end R2 failover");
    expect(readme).toContain("request_start");
    expect(readme).toContain("four times the selected RTT clamped to 5–10 seconds");
    expect(readme).toContain("can never extend the unchanged absolute 30-second deadline");
    expect(readme).toContain("Requests through 2 MiB and standard/R4 rollback retain their exact prior attempt behavior");
    expect(readme).toContain("never duplicated");
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
