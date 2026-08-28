import { describe, expect, it } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  assertFreshV2RollbackSandbox,
  resolveV2BootstrapProbeOptions,
  sanitizeV2BootstrapRenderLog,
} from "../scripts/probe_v2_bootstrap";
import { DEFAULT_V2_SIDECAR_REVISION } from "../src/v2/sidecar/assets";

const VALID = {
  GP_LIVE_V2_BOOTSTRAP: "1",
  GP_CANDIDATE: "1",
  GP_LIVE_V2_PROJECT_ID: "e631d12c-d917-4434-b360-41b4715d6679",
  PRISM_COOKIE_FILE: ".ghostprov/test-cookies.json",
  PORT: "18888",
};

describe("V2 live bootstrap probe guards", () => {
  it("requires every live, candidate, project, and cookie opt-in", () => {
    expect(() => resolveV2BootstrapProbeOptions({})).toThrow("GP_LIVE_V2_BOOTSTRAP=1");
    expect(() => resolveV2BootstrapProbeOptions({
      GP_LIVE_V2_BOOTSTRAP: "1",
    })).toThrow("GP_CANDIDATE=1");
    expect(() => resolveV2BootstrapProbeOptions({
      ...VALID,
      GP_LIVE_V2_PROJECT_ID: undefined,
    })).toThrow("GP_LIVE_V2_PROJECT_ID");
    expect(() => resolveV2BootstrapProbeOptions({
      ...VALID,
      PRISM_COOKIE_FILE: undefined,
    })).toThrow("PRISM_COOKIE_FILE");
  });

  it("refuses protected ports 8787/18787 and accepts exactly one explicit project", () => {
    expect(() => resolveV2BootstrapProbeOptions({
      ...VALID,
      PORT: "8787",
    })).toThrow("protected port 8787");
    expect(() => resolveV2BootstrapProbeOptions({
      ...VALID,
      PORT: "18787",
    })).toThrow("protected port 18787");
    expect(() => resolveV2BootstrapProbeOptions({
      ...VALID,
      GP_LIVE_V2_PROJECT_ID: `${VALID.GP_LIVE_V2_PROJECT_ID},69288e8b-c183-45f3-9131-d64b05b98928`,
    })).toThrow("exactly one");

    expect(resolveV2BootstrapProbeOptions(VALID)).toMatchObject({
      projectId: VALID.GP_LIVE_V2_PROJECT_ID,
      port: 18888,
      prismOrigin: "https://prism.openai.com",
      revision: DEFAULT_V2_SIDECAR_REVISION,
    });
  });

  it("selects revisions explicitly and requires a fresh prior session for rollback proof", () => {
    expect(resolveV2BootstrapProbeOptions({
      ...VALID,
      GP_LIVE_V2_REVISION: "r1",
      GP_LIVE_V2_ROLLBACK_FROM_R2_SANDBOX_SESSION_ID: "sandbox-session-r2",
    })).toMatchObject({
      revision: "r1",
      rollbackFromR2SandboxSessionId: "sandbox-session-r2",
    });
    expect(resolveV2BootstrapProbeOptions({
      ...VALID,
      GP_LIVE_V2_REVISION: "r3",
    })).toMatchObject({ revision: "r3" });
    expect(resolveV2BootstrapProbeOptions({
      ...VALID,
      GP_LIVE_V2_REVISION: "r7",
    })).toMatchObject({ revision: "r7" });
    expect(() => resolveV2BootstrapProbeOptions({
      ...VALID,
      GP_LIVE_V2_REVISION: "r8",
    })).toThrow(/unsupported.*r8/i);
    expect(() => resolveV2BootstrapProbeOptions({
      ...VALID,
      GP_LIVE_V2_REVISION: "r2",
      GP_LIVE_V2_ROLLBACK_FROM_R2_SANDBOX_SESSION_ID: "sandbox-session-r2",
    })).toThrow(/requires.*r1/i);
    expect(() => resolveV2BootstrapProbeOptions({
      ...VALID,
      GP_LIVE_V2_REVISION: "r1",
      GP_LIVE_V2_ROLLBACK_FROM_R2_SANDBOX_SESSION_ID: "bad session id",
    })).toThrow(/session ID is invalid/i);

    expect(() => assertFreshV2RollbackSandbox("sandbox-session-r2", "sandbox-session-r2"))
      .toThrow(/fresh sandbox session.*same project/i);
    expect(() => assertFreshV2RollbackSandbox("sandbox-session-r2", "sandbox-session-r1"))
      .not.toThrow();
  });

  it("contains no project-export, deletion, or production-port mutation path", async () => {
    const source = await readFile(
      new URL("../scripts/probe_v2_bootstrap.ts", import.meta.url),
      "utf8",
    );
    expect(source).not.toContain("/zip");
    expect(source).not.toMatch(/deleteProject|projects\.delete|server\.close/);
    expect(source).toContain("http://127.0.0.1:8787");
    expect(source).toContain('"ghostprovV2Data"');
    expect(source).toContain('"ghostprovV2Control"');
    expect(source).not.toMatch(/field:\s*["']ghostprovV2["']/);
  });

  it("redacts credentials and bounds live render diagnostics", () => {
    const secret = "sk-bootstrap-secret-123456789012345678901234";
    const diagnostic = sanitizeV2BootstrapRenderLog(
      `Authorization: Bearer ${secret}\ntoken=${secret}\n${"x".repeat(20_000)}`,
    );

    expect(diagnostic).not.toContain(secret);
    expect(diagnostic).toContain("[REDACTED]");
    expect(diagnostic.length).toBeLessThanOrEqual(8_192);
  });
});
