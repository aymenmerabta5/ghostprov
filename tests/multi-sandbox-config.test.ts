import { describe, expect, it } from "bun:test";
import { resolveMultiSandboxConfig } from "../src/config/multi-sandbox";
import type { MachineSample } from "../src/core/ports";
import { projectId } from "../src/sandbox/types";

const A = "e631d12c-d917-4434-b360-41b4715d6679";
const B = "69288e8b-c183-45f3-9131-d64b05b98928";
const MACHINE: MachineSample = {
  totalBytes: 32 * 1024 ** 3,
  freeBytes: 19 * 1024 ** 3,
  rssBytes: 320 * 1024 ** 2,
  heapBytes: 100 * 1024 ** 2,
  externalBytes: 20 * 1024 ** 2,
  logicalProcessors: 12,
};

describe("multi-sandbox configuration", () => {
  it("deduplicates an explicit project allowlist", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: `${A}, ${B},${A}`,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config.projectIds).toEqual([projectId(A), projectId(B)]);
    expect(config.cookieFile).toMatch(/prism-cookies\.json$/);
    expect(config.port).toBe(18787);
    expect(config.candidate).toBe(true);
  });

  it("defaults to authenticated V2-ready fleet policy", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config).not.toHaveProperty("transport");
    expect(config.sessionMode).toBe("authenticated");
    expect(config.allowAnonymous).toBe(false);
    expect(config.guestMaxProjects).toBe(1);
    expect(config.v2SandboxCapacity).toBe(128);
    expect(config.v2InitialActivePerSandbox).toBe(8);
    expect(config.v2MaxActivePerSandbox).toBe(16);
    expect(config.v2ResponseMaxBytes).toBe(64 * 1024 * 1024);
    expect(config.v2TransportReservationBytes).toBe(16 * 1024 * 1024);
    expect(config.v2YSweetRefreshMs).toBe(45 * 60 * 1000);
    expect(config.v2SidecarMemoryBudgetBytes).toBe(512 * 1024 * 1024);
    expect(config.v2DiagnosticsEnabled).toBe(false);
    expect(config.v2DiagnosticsIntervalMs).toBe(1_000);
    expect(config.v2BatchingEnabled).toBe(false);
    expect(config.desiredSandboxes).toBe(1);
    expect(config.heartbeatMs).toBe(10_000);
    expect(config.routerQueueTimeoutMs).toBe(30_000);
    expect(config.shutdownDrainMs).toBe(300_000);
  });

  it("parses bounded opt-in V2 diagnostics", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_CANDIDATE: "1",
      GP_V2_DIAGNOSTICS: "1",
      GP_V2_DIAGNOSTICS_INTERVAL_MS: "500",
      GP_V2_BATCHING: "1",
    }, MACHINE);
    expect(config.v2DiagnosticsEnabled).toBe(true);
    expect(config.v2DiagnosticsIntervalMs).toBe(500);
    expect(config.v2BatchingEnabled).toBe(true);
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_DIAGNOSTICS: "yes",
    }, MACHINE)).toThrow("GP_V2_DIAGNOSTICS");
  });

  it("rejects project creation and project-less V2 configuration", () => {
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_AUTO_CREATE_PROJECTS: "1",
      GP_TARGET_CONCURRENCY: "1024",
      GP_MAX_PROJECTS: "16",
      GP_CANDIDATE: "1",
    }, MACHINE)).toThrow("GP_AUTO_CREATE_PROJECTS");
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_CANDIDATE: "1",
    }, MACHINE)).toThrow("PRISM_PROJECT_IDS");
  });

  it("bounds transport-active capacity below the physical engine ceiling", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: `${A},${B}`,
      GP_TARGET_SANDBOXES: "1",
      GP_TARGET_CONCURRENCY: "1",
      GP_V2_INITIAL_ACTIVE_PER_SANDBOX: "4",
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "12",
    }, MACHINE);
    expect(config.v2InitialActivePerSandbox).toBe(4);
    expect(config.v2MaxActivePerSandbox).toBe(12);
    expect(config.desiredSandboxes).toBe(2);
    expect(config).not.toHaveProperty("targetSandboxes");
    expect(config).not.toHaveProperty("targetConcurrency");
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_INITIAL_ACTIVE_PER_SANDBOX: "9",
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "8",
    }, MACHINE)).toThrow("INITIAL_ACTIVE");
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "129",
    }, MACHINE)).toThrow("physical");
  });

  it("requires explicit guest mode and enforces its configured-project cap", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-guest.json",
      PRISM_PROJECT_IDS: `${A},${B}`,
      GP_PRISM_SESSION: "anonymous",
      GP_GUEST_MAX_PROJECTS: "2",
      GP_MAX_PROJECTS: "8",
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config.sessionMode).toBe("anonymous");
    expect(config.desiredSandboxes).toBe(2);
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-guest.json",
      PRISM_PROJECT_IDS: `${A},${B}`,
      GP_PRISM_SESSION: "anonymous",
      GP_GUEST_MAX_PROJECTS: "1",
      GP_MAX_PROJECTS: "8",
    }, MACHINE)).toThrow("GP_GUEST_MAX_PROJECTS=1");
  });

  it("rejects every non-explicit project reuse policy", () => {
    for (const projectReuse of ["managed", "eligible-owned"]) {
      expect(() => resolveMultiSandboxConfig({
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: A,
        GP_PROJECT_REUSE: projectReuse,
      }, MACHINE)).toThrow("must be explicit");
    }
  });

  it("preserves legacy PRISM_DOC_ID as a single-project allowlist", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_DOC_ID: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "prism_session_token=value",
      PORT: "18888",
    }, MACHINE);

    expect(config.projectIds).toEqual([projectId(A)]);
    expect(config.initialCookieHeader).toBe("prism_session_token=value");
    expect(config.port).toBe(18888);
  });

  it("rejects candidate port 8787", () => {
    expect(() => resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "session=value",
      GP_CANDIDATE: "1",
      PORT: "8787",
    }, MACHINE)).toThrow("8787");
  });

  it("rejects missing projects, duplicate invalid IDs, and missing cookie sources", () => {
    expect(() => resolveMultiSandboxConfig({ PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json" }, MACHINE)).toThrow("PRISM_PROJECT_IDS");
    expect(() => resolveMultiSandboxConfig({ PRISM_PROJECT_IDS: "bad", PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json" }, MACHINE)).toThrow("ProjectId");
    expect(() => resolveMultiSandboxConfig({ PRISM_PROJECT_IDS: A }, MACHINE)).toThrow("PRISM_COOKIE");
  });

  it("parses explicit heartbeat and project limits without weakening ten seconds", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "session=value",
      GP_HEARTBEAT_MS: "10000",
      GP_MAX_PROJECTS: "4",
    }, MACHINE);
    expect(config.heartbeatMs).toBe(10_000);
    expect(config.maxProjects).toBe(4);
    expect(() => resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "session=value",
      GP_HEARTBEAT_MS: "10001",
    }, MACHINE)).toThrow("10000");
  });
});
