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
    expect(config.slotsPerProject).toBe(30);
  });

  it("preserves legacy PRISM_DOC_ID as a single-project allowlist", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_DOC_ID: A,
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
      PRISM_COOKIE: "session=value",
      GP_CANDIDATE: "1",
      PORT: "8787",
    }, MACHINE)).toThrow("8787");
  });

  it("rejects missing projects, duplicate invalid IDs, and missing cookie sources", () => {
    expect(() => resolveMultiSandboxConfig({ PRISM_COOKIE: "session=value" }, MACHINE)).toThrow("PRISM_PROJECT_IDS");
    expect(() => resolveMultiSandboxConfig({ PRISM_PROJECT_IDS: "bad", PRISM_COOKIE: "session=value" }, MACHINE)).toThrow("ProjectId");
    expect(() => resolveMultiSandboxConfig({ PRISM_PROJECT_IDS: A }, MACHINE)).toThrow("PRISM_COOKIE");
  });

  it("parses explicit heartbeat and project limits without weakening ten seconds", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE: "session=value",
      GP_HEARTBEAT_MS: "10000",
      GP_MAX_PROJECTS: "4",
      GP_SLOTS: "24",
    }, MACHINE);
    expect(config.heartbeatMs).toBe(10_000);
    expect(config.maxProjects).toBe(4);
    expect(config.slotsPerProject).toBe(24);
    expect(() => resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE: "session=value",
      GP_HEARTBEAT_MS: "10001",
    }, MACHINE)).toThrow("10000");
    expect(() => resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE: "session=value",
      GP_SLOTS: "31",
    }, MACHINE)).toThrow("30");
  });
});
