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

  it("defaults to authenticated V2-ready fleet policy", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config.transport).toBe("auto");
    expect(config.sessionMode).toBe("authenticated");
    expect(config.allowAnonymous).toBe(false);
    expect(config.guestMaxProjects).toBe(1);
    expect(config.v2SandboxCapacity).toBe(128);
    expect(config.v2ResponseMaxBytes).toBe(64 * 1024 * 1024);
    expect(config.v2TransportReservationBytes).toBe(16 * 1024 * 1024);
    expect(config.v2YSweetRefreshMs).toBe(45 * 60 * 1000);
    expect(config.v2SidecarMemoryBudgetBytes).toBe(512 * 1024 * 1024);
    expect(config.desiredSandboxes).toBe(4);
    expect(config.heartbeatMs).toBe(10_000);
  });

  it("allows a project-less managed V2 fleet and derives its size", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_TRANSPORT: "v2",
      GP_AUTO_CREATE_PROJECTS: "1",
      GP_TARGET_CONCURRENCY: "1024",
      GP_MAX_PROJECTS: "16",
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config.projectIds).toEqual([]);
    expect(config.desiredSandboxes).toBe(8);
  });

  it("requires explicit guest mode and caps it independently", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-guest.json",
      GP_PRISM_SESSION: "anonymous",
      GP_TRANSPORT: "v2",
      GP_TARGET_SANDBOXES: "4",
      GP_GUEST_MAX_PROJECTS: "2",
      GP_MAX_PROJECTS: "8",
      GP_AUTO_CREATE_PROJECTS: "1",
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config.sessionMode).toBe("anonymous");
    expect(config.desiredSandboxes).toBe(2);
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-guest.json",
      GP_PRISM_SESSION: "anonymous",
      GP_TRANSPORT: "v1",
      PRISM_PROJECT_IDS: A,
      GP_CANDIDATE: "1",
    }, MACHINE)).toThrow("anonymous Prism sessions require GP_TRANSPORT=v2");
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
      GP_SLOTS: "24",
    }, MACHINE);
    expect(config.heartbeatMs).toBe(10_000);
    expect(config.maxProjects).toBe(4);
    expect(config.slotsPerProject).toBe(24);
    expect(() => resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "session=value",
      GP_HEARTBEAT_MS: "10001",
    }, MACHINE)).toThrow("10000");
    expect(() => resolveMultiSandboxConfig({
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "session=value",
      GP_SLOTS: "31",
    }, MACHINE)).toThrow("30");
  });
});
