import { describe, expect, it } from "bun:test";
import { resolveMultiSandboxConfig } from "../src/config/multi-sandbox";
import type { MachineSample } from "../src/core/ports";
import { projectId } from "../src/sandbox/types";
import { DEFAULT_V2_SIDECAR_REVISION } from "../src/v2/sidecar/assets";
import {
  V2_EVENT_JUMBO_OVERHEAD_BYTES,
  V2_EVENT_MAX_BUFFERED_EVENTS,
  V2_EVENT_PHYSICAL_FACTOR,
  V2_EVENT_RETAINED_FACTOR,
  V2_EVENT_RETAINED_OVERHEAD_BYTES,
  V2_EVENT_SOFT_BUFFER_BYTES,
} from "../src/v2/memory/event-memory-sizing";

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
  it("rejects a sidecar budget that cannot fit the legal jumbo event lane", () => {
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_CANDIDATE: "1",
      GP_V2_SIDECAR_MEMORY_BUDGET_BYTES: String(257 * 1024 * 1024),
    }, MACHINE)).toThrow("64 MiB atomic event");
  });

  it("rejects false response maxima until the sidecar protocol exceeds 64 MiB", () => {
    for (const bytes of [32 * 1024 * 1024, 128 * 1024 * 1024, 256 * 1024 * 1024]) {
      expect(() => resolveMultiSandboxConfig({
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: A,
        GP_CANDIDATE: "1",
        GP_V2_RESPONSE_MAX_BYTES: String(bytes),
      }, MACHINE)).toThrow("exactly 64 MiB");
    }
  });

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
    expect(config.apiKey).toBe("GHOST_IS_KING");
  });

  it("defaults to authenticated V2-ready fleet policy", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config).not.toHaveProperty("transport");
    expect(config.apiKey).toBe("GHOST_IS_KING");
    expect(config.sessionMode).toBe("authenticated");
    expect(config.allowAnonymous).toBe(false);
    expect(config.guestMaxProjects).toBe(1);
    expect(config.v2SandboxCapacity).toBe(128);
    expect(config.v2InitialActivePerSandbox).toBe(8);
    expect(config.v2MaxActivePerSandbox).toBe(8);
    expect(config.v2ResponseMaxBytes).toBe(64 * 1024 * 1024);
    expect(config.v2HostEventMemory).toEqual({
      softBufferBytes: V2_EVENT_SOFT_BUFFER_BYTES,
      physicalFactor: V2_EVENT_PHYSICAL_FACTOR,
      jumboOverheadBytes: V2_EVENT_JUMBO_OVERHEAD_BYTES,
      retainedFactor: V2_EVENT_RETAINED_FACTOR,
      retainedOverheadBytes: V2_EVENT_RETAINED_OVERHEAD_BYTES,
      maxBufferedEvents: V2_EVENT_MAX_BUFFERED_EVENTS,
      ordinaryBytes: (
        V2_EVENT_SOFT_BUFFER_BYTES * V2_EVENT_PHYSICAL_FACTOR +
        V2_EVENT_JUMBO_OVERHEAD_BYTES +
        V2_EVENT_SOFT_BUFFER_BYTES * V2_EVENT_RETAINED_FACTOR +
        V2_EVENT_MAX_BUFFERED_EVENTS * V2_EVENT_RETAINED_OVERHEAD_BYTES
      ) * 8,
      jumboBytes: 64 * 1024 * 1024 * V2_EVENT_PHYSICAL_FACTOR +
        V2_EVENT_JUMBO_OVERHEAD_BYTES,
      totalBytes: (
        V2_EVENT_SOFT_BUFFER_BYTES * V2_EVENT_PHYSICAL_FACTOR +
        V2_EVENT_JUMBO_OVERHEAD_BYTES +
        V2_EVENT_SOFT_BUFFER_BYTES * V2_EVENT_RETAINED_FACTOR +
        V2_EVENT_MAX_BUFFERED_EVENTS * V2_EVENT_RETAINED_OVERHEAD_BYTES
      ) * 8 +
        64 * 1024 * 1024 * V2_EVENT_PHYSICAL_FACTOR +
        V2_EVENT_JUMBO_OVERHEAD_BYTES,
    });
    expect(config.v2TransportReservationBytes).toBe(16 * 1024 * 1024);
    expect(config.v2YSweetRefreshMs).toBe(45 * 60 * 1000);
    expect(config.v2SidecarMemoryBudgetBytes).toBe(512 * 1024 * 1024);
    expect(config.v2DiagnosticsEnabled).toBe(false);
    expect(config.v2DiagnosticsIntervalMs).toBe(1_000);
    expect(config.v2BatchingEnabled).toBe(false);
    expect(config.v2SidecarRevision).toBe(DEFAULT_V2_SIDECAR_REVISION);
    expect(config.v2SidecarProjectRevisions).toEqual({});
    expect(config.desiredSandboxes).toBe(1);
    expect(config.heartbeatMs).toBe(10_000);
    expect(config.routerQueueTimeoutMs).toBe(30_000);
    expect(config.shutdownDrainMs).toBe(300_000);
  });

  it("accepts a configured inbound key and rejects unsafe key text", () => {
    expect(resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GHOSTPROV_API_KEY: "custom-local-key",
    }, MACHINE).apiKey).toBe("custom-local-key");
    for (const value of ["line\nbreak", "x".repeat(4_097)]) {
      expect(() => resolveMultiSandboxConfig({
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: A,
        GHOSTPROV_API_KEY: value,
      }, MACHINE)).toThrow("GHOSTPROV_API_KEY");
    }
  });

  it("supports immutable R1/R2/R3/R4/R5 releases and exact known-project overrides", () => {
    const config = resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: `${A},${B}`,
      GP_V2_SIDECAR_REVISION: "r1",
      GP_V2_SIDECAR_PROJECT_REVISIONS: `${B}=r5`,
    }, MACHINE);
    expect(config.v2SidecarRevision).toBe("r1");
    expect(config.v2SidecarProjectRevisions).toEqual({ [B]: "r5" });
    expect(Object.isFrozen(config.v2SidecarProjectRevisions)).toBe(true);

    for (const revisions of [
      `${B}=r6`,
      `${B}=r1,${B}=r2`,
      "*=r1",
      "not-an-assignment",
      "=r1",
    ]) {
      expect(() => resolveMultiSandboxConfig({
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: `${A},${B}`,
        GP_V2_SIDECAR_PROJECT_REVISIONS: revisions,
      }, MACHINE)).toThrow();
    }
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_SIDECAR_PROJECT_REVISIONS: `${B}=r1`,
    }, MACHINE)).toThrow(/unknown configured project/i);
    expect(() => resolveMultiSandboxConfig({
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_SIDECAR_REVISION: "R1",
    }, MACHINE)).toThrow(/unsupported.*R1/i);
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
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "8",
    }, MACHINE);
    expect(config.v2InitialActivePerSandbox).toBe(4);
    expect(config.v2MaxActivePerSandbox).toBe(8);
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
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "9",
    }, MACHINE)).toThrow("terminal-spool capacity");
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
