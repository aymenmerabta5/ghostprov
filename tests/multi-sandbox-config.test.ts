import { describe, expect, it } from "bun:test";
import { resolveMultiSandboxConfig } from "../src/config/multi-sandbox";
import type { MachineSample } from "../src/core/ports";
import {
  DEFAULT_CANDIDATE_PORT,
  PROTECTED_SERVICE_PORT,
} from "../src/core/ports";
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
const BASE_ENV = Object.freeze({ GHOSTPROV_API_KEY: "test-ghostprov-key-0123456789abcdef" });
const MACHINE: MachineSample = {
  totalBytes: 32 * 1024 ** 3,
  freeBytes: 19 * 1024 ** 3,
  rssBytes: 320 * 1024 ** 2,
  heapBytes: 100 * 1024 ** 2,
  externalBytes: 20 * 1024 ** 2,
  logicalProcessors: 12,
};

describe("multi-sandbox configuration", () => {
  it("publishes one protected-service and candidate port policy", () => {
    expect(PROTECTED_SERVICE_PORT).toBe(18_787);
    expect(DEFAULT_CANDIDATE_PORT).toBe(18_888);
    expect(resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
    }, MACHINE).port).toBe(PROTECTED_SERVICE_PORT);
    for (const value of ["true", " true ", "false", "2"]) {
      expect(() => resolveMultiSandboxConfig({
        ...BASE_ENV,
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: A,
        GP_CANDIDATE: value,
      }, MACHINE)).toThrow("GP_CANDIDATE must be 0 or 1");
    }
  });

  it("rejects a sidecar budget that cannot fit the legal jumbo event lane", () => {
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_CANDIDATE: "1",
      GP_V2_SIDECAR_MEMORY_BUDGET_BYTES: String(257 * 1024 * 1024),
    }, MACHINE)).toThrow("64 MiB atomic event");
  });

  it("rejects false response maxima until the sidecar protocol exceeds 64 MiB", () => {
    for (const bytes of [32 * 1024 * 1024, 128 * 1024 * 1024, 256 * 1024 * 1024]) {
      expect(() => resolveMultiSandboxConfig({
        ...BASE_ENV,
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: A,
        GP_CANDIDATE: "1",
        GP_V2_RESPONSE_MAX_BYTES: String(bytes),
      }, MACHINE)).toThrow("exactly 64 MiB");
    }
  });

  it("deduplicates an explicit project allowlist", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_PROJECT_IDS: `${A}, ${B},${A}`,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config.projectIds).toEqual([projectId(A), projectId(B)]);
    expect(config.cookieFile).toMatch(/prism-cookies\.json$/);
    expect(config.port).toBe(18_888);
    expect(config.candidate).toBe(true);
    expect(config.apiKey).toBe(BASE_ENV.GHOSTPROV_API_KEY);
  });

  it("defaults to an authenticated eight-project managed V2 fleet", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_CANDIDATE: "1",
    }, MACHINE);

    expect(config).not.toHaveProperty("transport");
    expect(config.apiKey).toBe(BASE_ENV.GHOSTPROV_API_KEY);
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
      ) * 8 * 8,
      jumboBytes: 64 * 1024 * 1024 * V2_EVENT_PHYSICAL_FACTOR +
        V2_EVENT_JUMBO_OVERHEAD_BYTES,
      totalBytes: (
        V2_EVENT_SOFT_BUFFER_BYTES * V2_EVENT_PHYSICAL_FACTOR +
        V2_EVENT_JUMBO_OVERHEAD_BYTES +
        V2_EVENT_SOFT_BUFFER_BYTES * V2_EVENT_RETAINED_FACTOR +
        V2_EVENT_MAX_BUFFERED_EVENTS * V2_EVENT_RETAINED_OVERHEAD_BYTES
      ) * 8 * 8 +
        64 * 1024 * 1024 * V2_EVENT_PHYSICAL_FACTOR +
        V2_EVENT_JUMBO_OVERHEAD_BYTES,
    });
    expect(config.v2TransportReservationBytes).toBe(16 * 1024 * 1024);
    expect(config.v2YSweetRefreshMs).toBe(45 * 60 * 1000);
    expect(config.v2SidecarMemoryBudgetBytes).toBe(512 * 1024 * 1024);
    expect(config.v2DiagnosticsEnabled).toBe(false);
    expect(config.v2DiagnosticsIntervalMs).toBe(1_000);
    expect(config.v2BatchingEnabled).toBe(true);
    expect(config.v2SidecarRevision).toBe(DEFAULT_V2_SIDECAR_REVISION);
    expect(config.v2SidecarProjectRevisions).toEqual({});
    expect(config.projectIds).toEqual([]);
    expect(config.desiredSandboxes).toBe(8);
    expect(config.fleetManifestFile).toBeNull();
    expect(config.projectReplacementFailureThreshold).toBe(3);
    expect(config.coldStartConcurrency).toBe(2);
    expect(config.heartbeatMs).toBe(10_000);
    expect(config.routerQueueTimeoutMs).toBe(30_000);
    expect(config.shutdownDrainMs).toBe(300_000);
  });

  it("accepts a configured inbound key and rejects unsafe key text", () => {
    const customKey = "custom-local-key-0123456789abcdef";
    expect(resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GHOSTPROV_API_KEY: customKey,
    }, MACHINE).apiKey).toBe(customKey);
    for (const value of ["too-short", "line\nbreak".repeat(4), "x".repeat(4_097)]) {
      expect(() => resolveMultiSandboxConfig({
        ...BASE_ENV,
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: A,
        GHOSTPROV_API_KEY: value,
      }, MACHINE)).toThrow("GHOSTPROV_API_KEY");
    }
  });

  it("supports immutable R1/R2/R3/R4/R5/R6/R7/R8 releases and exact known-project overrides", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: `${A},${B}`,
      GP_V2_SIDECAR_REVISION: "r1",
      GP_V2_SIDECAR_PROJECT_REVISIONS: `${B}=r8`,
    }, MACHINE);
    expect(config.v2SidecarRevision).toBe("r1");
    expect(config.v2SidecarProjectRevisions).toEqual({ [B]: "r8" });
    expect(Object.isFrozen(config.v2SidecarProjectRevisions)).toBe(true);

    for (const revisions of [
      `${B}=r9`,
      `${B}=r1,${B}=r2`,
      "*=r1",
      "not-an-assignment",
      "=r1",
    ]) {
      expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: `${A},${B}`,
        GP_V2_SIDECAR_PROJECT_REVISIONS: revisions,
      }, MACHINE)).toThrow();
    }
    expect(resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_SIDECAR_PROJECT_REVISIONS: `${B}=r1`,
    }, MACHINE).v2SidecarProjectRevisions).toEqual({ [B]: "r1" });
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_SIDECAR_REVISION: "R1",
    }, MACHINE)).toThrow(/unsupported.*R1/i);
  });

  it("parses bounded V2 diagnostics and the explicit batching rollback", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
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
    expect(resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_BATCHING: "0",
    }, MACHINE).v2BatchingEnabled).toBe(false);
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_DIAGNOSTICS: "yes",
    }, MACHINE)).toThrow("GP_V2_DIAGNOSTICS");
  });

  it("accepts project-less automatic provisioning and legacy creation flags", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_AUTO_CREATE_PROJECTS: "1",
      GP_TARGET_CONCURRENCY: "1024",
      GP_MAX_PROJECTS: "16",
      GP_CANDIDATE: "1",
    }, MACHINE);
    expect(config.projectIds).toEqual([]);
    expect(config.desiredSandboxes).toBe(8);
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_AUTO_CREATE_PROJECTS: "yes",
    }, MACHINE)).toThrow("GP_AUTO_CREATE_PROJECTS");
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_PROJECT_REPLACEMENT_FAILURES: "2",
    }, MACHINE)).toThrow("GP_PROJECT_REPLACEMENT_FAILURES");
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_TARGET_SANDBOXES: "4",
      GP_PROVISION_CONCURRENCY: "2",
      GP_COLD_START_CONCURRENCY: "3",
    }, MACHINE)).toThrow("GP_COLD_START_CONCURRENCY");
  });

  it("bounds transport-active capacity below the physical engine ceiling", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: `${A},${B}`,
      GP_TARGET_SANDBOXES: "1",
      GP_TARGET_CONCURRENCY: "1",
      GP_V2_INITIAL_ACTIVE_PER_SANDBOX: "4",
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "8",
    }, MACHINE);
    expect(config.v2InitialActivePerSandbox).toBe(4);
    expect(config.v2MaxActivePerSandbox).toBe(8);
    expect(config.desiredSandboxes).toBe(1);
    expect(config).not.toHaveProperty("targetSandboxes");
    expect(config).not.toHaveProperty("targetConcurrency");
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_INITIAL_ACTIVE_PER_SANDBOX: "9",
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "8",
    }, MACHINE)).toThrow("INITIAL_ACTIVE");
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "9",
    }, MACHINE)).toThrow("terminal-spool capacity");
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_PROJECT_IDS: A,
      GP_V2_MAX_ACTIVE_PER_SANDBOX: "129",
    }, MACHINE)).toThrow("physical");
  });

  it("requires explicit guest mode and enforces its configured-project cap", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
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
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-guest.json",
      PRISM_PROJECT_IDS: `${A},${B}`,
      GP_PRISM_SESSION: "anonymous",
      GP_GUEST_MAX_PROJECTS: "1",
      GP_MAX_PROJECTS: "8",
    }, MACHINE)).toThrow("GP_GUEST_MAX_PROJECTS=1");
  });

  it("accepts only non-adopting legacy project reuse values", () => {
    for (const projectReuse of ["explicit", "managed"]) {
      expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
        PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
        PRISM_PROJECT_IDS: A,
        GP_PROJECT_REUSE: projectReuse,
      }, MACHINE)).not.toThrow();
    }
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_PROJECT_REUSE: "eligible-owned",
    }, MACHINE)).toThrow("explicit or managed");
  });

  it("preserves legacy PRISM_DOC_ID as a single-project allowlist", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_DOC_ID: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "prism_session_token=value",
      PORT: "18888",
    }, MACHINE);

    expect(config.projectIds).toEqual([projectId(A)]);
    expect(config.initialCookieHeader).toBe("prism_session_token=value");
    expect(config.port).toBe(18888);
  });

  it("defaults candidates to 18888 and rejects the protected service port 18787", () => {
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "session=value",
      GP_CANDIDATE: "1",
      PORT: "18787",
    }, MACHINE)).toThrow("18787");
    expect(resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      GP_CANDIDATE: "1",
      PORT: "18888",
    }, MACHINE).port).toBe(18_888);
  });

  it("accepts missing migration IDs but rejects invalid IDs and missing cookie sources", () => {
    expect(resolveMultiSandboxConfig({
      ...BASE_ENV, PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json" }, MACHINE).projectIds).toEqual([]);
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV, PRISM_PROJECT_IDS: "bad", PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json" }, MACHINE)).toThrow("ProjectId");
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV, PRISM_PROJECT_IDS: A }, MACHINE)).toThrow("PRISM_COOKIE");
  });

  it("parses explicit heartbeat and project limits without weakening ten seconds", () => {
    const config = resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "session=value",
      GP_HEARTBEAT_MS: "10000",
      GP_MAX_PROJECTS: "4",
    }, MACHINE);
    expect(config.heartbeatMs).toBe(10_000);
    expect(config.maxProjects).toBe(4);
    expect(() => resolveMultiSandboxConfig({
      ...BASE_ENV,
      PRISM_PROJECT_IDS: A,
      PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
      PRISM_COOKIE: "session=value",
      GP_HEARTBEAT_MS: "10001",
    }, MACHINE)).toThrow("10000");
  });
});
