import { describe, expect, it } from "bun:test";
import type { BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import {
  GP_V2_SUPERVISOR_PROTOCOL,
  GP_V2_SUPERVISOR_VERSION,
  parseSupervisorHostMessage,
  parseSupervisorSandboxMessage,
  signSupervisorHostMessage,
  signSupervisorSandboxMessage,
  verifySupervisorHostMessage,
  verifySupervisorSandboxMessage,
} from "../src/v2/supervisor/protocol";

const BINDING: BootstrapBinding = Object.freeze({
  projectId: "11111111-1111-4111-8111-111111111111",
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  nonce: "supervisor-nonce-1",
  protocolRevision: "r2",
  schemaVersion: 2,
});

describe("V2 supervisor protocol", () => {
  it("signs and verifies an exact targeted launch command", async () => {
    const key = createCredentialRotationKey();
    const signed = await signSupervisorHostMessage(key, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL,
      version: GP_V2_SUPERVISOR_VERSION,
      direction: "host",
      binding: BINDING,
      supervisorInstanceId: "supervisor-instance-1",
      sequence: 1,
      message: {
        kind: "launch",
        sidecarBinding: { ...BINDING, nonce: "sidecar-launch-1" },
        entrypointPath: ".ghostprov-v2/revisions/r9/abc/def/sidecar.bundle.js",
        staticConfig: "{}",
      },
    });
    const parsed = parseSupervisorHostMessage(JSON.parse(JSON.stringify(signed)));
    expect(await verifySupervisorHostMessage(key, parsed)).toBe(true);
    expect(await verifySupervisorHostMessage(createCredentialRotationKey(), parsed)).toBe(false);
  });

  it("signs and verifies exact process-backed launch proof", async () => {
    const key = createCredentialRotationKey();
    const signed = await signSupervisorSandboxMessage(key, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL,
      version: GP_V2_SUPERVISOR_VERSION,
      direction: "sandbox",
      binding: BINDING,
      supervisorInstanceId: "supervisor-instance-1",
      sequence: 2,
      message: {
        kind: "launch_hello",
        sidecarBinding: { ...BINDING, nonce: "sidecar-launch-1" },
        encodedHello: "abc_DEF-123",
        process: { pid: 123, startTick: "456", processGroup: 123, session: 123 },
      },
    });
    const parsed = parseSupervisorSandboxMessage(JSON.parse(JSON.stringify(signed)));
    expect(await verifySupervisorSandboxMessage(key, parsed)).toBe(true);
  });

  it("rejects malformed fields, traversal, oversized values, and weak identities", async () => {
    const key = createCredentialRotationKey();
    const base = {
      protocol: GP_V2_SUPERVISOR_PROTOCOL,
      version: GP_V2_SUPERVISOR_VERSION,
      direction: "host" as const,
      binding: BINDING,
      supervisorInstanceId: "supervisor-instance-1",
      sequence: 1,
    };
    await expect(signSupervisorHostMessage(key, {
      ...base,
      message: {
        kind: "launch",
        sidecarBinding: BINDING,
        entrypointPath: "../sidecar.bundle.js",
        staticConfig: "{}",
      },
    })).rejects.toThrow(/entrypoint/i);
    expect(() => parseSupervisorSandboxMessage({
      protocol: GP_V2_SUPERVISOR_PROTOCOL,
      version: GP_V2_SUPERVISOR_VERSION,
      direction: "sandbox",
      binding: BINDING,
      supervisorInstanceId: "supervisor-instance-1",
      sequence: 1,
      message: {
        kind: "launch_hello",
        sidecarBinding: BINDING,
        encodedHello: "abc",
        process: { pid: 3, startTick: "4", processGroup: 3, session: 4 },
      },
      mac: "A".repeat(43),
    })).toThrow(/process identity/i);
  });

  it("rejects unknown, duplicate, and excessive stop nonces", async () => {
    const key = createCredentialRotationKey();
    const base = {
      protocol: GP_V2_SUPERVISOR_PROTOCOL,
      version: GP_V2_SUPERVISOR_VERSION,
      direction: "host" as const,
      binding: BINDING,
      supervisorInstanceId: "supervisor-instance-1",
      sequence: 1,
    };
    await expect(signSupervisorHostMessage(key, {
      ...base,
      message: { kind: "stop", launchNonces: ["same", "same"], reason: "recovery" },
    })).rejects.toThrow(/nonce set/i);
    await expect(signSupervisorHostMessage(key, {
      ...base,
      message: { kind: "stop", launchNonces: Array.from({ length: 65 }, (_, i) => `n${i}`), reason: "recovery" },
    })).rejects.toThrow(/bounds/i);
  });
});
