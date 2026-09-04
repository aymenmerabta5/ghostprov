import { describe, expect, it } from "bun:test";
import {
  supervisorInstallCommand,
  supervisorInstallScript,
  supervisorStaticConfig,
} from "../src/v2/supervisor/launcher";

const binding = Object.freeze({
  projectId: "11111111-1111-4111-8111-111111111111",
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 3,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  nonce: "supervisor-nonce-1",
  protocolRevision: "r2" as const,
  schemaVersion: 2 as const,
});

const entrypoint = `.ghostprov-v2/revisions/r9/${"b".repeat(64)}/${"a".repeat(64)}/sidecar.bundle.js`;

describe("V2 supervisor launcher", () => {
  it("builds exact static authority and a deterministic bounded installer", () => {
    expect(JSON.parse(supervisorStaticConfig({ binding, entrypointPath: entrypoint }))).toEqual({
      version: 1,
      binding,
      entrypointPath: entrypoint,
      connectTimeoutMs: 20_000,
    });
    const script = supervisorInstallScript({
      entrypointPath: entrypoint,
      supervisorNonce: binding.nonce,
    });
    expect(script).toContain(`ENTRYPOINT_RELATIVE='${entrypoint}'`);
    expect(script).toContain('ENTRYPOINT="$WORKSPACE/$ENTRYPOINT_RELATIVE"');
    expect(script).toContain('"--allow-read=$WORKSPACE/.ghostprov-v2,');
    expect(script).not.toContain(`/workspace/${entrypoint}`);
    expect(script).toContain("--ghostprov-install-supervisor=$NONCE");
    expect(script).toContain('if [ -n "$DENO" ] && [ ! -x "$DENO" ]; then DENO=; fi');
    expect(script).toContain("--allow-run --allow-sys");
    expect(script).not.toContain("pgrep");
    expect(script).not.toContain("date -u");
    expect(supervisorInstallScript({ entrypointPath: entrypoint, supervisorNonce: binding.nonce }))
      .toBe(script);
    const command = supervisorInstallCommand({ entrypointPath: entrypoint, supervisorNonce: binding.nonce });
    expect(command).toContain('ENTRYPOINT="$WORKSPACE/$ENTRYPOINT_RELATIVE"');
    expect(command).toContain('"--allow-read=$WORKSPACE/.ghostprov-v2,');
    expect(command).not.toContain(" --allow-read;");
  });

  it("rejects traversal, unregistered paths, and unsafe nonces", () => {
    expect(() => supervisorInstallScript({ entrypointPath: "../sidecar.bundle.js", supervisorNonce: binding.nonce }))
      .toThrow(/entrypoint/i);
    expect(() => supervisorInstallScript({ entrypointPath: entrypoint, supervisorNonce: "bad nonce" }))
      .toThrow(/nonce/i);
  });
});
