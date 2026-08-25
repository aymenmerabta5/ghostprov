import { describe, expect, it } from "bun:test";
import {
  parseProvisionedSandbox,
  parseResourceCredential,
  parseYSweetEnvelope,
} from "../src/prism/protocol";
import {
  projectId,
  sandboxId,
  sandboxSessionId,
} from "../src/sandbox/types";

const PROJECT = "e631d12c-d917-4434-b360-41b4715d6679";
const SANDBOX = "750ba53d-dc42-40f0-933b-a64bdf3656ca";

describe("Prism protocol types", () => {
  it("validates opaque identifiers at the boundary", () => {
    expect(projectId(PROJECT)).toBe(projectId(PROJECT));
    expect(sandboxId(SANDBOX)).toBe(sandboxId(SANDBOX));
    expect(() => projectId("not-a-uuid")).toThrow("ProjectId");
    expect(() => sandboxId("")).toThrow("SandboxId");
  });

  it("parses and normalizes a provisioned sandbox without exposing its token", () => {
    const value = parseProvisionedSandbox({
      url: "/s/sandboxes/proxy",
      token: "sandbox-secret-value",
      sandbox_id: SANDBOX,
      sandbox_session_id: "session-123",
      sandbox_session_first_seen_at_ms: 1234,
    });

    expect(value.url).toBe("/s/sandboxes/proxy/");
    expect(value.sandboxId).toBe(sandboxId(SANDBOX));
    expect(value.sessionId).toBe(sandboxSessionId("session-123"));
    expect(value.token.reveal()).toBe("sandbox-secret-value");
    expect(String(value.token)).toBe("[REDACTED]");
    expect(JSON.stringify(value.token)).toBe('"[REDACTED]"');
  });

  it("rejects incomplete provisioning and Y-Sweet envelopes", () => {
    expect(() => parseProvisionedSandbox({ url: "/s/sandboxes/proxy" })).toThrow("token");
    expect(() => parseProvisionedSandbox({ token: "secret" })).toThrow("url");
    expect(() => parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      authorization: "full",
    })).toThrow("token");
  });

  it("parses Y-Sweet and resource credentials with typed expiries", () => {
    const y = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "ysweet-secret",
      authorization: "full",
    });
    const resource = parseResourceCredential({
      access_token: "resource-secret",
      expires_at: 2_000_000_000,
      max_age_seconds: 3600,
      scopes: ["project_files:read", "render_results:write"],
      resources_base_url: "https://prism.test/s/sandbox-resources",
    }, 1_000);

    expect(y.docId).toBe(projectId(PROJECT));
    expect(y.token.reveal()).toBe("ysweet-secret");
    expect(resource.expiresAtMs).toBe(2_000_000_000_000);
    expect(resource.baseUrl).toBe("https://prism.test/s/sandbox-resources/");
    expect(resource.scopes).toEqual(["project_files:read", "render_results:write"]);
  });
});
