import { describe, expect, it } from "bun:test";
import { loadGitCleanFilterHook } from "../src/v2/supervisor/git-clean-filter";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import { createCredentialRotationKey, type BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { directSupervisorLaunchFiles } from "../src/v2/supervisor/r10-direct-launcher";
import { supervisorStaticConfig } from "../src/v2/supervisor/r10-launcher";
import type { GuardedYDocMutation } from "../src/sandbox/ydoc";
import { GIT_TEXTCONV_INPUT_SHA256, GitTextconvBootstrap, createGitTextconvGateway, loadGitTextconvInput,
  makeGitTextconvDeployment, translateGitTextconvFile } from "../src/v2/supervisor/git-textconv";

const SESSION = "11111111-1111-4111-8111-111111111111";
const OTHER = "22222222-2222-4222-8222-222222222222";
const [core, relay, assets] = await Promise.all([loadGitCleanFilterHook(), loadGitTextconvInput(), loadV2SidecarAssets({ revision: "r10" })]);
const prepared = makeGitTextconvDeployment(SESSION, core, "#!/bin/sh\nexit 0\n", 1000);
const relative = (file: string) => `write-file?filename=${encodeURIComponent(`..${prepared.base.directory}/${file}`)}`;
const receipt = { version: 1, sessionId: SESSION, outcome: "submitted", configRestored: true, lockReleased: true,
  inputVerified: true, payloadWritten: true, launchSubmitted: true } as const;

function gatewayFixture(options: { relayFailure?: boolean; tokenFailure?: boolean } = {}) {
  const calls: { operation: string; suffix?: string; init?: RequestInit; body?: unknown }[] = [];
  const target = {
    async postJson(operation: string, body: unknown) {
      calls.push({ operation, body });
      if (operation === "token" && options.tokenFailure) throw new Error("Synthetic ambiguous token response");
      return new Response(null, { status: 200 });
    },
    async fetchText(suffix: string, init: RequestInit = {}) {
      calls.push({ operation: "fetchText", suffix, init });
      return { status: options.relayFailure && suffix.includes("textconv-input.py") ? 500 : 200, text: "", headers: new Headers() };
    },
    async syncWorkspace() {}, async triggerRender() {},
  };
  const mapped = createGitTextconvGateway(target, prepared, core, relay);
  const stage = async () => {
    for (const [file, content] of [["hook.py", core], ["plan.json", JSON.stringify(prepared.base.plan)],
      ["attributes", prepared.base.attributes], ["replacement.gitconfig", prepared.base.config]]) {
      await mapped.gateway.fetchText(relative(file!), { method: "POST", body: content! });
    }
  };
  return { calls, mapped, stage };
}

async function r10Fixture(options: { renderFailure?: boolean; badSession?: boolean; relayFailure?: boolean } = {}) {
  const binding: BootstrapBinding = { projectId: SESSION, sandboxSessionId: "offline-sandbox", nonce: "offline-textconv-supervisor",
    leaderGeneration: 1, buildHash: assets.buildHash, schemaHash: assets.schemaHash, protocolRevision: "r2", schemaVersion: 2 };
  const ySweet = parseYSweetEnvelope({ url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/", docId: SESSION,
    token: "synthetic-offline-token", authorization: "full" });
  const launch = directSupervisorLaunchFiles({ entrypointPath: assets.entrypointPath, supervisorNonce: binding.nonce });
  const staged = new Map([
    ["exec", launch.launcher], [".ghostprov-v2/direct-exec/install.sh", launch.installer],
    [".ghostprov-v2/supervisor/static.json", supervisorStaticConfig({ binding, entrypointPath: assets.entrypointPath })],
    [".ghostprov-v2/supervisor/bootstrap.enc.json", JSON.stringify(await sealSupervisorSessionBootstrap({ sessionId: SESSION, binding,
      payload: { instanceId: "offline-textconv-instance", credential: { url: ySweet.url, baseUrl: ySweet.baseUrl, docId: ySweet.docId,
        token: ySweet.token.reveal(), authorization: ySweet.authorization, rotationKey: createCredentialRotationKey() } } }))],
  ]);
  const calls: { operation: string; body?: unknown; suffix?: string }[] = [];
  let removed = false;
  const gateway = {
    async postJson(operation: string, body: unknown) { calls.push({ operation, body }); if (operation === "delete-file") removed = true; return new Response(null, { status: 200 }); },
    async fetchText(suffix: string, init: RequestInit = {}) {
      calls.push({ operation: "fetchText", suffix, body: init.body });
      if (suffix.startsWith("write-file?")) return { status: options.relayFailure && suffix.includes("textconv-input.py") ? 500 : 200, text: "", headers: new Headers() };
      return { status: removed ? 404 : 200, text: removed ? "" : JSON.stringify({ body_b64: Buffer.from(JSON.stringify({
        ...receipt, sessionId: options.badSession ? OTHER : SESSION })).toString("base64") }), headers: new Headers() };
    },
    async syncWorkspace() {},
    async triggerRender() { calls.push({ operation: "render" }); if (options.renderFailure) throw new Error("Synthetic ambiguous Render"); },
  };
  const ydoc = {
    async readPath(file: string) { return staged.get(file) ?? null; },
    async writePath() { throw new Error("Unexpected direct write"); }, async tombstonePath() { throw new Error("Unexpected direct deletion"); },
    async compareAndMutatePaths(input: GuardedYDocMutation) {
      if (input.guards.some(guard => (staged.get(guard.path) ?? null) !== guard.equals)) return false;
      for (const mutation of input.mutations) {
        if (mutation.kind === "write") staged.set(mutation.path, mutation.content);
        else staged.delete(mutation.path);
      }
      return true;
    },
    async renderSyncState() { return { clientStateVector: "", clientDeleteSetUpdate: "" }; },
  };
  return { bootstrap: new GitTextconvBootstrap(), input: { assets, binding, ySweet, ydoc, gateway }, calls, staged };
}

describe("textconv deployment projection", () => {
  it("selects the distinct display callback and pins its input adapter", () => {
    expect(prepared.config).toContain("textconv =");
    expect(prepared.config).toContain(GIT_TEXTCONV_INPUT_SHA256);
    expect(prepared.config).toContain("cachetextconv = false");
    expect(prepared.config).not.toContain("clean =");
    expect(prepared.attributes).toContain("diff=ghostprov_textconv");
  });
  it("changes only configuration identity in the original canonical plan", () => {
    const original = { ...prepared.base.plan, expiresAtUnixMs: 2000 };
    const translated = translateGitTextconvFile(prepared, "plan.json", JSON.stringify(original));
    expect(JSON.parse(translated)).toEqual({ ...original, configSha256: prepared.configSha256 });
    expect(original.configSha256).toBe(prepared.base.plan.configSha256);
    expect(translateGitTextconvFile(prepared, "unrelated.json", "unchanged")).toBe("unchanged");
  });
  it("rejects duplicate, reordered, malformed and foreign plans", () => {
    const original = JSON.stringify(prepared.base.plan);
    const bad = ["null", "[]", '{"version":1,' + original.slice(1), JSON.stringify(prepared.base.plan, null, 2),
      JSON.stringify({ ...prepared.base.plan, sessionId: OTHER }), JSON.stringify({ ...prepared.base.plan, extra: true }),
      JSON.stringify({ ...prepared.base.plan, expiresAtUnixMs: null }), JSON.stringify({ ...prepared.base.plan, expiresAtUnixMs: true }),
      "x".repeat(8193)];
    for (const value of bad) expect(() => translateGitTextconvFile(prepared, "plan.json", value)).toThrow();
    expect(() => translateGitTextconvFile(prepared, "attributes", prepared.base.attributes + "\n")).toThrow();
    expect(() => translateGitTextconvFile(prepared, "replacement.gitconfig", prepared.base.config + "\n")).toThrow();
  });
  it("rejects changed core or relay bytes before creating a deployment", () => {
    const f = gatewayFixture();
    expect(() => createGitTextconvGateway(f.mapped.gateway, prepared, core + "\n", relay)).toThrow();
    expect(() => createGitTextconvGateway(f.mapped.gateway, prepared, core, relay + "\n")).toThrow();
    expect(f.calls).toHaveLength(0);
  });
  it("stages the relay once and preserves unrelated writes, headers and signals", async () => {
    const f = gatewayFixture();
    const init = { method: "POST", body: "unchanged", headers: new Headers({ "x-offline": "1" }), signal: new AbortController().signal };
    await f.mapped.gateway.fetchText("write-file?filename=unrelated", init);
    expect(f.calls[0]!.init).toBe(init);
    await f.stage();
    expect(f.calls.filter(c => c.suffix?.includes("textconv-input.py"))).toHaveLength(1);
    const config = f.calls.find(c => c.suffix?.includes("replacement.gitconfig"));
    expect(config?.init?.body).toBe(prepared.config);
    expect(f.mapped.ready()).toBe(false);
    await f.mapped.gateway.postJson("token", {}, new AbortController().signal);
    expect(f.mapped.ready()).toBe(true);
  });
  it("rejects incomplete, duplicate and late arming", async () => {
    const incomplete = gatewayFixture();
    await expect(incomplete.mapped.gateway.postJson("token", {})).rejects.toThrow();
    expect(incomplete.calls).toHaveLength(0);
    const f = gatewayFixture();
    await f.stage();
    await expect(f.mapped.gateway.fetchText(relative("attributes"), { method: "POST", body: prepared.base.attributes })).rejects.toThrow();
    await f.mapped.gateway.postJson("token", {});
    await expect(f.mapped.gateway.postJson("token", {})).rejects.toThrow();
    await expect(f.mapped.gateway.fetchText(relative("hook.py"), { method: "POST", body: core })).rejects.toThrow();
    expect(f.calls.filter(c => c.operation === "token")).toHaveLength(1);
  });
  it("rejects query/fragment aliases without staging another source", async () => {
    for (const [suffix, body] of [[relative("hook.py") + "&x=1", core], [relative("hook.py") + "#extra", core],
      [relative("attributes") + "&filename=x", prepared.base.attributes]]) {
      const f = gatewayFixture();
      await expect(f.mapped.gateway.fetchText(suffix!, { method: "POST", body: body! })).rejects.toThrow();
      expect(f.calls).toHaveLength(0);
    }
  });
  it("does not retry an ambiguous token or failed relay write", async () => {
    const token = gatewayFixture({ tokenFailure: true });
    await token.stage();
    await expect(token.mapped.gateway.postJson("token", {})).rejects.toThrow();
    await expect(token.mapped.gateway.postJson("token", {})).rejects.toThrow();
    expect(token.calls.filter(c => c.operation === "token")).toHaveLength(1);
    const relayFail = gatewayFixture({ relayFailure: true });
    await expect(relayFail.stage()).rejects.toThrow("input staging rejected");
    await expect(relayFail.stage()).rejects.toThrow("shared executor staging");
    expect(relayFail.calls).toHaveLength(1);
  });
});

describe("textconv over the audited R10 lifecycle", () => {
  it("uses the frozen release and returns a distinct method identity", async () => {
    const f = await r10Fixture();
    await expect(f.bootstrap.start(f.input, SESSION)).resolves.toMatchObject({ sessionId: SESSION, jobId: `git-textconv-${SESSION}` });
    expect(f.bootstrap.result).toEqual(receipt);
    expect(f.calls.filter(c => c.operation === "render")).toHaveLength(1);
    expect(f.calls.filter(c => c.suffix?.includes("textconv-input.py"))).toHaveLength(1);
    const calls = f.calls.length;
    await expect(f.bootstrap.start(f.input, SESSION)).rejects.toThrow("never resubmitted");
    expect(f.calls).toHaveLength(calls);
  });
  it("rejects the wrong sealed session and changed assets before any filesystem call", async () => {
    const f = await r10Fixture();
    await expect(f.bootstrap.start(f.input, OTHER)).rejects.toThrow();
    expect(f.calls).toHaveLength(0);
    const changed = await r10Fixture();
    await expect(changed.bootstrap.start({ ...changed.input, assets: { ...assets,
      files: assets.files.map(file => ({ ...file, content: file.content + "changed" })) } }, SESSION)).rejects.toThrow();
    expect(changed.calls).toHaveLength(0);
  });
  it("keeps ambiguous work fenced and refuses unproved cleanup", async () => {
    const f = await r10Fixture({ renderFailure: true });
    await expect(f.bootstrap.start(f.input, SESSION)).rejects.toThrow();
    await expect(f.bootstrap.cleanupAfterSignedShutdown(true, AbortSignal.timeout(1000))).rejects.toThrow();
    expect(f.staged.has(".ghostprov-v2/clean-bootstrap-owner.json")).toBe(true);
    expect(f.calls.some(c => c.operation === "delete-file")).toBe(false);
  });
  it("retains exact signed cleanup and removes the shared owner and private workspace", async () => {
    const f = await r10Fixture();
    await f.bootstrap.start(f.input, SESSION);
    await expect(f.bootstrap.cleanupAfterSignedShutdown(false, AbortSignal.timeout(1000))).rejects.toThrow();
    await f.bootstrap.cleanupAfterSignedShutdown(true, AbortSignal.timeout(1000));
    await f.bootstrap.cleanupAfterSignedShutdown(true, AbortSignal.timeout(1000));
    expect(f.bootstrap.cleaned).toBe(true);
    expect(f.staged.has(".ghostprov-v2/clean-bootstrap-owner.json")).toBe(false);
    expect(f.calls.filter(c => c.operation === "delete-file").map(c => c.body)).toEqual([{ filename: `../codex_workspace/${SESSION}` }]);
  });
});
