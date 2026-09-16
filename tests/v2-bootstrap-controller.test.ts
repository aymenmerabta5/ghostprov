import { describe, expect, it } from "bun:test";
import type { Clock, RandomSource } from "../src/core/ports";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";
import type { AwarenessPeerUpdate } from "../src/v2/awareness/channel";
import {
  V2BootstrapController,
  V2BootstrapError,
  type V2BootstrapAwareness,
  type V2BootstrapGateway,
  type V2BootstrapYDoc,
} from "../src/v2/bootstrap/controller";
import {
  createBootstrapKeyPair,
  openCredentialRotation,
  openBootstrapCredential,
  type BootstrapCredentialPayload,
  type BootstrapBinding,
  type SealedBootstrapCredential,
} from "../src/v2/bootstrap/crypto";
import {
  base64UrlEncode,
  canonicalJson,
  type AwarenessEnvelope,
  type HostFrame,
  type SidecarFrame,
  utf8Bytes,
} from "../src/v2/protocol/shared";
import {
  loadV2SidecarAssets,
  V2_R10_RELEASE,
  V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY,
  V2_R10_C1091742_RETIREMENT_IDENTITY,
  type V2SidecarRevision,
} from "../src/v2/sidecar/assets";
import type { V2BootstrapSupervisor } from "../src/v2/supervisor/host";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const ASSETS = await loadV2SidecarAssets({ revision: "r1" });
const R2_ASSETS = await loadV2SidecarAssets({ revision: "r2" });
const R3_ASSETS = await loadV2SidecarAssets({ revision: "r3" });
const R4_ASSETS = await loadV2SidecarAssets({ revision: "r4" });
const R5_ASSETS = await loadV2SidecarAssets({ revision: "r5" });
const R6_ASSETS = await loadV2SidecarAssets({ revision: "r6" });
const R7_ASSETS = await loadV2SidecarAssets({ revision: "r7" });
const R8_ASSETS = await loadV2SidecarAssets({ revision: "r8" });
const R9_ASSETS = await loadV2SidecarAssets({ revision: "r9" });
const RELEASE_ASSETS = Object.freeze([
  ASSETS,
  R2_ASSETS,
  R3_ASSETS,
  R4_ASSETS,
  R5_ASSETS,
  R6_ASSETS,
  R7_ASSETS,
  R8_ASSETS,
  R9_ASSETS,
]);
const BUILD_HASH = ASSETS.buildHash;
const SCHEMA_HASH = ASSETS.schemaHash;
const INITIAL_R2_RETIREMENT_IDENTITY = Object.freeze({
  buildHash: "21457e48716e99e82d48521ca43c60335ffd9933f4e1e258e252709c9525da6a",
  schemaHash: "6c5d2c5cf60e0338d9d907f20636a0b3a6c40abca3bbc806fb59d0518a0af5bd",
});
const TOKEN = `ysweet-plaintext-canary-${"x".repeat(48)}`;

type Fault =
  | "none"
  | "wrong-nonce"
  | "wrong-build"
  | "wrong-generation"
  | "stale-instance"
  | "sidecar-log"
  | "sidecar-fatal"
  | "post-hello-fatal"
  | "same-owner-retry"
  | "same-owner-no-cache"
  | "replaced-binding"
  | "sidecar-log-secret"
  | "timeout"
  | "first-wrong-nonce";

class FakeClock implements Clock {
  value = 1_787_400_000_000;
  now(): number { return this.value; }
  async sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new DOMException("aborted", "AbortError");
    this.value += ms;
    await Promise.resolve();
  }
  async waitUntil(deadlineMs: number, signal?: AbortSignal): Promise<void> {
    await this.sleep(Math.max(0, deadlineMs - this.value), signal);
  }
}

class FakeRandom implements RandomSource {
  sequence = 0;
  next(): number { return 0.5; }
  uuid(): string {
    this.sequence++;
    return `00000000-0000-4000-8000-${String(this.sequence).padStart(12, "0")}`;
  }
}

class FakeYDoc implements V2BootstrapYDoc {
  readonly files = new Map<string, string>();
  readonly writes: Array<{ path: string; content: string }> = [];
  readonly deleted: string[] = [];
  onWrite?: (path: string, content: string) => void | Promise<void>;

  async readPath(path: string): Promise<string | null> {
    return this.files.get(path) ?? null;
  }
  async writePath(path: string, content: string): Promise<void> {
    this.files.set(path, content);
    this.writes.push({ path, content });
    await this.onWrite?.(path, content);
  }
  async tombstonePath(path: string): Promise<boolean> {
    this.deleted.push(path);
    const existed = this.files.has(path);
    this.files.delete(path);
    return existed;
  }
  async compareAndMutatePaths(input: Parameters<V2BootstrapYDoc["compareAndMutatePaths"]>[0]) {
    if (input.guards.some(guard => (this.files.get(guard.path) ?? null) !== guard.equals)) {
      return false;
    }
    const callbacks: Array<{ path: string; content: string }> = [];
    for (const mutation of input.mutations) {
      if (mutation.kind === "write") {
        this.files.set(mutation.path, mutation.content);
        this.writes.push({ path: mutation.path, content: mutation.content });
        callbacks.push({ path: mutation.path, content: mutation.content });
      } else {
        this.deleted.push(mutation.path);
        this.files.delete(mutation.path);
      }
    }
    for (const mutation of callbacks) await this.onWrite?.(mutation.path, mutation.content);
    return true;
  }
  async renderSyncState() {
    return {
      clientStateVector: "state-vector",
      clientDeleteSetUpdate: "delete-set",
    };
  }
}

class FakeAwareness implements V2BootstrapAwareness {
  readonly projectId = PROJECT;
  readonly localPeerId = "host-bootstrap";
  readonly listeners = new Set<(update: AwarenessPeerUpdate<SidecarFrame>) => void>();
  readonly published: AwarenessEnvelope<HostFrame>[] = [];
  connects = 0;
  clears = 0;
  onPublish?: (envelope: AwarenessEnvelope<HostFrame>) => void;

  async connect(): Promise<void> { this.connects++; }
  publish(envelope: AwarenessEnvelope<HostFrame>): void {
    this.published.push(envelope);
    this.onPublish?.(envelope);
  }
  clear(): void { this.clears++; }
  subscribe(listener: (update: AwarenessPeerUpdate<SidecarFrame>) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  peerIds(): readonly string[] { return []; }
  async close(): Promise<void> {}
  sidecar(envelope: AwarenessEnvelope<SidecarFrame>): void {
    for (const listener of this.listeners) {
      listener({ peerClientId: "sidecar", envelope });
    }
  }
}

class FakeGateway implements V2BootstrapGateway {
  readonly calls: string[] = [];
  readonly nonces: string[] = [];
  readonly launchers: string[] = [];
  readonly mainDocuments: string[] = [];
  logs = "";
  renders = 0;
  bootstrapRenders = 0;
  awaitingReadiness = false;
  current: {
    binding: BootstrapBinding;
    instanceId: string;
    keys: Awaited<ReturnType<typeof createBootstrapKeyPair>>;
  } | null = null;

  constructor(
    readonly ydoc: FakeYDoc,
    readonly fault: Fault,
  ) {}

  async syncWorkspace(): Promise<void> { this.calls.push("wait-for-sync"); }
  async triggerRender(request?: { mainDocument: string }): Promise<void> {
    this.calls.push("render");
    this.renders++;
    this.mainDocuments.push(request?.mainDocument ?? "");
    this.launchers.push(this.ydoc.files.get(".latexmkrc") ?? "");
    const retirementLauncher = this.launchers.at(-1)!;
    if (retirementLauncher.includes("GhostProv V2 sidecar termination launcher")) {
      const retirementNonce = /my \$gp_retirement_nonce = '([^']+)'/.exec(
        retirementLauncher,
      )?.[1];
      if (!retirementNonce) throw new Error("termination launcher omitted its proof nonce");
      this.logs = `render starting\nGPV2_RETIRED ${retirementNonce}\n) render continues\n`;
      return;
    }
    if (this.fault === "replaced-binding") {
      this.ydoc.files.set(".ghostprov-v2/static.json", "foreign-generation");
      this.logs = "render accepted without a bootstrap marker";
      return;
    }
    if (this.awaitingReadiness) {
      const message = this.fault === "post-hello-fatal"
        ? JSON.stringify({
            event: "ghostprov_v2_sidecar_fatal",
            error_name: "SidecarOwnershipError",
            error_code: "lost",
            cause_name: "Error",
            phase: "engine_start",
            permission: "write",
          })
        : "sidecar log is missing";
      const wrapped = Buffer.from(message).toString("hex").match(/.{1,48}/g)!.join("\n");
      this.logs = `render starting\nGPV2_SIDECAR_HEX ${wrapped}\n) render continues\n`;
      return;
    }
    const stagedLauncher = this.ydoc.files.get(".latexmkrc") ?? "";
    const relayNonce = /\.ghostprov-v2\/relay\/hello\.([^'\\]+)\.txt/.exec(
      stagedLauncher.includes("R9 relay hello printer") ? stagedLauncher : "",
    )?.[1];
    if (relayNonce) {
      const helloFile = this.ydoc.files.get(`.ghostprov-v2/relay/hello.${relayNonce}.txt`);
      this.logs = helloFile === undefined
        ? "render starting\n(relay hello pending)\n) render continues\n"
        : `render starting\n${helloFile}\n) render continues\n`;
      return;
    }
    const config = JSON.parse(this.ydoc.files.get(".ghostprov-v2/static.json")!) as {
      binding: BootstrapBinding;
    };
    this.nonces.push(config.binding.nonce);
    this.bootstrapRenders++;
    if (
      this.fault === "same-owner-no-cache" ||
      (this.fault === "same-owner-retry" && this.bootstrapRenders > 1)
    ) {
      this.logs = `render starting\nGPV2_OWNER_REUSED ${config.binding.nonce}\n) render continues\n`;
      return;
    }
    const helloBinding = (
      this.fault === "wrong-nonce" ||
      (this.fault === "first-wrong-nonce" && this.renders === 1)
    )
      ? { ...config.binding, nonce: `stale-${config.binding.nonce}` }
      : config.binding;
    const keys = await createBootstrapKeyPair(helloBinding);
    const instanceId = `sidecar-instance-${this.renders}`;
    this.current = { binding: helloBinding, instanceId, keys };
    if (
      this.fault === "sidecar-log" ||
      this.fault === "sidecar-fatal" ||
      this.fault === "sidecar-log-secret"
    ) {
      const message = this.fault === "sidecar-fatal"
        ? JSON.stringify({
            event: "ghostprov_v2_sidecar_fatal",
            error_name: "SidecarOwnershipError",
            error_code: "contended",
            cause_name: null,
          })
        : this.fault === "sidecar-log-secret"
        ? "Authorization: Bearer sk-bootstrap-secret-123456789012345678901234"
        : "Can't exec /code/crixet/deno: No such file or directory";
      const wrapped = Buffer.from(message).toString("hex").match(/.{1,48}/g)!.join("\n");
      this.logs = `render starting\nGPV2_SIDECAR_HEX ${wrapped}\n) render continues\n`;
      return;
    }
    const encoded = base64UrlEncode(utf8Bytes(canonicalJson({
      version: 1,
      binding: helloBinding,
      instanceId,
      publicJwk: keys.publicJwk,
    })));
    this.logs = `render starting\nGPV2_BOOTSTRAP ${encoded.match(/.{1,60}/g)!.join("\n")}\n) render continues\n`;
  }
  async fetchText(suffix: string): Promise<{ status: number; text: string; headers: Headers }> {
    this.calls.push(suffix);
    return { status: 200, text: this.logs, headers: new Headers() };
  }
}

function readyEnvelope(
  binding: BootstrapBinding,
  instanceId: string,
  fault: Fault,
): AwarenessEnvelope<SidecarFrame> {
  const common = {
    protocol: "gp-awareness-v2" as const,
    schemaHash: binding.schemaHash,
    buildHash: fault === "wrong-build" ? "f".repeat(64) : binding.buildHash,
    projectId: PROJECT,
    sessionId: binding.sandboxSessionId,
    peerId: "sidecar",
    instanceId: fault === "stale-instance" ? "stale-sidecar" : instanceId,
    leaderGeneration: fault === "wrong-generation"
      ? binding.leaderGeneration + 1
      : binding.leaderGeneration,
    epoch: 1,
    revision: 1,
    ack: { epoch: 1, sequence: -1 },
    credits: 8,
    active: 0,
    queued: 0,
    draining: false,
    frames: [],
  };
  return binding.protocolRevision === "r2"
    ? {
        ...common,
        protocolRevision: "r2",
        schemaVersion: 2,
        launchNonce: binding.nonce,
        targetInstanceId: binding.nonce,
      }
    : { ...common, schemaVersion: 1 };
}

function bootstrapHarness(
  fault: Fault = "none",
  revision?: V2SidecarRevision | string,
  maxActivePerSandbox = 8,
  assets = ASSETS,
  controllerTiming: { attempts?: number; timeoutMs?: number; pollMs?: number; bootstrapTimeoutMs?: number; launchMode?: "render" | "relay" } = {},
) {
  const clock = new FakeClock();
  const random = new FakeRandom();
  const ydoc = new FakeYDoc();
  const awareness = new FakeAwareness();
  const gateway = new FakeGateway(ydoc, fault);
  const decrypted: string[] = [];
  const decryptedCredentials: BootstrapCredentialPayload[] = [];
  let ciphertextWrites = 0;
  const ySweet = parseYSweetEnvelope({
    url: `wss://prism.test/y/d/${PROJECT}/ws`,
    baseUrl: `https://prism.test/y/d/${PROJECT}`,
    docId: PROJECT,
    token: TOKEN,
    authorization: "full",
  });
  let supervisorLaunches = 0;
  let supervisorCredentials = 0;
  let supervisorStops = 0;
  const supervisor: V2BootstrapSupervisor = {
    binding: {
      projectId: PROJECT,
      sandboxSessionId: "sandbox-session-1",
      leaderGeneration: 4,
      buildHash: assets.buildHash,
      schemaHash: assets.schemaHash,
      nonce: `supervisor-${assets.revision}`,
      protocolRevision: "r2",
      schemaVersion: 2,
    },
    instanceId: "supervisor-instance-1",
    async ensureReady() {},
    async connect() {},
    async launch(input) {
      supervisorLaunches++;
      const keys = await createBootstrapKeyPair(input.sidecarBinding);
      const instanceId = "sidecar-instance-supervised";
      gateway.current = { binding: input.sidecarBinding, instanceId, keys };
      return {
        encodedHello: base64UrlEncode(utf8Bytes(canonicalJson({
          version: 1,
          binding: input.sidecarBinding,
          instanceId,
          publicJwk: keys.publicJwk,
        }))),
        process: { pid: 123, startTick: "456", processGroup: 123, session: 123 },
      };
    },
    async deliverCredential(input) {
      supervisorCredentials++;
      const current = gateway.current!;
      const credential = await openBootstrapCredential({
        sealed: input.sealed,
        sidecarPrivateKey: current.keys.privateKey,
        expectedBinding: current.binding,
      });
      decrypted.push(credential.token);
      decryptedCredentials.push(credential);
      if (fault !== "timeout" && fault !== "post-hello-fatal") {
        awareness.sidecar(readyEnvelope(current.binding, current.instanceId, fault));
      }
    },
    async stop(input) {
      supervisorStops++;
      return input.launchNonces[0] ?? null;
    },
    async rotateCredential() {},
    async shutdown() {},
    async close() {},
  };
  ydoc.onWrite = async (path, content) => {
    if (path === ".latexmkrc" && content.includes("temporary bootstrap launcher")) {
      gateway.awaitingReadiness = false;
      return;
    }
    if (path !== ".ghostprov-v2/bootstrap.enc.json") return;
    const current = gateway.current!;
    const credential = await openBootstrapCredential({
      sealed: JSON.parse(content) as SealedBootstrapCredential,
      sidecarPrivateKey: current.keys.privateKey,
      expectedBinding: current.binding,
    });
    decrypted.push(credential.token);
    decryptedCredentials.push(credential);
    ciphertextWrites++;
    gateway.awaitingReadiness = true;
    if (fault === "same-owner-retry" && ciphertextWrites === 1) return;
    if (fault !== "timeout" && fault !== "post-hello-fatal") {
      awareness.sidecar(readyEnvelope(current.binding, current.instanceId, fault));
      gateway.awaitingReadiness = false;
    }
  };
  const controller = new V2BootstrapController({
    projectId: PROJECT,
    workerIndex: 7,
    sandboxSessionId: "sandbox-session-1",
    leaderGeneration: 4,
    ySweet,
    ydoc,
    gateway,
    awareness,
    assets,
    ...(revision === undefined ? {} : { revision }),
    ...(assets.protocolRevision === "r2"
      ? { launchNonce: `${assets.revision}-launch-nonce` }
      : {}),
    attempts: controllerTiming.attempts ?? 3,
    timeoutMs: controllerTiming.timeoutMs ?? 20,
    pollMs: controllerTiming.pollMs ?? 5,
    ...(controllerTiming.bootstrapTimeoutMs === undefined
      ? {}
      : { bootstrapTimeoutMs: controllerTiming.bootstrapTimeoutMs }),
    ...(controllerTiming.launchMode === undefined ? {} : { launchMode: controllerTiming.launchMode }),
    ...(controllerTiming.launchMode === "relay" ? { supervisor } : {}),
    clock,
    random,
    diagnostics: { enabled: true, intervalMs: 1_000 },
    initialActivePerSandbox: 4,
    maxActivePerSandbox,
  });
  return {
    controller,
    clock,
    random,
    ydoc,
    awareness,
    gateway,
    decrypted,
    decryptedCredentials,
    supervisor,
    get supervisorLaunches() { return supervisorLaunches; },
    get supervisorCredentials() { return supervisorCredentials; },
    get supervisorStops() { return supervisorStops; },
    ySweet,
  };
}

describe("V2BootstrapController", () => {
  it("derives an omitted revision from exact assets and rejects a mismatched selection", () => {
    expect(() => bootstrapHarness("none")).not.toThrow();
    expect(() => bootstrapHarness("none", undefined, 9)).toThrow("terminal-spool capacity");
    expect(() => bootstrapHarness("none", "r2"))
      .toThrow(/selected V2 revision r2 does not match supplied assets r1/i);
  });

  it("launches immutable R4 over the R2 wire protocol", async () => {
    const harness = bootstrapHarness("none", "r4", 8, R4_ASSETS);
    await expect(harness.controller.ensureReady()).resolves.toMatchObject({
      protocolRevision: "r2",
      buildHash: R4_ASSETS.buildHash,
      schemaHash: R4_ASSETS.schemaHash,
    });
    expect(harness.gateway.launchers[0]).toContain(
      `/workspace/${R4_ASSETS.entrypointPath}`,
    );
    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/revision.json")!))
      .toMatchObject({ revision: "r4", protocolRevision: "r2" });
  });

  it("binds immutable R5 and bulk-v1 into the exact sidecar static config", async () => {
    const harness = bootstrapHarness("none", "r5", 8, R5_ASSETS);
    await expect(harness.controller.ensureReady()).resolves.toMatchObject({
      protocolRevision: "r2",
      buildHash: R5_ASSETS.buildHash,
      schemaHash: R5_ASSETS.schemaHash,
    });
    expect(harness.gateway.launchers[0]).toContain(
      `/workspace/${R5_ASSETS.entrypointPath}`,
    );
    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/static.json")!))
      .toMatchObject({ transportProfile: "bulk-v1" });
    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/revision.json")!))
      .toMatchObject({ revision: "r5", protocolRevision: "r2" });
  });

  it("publishes the R9 sandbox-relay credential window into the static config", async () => {
    const harness = bootstrapHarness("none", "r9", 8, R9_ASSETS);
    await harness.controller.ensureReady();
    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/static.json")!))
      .toMatchObject({ bootstrapTimeoutMs: 300_000 });

    const custom = bootstrapHarness("none", "r9", 8, R9_ASSETS, { bootstrapTimeoutMs: 60_000 });
    await custom.controller.ensureReady();
    expect(JSON.parse(custom.ydoc.files.get(".ghostprov-v2/static.json")!))
      .toMatchObject({ bootstrapTimeoutMs: 60_000 });

    expect(() => bootstrapHarness("none", "r9", 8, R9_ASSETS, { bootstrapTimeoutMs: 29_999 }))
      .toThrow(/bootstrapTimeoutMs/);
    expect(() => bootstrapHarness("none", "r9", 8, R9_ASSETS, { bootstrapTimeoutMs: 600_001 }))
      .toThrow(/bootstrapTimeoutMs/);
  });

  it("bootstraps through the persistent sandbox supervisor without Render", async () => {
    const harness = bootstrapHarness("none", "r9", 8, R9_ASSETS, { launchMode: "relay" });
    const descriptor = await harness.controller.ensureReady();
    expect(descriptor).toMatchObject({
      instanceId: "sidecar-instance-supervised",
      releaseRevision: "r9",
      protocolRevision: "r2",
      launchNonce: "r9-launch-nonce",
    });
    expect(harness.supervisorLaunches).toBe(1);
    expect(harness.supervisorCredentials).toBe(1);
    expect(harness.gateway.renders).toBe(0);
    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/static.json")!))
      .toMatchObject({ bootstrapTimeoutMs: 300_000 });
    expect(harness.ydoc.files.get(".ghostprov-v2/bootstrap.enc.json")).toBeUndefined();
    expect(harness.ydoc.files.get(".latexmkrc")).toContain("launcher inactive");
  });

  it("coalesces concurrent supervisor-backed readiness without duplicate launch", async () => {
    const harness = bootstrapHarness("none", "r9", 8, R9_ASSETS, { launchMode: "relay" });
    const [first, second] = await Promise.all([
      harness.controller.ensureReady(),
      harness.controller.ensureReady(),
    ]);
    expect(first).toEqual(second);
    expect(first.instanceId).toBe("sidecar-instance-supervised");
    expect(harness.supervisorLaunches).toBe(1);
    expect(harness.gateway.renders).toBe(0);
  });

  it("uses supervisor-owned exact stop proof for relay retirement", async () => {
    const harness = bootstrapHarness("none", "r9", 8, R9_ASSETS, { launchMode: "relay" });
    await harness.controller.ensureReady();
    await harness.controller.forceStop("supervisor recovery");
    expect(harness.supervisorStops).toBe(1);
    expect(harness.gateway.renders).toBe(0);
  });

  it("rejects relay launch mode outside the R2 wire protocol", () => {
    expect(() => bootstrapHarness("none", "r1", 8, ASSETS, { launchMode: "relay" }))
      .toThrow(/relay.*R2|R2.*relay/i);
  });

  it("matches only exact registered sidecar entrypoints including archived R10", async () => {
    const harness = bootstrapHarness("none", "r8", 8, R8_ASSETS);
    await harness.controller.ensureReady();
    const launcher = harness.gateway.launchers[0]!;

    const patternFor = (helper: "gp_exact_r2_entrypoint" | "gp_exact_sidecar_entrypoint") => {
      const helperStart = launcher.indexOf(`sub ${helper}`);
      const patternStart = launcher.indexOf("m{", helperStart) + 2;
      const patternEnd = launcher.indexOf("} ? $gp_args[8] : undef;", patternStart);
      expect(helperStart).toBeGreaterThan(-1);
      expect(patternStart).toBeGreaterThan(1);
      expect(patternEnd).toBeGreaterThan(patternStart);
      const source = launcher.slice(patternStart, patternEnd);
      expect(source.endsWith("\\z")).toBe(true);
      return {
        source,
        regex: new RegExp(`${source.slice(0, -2)}(?![\\s\\S])`),
      };
    };

    const allReleases = patternFor("gp_exact_sidecar_entrypoint");
    const r2Releases = patternFor("gp_exact_r2_entrypoint");
    expect(allReleases.source).not.toContain("[a-f0-9]{64}");
    expect(r2Releases.source).not.toContain("[a-f0-9]{64}");
    for (const exactArgvProof of [
      "return () unless @gp_args == 11;",
      "return () unless $gp_args[1] eq 'run';",
      "return () unless $gp_args[2] eq '--cached-only';",
      "return () unless $gp_args[7] eq '--allow-write=/tmp/ghostprov-v2';",
      "return 0 unless $gp_args[8] eq $gp_expected_entrypoint;",
      'return 0 unless $gp_args[9] eq "--ghostprov-launch-nonce=$gp_nonce";',
      'return 0 unless $gp_args[10] eq "--ghostprov-process-start=$gp_start";',
    ]) expect(launcher).toContain(exactArgvProof);

    for (const assets of RELEASE_ASSETS) {
      const entrypoint = `/workspace/${assets.entrypointPath}`;
      expect(allReleases.regex.test(entrypoint)).toBe(true);
      expect(r2Releases.regex.test(entrypoint)).toBe(assets.protocolRevision === "r2");

      const foreignBuild = RELEASE_ASSETS.find(candidate => (
        candidate.buildHash !== assets.buildHash
      ))!;
      const foreignSchema = RELEASE_ASSETS.find(candidate => (
        candidate.schemaHash !== assets.schemaHash
      ))!;
      const root = `/workspace/.ghostprov-v2/revisions/${assets.revision}`;
      const invalidEntrypoints = [
        `${root}/${"0".repeat(64)}/${assets.buildHash}/sidecar.bundle.js`,
        `${root}/${assets.schemaHash}/${"f".repeat(64)}/sidecar.bundle.js`,
        `${root}/${assets.schemaHash}/${foreignBuild.buildHash}/sidecar.bundle.js`,
        `${root}/${foreignSchema.schemaHash}/${assets.buildHash}/sidecar.bundle.js`,
        `prefix${entrypoint}`,
        `${entrypoint}.old`,
        `${entrypoint}/suffix`,
        `${entrypoint}\n`,
        `${entrypoint}\r\n`,
      ];
      for (const invalid of invalidEntrypoints) {
        expect(allReleases.regex.test(invalid)).toBe(false);
        expect(r2Releases.regex.test(invalid)).toBe(false);
      }
    }

    const mismatchedR9Entrypoint = `/workspace/.ghostprov-v2/revisions/r9/${R8_ASSETS.schemaHash}/${R8_ASSETS.buildHash}/sidecar.bundle.js`;
    expect(allReleases.regex.test(mismatchedR9Entrypoint)).toBe(false);
    expect(r2Releases.regex.test(mismatchedR9Entrypoint)).toBe(false);
    const r10Entrypoint = `/workspace/.ghostprov-v2/revisions/r10/${R9_ASSETS.schemaHash}/${R9_ASSETS.buildHash}/sidecar.bundle.js`;
    expect(allReleases.regex.test(r10Entrypoint)).toBe(false);
    expect(r2Releases.regex.test(r10Entrypoint)).toBe(false);

    const initialR2Entrypoint = `/workspace/.ghostprov-v2/revisions/r2/${INITIAL_R2_RETIREMENT_IDENTITY.schemaHash}/${INITIAL_R2_RETIREMENT_IDENTITY.buildHash}/sidecar.bundle.js`;
    expect(allReleases.regex.test(initialR2Entrypoint)).toBe(true);
    expect(r2Releases.regex.test(initialR2Entrypoint)).toBe(true);
    for (const identity of [V2_R10_RELEASE, V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY, V2_R10_C1091742_RETIREMENT_IDENTITY]) {
      const exact = `/workspace/.ghostprov-v2/revisions/r10/${identity.schemaHash}/${identity.buildHash}/sidecar.bundle.js`;
      expect(allReleases.regex.test(exact)).toBe(true);
      expect(r2Releases.regex.test(exact)).toBe(true);
      for (const invalid of [exact + "\n", exact + ".old", "prefix" + exact,
        exact.replace(identity.buildHash, identity.schemaHash), exact.replace(identity.schemaHash, R2_ASSETS.schemaHash)]) {
        expect(allReleases.regex.test(invalid)).toBe(false);
        expect(r2Releases.regex.test(invalid)).toBe(false);
      }
    }
  });

  it("rejects an unregistered sidecar revision before generating a launcher", () => {
    expect(() => bootstrapHarness("none", "r11", 8, R8_ASSETS))
      .toThrow("Unsupported V2 sidecar revision: r11");
    expect(() => bootstrapHarness("none", "r10", 8, R8_ASSETS))
      .toThrow("Selected V2 revision r10 does not match supplied assets r8");
  });

  it("deploys non-secret assets, seals credentials, and accepts only bound readiness", async () => {
    const harness = bootstrapHarness();

    await expect(harness.controller.ensureReady()).resolves.toEqual({
      instanceId: "sidecar-instance-1",
      releaseRevision: "r1",
      protocolRevision: "r1",
      launchNonce: "00000000-0000-4000-8000-000000000001",
      buildHash: BUILD_HASH,
      schemaHash: SCHEMA_HASH,
      leaderGeneration: 4,
      capacity: 8,
    });
    expect(harness.decrypted).toEqual([TOKEN]);
    expect(harness.gateway.calls.some(call => call.includes("zip"))).toBe(false);
    expect(harness.ydoc.writes.every(write => !write.content.includes(TOKEN))).toBe(true);
    expect(harness.ydoc.deleted).toContain(".ghostprov-v2/bootstrap.enc.json");
    expect(harness.ydoc.deleted).toEqual(expect.arrayContaining([
      ".ghostprov-v2/deno.json",
      ".ghostprov-v2/sandbox",
      ".ghostprov-v2/src",
    ]));
    expect(harness.ydoc.files.get(".latexmkrc")).toContain("GhostProv V2 launcher inactive");
    expect(harness.gateway.launchers[0]).toContain("/tmp/ghostprov-v2/sidecar.pid");
    expect(harness.awareness.connects).toBe(1);
    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/static.json")!)).toMatchObject({
      diagnostics: { enabled: true, intervalMs: 1_000 },
      initialActivePerSandbox: 4,
      maxActivePerSandbox: 8,
    });
    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/revision.json")!)).toEqual({
      protocol: "gp-awareness-v2",
      version: 1,
      revision: "r1",
      protocolRevision: "r1",
      buildHash: BUILD_HASH,
      schemaHash: SCHEMA_HASH,
      manifestPath: ASSETS.manifest.path,
      entrypointPath: ASSETS.entrypointPath,
    });
    expect(harness.ydoc.files.get(ASSETS.manifest.path)).toBe(ASSETS.manifest.content);
  });

  it("retries with a fresh nonce and succeeds after a stale hello", async () => {
    const harness = bootstrapHarness("first-wrong-nonce");

    await expect(harness.controller.ensureReady()).resolves.toMatchObject({
      instanceId: "sidecar-instance-2",
    });
    expect(harness.gateway.nonces).toHaveLength(2);
    expect(new Set(harness.gateway.nonces).size).toBe(2);
    expect(harness.ydoc.deleted.filter(path => path.endsWith("bootstrap.enc.json"))).toHaveLength(2);
  });

  it("adopts a proven same-nonce owner with its cached one-shot hello", async () => {
    const harness = bootstrapHarness("same-owner-retry", "r2", 8, R2_ASSETS);

    await expect(harness.controller.ensureReady()).resolves.toMatchObject({
      instanceId: "sidecar-instance-1",
      launchNonce: "r2-launch-nonce",
    });

    expect(harness.gateway.nonces).toEqual(["r2-launch-nonce", "r2-launch-nonce"]);
    expect(harness.gateway.bootstrapRenders).toBe(2);
    expect(harness.gateway.launchers[2]).toContain("temporary bootstrap launcher");
    expect(harness.decrypted).toEqual([TOKEN, TOKEN]);
    expect(harness.decryptedCredentials[1]!.rotationKey)
      .toBe(harness.decryptedCredentials[0]!.rotationKey);
  });

  it("fails a retained same-nonce owner in bounded attempts without a cached hello", async () => {
    const harness = bootstrapHarness("same-owner-no-cache", "r2", 8, R2_ASSETS);

    await expect(harness.controller.ensureReady()).rejects.toMatchObject({
      name: "V2BootstrapError",
      kind: "hello",
      message: expect.stringContaining("without its cached bootstrap hello"),
    });

    expect(harness.gateway.renders).toBe(3);
    expect(harness.gateway.bootstrapRenders).toBe(3);
    expect(harness.decrypted).toEqual([]);
  });

  it("runs exact prior-owner retirement again before a retry fork", async () => {
    const harness = bootstrapHarness("first-wrong-nonce", "r2", 8, R2_ASSETS);

    await expect(harness.controller.ensureReady()).resolves.toMatchObject({
      instanceId: "sidecar-instance-2",
    });

    expect(harness.gateway.launchers).toHaveLength(2);
    for (const launcher of harness.gateway.launchers) {
      const takeover = launcher.indexOf("my $gp_prior_owner = $gp_deno && !$gp_launch_error ? gp_owner_record_if_locked");
      expect(takeover).toBeGreaterThan(-1);
      expect(takeover).toBeLessThan(launcher.indexOf("my $gp_pid = fork()"));
      expect(launcher).toContain(
        "gp_stop_if_current($gp_owner_file, $gp_prior_owner, 1, $gp_prior_pid, $gp_prior_nonce, $gp_prior_start, $gp_prior_entrypoint, $gp_static_path, $gp_expected_static)",
      );
    }
  });

  it("atomically advances the build marker for the same V2 worker identity", async () => {
    const harness = bootstrapHarness();
    harness.ydoc.files.set(".ghostprov-v2/worker.json", canonicalJson({
      protocol: "gp-awareness-v2",
      workerIndex: 7,
      buildHash: "0".repeat(64),
      createdAtMs: 1_787_300_000_000,
    }));

    await harness.controller.ensureReady();

    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/worker.json")!)).toEqual({
      protocol: "gp-awareness-v2",
      workerIndex: 7,
      buildHash: BUILD_HASH,
      createdAtMs: 1_787_300_000_000,
    });
  });

  it("renders the nonce-specific bootstrap bridge through root main.tex", async () => {
    const harness = bootstrapHarness();
    harness.ydoc.files.set("main.tex", "legacy V1 document\n");

    await harness.controller.ensureReady();

    const nonce = "00000000-0000-4000-8000-000000000001";
    expect(harness.gateway.mainDocuments).toEqual(["main.tex"]);
    expect(harness.ydoc.writes.some(write => write.path === ".latexmkrc")).toBe(true);
    expect(harness.ydoc.writes.some(write => write.path.endsWith("/.latexmkrc"))).toBe(false);
    expect(harness.ydoc.files.get("main.tex")).not.toBe("legacy V1 document\n");
    expect(harness.ydoc.writes.some(write => (
      write.path === "main.tex" &&
      write.content.includes(`\\InputIfFileExists{.ghostprov-v2/bootstrap-output.${nonce}.tex}{}{}`)
    ))).toBe(true);
    expect(harness.ydoc.files.get("main.tex")).toContain("launcher inactive");
  });

  it("publishes the bootstrap hello and bounded child diagnostics through the TeX log", async () => {
    const harness = bootstrapHarness("timeout");

    await expect(harness.controller.ensureReady()).rejects.toBeInstanceOf(V2BootstrapError);

    const nonce = "00000000-0000-4000-8000-000000000001";
    expect(harness.gateway.launchers[0])
      .toContain(`bootstrap-output.${nonce}.tex`);
    expect(harness.gateway.launchers[0]).toContain(`sidecar.${nonce}.log`);
    expect(harness.gateway.launchers[0]).not.toContain("'/tmp/ghostprov-v2/sidecar.log'");
    expect(harness.gateway.launchers[0])
      .not.toContain("'/workspace/.ghostprov-v2/bootstrap-output.tex'");
    expect(harness.gateway.launchers[0]).toContain("typeout{$gp_marker}");
    expect(harness.gateway.launchers[0]).toContain("GPV2_SIDECAR_HEX ");
    expect(harness.gateway.launchers[0]).toContain("command -v deno");
    expect(harness.gateway.launchers[0]).toContain("/usr/local/bin/deno");
    expect(harness.gateway.launchers[0]).toContain("$gp_deno");
    expect(harness.gateway.launchers[0]).not.toContain("exec 'setsid', '/code/crixet/deno'");
    expect(harness.gateway.launchers[0]).toContain(`/workspace/${ASSETS.entrypointPath}`);
    expect(harness.gateway.launchers[0]).not.toContain("--config=/workspace/.ghostprov-v2/deno.json");
    expect(harness.gateway.launchers[0]).toContain("NODE_ENV,PRODUCTION,FORCE_COLOR,NO_COLOR,COLORTERM,TERM");
    expect(harness.ydoc.writes.some(write => (
      write.path === "main.tex" &&
      write.content.includes(
        `\\InputIfFileExists{.ghostprov-v2/bootstrap-output.${nonce}.tex}{}{}`,
      )
    ))).toBe(true);
  });

  it("stages a candidate and never opens the incumbent PID for in-place overwrite", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();
    const launcher = harness.gateway.launchers[0]!;

    const candidateWrite = launcher.indexOf(
      "open my $gp_pid_fh, '>', $gp_candidate_pid_file",
    );
    const helloProof = launcher.indexOf("if ($gp_hello_seen) {\n  my $gp_promoted", candidateWrite);
    const atomicPromotion = launcher.indexOf(
      "rename $gp_candidate_pid_file, $gp_pid_file",
    );

    expect(candidateWrite).toBeGreaterThan(-1);
    expect(helloProof).toBeGreaterThan(candidateWrite);
    expect(atomicPromotion).toBeGreaterThan(helloProof);
    expect(launcher).not.toContain("open my $gp_pid_fh, '>', $gp_pid_file");
    expect(launcher).toContain("$gp_pid $gp_launch_nonce $gp_child_start\\n");
    expect(launcher).toContain("--ghostprov-launch-nonce=$gp_launch_nonce");
    expect(launcher).toContain("--ghostprov-process-start=$gp_child_verified_start");
    expect(launcher.indexOf("exit 126 unless $gp_child_candidate_ready"))
      .toBeLessThan(launcher.indexOf("exec 'setsid'"));
    expect(launcher).toContain("$gp_fields[1] eq \"$gp_pid\"");
    expect(launcher).toContain("$gp_fields[18] eq \"$gp_start\"");
  });

  it("keys each R2 hello to its fork and requires the locked owner before PID promotion", async () => {
    const harness = bootstrapHarness("none", "r2", 8, R2_ASSETS);
    await harness.controller.ensureReady();
    const launcher = harness.gateway.launchers[0]!;

    expect(launcher).toContain("my $gp_require_owner = 1;");
    expect(launcher).toContain(
      "$gp_dir/bootstrap-hello.$gp_launch_nonce.$gp_pid",
    );
    expect(launcher).toContain(
      "$gp_dir/bootstrap-hello.$gp_launch_nonce.$$",
    );
    const ownerBranch = launcher.indexOf("if ($gp_require_owner) {");
    const frozenR1Branch = launcher.indexOf("  } else {", ownerBranch);
    const launcherRename = launcher.indexOf(
      "rename $gp_candidate_pid_file, $gp_pid_file",
      ownerBranch,
    );
    expect(ownerBranch).toBeGreaterThan(-1);
    expect(frozenR1Branch).toBeGreaterThan(ownerBranch);
    expect(launcher.slice(ownerBranch, frozenR1Branch)).not.toContain(
      "rename $gp_candidate_pid_file",
    );
    expect(launcherRename).toBeGreaterThan(frozenR1Branch);
    expect(launcher).toContain(
      "owned sidecar did not atomically promote its process identity",
    );
  });

  it("retires a strictly proven prior immutable owner before forking a fresh build", async () => {
    const harness = bootstrapHarness("none", "r2", 8, R2_ASSETS);
    await harness.controller.ensureReady();
    const launcher = harness.gateway.launchers[0]!;
    const denoDiscovery = launcher.indexOf("my $gp_deno = ''");
    const authority = launcher.indexOf("bootstrap launcher binding is no longer current");
    const takeover = launcher.indexOf("my $gp_prior_owner = $gp_deno && !$gp_launch_error ? gp_owner_record_if_locked");
    const fork = launcher.indexOf("my $gp_pid = fork()");
    const authoritativeOutput = launcher.lastIndexOf(
      "if ($gp_authoritative && length($gp_marker) && gp_static_authority_is_current($gp_static_path, $gp_expected_static)) {",
    );
    const outputWrite = launcher.lastIndexOf("open my $gp_tex_fh, '>', $gp_tex");

    expect(takeover).toBeGreaterThan(-1);
    expect(denoDiscovery).toBeLessThan(takeover);
    expect(authority).toBeGreaterThan(-1);
    expect(authority).toBeLessThan(takeover);
    expect(takeover).toBeLessThan(fork);
    expect(authoritativeOutput).toBeGreaterThan(fork);
    expect(outputWrite).toBeGreaterThan(authoritativeOutput);
    expect(launcher).toContain("$gp_authoritative = 0; $gp_launch_error = 'bootstrap launcher binding is no longer current'");
    expect(launcher).toContain("$gp_current_static ne $gp_expected_static");
    expect(launcher).toContain("if ($gp_deno && !$gp_launch_error && defined $gp_prior_owner)");
    expect(launcher).toContain("sub gp_exact_r2_entrypoint");
    expect(launcher).toContain(
      `/workspace/\\.ghostprov-v2/revisions/r2/${R2_ASSETS.schemaHash}/${R2_ASSETS.buildHash}/sidecar\\.bundle\\.js`,
    );
    expect(launcher).toContain(
      `/workspace/\\.ghostprov-v2/revisions/r7/${R7_ASSETS.schemaHash}/${R7_ASSETS.buildHash}/sidecar\\.bundle\\.js`,
    );
    expect(launcher).toContain(
      `/workspace/\\.ghostprov-v2/revisions/r8/${R8_ASSETS.schemaHash}/${R8_ASSETS.buildHash}/sidecar\\.bundle\\.js`,
    );
    expect(launcher).toContain(
      "gp_process_matches($gp_prior_pid, $gp_prior_nonce, $gp_prior_start, $gp_prior_entrypoint)",
    );
    expect(launcher).toContain(
      "gp_stop_if_current($gp_owner_file, $gp_prior_owner, 1, $gp_prior_pid, $gp_prior_nonce, $gp_prior_start, $gp_prior_entrypoint, $gp_static_path, $gp_expected_static)",
    );
    expect(launcher).toContain(
      "gp_unlink_pid_if_unowned_and_same($gp_owner_file, $gp_pid_file, $gp_prior_pid_record)",
    );
    expect(launcher).toContain("existing sidecar ownership lock did not release");
    expect(launcher).toContain("$gp_prior_nonce eq $gp_launch_nonce && $gp_prior_entrypoint eq $gp_expected");
    expect(launcher).toContain("$gp_reused_owner = 1;");
    expect(launcher).toContain("$gp_marker = \"GPV2_OWNER_REUSED $gp_launch_nonce\"");
    expect(launcher).toContain(
      "if ($gp_deno && !$gp_launch_error && !$gp_reused_owner && !$gp_repeated_attempt)",
    );
    expect(launcher).toContain("$gp_attempt_claimed && !$gp_hello_seen");
    expect(launcher).not.toContain("current owned sidecar hello is unavailable");
    expect(launcher).toContain(
      "my $gp_prior_entrypoint = defined($gp_prior_pid) ? gp_exact_sidecar_entrypoint($gp_prior_pid) : undef",
    );
    expect(launcher).not.toContain(
      "my $gp_prior_entrypoint = defined($gp_prior_pid) ? gp_exact_r2_entrypoint($gp_prior_pid) : undef",
    );
    expect(launcher).toContain("sub gp_exact_sidecar_entrypoint");
    expect(launcher).toContain(
      `/workspace/\\.ghostprov-v2/revisions/r1/${ASSETS.schemaHash}/${ASSETS.buildHash}/sidecar\\.bundle\\.js`,
    );
    expect(launcher).toContain(
      "gp_stop_if_current($gp_pid_file, $gp_unowned_pid_record, 0, $gp_unowned_pid, $gp_unowned_nonce, $gp_unowned_start, $gp_unowned_entrypoint, $gp_static_path, $gp_expected_static)",
    );
    expect(launcher).toContain("sidecar ownership changed during startup preflight");
    expect(launcher).toContain("sub gp_process_state_start");
    expect(launcher).toContain("$gp_state ne 'Z' && $gp_state ne 'X' && $gp_state ne 'x'");
    expect(launcher).toContain("$gp_live_state eq 'Z' || $gp_live_state eq 'X' || $gp_live_state eq 'x'");
    expect(launcher).not.toContain(
      "gp_stop_if_current($gp_owner_file, $gp_prior_owner, 1, $gp_prior_pid, $gp_prior_nonce, $gp_prior_start, $gp_expected)",
    );
  });

  it("keeps a crashed promoted same-nonce launch diagnostic-only", async () => {
    const harness = bootstrapHarness("none", "r2", 8, R2_ASSETS);
    await harness.controller.ensureReady();
    const launcher = harness.gateway.launchers[0]!;
    const sameNonceProof = launcher.indexOf(
      "defined($gp_prior_start) && $gp_prior_nonce eq $gp_launch_nonce",
    );
    const preflight = launcher.indexOf(
      "open my $gp_preflight_owner_fh, '+>>', $gp_owner_file",
    );
    const attemptClaim = launcher.indexOf("mkdir $gp_attempt_path, 0700");
    const fork = launcher.indexOf("my $gp_pid = fork()");
    const logUnlink = launcher.indexOf("unlink $gp_log if -e $gp_log", attemptClaim);

    expect(launcher).toContain("bootstrap-attempt.$gp_launch_nonce");
    expect(launcher).toContain("my $gp_attempt_claim_limit = 256");
    expect(launcher).toContain(
      "^bootstrap-attempt\\.[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$",
    );
    expect(launcher).toContain("bootstrap attempt claim capacity is exhausted");
    expect(launcher).toContain("last if $gp_attempt_claim_count >= $gp_attempt_claim_limit");
    expect(launcher).toContain("$gp_repeated_attempt = 1");
    expect(launcher).toContain("same-nonce sidecar no longer has a live owner");
    expect(launcher).toContain("$gp_marker = gp_hex_diag($gp_repeated_diag)");
    expect(sameNonceProof).toBeGreaterThan(-1);
    expect(sameNonceProof).toBeLessThan(preflight);
    expect(attemptClaim).toBeLessThan(fork);
    expect(launcher).toContain(
      "if ($gp_deno && !$gp_launch_error && $gp_attempt_claimed)",
    );
    expect(logUnlink).toBeGreaterThan(attemptClaim);
    expect(logUnlink).toBeLessThan(fork);
  });

  it("rechecks stale-launch authority at every signal, claim, and fork boundary", async () => {
    const harness = bootstrapHarness("none", "r2", 8, R2_ASSETS);
    await harness.controller.ensureReady();
    const launcher = harness.gateway.launchers[0]!;
    const signalHelper = launcher.indexOf("sub gp_signal_if_authoritative");
    const signalAuthority = launcher.indexOf(
      "return 0 unless gp_static_authority_is_current($gp_static_path, $gp_expected_static)",
      signalHelper,
    );
    const destructiveSignal = launcher.indexOf("return kill $gp_signal, $gp_target", signalHelper);
    const claim = launcher.indexOf(
      "gp_static_authority_is_current($gp_static_path, $gp_expected_static) && mkdir $gp_attempt_path",
    );
    const fork = launcher.indexOf("my $gp_pid = fork()");
    const forkAuthority = launcher.lastIndexOf(
      "!gp_static_authority_is_current($gp_static_path, $gp_expected_static)",
      fork,
    );

    expect(signalHelper).toBeGreaterThan(-1);
    expect(signalAuthority).toBeGreaterThan(signalHelper);
    expect(destructiveSignal).toBeGreaterThan(signalAuthority);
    expect(launcher).not.toContain("kill 'TERM'");
    expect(launcher).not.toContain("kill 'KILL'");
    expect(launcher).toContain(
      "gp_signal_if_authoritative('TERM', -$gp_pid, $gp_static_path, $gp_expected_static)",
    );
    expect(launcher).toContain(
      "gp_signal_if_authoritative('KILL', -$gp_pid, $gp_static_path, $gp_expected_static)",
    );
    expect(claim).toBeGreaterThan(-1);
    expect(claim).toBeLessThan(fork);
    expect(forkAuthority).toBeGreaterThan(claim);
    expect(forkAuthority).toBeLessThan(fork);
    expect(launcher).toContain(
      "length($gp_marker) && gp_static_authority_is_current($gp_static_path, $gp_expected_static)",
    );
  });

  it("reaps an exit-fast contender and removes only its exact candidate identity", async () => {
    const harness = bootstrapHarness("none", "r2", 8, R2_ASSETS);
    await harness.controller.ensureReady();
    const launcher = harness.gateway.launchers[0]!;
    const helloPoll = launcher.indexOf(
      "for (my $gp_try = 0; $gp_deno && !$gp_launch_error && $gp_attempt_claimed && !$gp_hello_seen && $gp_try < 500",
    );
    const exited = launcher.indexOf(
      "my $gp_waited = waitpid($gp_child_pid, 1)",
      helloPoll,
    );
    const pollSleep = launcher.indexOf("0.05;", helloPoll);

    expect(helloPoll).toBeGreaterThan(-1);
    expect(exited).toBeGreaterThan(helloPoll);
    expect(exited).toBeLessThan(pollSleep);
    expect(launcher).toContain("sidecar exited before bootstrap hello");
    expect(launcher).toContain("sub gp_process_is_same_live");
    expect(launcher).toContain("!gp_process_is_same_live($gp_child_pid, $gp_child_start)");
    expect(launcher).toContain("$gp_try < 20 && gp_group_alive($gp_pid)");
    expect(launcher).toContain("$gp_try < 300 && !$gp_promoted");
    expect(launcher).toContain(
      "$gp_diag = $gp_sidecar_diag if defined($gp_sidecar_diag) && length($gp_sidecar_diag)",
    );
    expect(launcher).toContain(
      "($gp_reaped || $gp_stopped || !gp_group_alive($gp_child_pid)) && gp_record_is($gp_candidate_pid_file, $gp_candidate_record)",
    );
  });

  it("surfaces a sanitized sidecar launch failure instead of a generic hello timeout", async () => {
    const harness = bootstrapHarness("sidecar-log");

    await expect(harness.controller.ensureReady()).rejects.toMatchObject({
      name: "V2BootstrapError",
      kind: "hello",
      message: "V2 sidecar failed before bootstrap hello: Can't exec /code/crixet/deno: No such file or directory",
    });
  });

  it("preserves the exact allowlisted sidecar fatal category for recovery telemetry", async () => {
    const harness = bootstrapHarness("sidecar-fatal");

    await expect(harness.controller.ensureReady()).rejects.toMatchObject({
      name: "V2BootstrapError",
      kind: "hello",
      sidecarDiagnostic: {
        errorName: "SidecarOwnershipError",
        errorCode: "contended",
        causeName: null,
      },
    });
  });

  it("captures an exact sidecar fatal record when readiness fails after hello", async () => {
    const harness = bootstrapHarness("post-hello-fatal");

    await expect(harness.controller.ensureReady()).rejects.toMatchObject({
      name: "V2BootstrapError",
      kind: "readiness",
      sidecarDiagnostic: {
        errorName: "SidecarOwnershipError",
        errorCode: "lost",
        causeName: "Error",
        phase: "engine_start",
        permission: "write",
      },
    });
    const launcher = harness.gateway.launchers[0]!;
    expect(launcher).toContain("sub gp_read_tail");
    expect(launcher).toContain("read $gp_fh, $gp_value, $gp_limit");
    expect(launcher).toContain("gp_read_tail($gp_log, 4096)");
    expect(launcher).toContain("$gp_secondary_marker");
    expect(harness.ydoc.writes.some(write => (
      write.content.includes("bounded sidecar diagnostic launcher")
    ))).toBe(false);
  });

  it("keeps delayed renders on root main with nonce-specific log and output artifacts", async () => {
    const harness = bootstrapHarness("post-hello-fatal", "r2", 8, R2_ASSETS);
    await expect(harness.controller.ensureReady()).rejects.toBeInstanceOf(V2BootstrapError);

    const nonce = "r2-launch-nonce";
    const outputPath = `.ghostprov-v2/bootstrap-output.${nonce}.tex`;
    expect(new Set(harness.gateway.mainDocuments)).toEqual(new Set(["main.tex"]));
    expect(harness.gateway.launchers.every(launcher => (
      launcher.includes(`/tmp/ghostprov-v2/sidecar.${nonce}.log`) &&
      launcher.includes(`/workspace/${outputPath}`)
    ))).toBe(true);
    expect(harness.ydoc.deleted).toEqual(expect.arrayContaining([outputPath]));
    expect(harness.ydoc.deleted).not.toContain("main.tex");
    expect(harness.ydoc.writes.some(write => (
      write.path === "main.tex" &&
      write.content.includes(`\\InputIfFileExists{${outputPath}}{}{}`)
    ))).toBe(true);
  });

  it.each(["state", "render", "fetch"] as const)(
    "keeps the post-hello diagnostic deadline when %s ignores cancellation",
    async stage => {
      const harness = bootstrapHarness(
        "post-hello-fatal",
        undefined,
        8,
        ASSETS,
        { attempts: 1, timeoutMs: 20, pollMs: 5 },
      );
      const never = () => new Promise<void>(() => undefined);
      const originalState = harness.ydoc.renderSyncState.bind(harness.ydoc);
      const originalRender = harness.gateway.triggerRender.bind(harness.gateway);
      const originalFetch = harness.gateway.fetchText.bind(harness.gateway);
      if (stage === "state") {
        harness.ydoc.renderSyncState = async () => {
          if (harness.gateway.awaitingReadiness) {
            await never();
          }
          return originalState();
        };
      } else if (stage === "render") {
        harness.gateway.triggerRender = async request => {
          if (harness.gateway.awaitingReadiness) {
            await never();
          }
          await originalRender(request);
        };
      } else {
        harness.gateway.fetchText = async suffix => {
          if (harness.gateway.awaitingReadiness) {
            await never();
          }
          return originalFetch(suffix);
        };
      }

      const startedAt = performance.now();
      await expect(harness.controller.ensureReady()).rejects.toMatchObject({
        name: "V2BootstrapError",
        kind: "readiness",
      });
      expect(performance.now() - startedAt).toBeLessThan(1_000);
    },
  );

  it("redacts credential-shaped values from sidecar launch diagnostics", async () => {
    const harness = bootstrapHarness("sidecar-log-secret");

    const error = await harness.controller.ensureReady().catch(value => value);

    expect(error).toMatchObject({
      name: "V2BootstrapError",
      kind: "hello",
    });
    expect(error.message).toContain("Authorization: Bearer [REDACTED]");
    expect(error.message).not.toContain("sk-bootstrap-secret");
  });

  it("fails closed before launch when an immutable addressed asset was altered", async () => {
    const harness = bootstrapHarness();
    harness.ydoc.files.set(ASSETS.entrypointPath, "altered immutable bundle\n");

    await expect(harness.controller.ensureReady()).rejects.toMatchObject({
      name: "V2BootstrapError",
      kind: "deploy",
      message: expect.stringContaining("Immutable V2 sidecar asset collision"),
    });

    expect(harness.gateway.renders).toBe(0);
    expect(harness.ydoc.files.has(".ghostprov-v2/revision.json")).toBe(false);
  });

  it("requests a proven drain before clearing sidecar presence", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();
    const current = harness.gateway.current!;
    harness.awareness.onPublish = envelope => {
      const drain = envelope.frames.find(frame => frame.kind === "drain");
      if (!drain) return;
      harness.awareness.sidecar({
        ...readyEnvelope(current.binding, current.instanceId, "none"),
        revision: 2,
        ack: { epoch: envelope.epoch, sequence: drain.sequence },
        credits: 0,
        draining: true,
        frames: [{
          kind: "drained",
          active: 0,
          queued: 0,
          epoch: 1,
          sequence: 0,
        }],
      });
    };

    await harness.controller.drainAndStop("test shutdown");

    expect(harness.awareness.published.some(envelope => (
      envelope.frames.some(frame => frame.kind === "drain" && frame.reason === "test shutdown")
    ))).toBe(true);
    expect(harness.gateway.launchers.at(-1)).toContain("sidecar termination launcher");
    expect(harness.gateway.logs).toContain("GPV2_RETIRED");
    expect(harness.awareness.clears).toBe(1);
  });

  it("force-stops the exact launched process once and restores the inert launcher", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();

    await Promise.all([
      harness.controller.forceStop("supervisor recovery"),
      harness.controller.forceStop("duplicate recovery"),
    ]);

    expect(harness.gateway.renders).toBe(2);
    expect(harness.gateway.launchers[1]).toContain("sidecar termination launcher");
    expect(harness.gateway.launchers[1]).toContain("/tmp/ghostprov-v2/sidecar.pid");
    expect(harness.gateway.launchers[1]).toContain(
      "'00000000-0000-4000-8000-000000000001'",
    );
    expect(harness.gateway.launchers[1]).toContain("%gp_expected_nonces");
    expect(harness.gateway.launchers[1]).toContain("gp_stop_if_current($gp_owner_file");
    expect(harness.gateway.launchers[1]).toContain(
      "$gp_expected, $gp_static_path, $gp_expected_static",
    );
    expect(harness.gateway.launchers[1]).toContain("GPV2_RETIRED");
    expect(harness.gateway.launchers[1]).toContain("$gp_expected_static");
    expect(harness.gateway.launchers[1]).toContain("bootstrap-settled.$gp_nonce");
    expect(harness.gateway.launchers[1]).toContain("gp_group_alive($gp_group)");
    expect(harness.gateway.logs).toContain("GPV2_RETIRED");
    expect(harness.gateway.launchers[1]).toContain(
      "flock($gp_owner_fh, LOCK_EX | LOCK_NB)",
    );
    expect(harness.gateway.launchers[1]).not.toContain("  unlink $gp_pid_file;");
    expect(harness.ydoc.files.get(".latexmkrc"))
      .toContain("GhostProv V2 launcher inactive");
    expect(harness.awareness.clears).toBe(1);
    await expect(harness.controller.ensureReady()).rejects.toThrow("retired");
  });

  it("does not report forced retirement from async render acceptance without fresh death proof", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();
    const startedAt = harness.clock.now();
    harness.gateway.triggerRender = async request => {
      harness.gateway.calls.push("render");
      harness.gateway.renders++;
      harness.gateway.mainDocuments.push(request?.mainDocument ?? "");
      harness.gateway.launchers.push(harness.ydoc.files.get(".latexmkrc") ?? "");
      // A conforming async render request may be accepted and then never run.
    };

    await expect(harness.controller.forceStop("lost async render"))
      .rejects.toThrow(/retirement proof|terminate/i);
    expect(harness.clock.now() - startedAt).toBeGreaterThanOrEqual(20);
    expect(harness.ydoc.files.get(".latexmkrc"))
      .toContain("GhostProv V2 launcher inactive");
  });

  it("rejects a stale retirement marker instead of treating marker presence as proof", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();
    harness.gateway.triggerRender = async request => {
      harness.gateway.calls.push("render");
      harness.gateway.renders++;
      harness.gateway.mainDocuments.push(request?.mainDocument ?? "");
      harness.gateway.launchers.push(harness.ydoc.files.get(".latexmkrc") ?? "");
      harness.gateway.logs = "render starting\nGPV2_RETIRED stale-retirement-token\n)\n";
    };

    await expect(harness.controller.forceStop("stale marker"))
      .rejects.toThrow("retirement proof");
  });

  it("does not let detached retirement cleanup overwrite a newer bootstrap generation", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();
    let releaseTermination!: () => void;
    const terminationGate = new Promise<void>(resolve => { releaseTermination = resolve; });
    let retirementStarted!: () => void;
    const started = new Promise<void>(resolve => { retirementStarted = resolve; });
    harness.gateway.triggerRender = async () => {
      retirementStarted();
      await terminationGate;
      throw new Error("late retirement render failed");
    };
    const reason = new DOMException("shutdown deadline expired", "TimeoutError");
    const retiring = harness.controller.forceStop(
      "expired shutdown",
      AbortSignal.abort(reason),
    );
    await expect(retiring).rejects.toBe(reason);
    await started;

    harness.ydoc.files.set(".ghostprov-v2/static.json", "NEW_GENERATION_STATIC");
    harness.ydoc.files.set(".ghostprov-v2/bootstrap.enc.json", "NEW_GENERATION_CIPHERTEXT");
    harness.ydoc.files.set(".latexmkrc", "NEW_GENERATION_LAUNCHER");
    releaseTermination();
    await Bun.sleep(10);

    expect(harness.ydoc.files.get(".ghostprov-v2/static.json"))
      .toBe("NEW_GENERATION_STATIC");
    expect(harness.ydoc.files.get(".ghostprov-v2/bootstrap.enc.json"))
      .toBe("NEW_GENERATION_CIPHERTEXT");
    expect(harness.ydoc.files.get(".latexmkrc")).toBe("NEW_GENERATION_LAUNCHER");
  });

  it("serializes two controllers and makes superseded cleanup leave the new owner intact", async () => {
    const old = bootstrapHarness();
    await old.controller.ensureReady();
    old.gateway.triggerRender = async request => {
      old.gateway.calls.push("render");
      old.gateway.renders++;
      old.gateway.mainDocuments.push(request?.mainDocument ?? "");
      old.gateway.launchers.push(old.ydoc.files.get(".latexmkrc") ?? "");
    };
    let releaseOldPoll!: () => void;
    const oldPollGate = new Promise<void>(resolve => { releaseOldPoll = resolve; });
    let oldPollStarted!: () => void;
    const oldPolling = new Promise<void>(resolve => { oldPollStarted = resolve; });
    old.gateway.fetchText = async () => {
      oldPollStarted();
      await oldPollGate;
      return { status: 200, text: "", headers: new Headers() };
    };
    const reason = new DOMException("old deadline", "TimeoutError");
    await expect(old.controller.forceStop("old generation", AbortSignal.abort(reason)))
      .rejects.toBe(reason);
    await oldPolling;

    const nextAwareness = new FakeAwareness();
    const nextGateway = new FakeGateway(old.ydoc, "timeout");
    let releaseNextRender!: () => void;
    const nextRenderGate = new Promise<void>(resolve => { releaseNextRender = resolve; });
    let nextRenderStarted!: () => void;
    const nextRendering = new Promise<void>(resolve => { nextRenderStarted = resolve; });
    nextGateway.triggerRender = async request => {
      nextGateway.calls.push("render");
      nextGateway.renders++;
      nextGateway.mainDocuments.push(request?.mainDocument ?? "");
      nextGateway.launchers.push(old.ydoc.files.get(".latexmkrc") ?? "");
      nextRenderStarted();
      await nextRenderGate;
    };
    let releaseNextFetch!: () => void;
    const nextFetchGate = new Promise<void>(resolve => { releaseNextFetch = resolve; });
    let nextFetchStarted!: () => void;
    const nextFetching = new Promise<void>(resolve => { nextFetchStarted = resolve; });
    let firstNextFetch = true;
    nextGateway.fetchText = async () => {
      if (firstNextFetch) {
        firstNextFetch = false;
        nextFetchStarted();
        await nextFetchGate;
      }
      return { status: 200, text: "", headers: new Headers() };
    };
    old.ydoc.onWrite = undefined;
    const next = new V2BootstrapController({
      projectId: PROJECT,
      workerIndex: 7,
      sandboxSessionId: "sandbox-session-2",
      leaderGeneration: 5,
      ySweet: old.ySweet,
      ydoc: old.ydoc,
      gateway: nextGateway,
      awareness: nextAwareness,
      assets: ASSETS,
      attempts: 1,
      timeoutMs: 20,
      pollMs: 5,
      clock: new FakeClock(),
      random: new FakeRandom(),
      initialActivePerSandbox: 4,
      maxActivePerSandbox: 8,
    });
    const nextEnsuring = next.ensureReady();
    await nextRendering;
    const nextStatic = old.ydoc.files.get(".ghostprov-v2/static.json");
    const nextLauncher = old.ydoc.files.get(".latexmkrc");
    old.ydoc.files.set(".ghostprov-v2/bootstrap.enc.json", "NEXT_CIPHERTEXT");

    releaseOldPoll();
    releaseNextRender();
    await nextFetching;
    for (let turn = 0; turn < 50 && old.awareness.clears < 1; turn++) {
      await Promise.resolve();
    }

    expect(old.awareness.clears).toBe(1);
    expect(old.ydoc.files.get(".ghostprov-v2/static.json")).toBe(nextStatic);
    expect(old.ydoc.files.get(".latexmkrc")).toBe(nextLauncher);
    expect(old.ydoc.files.get(".ghostprov-v2/bootstrap.enc.json")).toBe("NEXT_CIPHERTEXT");

    releaseNextFetch();
    await expect(nextEnsuring).rejects.toBeInstanceOf(V2BootstrapError);
  });

  it("does not let a released old controller reclaim authority from its replacement", async () => {
    const old = bootstrapHarness();
    await old.controller.ensureReady();
    const oldCurrent = old.gateway.current!;
    old.awareness.onPublish = envelope => {
      const drain = envelope.frames.find(frame => frame.kind === "drain");
      if (!drain) return;
      old.awareness.sidecar({
        ...readyEnvelope(oldCurrent.binding, oldCurrent.instanceId, "none"),
        revision: 2,
        ack: { epoch: envelope.epoch, sequence: drain.sequence },
        credits: 0,
        draining: true,
        frames: [{
          kind: "drained",
          active: 0,
          queued: 0,
          epoch: 1,
          sequence: 0,
        }],
      });
    };
    await old.controller.drainAndStop("old generation retired");

    const nextAwareness = new FakeAwareness();
    const nextGateway = new FakeGateway(old.ydoc, "none");
    old.ydoc.onWrite = async (path, content) => {
      if (path === ".latexmkrc" && content.includes("temporary bootstrap launcher")) {
        nextGateway.awaitingReadiness = false;
        return;
      }
      if (path !== ".ghostprov-v2/bootstrap.enc.json") return;
      const current = nextGateway.current!;
      nextAwareness.sidecar(readyEnvelope(current.binding, current.instanceId, "none"));
    };
    const next = new V2BootstrapController({
      projectId: PROJECT,
      workerIndex: 7,
      sandboxSessionId: "sandbox-session-2",
      leaderGeneration: 5,
      ySweet: old.ySweet,
      ydoc: old.ydoc,
      gateway: nextGateway,
      awareness: nextAwareness,
      assets: ASSETS,
      attempts: 1,
      timeoutMs: 20,
      pollMs: 5,
      clock: new FakeClock(),
      random: new FakeRandom(),
      initialActivePerSandbox: 4,
      maxActivePerSandbox: 8,
    });
    await next.ensureReady();
    const nextStatic = old.ydoc.files.get(".ghostprov-v2/static.json");

    await expect(old.controller.forceStop("stale old force")).resolves.toBeUndefined();
    expect(old.ydoc.files.get(".ghostprov-v2/static.json")).toBe(nextStatic);
    await expect(next.forceStop("new owner retirement")).resolves.toBeUndefined();
    expect(nextGateway.logs).toContain("GPV2_RETIRED");
  });

  it("observes physical retirement after its caller deadline already expired", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();
    harness.gateway.triggerRender = async () => {
      throw new Error("late retirement failed");
    };
    const reason = new DOMException("shutdown deadline expired", "TimeoutError");

    await expect(harness.controller.forceStop(
      "expired shutdown",
      AbortSignal.abort(reason),
    )).rejects.toBe(reason);

    // The caller is already released, but the source retirement still owns a
    // rejection consumer and completes its local inert-launcher cleanup.
    await Bun.sleep(10);
    expect(harness.ydoc.files.get(".latexmkrc"))
      .toContain("GhostProv V2 launcher inactive");
    expect(harness.awareness.clears).toBe(1);
  });

  it("keeps the retirement witness false after public cancellation until its actual owner settles", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();
    let release!: () => void;
    const pending = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    harness.gateway.triggerRender = async () => { entered(); await pending; throw new Error("offline remote proof unavailable"); };
    const cancellation = new AbortController();
    const retiring = harness.controller.forceStop("offline cleanup", cancellation.signal);
    await started;
    cancellation.abort();
    await expect(retiring).rejects.toBeInstanceOf(DOMException);
    expect(harness.controller.retirementSettled()).toBe(false);
    release();
    await expect(harness.controller.forceStop("join exact owner")).rejects.toThrow();
    expect(harness.controller.retirementSettled()).toBe(true);
    expect(harness.awareness.clears).toBe(1);
  });

  it("retains every possibly launched retry nonce for forced termination", async () => {
    const harness = bootstrapHarness("first-wrong-nonce");
    await harness.controller.ensureReady();

    await harness.controller.forceStop("stop successful retry");
    const termination = harness.gateway.launchers.at(-1)!;

    expect(harness.gateway.nonces.slice(0, 2)).toEqual([
      "00000000-0000-4000-8000-000000000001",
      "00000000-0000-4000-8000-000000000002",
    ]);
    expect(termination).toContain(
      "'00000000-0000-4000-8000-000000000002'",
    );
    expect(termination).toContain(
      "'00000000-0000-4000-8000-000000000001'",
    );
  });

  it("retains an earlier launch when a later attempt fails before render", async () => {
    const harness = bootstrapHarness(
      "first-wrong-nonce",
      undefined,
      8,
      ASSETS,
      { attempts: 2 },
    );
    const originalWrite = harness.ydoc.onWrite;
    let stagedLaunchers = 0;
    harness.ydoc.onWrite = async (path, content) => {
      if (path === ".latexmkrc" && content.includes("temporary bootstrap launcher")) {
        stagedLaunchers++;
        if (stagedLaunchers === 2) throw new Error("second attempt failed before render");
      }
      await originalWrite?.(path, content);
    };

    await expect(harness.controller.ensureReady()).rejects.toThrow("stage V2 bootstrap");
    expect(harness.gateway.nonces).toEqual([
      "00000000-0000-4000-8000-000000000001",
    ]);

    await expect(harness.controller.forceStop("retire every attempt")).resolves.toBeUndefined();
    const termination = harness.gateway.launchers.at(-1)!;
    expect(termination).toContain("'00000000-0000-4000-8000-000000000001'");
    expect(termination).toContain("'00000000-0000-4000-8000-000000000002'");
  });

  it("bounds and retires launch identities across repeated ensure calls", async () => {
    const harness = bootstrapHarness("timeout", undefined, 8, ASSETS, { attempts: 1 });
    for (let call = 0; call < 4; call++) {
      await expect(harness.controller.ensureReady()).rejects.toBeInstanceOf(V2BootstrapError);
    }

    await expect(harness.controller.forceStop("repeated ensure cleanup"))
      .resolves.toBeUndefined();
    const termination = harness.gateway.launchers.at(-1)!;
    for (let nonce = 1; nonce <= 4; nonce++) {
      expect(termination).toContain(
        `'00000000-0000-4000-8000-${String(nonce).padStart(12, "0")}'`,
      );
    }
    expect(termination).toContain("$gp_attempt_claim_count < $gp_attempt_claim_limit");
  });

  it("retires before launch without triggering a render", async () => {
    const harness = bootstrapHarness();

    await expect(harness.controller.forceStop("never launched")).resolves.toBeUndefined();

    expect(harness.gateway.renders).toBe(0);
    expect(harness.ydoc.files.get(".latexmkrc")).toBeUndefined();
    expect(harness.awareness.clears).toBe(1);
  });

  it("restores the inert launcher even when remote drain proof times out", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();

    await expect(harness.controller.drainAndStop("missing drain proof"))
      .rejects.toBeInstanceOf(V2BootstrapError);

    expect(harness.ydoc.files.get(".latexmkrc"))
      .toContain("GhostProv V2 launcher inactive");
    expect(harness.awareness.clears).toBe(1);
    expect(harness.ydoc.deleted).toContain(".ghostprov-v2/bootstrap.enc.json");
  });

  it("cannot publish readiness after force-stop races an abort-ignoring launch", async () => {
    const harness = bootstrapHarness();
    const originalRender = harness.gateway.triggerRender.bind(harness.gateway);
    let firstRenderStarted!: () => void;
    const started = new Promise<void>(resolve => { firstRenderStarted = resolve; });
    let releaseFirst!: () => void;
    const blocked = new Promise<void>(resolve => { releaseFirst = resolve; });
    let first = true;
    harness.gateway.triggerRender = async request => {
      if (first) {
        first = false;
        firstRenderStarted();
        await blocked;
      }
      await originalRender(request);
    };
    const ensuring = harness.controller.ensureReady();
    const failed = ensuring.catch(error => error);
    await started;

    const stopping = harness.controller.forceStop("recovery during launch");
    expect(harness.controller.retirementSettled()).toBe(false);
    releaseFirst();
    await expect(stopping).resolves.toBeUndefined();
    expect(await failed).toMatchObject({ name: "AbortError" });
    expect(harness.ydoc.files.get(".latexmkrc"))
      .toContain("GhostProv V2 launcher inactive");
    await expect(harness.controller.ensureReady()).rejects.toThrow("retired");
    for (let turn = 0; turn < 20; turn++) await Promise.resolve();
    expect(harness.controller.retirementSettled()).toBe(true);
  });

  it("proves a bound ping round trip before handing the sidecar to transport", async () => {
    const harness = bootstrapHarness();
    await harness.controller.ensureReady();
    const current = harness.gateway.current!;
    harness.awareness.onPublish = envelope => {
      const ping = envelope.frames.find(frame => frame.kind === "ping");
      if (!ping) return;
      harness.awareness.sidecar({
        ...readyEnvelope(current.binding, current.instanceId, "none"),
        revision: 2,
        ack: { epoch: envelope.epoch, sequence: ping.sequence },
        frames: [{
          kind: "pong",
          sentAtMs: ping.sentAtMs,
          epoch: 1,
          sequence: 0,
        }],
      });
    };

    await expect(harness.controller.ping()).resolves.toBe(0);
    expect(harness.awareness.published.at(-1)?.frames[0]).toMatchObject({
      kind: "ping",
      sequence: 0,
    });
  });

  it("seals same-instance credential rotation with the in-memory bootstrap key", async () => {
    const harness = bootstrapHarness();
    const descriptor = await harness.controller.ensureReady();
    const rotated = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "rotated-y-token",
      authorization: "full",
    });

    const serialized = await harness.controller.sealCredentialRotation(rotated, "rotation-1");

    expect(serialized).not.toContain("rotated-y-token");
    await expect(openCredentialRotation({
      rotationKey: harness.decryptedCredentials[0]!.rotationKey,
      sealed: JSON.parse(serialized),
      expectedBinding: {
        projectId: PROJECT,
        sandboxSessionId: "sandbox-session-1",
        leaderGeneration: 4,
        buildHash: BUILD_HASH,
        schemaHash: SCHEMA_HASH,
        instanceId: descriptor.instanceId,
        rotationId: "rotation-1",
      },
    })).resolves.toMatchObject({ token: "rotated-y-token" });
  });

  it("fails fast when another generation replaces root bootstrap authority", async () => {
    const harness = bootstrapHarness("replaced-binding");

    await expect(harness.controller.ensureReady()).rejects.toMatchObject({
      name: "V2BootstrapError",
      kind: "deploy",
      message: "V2 bootstrap workspace authority was replaced by another generation",
    });
    expect(harness.gateway.renders).toBe(3);
    expect(harness.clock.value).toBe(1_787_400_000_000);
  });

  it.each([
    "wrong-nonce",
    "wrong-build",
    "wrong-generation",
    "stale-instance",
    "timeout",
  ] as const)("fails closed after bounded retries on %s", async fault => {
    const harness = bootstrapHarness(fault);

    await expect(harness.controller.ensureReady()).rejects.toBeInstanceOf(V2BootstrapError);
    expect(harness.gateway.renders).toBe(fault === "wrong-nonce" ? 3 : 6);
    expect(new Set(harness.gateway.nonces).size).toBe(3);
    expect(harness.gateway.calls.some(call => call.includes("zip"))).toBe(false);
    expect(harness.ydoc.writes.every(write => !write.content.includes(TOKEN))).toBe(true);
    expect(harness.ydoc.files.get(".latexmkrc")).toContain("GhostProv V2 launcher inactive");
  });
});

describe("V2BootstrapController supervisor boundary", () => {
  it("does not fall back to Render when the sandbox supervisor rejects launch", async () => {
    const harness = bootstrapHarness("none", "r9", 8, R9_ASSETS, { launchMode: "relay" });
    (harness.supervisor as any).launch = async () => {
      throw new Error("supervisor unavailable");
    };
    await expect(harness.controller.ensureReady()).rejects.toBeInstanceOf(V2BootstrapError);
    expect(harness.gateway.renders).toBe(0);
  });

  it("requires an explicit supervisor for R9 relay mode", () => {
    const harness = bootstrapHarness("none", "r9", 8, R9_ASSETS);
    expect(() => new V2BootstrapController({
      projectId: PROJECT,
      workerIndex: 7,
      sandboxSessionId: "sandbox-session-1",
      leaderGeneration: 4,
      ySweet: harness.ySweet,
      ydoc: harness.ydoc,
      gateway: harness.gateway,
      awareness: harness.awareness,
      assets: R9_ASSETS,
      revision: "r9",
      launchNonce: "r9-launch-nonce",
      launchMode: "relay",
    })).toThrow(/requires a sandbox supervisor/i);
  });
});
