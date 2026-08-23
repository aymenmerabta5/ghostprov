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
import type { V2SidecarAssets } from "../src/v2/sidecar/assets";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const BUILD_HASH = "a".repeat(64);
const SCHEMA_HASH = "b".repeat(64);
const TOKEN = `ysweet-plaintext-canary-${"x".repeat(48)}`;

type Fault =
  | "none"
  | "wrong-nonce"
  | "wrong-build"
  | "wrong-generation"
  | "stale-instance"
  | "sidecar-log"
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
    const config = JSON.parse(this.ydoc.files.get(".ghostprov-v2/static.json")!) as {
      binding: BootstrapBinding;
    };
    this.nonces.push(config.binding.nonce);
    const helloBinding = (
      this.fault === "wrong-nonce" ||
      (this.fault === "first-wrong-nonce" && this.renders === 1)
    )
      ? { ...config.binding, nonce: `stale-${config.binding.nonce}` }
      : config.binding;
    const keys = await createBootstrapKeyPair(helloBinding);
    const instanceId = `sidecar-instance-${this.renders}`;
    this.current = { binding: helloBinding, instanceId, keys };
    if (this.fault === "sidecar-log" || this.fault === "sidecar-log-secret") {
      const message = this.fault === "sidecar-log-secret"
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

const ASSETS: V2SidecarAssets = {
  buildHash: BUILD_HASH,
  schemaHash: SCHEMA_HASH,
  obsoletePaths: Object.freeze([
    ".ghostprov-v2/deno.json",
    ".ghostprov-v2/sandbox",
    ".ghostprov-v2/src",
  ]),
  files: Object.freeze([
    {
      path: ".ghostprov-v2/deno.json",
      content: "{}\n",
      sha256: "c".repeat(64),
    },
    {
      path: ".ghostprov-v2/sandbox/v2/main.ts",
      content: "await Promise.resolve();\n",
      sha256: "d".repeat(64),
    },
  ]),
};

function readyEnvelope(
  binding: BootstrapBinding,
  instanceId: string,
  fault: Fault,
): AwarenessEnvelope<SidecarFrame> {
  return {
    protocol: "gp-awareness-v2",
    schemaVersion: 1,
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
    credits: 128,
    active: 0,
    queued: 0,
    draining: false,
    frames: [],
  };
}

function bootstrapHarness(fault: Fault = "none") {
  const clock = new FakeClock();
  const random = new FakeRandom();
  const ydoc = new FakeYDoc();
  const awareness = new FakeAwareness();
  const gateway = new FakeGateway(ydoc, fault);
  const decrypted: string[] = [];
  const decryptedCredentials: BootstrapCredentialPayload[] = [];
  const ySweet = parseYSweetEnvelope({
    url: `wss://prism.test/y/d/${PROJECT}/ws`,
    baseUrl: `https://prism.test/y/d/${PROJECT}`,
    docId: PROJECT,
    token: TOKEN,
    authorization: "full",
  });
  ydoc.onWrite = async (path, content) => {
    if (path !== ".ghostprov-v2/bootstrap.enc.json") return;
    const current = gateway.current!;
    const credential = await openBootstrapCredential({
      sealed: JSON.parse(content) as SealedBootstrapCredential,
      sidecarPrivateKey: current.keys.privateKey,
      expectedBinding: current.binding,
    });
    decrypted.push(credential.token);
    decryptedCredentials.push(credential);
    if (fault !== "timeout") {
      awareness.sidecar(readyEnvelope(current.binding, current.instanceId, fault));
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
    assets: ASSETS,
    attempts: 3,
    timeoutMs: 20,
    pollMs: 5,
    clock,
    random,
    diagnostics: { enabled: true, intervalMs: 1_000 },
    initialActivePerSandbox: 4,
    maxActivePerSandbox: 12,
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
    ySweet,
  };
}

describe("V2BootstrapController", () => {
  it("deploys non-secret assets, seals credentials, and accepts only bound readiness", async () => {
    const harness = bootstrapHarness();

    await expect(harness.controller.ensureReady()).resolves.toEqual({
      instanceId: "sidecar-instance-1",
      buildHash: BUILD_HASH,
      schemaHash: SCHEMA_HASH,
      leaderGeneration: 4,
      capacity: 12,
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
    expect(harness.awareness.connects).toBe(1);
    expect(JSON.parse(harness.ydoc.files.get(".ghostprov-v2/static.json")!)).toMatchObject({
      diagnostics: { enabled: true, intervalMs: 1_000 },
      initialActivePerSandbox: 4,
      maxActivePerSandbox: 12,
    });
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

  it("replaces a legacy root main file with the V2 bootstrap log bridge", async () => {
    const harness = bootstrapHarness();
    harness.ydoc.files.set("main.tex", "legacy V1 document\n");

    await harness.controller.ensureReady();

    expect(harness.gateway.mainDocuments).toEqual(["main.tex"]);
    expect(harness.ydoc.files.get("main.tex")).not.toBe("legacy V1 document\n");
    expect(harness.ydoc.files.get("main.tex"))
      .toContain("\\InputIfFileExists{.ghostprov-v2/bootstrap-output.tex}{}{}");
  });

  it("publishes the bootstrap hello and bounded child diagnostics through the TeX log", async () => {
    const harness = bootstrapHarness("timeout");

    await expect(harness.controller.ensureReady()).rejects.toBeInstanceOf(V2BootstrapError);

    expect(harness.gateway.launchers[0]).toContain("bootstrap-output.tex");
    expect(harness.gateway.launchers[0]).toContain("typeout{$gp_marker}");
    expect(harness.gateway.launchers[0]).toContain("GPV2_SIDECAR_HEX ");
    expect(harness.gateway.launchers[0]).toContain("/tmp/ghostprov-v2/sidecar.log");
    expect(harness.gateway.launchers[0]).toContain("command -v deno");
    expect(harness.gateway.launchers[0]).toContain("/usr/local/bin/deno");
    expect(harness.gateway.launchers[0]).toContain("$gp_deno");
    expect(harness.gateway.launchers[0]).not.toContain("exec 'setsid', '/code/crixet/deno'");
    expect(harness.gateway.launchers[0]).toContain("/workspace/.ghostprov-v2/sidecar.bundle.js");
    expect(harness.gateway.launchers[0]).not.toContain("--config=/workspace/.ghostprov-v2/deno.json");
    expect(harness.gateway.launchers[0]).toContain("NODE_ENV,PRODUCTION,FORCE_COLOR,NO_COLOR,COLORTERM,TERM");
    expect(harness.ydoc.writes.find(write => write.path === "main.tex")?.content)
      .toContain("\\InputIfFileExists{.ghostprov-v2/bootstrap-output.tex}{}{}");
  });

  it("surfaces a sanitized sidecar launch failure instead of a generic hello timeout", async () => {
    const harness = bootstrapHarness("sidecar-log");

    await expect(harness.controller.ensureReady()).rejects.toMatchObject({
      name: "V2BootstrapError",
      kind: "hello",
      message: "V2 sidecar failed before bootstrap hello: Can't exec /code/crixet/deno: No such file or directory",
    });
  });

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
    expect(harness.awareness.clears).toBe(1);
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

  it.each([
    "wrong-nonce",
    "wrong-build",
    "wrong-generation",
    "stale-instance",
    "timeout",
  ] as const)("fails closed after bounded retries on %s", async fault => {
    const harness = bootstrapHarness(fault);

    await expect(harness.controller.ensureReady()).rejects.toBeInstanceOf(V2BootstrapError);
    expect(harness.gateway.renders).toBe(3);
    expect(new Set(harness.gateway.nonces).size).toBe(3);
    expect(harness.gateway.calls.some(call => call.includes("zip"))).toBe(false);
    expect(harness.ydoc.writes.every(write => !write.content.includes(TOKEN))).toBe(true);
    expect(harness.ydoc.files.get(".latexmkrc")).toContain("GhostProv V2 launcher inactive");
  });
});
