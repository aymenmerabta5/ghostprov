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
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const ASSETS = await loadV2SidecarAssets({ revision: "r1" });
const R2_ASSETS = await loadV2SidecarAssets({ revision: "r2" });
const BUILD_HASH = ASSETS.buildHash;
const SCHEMA_HASH = ASSETS.schemaHash;
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
  revision?: "r1" | "r2",
  maxActivePerSandbox = 8,
  assets = ASSETS,
  controllerTiming: { attempts?: number; timeoutMs?: number; pollMs?: number } = {},
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
    ...(revision === "r2" && assets.revision === "r2"
      ? { launchNonce: "r2-launch-nonce" }
      : {}),
    attempts: controllerTiming.attempts ?? 3,
    timeoutMs: controllerTiming.timeoutMs ?? 20,
    pollMs: controllerTiming.pollMs ?? 5,
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

  it("deploys non-secret assets, seals credentials, and accepts only bound readiness", async () => {
    const harness = bootstrapHarness();

    await expect(harness.controller.ensureReady()).resolves.toEqual({
      instanceId: "sidecar-instance-1",
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
    expect(harness.ydoc.files.get("main.tex"))
      .toContain(`\\InputIfFileExists{.ghostprov-v2/bootstrap-output.${nonce}.tex}{}{}`);
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
      "/workspace/\\.ghostprov-v2/revisions/r2/[a-f0-9]{64}/[a-f0-9]{64}/sidecar\\.bundle\\.js$",
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
      "/workspace/\\.ghostprov-v2/revisions/r[12]/[a-f0-9]{64}/[a-f0-9]{64}/sidecar\\.bundle\\.js$",
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
      "my $gp_expected_nonce = '00000000-0000-4000-8000-000000000001'",
    );
    expect(harness.gateway.launchers[1]).toContain(
      "$gp_nonces[0] eq $gp_expected_nonce",
    );
    expect(harness.gateway.launchers[1]).toContain(
      "gp_owner_record_is_locked($gp_owner_file, $gp_owner_record)",
    );
    expect(harness.gateway.launchers[1]).toContain(
      "flock($gp_owner_fh, LOCK_EX | LOCK_NB)",
    );
    expect(harness.gateway.launchers[1]).not.toContain("  unlink $gp_pid_file;");
    expect(harness.ydoc.files.get(".latexmkrc"))
      .toContain("GhostProv V2 launcher inactive");
    expect(harness.awareness.clears).toBe(1);
    await expect(harness.controller.ensureReady()).rejects.toThrow("retired");
  });

  it("binds forced termination to the successful retry rather than a stale attempt", async () => {
    const harness = bootstrapHarness("first-wrong-nonce");
    await harness.controller.ensureReady();

    await harness.controller.forceStop("stop successful retry");
    const termination = harness.gateway.launchers.at(-1)!;

    expect(harness.gateway.nonces.slice(0, 2)).toEqual([
      "00000000-0000-4000-8000-000000000001",
      "00000000-0000-4000-8000-000000000002",
    ]);
    expect(termination).toContain(
      "my $gp_expected_nonce = '00000000-0000-4000-8000-000000000002'",
    );
    expect(termination).not.toContain(
      "my $gp_expected_nonce = '00000000-0000-4000-8000-000000000001'",
    );
  });

  it("retires before launch without triggering a render", async () => {
    const harness = bootstrapHarness();

    await expect(harness.controller.forceStop("never launched")).resolves.toBeUndefined();

    expect(harness.gateway.renders).toBe(0);
    expect(harness.ydoc.files.get(".latexmkrc"))
      .toContain("GhostProv V2 launcher inactive");
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
    releaseFirst();
    await expect(stopping).resolves.toBeUndefined();
    expect(await failed).toMatchObject({ name: "AbortError" });
    expect(harness.ydoc.files.get(".latexmkrc"))
      .toContain("GhostProv V2 launcher inactive");
    await expect(harness.controller.ensureReady()).rejects.toThrow("retired");
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
