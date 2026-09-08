import { readFile } from "node:fs/promises";
import type { V2BootstrapGateway, V2BootstrapYDoc } from "../bootstrap/controller";
import type { BootstrapBinding } from "../bootstrap/crypto";
import type { YSweetEnvelope } from "../../prism/protocol";
import { assertV2SidecarAssets, V2_R10_RELEASE, type V2SidecarAssets } from "../sidecar/assets";
import { directSupervisorExecBody } from "../../prism/direct-exec";
import { openSupervisorSessionBootstrap } from "./session-bootstrap";

/** Host bootstrap assets; independent of the frozen R10 sidecar bundle. */
export const GIT_SSH_SIGNING_HOOK_SHA256 = "55c887e041f792f881ab2e6c14142e1da218a20cc45d1265188070cddc2d2a22";
export const GIT_SSH_SIGNER_SHA256 = "492d2fd30686527cef50e65e864ccfc22e299f45ffdbf8e1e8d2c669b5547eff";
const PREFIX = ".ghostprov-v2/ssh-bootstrap";
const OUTCOMES = ["submitted", "cancelled", "expired", "foreign_lock", "foreign_config", "restore_identity",
  "missing_original", "bounded_file", "identity_mismatch", "invalid_arguments", "duplicate", "signing_failed",
  "local_failure", "interrupted", "ambiguous"] as const;
const sha256 = (value: string) => new Bun.CryptoHasher("sha256").update(value).digest("hex");
let sources: Promise<{ readonly hook: string; readonly signer: string }> | null = null;

export function loadGitSshSigningSources() {
  sources ??= (async () => {
    const [hook, signer] = await Promise.all(["./git-ssh-signing-hook.py", "./git-sshsig.mjs"].map(async file =>
      (await readFile(new URL(file, import.meta.url), "utf8")).replaceAll("\r\n", "\n")));
    if (Buffer.byteLength(hook!) > 65536 || Buffer.byteLength(signer!) > 65536 || sha256(hook!) !== GIT_SSH_SIGNING_HOOK_SHA256 ||
        sha256(signer!) !== GIT_SSH_SIGNER_SHA256) throw new Error("Git SSH signing asset identity mismatch");
    return Object.freeze({ hook: hook!, signer: signer! });
  })();
  return sources;
}

export interface GitSshSigningInput {
  readonly binding: BootstrapBinding;
  readonly ySweet: YSweetEnvelope;
  readonly ydoc: V2BootstrapYDoc;
  readonly gateway: V2BootstrapGateway;
  readonly assets: V2SidecarAssets;
}

type FsGateway = V2BootstrapGateway & {
  postJson(operation: string, body: unknown, signal?: AbortSignal): Promise<Response>;
};

export interface GitSshSigningResult {
  readonly version: 1;
  readonly sessionId: string;
  readonly outcome: typeof OUTCOMES[number];
  readonly configRestored: boolean;
  readonly lockReleased: boolean;
  readonly signingComplete: boolean;
  readonly launchSubmitted: boolean;
  readonly keysRemoved: boolean;
}

export function makeGitSshSigningPlan(sessionId: string, launcher: string, expiresAtUnixMs: number) {
  directSupervisorExecBody({ sessionId });
  if (Buffer.byteLength(launcher) > 65536 || !launcher.startsWith("#!/bin/sh\n") ||
      !Number.isSafeInteger(expiresAtUnixMs) || expiresAtUnixMs < 1) throw new Error("Invalid bounded SSH launch plan");
  const workspace = `/codex_workspace/${sessionId}`;
  const directory = `${workspace}/${PREFIX}`;
  const gitConfig = "/home/sandbox/.gitconfig";
  const configLock = `${gitConfig}.lock`;
  const config = `[commit]\n\tgpgSign = true\n[gpg]\n\tformat = ssh\n[gpg "ssh"]\n` +
    `\tprogram = ${directory}/hook.py\n\tdefaultKeyCommand = "/usr/bin/python3 ${directory}/hook.py --prepare"\n`;
  return Object.freeze({ workspace, directory, gitConfig, configLock, config, plan: Object.freeze({
    version: 1, sessionId, workspace, gitDirectory: "/workspace", gitConfig, configLock,
    configSha256: sha256(config), hookSha256: GIT_SSH_SIGNING_HOOK_SHA256, launchSha256: sha256(launcher),
    gitTempDirectory: "/tmp", expiresAtUnixMs, denoPath: "/usr/local/bin/deno", signerSha256: GIT_SSH_SIGNER_SHA256,
  }) });
}

export function parseGitSshSigningResult(value: unknown, sessionId: string): GitSshSigningResult {
  directSupervisorExecBody({ sessionId });
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid Git SSH signing receipt");
  const raw = value as Record<string, unknown>;
  const fields = ["version", "sessionId", "outcome", "configRestored", "lockReleased", "signingComplete", "launchSubmitted", "keysRemoved"];
  const flags = fields.slice(3);
  if (Object.keys(raw).sort().join() !== fields.sort().join() || raw.version !== 1 || raw.sessionId !== sessionId ||
      typeof raw.outcome !== "string" || !OUTCOMES.some(outcome => outcome === raw.outcome) ||
      flags.some(field => typeof raw[field] !== "boolean") ||
      (raw.outcome === "submitted" && flags.some(field => raw[field] !== true)) ||
      (raw.outcome !== "submitted" && raw.outcome !== "ambiguous" && raw.launchSubmitted !== false)) {
    throw new Error("Invalid Git SSH signing receipt");
  }
  return Object.freeze(raw) as unknown as GitSshSigningResult;
}

export async function readGitSshSigningResult(gateway: V2BootstrapGateway, sessionId: string, signal: AbortSignal): Promise<GitSshSigningResult | null> {
  directSupervisorExecBody({ sessionId });
  const response = await gateway.fetchText(`codex/read_codex_workspace_file?path=${encodeURIComponent(`${PREFIX}/result.json`)}&session_id=${sessionId}`, { signal });
  if (response.status === 404) return null;
  if (response.status !== 200 || Buffer.byteLength(response.text) > 16 * 1024) throw new Error("Bounded Git SSH receipt unavailable");
  const wrapper: unknown = JSON.parse(response.text);
  if (!wrapper || typeof wrapper !== "object" || !("body_b64" in wrapper) || typeof wrapper.body_b64 !== "string" ||
      wrapper.body_b64.length > 8192 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(wrapper.body_b64)) {
    throw new Error("Invalid Git SSH evidence wrapper");
  }
  const decoded = Buffer.from(wrapper.body_b64, "base64");
  if (decoded.toString("base64") !== wrapper.body_b64) throw new Error("Noncanonical Git SSH evidence encoding");
  return parseGitSshSigningResult(JSON.parse(decoded.toString("utf8")), sessionId);
}

async function post(gateway: FsGateway, operation: string, body: unknown, signal: AbortSignal): Promise<number> {
  const response = await gateway.postJson(operation, body, signal);
  await response.body?.cancel();
  return response.status;
}

async function postOk(gateway: FsGateway, operation: string, body: unknown, signal: AbortSignal): Promise<void> {
  const status = await post(gateway, operation, body, signal);
  if (status !== 200) throw Object.assign(new Error("Git SSH filesystem operation rejected"), { operation, status });
}

async function put(gateway: FsGateway, absolute: string, content: string, signal: AbortSignal): Promise<void> {
  const response = await gateway.fetchText(`write-file?filename=${encodeURIComponent(`..${absolute}`)}`, { method: "POST", body: content, signal });
  if (response.status !== 200) throw Object.assign(new Error("Git SSH file staging rejected"), { status: response.status });
}

function pause(signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); signal.removeEventListener("abort", abort); reject(signal.reason); };
    const timer = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, 250);
    signal.addEventListener("abort", abort, { once: true });
  });
}

/** A single attempt, tied to the supervisor's sealed private execution session. */
export class GitSshSigningBootstrap {
  #attempted = false;
  #gateway: FsGateway | null = null;
  #sessionId: string | null = null;
  #workspace: string | null = null;
  #result: GitSshSigningResult | null = null;
  #cleanup: Promise<void> | null = null;
  get attempted() { return this.#attempted; }
  get sessionId() { return this.#sessionId; }
  get result() { return this.#result; }

  async start(input: GitSshSigningInput, sessionId: string, signal?: AbortSignal) {
    signal?.throwIfAborted();
    if (this.#attempted) throw new Error("A Git SSH signing bootstrap is never resubmitted");
    this.#attempted = true;
    directSupervisorExecBody({ sessionId });
    if (input.assets.revision !== "r10" || input.assets.buildHash !== V2_R10_RELEASE.buildHash) throw new Error("Git SSH signing requires the frozen R10 release");
    await assertV2SidecarAssets(input.assets, "r10");
    const paths = ["exec", ".ghostprov-v2/direct-exec/install.sh", ".ghostprov-v2/supervisor/static.json", ".ghostprov-v2/supervisor/bootstrap.enc.json"] as const;
    const transients = await Promise.all(paths.map(async file => {
      const content = await input.ydoc.readPath(file);
      if (content === null || Buffer.byteLength(content) > 32 * 1024) throw new Error("Bounded supervisor staging material is missing");
      return { path: file, content };
    }));
    await openSupervisorSessionBootstrap({ sessionId, expectedBinding: input.binding, sealed: JSON.parse(transients[3]!.content) });
    const { hook, signer } = await loadGitSshSigningSources();
    const prepared = makeGitSshSigningPlan(sessionId, transients[0]!.content, Date.now() + 90_000);
    const entries = [...input.assets.files, input.assets.manifest, ...transients,
      { path: `${PREFIX}/hook.py`, content: hook }, { path: `${PREFIX}/sshsig.mjs`, content: signer },
      { path: `${PREFIX}/plan.json`, content: JSON.stringify(prepared.plan) },
      { path: `${PREFIX}/config-lock/owner`, content: `${sessionId}\n` },
      { path: `${PREFIX}/replacement.gitconfig`, content: prepared.config }];
    if (entries.length > 16 || new Set(entries.map(file => file.path)).size !== entries.length ||
        entries.some(file => file.path.length > 1024 || file.path.trim() !== file.path ||
          !/^(?:exec|\.ghostprov-v2\/(?:[A-Za-z0-9._-]+\/)*[A-Za-z0-9._-]+)$/.test(file.path) ||
          file.path.split("/").some(part => part === "." || part === "..")) ||
        entries.reduce((sum, file) => sum + Buffer.byteLength(file.content), 0) > 1024 * 1024) throw new Error("Git SSH staging exceeded its bound");
    if (typeof (input.gateway as Partial<FsGateway>).postJson !== "function") throw new Error("Git SSH filesystem control unavailable");
    const gateway = input.gateway as FsGateway;
    this.#gateway = gateway;
    this.#sessionId = sessionId;
    this.#workspace = prepared.workspace;
    const bounded = AbortSignal.any([...(signal ? [signal] : []), AbortSignal.timeout(75_000)]);
    const directories = [...new Set(["", ...entries.map(file => file.path.includes("/") ? file.path.slice(0, file.path.lastIndexOf("/")) : "")])];
    await postOk(gateway, "init", {}, bounded);
    for (const relative of directories.sort((a, b) => a.length - b.length)) {
      await postOk(gateway, "write-directory", { filename: `..${prepared.workspace}${relative ? `/${relative}` : ""}` }, bounded);
    }
    for (const file of entries) await put(gateway, `${prepared.workspace}/${file.path}`, file.content, bounded);
    try {
      await postOk(gateway, "rename-file", { oldPath: `..${prepared.directory}/config-lock`, newPath: `..${prepared.configLock}` }, bounded);
    } catch (cause) {
      // The configuration and Git trigger are untouched. Retire only the
      // fresh staged workspace; an ambiguous lock rename never permits
      // deleting or overwriting the global configuration lock.
      await postOk(gateway, "delete-file", { filename: `..${prepared.workspace}` }, AbortSignal.timeout(15_000)).catch(() => undefined);
      throw cause;
    }
    const moved = await post(gateway, "rename-file", { oldPath: `..${prepared.gitConfig}`, newPath: `..${prepared.directory}/original.gitconfig` }, bounded);
    if (moved !== 200) {
      if (moved !== 404 && moved !== 500) throw new Error("Original Git configuration preservation rejected");
      // A failed rename alone is not absence proof. mkdir at the exact old
      // name must succeed before an absence marker may be installed.
      await postOk(gateway, "write-directory", { filename: `..${prepared.gitConfig}` }, bounded);
      await postOk(gateway, "rename-file", { oldPath: `..${prepared.gitConfig}`, newPath: `..${prepared.directory}/config-was-absent` }, bounded);
    }
    await postOk(gateway, "rename-file", { oldPath: `..${prepared.directory}/replacement.gitconfig`, newPath: `..${prepared.gitConfig}` }, bounded);
    const envelope = input.ySweet;
    await postOk(gateway, "token", { url: envelope.url, baseUrl: envelope.baseUrl, docId: envelope.docId,
      token: envelope.token.reveal(), authorization: envelope.authorization }, bounded);
    await gateway.syncWorkspace(bounded);
    for (let count = 0; count < 60; count++) {
      bounded.throwIfAborted();
      const result = await readGitSshSigningResult(gateway, sessionId, bounded);
      if (result !== null) {
        this.#result = result;
        if (result.outcome !== "submitted") throw Object.assign(new Error("Git SSH signing rejected startup"), { code: result.outcome });
        return Object.freeze({ sessionId, workspaceRoot: prepared.workspace, jobId: `git-ssh-signing-${sessionId}` });
      }
      await pause(bounded);
    }
    throw new Error("Git SSH signing receipt missing; execution is not retryable");
  }

  /** Never reset the sandbox filesystem while a child's cleanup is ambiguous. */
  cleanupAfterSignedShutdown(signedShutdown: boolean, signal: AbortSignal): Promise<void> {
    if (!signedShutdown || !this.#gateway || !this.#sessionId || !this.#workspace || this.#result?.outcome !== "submitted") {
      return Promise.reject(new Error("Git SSH workspace cleanup requires signed shutdown and a restored-config receipt"));
    }
    if (this.#cleanup) return this.#cleanup;
    const gateway = this.#gateway;
    const sessionId = this.#sessionId;
    const workspace = this.#workspace;
    this.#cleanup = (async () => {
      if ((await readGitSshSigningResult(gateway, sessionId, signal))?.outcome !== "submitted") throw new Error("Git SSH cleanup authority changed");
      await postOk(gateway, "init", {}, signal);
      await put(gateway, `${workspace}/${PREFIX}/cancelled`, "cancelled\n", signal);
      await postOk(gateway, "delete-file", { filename: `..${workspace}` }, signal);
      if (await readGitSshSigningResult(gateway, sessionId, signal) !== null) throw new Error("Git SSH workspace deletion was not verified");
    })();
    return this.#cleanup;
  }
}
