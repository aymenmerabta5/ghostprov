import { readFile } from "node:fs/promises";
import type { V2BootstrapGateway, V2BootstrapYDoc } from "../bootstrap/controller";
import type { BootstrapBinding } from "../bootstrap/crypto";
import type { YSweetEnvelope } from "../../prism/protocol";
import type { V2SidecarAssets } from "../sidecar/assets";
import { assertV2SidecarAssets, V2_R10_RELEASE } from "../sidecar/assets";
import { directSupervisorExecBody } from "../../prism/direct-exec";
import { openSupervisorSessionBootstrap } from "./session-bootstrap";

/** This host-side hook is independent of the frozen R10 sidecar identity. */
export const GIT_FSMONITOR_HOOK_SHA256 = "86ce17d9629b428dbf8c197f36513e6ee771b6fcae680e9abe10c43c49cd8652";
let hookSource: Promise<string> | null = null;

async function loadGitFsmonitorHook(): Promise<string> {
  // One immutable, bounded source asset per host process. Normalize checkout
  // line endings before verifying and staging the exact tested Python bytes.
  hookSource ??= (async () => {
    const hook = (await readFile(new URL("./git-fsmonitor-hook.py", import.meta.url), "utf8")).replaceAll("\r\n", "\n");
    if (Buffer.byteLength(hook) > 12 * 1024 || new Bun.CryptoHasher("sha256").update(hook).digest("hex") !== GIT_FSMONITOR_HOOK_SHA256) {
      throw new Error("Git fsmonitor hook identity mismatch");
    }
    return hook;
  })();
  return hookSource;
}

export interface GitFsmonitorBootstrapInput {
  readonly binding: BootstrapBinding;
  readonly ySweet: YSweetEnvelope;
  readonly ydoc: V2BootstrapYDoc;
  readonly gateway: V2BootstrapGateway;
  readonly assets: V2SidecarAssets;
}

export type FsGateway = V2BootstrapGateway & {
  postJson(suffix: string, json: unknown, signal?: AbortSignal): Promise<Response>;
};

export interface GitFsmonitorResult {
  readonly version: 1;
  readonly sessionId: string;
  readonly outcome: "submitted" | "cancelled" | "expired" | "foreign_lock" | "foreign_config" | "restore_identity" | "missing_original" | "bounded_file" | "local_failure";
  readonly configRestored: boolean;
  readonly lockReleased: boolean;
  readonly launchSubmitted: boolean;
}

export function parseGitFsmonitorResult(value: unknown, sessionId: string): GitFsmonitorResult {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid Git hook receipt");
  const raw = value as Record<string, unknown>;
  const outcomes = ["submitted", "cancelled", "expired", "foreign_lock", "foreign_config", "restore_identity", "missing_original", "bounded_file", "local_failure"];
  if (Object.keys(raw).sort().join() !== ["version", "sessionId", "outcome", "configRestored", "lockReleased", "launchSubmitted"].sort().join() ||
      raw.version !== 1 || raw.sessionId !== sessionId || typeof raw.outcome !== "string" || !outcomes.includes(raw.outcome) ||
      typeof raw.configRestored !== "boolean" || typeof raw.lockReleased !== "boolean" || typeof raw.launchSubmitted !== "boolean" ||
      (raw.outcome === "submitted" && (raw.configRestored !== true || raw.lockReleased !== true || raw.launchSubmitted !== true)) ||
      (raw.outcome !== "submitted" && raw.launchSubmitted !== false)) throw new Error("Invalid Git hook receipt");
  return Object.freeze(raw) as unknown as GitFsmonitorResult;
}

export function makeGitFsmonitorPlan(sessionId: string, expiresAtSeconds: number) {
  directSupervisorExecBody({ sessionId }); // Reuse the strict private execution identity contract.
  if (!Number.isSafeInteger(expiresAtSeconds) || expiresAtSeconds < 1) throw new Error("Invalid hook expiry");
  const workspace = `/codex_workspace/${sessionId}`;
  const directory = `${workspace}/.ghostprov-v2/git-bootstrap`;
  const gitConfig = "/home/sandbox/.gitconfig";
  const configLock = `${gitConfig}.lock`;
  const config = `[core]\n\tfsmonitor = \"/usr/bin/python3 ${directory}/hook.py\"\n`;
  const configSha256 = new Bun.CryptoHasher("sha256").update(config).digest("hex");
  return Object.freeze({ workspace, directory, gitConfig, configLock, config,
    plan: Object.freeze({ sessionId, workspace, gitConfig, configLock, configSha256, expiresAtSeconds }) });
}

function gatewayOf(gateway: V2BootstrapGateway): FsGateway {
  if (typeof (gateway as Partial<FsGateway>).postJson !== "function") throw new Error("Filesystem control is unavailable");
  return gateway as FsGateway;
}

async function post(gateway: FsGateway, suffix: string, json: unknown, signal: AbortSignal) {
  const response = await gateway.postJson(suffix, json, signal);
  const status = response.status;
  await response.body?.cancel();
  return status;
}

async function postOk(gateway: FsGateway, suffix: string, json: unknown, signal: AbortSignal) {
  const status = await post(gateway, suffix, json, signal);
  if (status !== 200) throw Object.assign(new Error("Git bootstrap control operation rejected"), { operation: suffix, status });
}

async function put(gateway: FsGateway, absolute: string, content: string, signal: AbortSignal) {
  const result = await gateway.fetchText(`write-file?filename=${encodeURIComponent(`..${absolute}`)}`, {
    method: "POST", body: content, signal,
  });
  if (result.status !== 200) throw Object.assign(new Error("Git bootstrap file staging failed"), { status: result.status });
}

export async function readGitFsmonitorResult(gateway: V2BootstrapGateway, sessionId: string, signal: AbortSignal): Promise<GitFsmonitorResult | null> {
  const result = await gateway.fetchText(`codex/read_codex_workspace_file?path=${encodeURIComponent(".ghostprov-v2/git-bootstrap/result.json")}&session_id=${encodeURIComponent(sessionId)}`, { signal });
  if (result.status === 404) return null;
  if (result.status !== 200 || result.text.length > 8192) throw new Error("Git hook receipt unavailable");
  const wrapper: unknown = JSON.parse(result.text);
  if (!wrapper || typeof wrapper !== "object" || !("body_b64" in wrapper) || typeof wrapper.body_b64 !== "string" || wrapper.body_b64.length > 4096) throw new Error("Git hook receipt exceeded its bound");
  return parseGitFsmonitorResult(JSON.parse(Buffer.from(wrapper.body_b64, "base64").toString("utf8")), sessionId);
}

export class GitFsmonitorBootstrap {
  #attempted = false;
  #gateway: FsGateway | null = null;
  #sessionId: string | null = null;
  #workspace: string | null = null;
  #result: GitFsmonitorResult | null = null;
  #cleanup: Promise<void> | null = null;
  get result() { return this.#result; }
  get sessionId() { return this.#sessionId; }
  get attempted() { return this.#attempted; }

  async start(input: GitFsmonitorBootstrapInput, sessionId: string, signal?: AbortSignal) {
    signal?.throwIfAborted();
    if (this.#attempted) throw new Error("A Git fsmonitor bootstrap is never resubmitted");
    this.#attempted = true;
    directSupervisorExecBody({ sessionId });
    if (input.assets.revision !== "r10" || input.assets.buildHash !== V2_R10_RELEASE.buildHash) throw new Error("Git fsmonitor bootstrap requires the frozen R10 release");
    await assertV2SidecarAssets(input.assets, "r10");
    const prepared = makeGitFsmonitorPlan(sessionId, Math.floor(Date.now() / 1000) + 90);
    const gateway = gatewayOf(input.gateway);
    const bounded = AbortSignal.any([...(signal ? [signal] : []), AbortSignal.timeout(75_000)]);
    const paths = ["exec", ".ghostprov-v2/direct-exec/install.sh", ".ghostprov-v2/supervisor/static.json", ".ghostprov-v2/supervisor/bootstrap.enc.json"] as const;
    const transients = await Promise.all(paths.map(async file => {
      const content = await input.ydoc.readPath(file);
      if (content === null || content.length > 32 * 1024) throw new Error("Bounded supervisor staging material is missing");
      return { path: file, content };
    }));
    await openSupervisorSessionBootstrap({ sessionId, expectedBinding: input.binding, sealed: JSON.parse(transients[3]!.content) });
    const hook = await loadGitFsmonitorHook();
    const entries = [...input.assets.files, input.assets.manifest, ...transients,
      { path: ".ghostprov-v2/git-bootstrap/hook.py", content: hook },
      { path: ".ghostprov-v2/git-bootstrap/plan.json", content: JSON.stringify(prepared.plan) },
      { path: ".ghostprov-v2/git-bootstrap/config-lock/owner", content: `${sessionId}\n` },
      { path: ".ghostprov-v2/git-bootstrap/replacement.gitconfig", content: prepared.config }];
    if (entries.length > 16 || entries.reduce((sum, file) => sum + Buffer.byteLength(file.content), 0) > 1024 * 1024) throw new Error("Git bootstrap staging exceeded its bound");
    const directories = [...new Set(["", ...entries.map(file => file.path.includes("/") ? file.path.slice(0, file.path.lastIndexOf("/")) : "")])].sort((a, b) => a.length - b.length);
    this.#gateway = gateway;
    this.#sessionId = sessionId;
    this.#workspace = prepared.workspace;
    await postOk(gateway, "init", {}, bounded);
    for (const relative of directories) await postOk(gateway, "write-directory", { filename: `..${prepared.workspace}${relative ? `/${relative}` : ""}` }, bounded);
    for (const file of entries) await put(gateway, `${prepared.workspace}/${file.path}`, file.content, bounded);
    // Nonempty directory rename is atomic. It conflicts with a normal Git
    // lock file and with another owned bootstrap's nonempty lock directory.
    try {
      await postOk(gateway, "rename-file", { oldPath: `..${prepared.directory}/config-lock`, newPath: `..${prepared.configLock}` }, bounded);
    } catch (cause) {
      // Neither the global configuration nor the trigger has been touched.
      // Remove only this fresh execution workspace so repeated lock conflicts
      // cannot accumulate staged bundles. Never remove the conflicting lock:
      // an ambiguous rename reply is not proof of its current ownership.
      await postOk(gateway, "delete-file", { filename: `..${prepared.workspace}` }, AbortSignal.timeout(15_000)).catch(() => undefined);
      throw cause;
    }
    const backup = `..${prepared.directory}/original.gitconfig`;
    const moved = await post(gateway, "rename-file", { oldPath: `..${prepared.gitConfig}`, newPath: backup }, bounded);
    if (moved !== 200) {
      if (moved !== 404 && moved !== 500) throw new Error("Original Git configuration could not be preserved");
      // Do not interpret a generic error as absence. A successful mkdir proves
      // there is no regular file at the source, while the backup (if any)
      // remains authoritative to the hook even if the prior reply was wrong.
      await postOk(gateway, "write-directory", { filename: `..${prepared.gitConfig}` }, bounded);
      await postOk(gateway, "rename-file", { oldPath: `..${prepared.gitConfig}`, newPath: `..${prepared.directory}/config-was-absent` }, bounded);
    }
    await postOk(gateway, "rename-file", { oldPath: `..${prepared.directory}/replacement.gitconfig`, newPath: `..${prepared.gitConfig}` }, bounded);
    const envelope = input.ySweet;
    await postOk(gateway, "token", { url: envelope.url, baseUrl: envelope.baseUrl, docId: envelope.docId,
      token: envelope.token.reveal(), authorization: envelope.authorization }, bounded);
    await gateway.syncWorkspace(bounded);
    for (let count = 0; count < 40; count++) {
      bounded.throwIfAborted();
      const receipt = await readGitFsmonitorResult(gateway, sessionId, bounded);
      if (receipt !== null) {
        this.#result = receipt;
        if (receipt.outcome !== "submitted") throw Object.assign(new Error("Git fsmonitor hook rejected startup"), { code: receipt.outcome });
        return Object.freeze({ sessionId, workspaceRoot: prepared.workspace, jobId: `git-owned-${sessionId}` });
      }
      await new Promise<void>((resolve, reject) => {
        const abort = () => { clearTimeout(timer); reject(bounded.reason); };
        const timer = setTimeout(() => { bounded.removeEventListener("abort", abort); resolve(); }, 250);
        bounded.addEventListener("abort", abort, { once: true });
      });
    }
    throw new Error("Git fsmonitor hook did not supply its bounded receipt");
  }

  /** Only the authenticated supervisor may authorize deleting its workspace. */
  async cleanupAfterSignedShutdown(signedShutdown: boolean, signal: AbortSignal) {
    if (!signedShutdown || this.#result?.outcome !== "submitted" || !this.#gateway || !this.#workspace) throw new Error("Remote cleanup requires signed shutdown and a restored-config receipt");
    const gateway = this.#gateway;
    const workspace = this.#workspace;
    this.#cleanup ??= (async () => {
      await postOk(gateway, "init", {}, signal);
      await postOk(gateway, "delete-file", { filename: `..${workspace}` }, signal);
    })();
    return this.#cleanup;
  }
}
