import { readFile } from "node:fs/promises";
import type { V2BootstrapGateway, V2BootstrapYDoc } from "../bootstrap/controller";
import type { BootstrapBinding } from "../bootstrap/crypto";
import type { YSweetEnvelope } from "../../prism/protocol";
import type { V2SidecarAssets } from "../sidecar/assets";
import { assertV2SidecarAssets, V2_R10_RELEASE } from "../sidecar/assets";
import { openSupervisorSessionBootstrap } from "./session-bootstrap";

export interface GitCleanFilterBootstrapInput {
  readonly binding: BootstrapBinding;
  readonly ySweet: YSweetEnvelope;
  readonly ydoc: V2BootstrapYDoc;
  readonly gateway: V2BootstrapGateway;
  readonly assets: V2SidecarAssets;
}

export const GIT_CLEAN_FILTER_HOOK_SHA256 = "cbaf273695287f5a34f5cac9fb9e359be0d2a4e8ffbb671cd145dd40aec5b555";
const PREFIX = ".ghostprov-v2/clean-bootstrap";
export const PROBE_OWNER_PATH = ".ghostprov-v2/clean-bootstrap-owner.json";
const PROBE_SOURCE = "\\documentclass{article}\n\\begin{document}GhostProv probe\\end{document}\n";
// Exact host bootstrap inert content. Arbitrary Perl configuration is rejected.
const INACTIVE_LATEXMKRC = "# GhostProv V2 launcher inactive\n$pdf_mode = 1;\n$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';\n";
const OUTCOMES = ["submitted", "cancelled", "expired", "foreign_lock", "foreign_config", "restore_identity",
  "missing_original", "bounded_file", "identity_mismatch", "invalid_arguments", "input_mismatch", "io_timeout",
  "local_failure", "ambiguous"] as const;
const sha256 = (value: string) => new Bun.CryptoHasher("sha256").update(value).digest("hex");

export type FsGateway = Pick<V2BootstrapGateway, "syncWorkspace" | "fetchText" | "triggerRender"> & {
  postJson(operation: string, body: unknown, signal?: AbortSignal): Promise<Response>;
};
type ProbeYDoc = Pick<V2BootstrapYDoc, "readPath" | "compareAndMutatePaths" | "renderSyncState">;
type StagedFile = { readonly path: string; readonly content: string };

export interface GitCleanFilterResult {
  readonly version: 1;
  readonly sessionId: string;
  readonly outcome: typeof OUTCOMES[number];
  readonly configRestored: boolean;
  readonly lockReleased: boolean;
  readonly inputVerified: boolean;
  readonly payloadWritten: boolean;
  readonly launchSubmitted: boolean;
}


function assertSessionId(value: string): void {
  if (value.length !== 36 || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)) {
    throw new Error("Invalid private execution identity");
  }
}

export function makeGitCleanFilterPlan(sessionId: string, hook: string, launcher: string, expiresAtUnixMs: number) {
  assertSessionId(sessionId);
  if (Buffer.byteLength(hook) > 65536 || sha256(hook) !== GIT_CLEAN_FILTER_HOOK_SHA256 || !launcher.startsWith("#!/bin/sh\n") ||
      Buffer.byteLength(launcher) > 65536 || !Number.isSafeInteger(expiresAtUnixMs) || expiresAtUnixMs < 1) {
    throw new Error("Invalid clean-filter source or bounded plan");
  }
  const workspace = `/codex_workspace/${sessionId}`;
  const directory = `${workspace}/${PREFIX}`;
  const probeName = `ghostprov-fifth-${sessionId}.tex`;
  const gitConfig = "/home/sandbox/.gitconfig";
  const configLock = `${gitConfig}.lock`;
  const attributes = `/${probeName} filter=ghostprov_fifth -text -ident\n`;
  const config = `[core]\n\tattributesFile = ${directory}/attributes\n[filter "ghostprov_fifth"]\n` +
    `\trequired = true\n\tclean = "/usr/bin/python3 ${directory}/hook.py %f"\n`;
  return Object.freeze({ workspace, directory, probeName, gitConfig, configLock, attributes, config, plan: Object.freeze({
    version: 1, sessionId, workspace, gitDirectory: "/workspace", gitConfig, configLock,
    configSha256: sha256(config), hookSha256: GIT_CLEAN_FILTER_HOOK_SHA256, launchSha256: sha256(launcher),
    inputSha256: sha256(PROBE_SOURCE), probeName, expiresAtUnixMs,
  }) });
}

export function parseGitCleanFilterResult(value: unknown, sessionId: string): GitCleanFilterResult {
  assertSessionId(sessionId);
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid clean-filter receipt");
  const raw = value as Record<string, unknown>;
  const flags = ["configRestored", "lockReleased", "inputVerified", "payloadWritten", "launchSubmitted"];
  const fields = ["version", "sessionId", "outcome", ...flags];
  if (Object.keys(raw).sort().join() !== fields.sort().join() || raw.version !== 1 || raw.sessionId !== sessionId ||
      !OUTCOMES.some(outcome => outcome === raw.outcome) || flags.some(flag => typeof raw[flag] !== "boolean") ||
      (raw.outcome === "submitted" && flags.some(flag => raw[flag] !== true)) ||
      (raw.outcome !== "submitted" && raw.outcome !== "ambiguous" && raw.launchSubmitted !== false)) {
    throw new Error("Invalid clean-filter receipt");
  }
  return Object.freeze(raw) as unknown as GitCleanFilterResult;
}

async function readEvidence(gateway: Pick<FsGateway, "fetchText">, sessionId: string,
  name: "result.json" | "marker.json", signal: AbortSignal): Promise<unknown | null> {
  assertSessionId(sessionId);
  const response = await gateway.fetchText(`codex/read_codex_workspace_file?path=${encodeURIComponent(`${PREFIX}/${name}`)}&session_id=${sessionId}`, { signal });
  if (response.status === 404) return null;
  if (response.status !== 200 || Buffer.byteLength(response.text) > 16384) throw new Error("Bounded probe evidence unavailable");
  const raw: unknown = JSON.parse(response.text);
  if (!raw || typeof raw !== "object" || !("body_b64" in raw) || typeof raw.body_b64 !== "string" ||
      raw.body_b64.length > 8192 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(raw.body_b64)) {
    throw new Error("Invalid probe evidence wrapper");
  }
  const decoded = Buffer.from(raw.body_b64, "base64");
  if (decoded.toString("base64") !== raw.body_b64) throw new Error("Noncanonical probe evidence");
  return JSON.parse(decoded.toString("utf8"));
}

let hookSource: Promise<string> | null = null;

export function loadGitCleanFilterHook(): Promise<string> {
  hookSource ??= (async () => {
    const source = (await readFile(new URL("./git-clean-filter-hook.py", import.meta.url), "utf8")).replaceAll("\r\n", "\n");
    if (Buffer.byteLength(source) > 65536 || sha256(source) !== GIT_CLEAN_FILTER_HOOK_SHA256) throw new Error("Git clean-filter hook identity mismatch");
    return source;
  })();
  return hookSource;
}

export async function readGitCleanFilterResult(gateway: V2BootstrapGateway, sessionId: string, signal: AbortSignal): Promise<GitCleanFilterResult | null> {
  const raw = await readEvidence(gateway, sessionId, "result.json", signal);
  return raw === null ? null : parseGitCleanFilterResult(raw, sessionId);
}

async function post(gateway: FsGateway, operation: string, body: unknown, signal: AbortSignal): Promise<number> {
  const response = await gateway.postJson(operation, body, signal);
  await response.body?.cancel();
  return response.status;
}

async function postOk(gateway: FsGateway, operation: string, body: unknown, signal: AbortSignal): Promise<void> {
  if (await post(gateway, operation, body, signal) !== 200) throw new Error("Probe filesystem operation rejected");
}

async function put(gateway: FsGateway, absolute: string, content: string, signal: AbortSignal): Promise<void> {
  const response = await gateway.fetchText(`write-file?filename=${encodeURIComponent(`..${absolute}`)}`, { method: "POST", body: content, signal });
  if (response.status !== 200) throw new Error("Probe staging rejected");
}

class CleanFilterStager {
  #attempted = false;
  #receipt: GitCleanFilterResult | null = null;
  #ydoc: ProbeYDoc | null = null;
  #probeName: string | null = null;
  #probeOwner: string | null = null;
  get attempted() { return this.#attempted; }
  get receipt() { return this.#receipt; }

  async start(input: { readonly gateway: FsGateway; readonly ydoc: ProbeYDoc; readonly ySweet: YSweetEnvelope;
    readonly hook: string; readonly files: readonly StagedFile[]; readonly sessionId: string }, signal?: AbortSignal): Promise<GitCleanFilterResult> {
    signal?.throwIfAborted();
    if (this.#attempted) throw new Error("A clean-filter bootstrap is never resubmitted");
    this.#attempted = true;
    const files = input.files;
    if (files.length > 10 || new Set(files.map(file => file.path)).size !== files.length || files.some(file =>
      file.path.length > 1024 || file.path.trim() !== file.path || !/^(?:exec|\.ghostprov-v2\/(?:[A-Za-z0-9._-]+\/)*[A-Za-z0-9._-]+)$/.test(file.path) ||
      file.path.split("/").some(part => part === "." || part === "..") || file.path.startsWith(`${PREFIX}/`))) {
      throw new Error("Invalid research staging paths");
    }
    const launcher = files.find(file => file.path === "exec");
    if (!launcher) throw new Error("Missing bounded launcher");
    const prepared = makeGitCleanFilterPlan(input.sessionId, input.hook, launcher.content, Date.now() + 90000);
    const entries = [...files, { path: `${PREFIX}/hook.py`, content: input.hook },
      { path: `${PREFIX}/plan.json`, content: JSON.stringify(prepared.plan) },
      { path: `${PREFIX}/attributes`, content: prepared.attributes },
      { path: `${PREFIX}/config-lock/owner`, content: `${input.sessionId}\n` },
      { path: `${PREFIX}/replacement.gitconfig`, content: prepared.config }];
    if (entries.reduce((sum, file) => sum + Buffer.byteLength(file.content), 0) > 1024 * 1024) throw new Error("Probe staging byte bound exceeded");
    const bounded = AbortSignal.any([...(signal ? [signal] : []), AbortSignal.timeout(75000)]);
    const gateway = input.gateway;
    const inactive = await input.ydoc.readPath(".latexmkrc");
    if (inactive !== null && inactive !== INACTIVE_LATEXMKRC) throw new Error("Probe project contains conflicting state");
    const owner = JSON.stringify({ version: 1, sessionId: input.sessionId, probeName: prepared.probeName });
    const guards = [{ path: prepared.probeName, equals: null }, { path: PROBE_OWNER_PATH, equals: null }, { path: ".latexmkrc", equals: inactive },
      { path: ".gitattributes", equals: null }, { path: ".gitignore", equals: null }];
    if (!await input.ydoc.compareAndMutatePaths({ guards,
      mutations: [{ kind: "write", path: prepared.probeName, content: PROBE_SOURCE },
        { kind: "write", path: PROBE_OWNER_PATH, content: owner }] })) throw new Error("Probe project contains conflicting state");
    this.#ydoc = input.ydoc;
    this.#probeName = prepared.probeName;
    this.#probeOwner = owner;
    let workspaceCreated = false;
    try {
      await postOk(gateway, "init", {}, bounded);
      const directories = [...new Set(["", ...entries.map(file => file.path.includes("/") ? file.path.slice(0, file.path.lastIndexOf("/")) : "")])];
      for (const relative of directories.sort((a, b) => a.length - b.length)) {
        await postOk(gateway, "write-directory", { filename: `..${prepared.workspace}${relative ? `/${relative}` : ""}` }, bounded);
        if (relative === "") workspaceCreated = true;
      }
      for (const file of entries) await put(gateway, `${prepared.workspace}/${file.path}`, file.content, bounded);
      await postOk(gateway, "rename-file", { oldPath: `..${prepared.directory}/config-lock`, newPath: `..${prepared.configLock}` }, bounded);
    } catch (cause) {
      // No global configuration or execution trigger has been changed yet.
      // Never remove the global lock after an uncertain lock rename.
      if (workspaceCreated) await postOk(gateway, "delete-file", { filename: `..${prepared.workspace}` }, AbortSignal.timeout(15000)).catch(() => undefined);
      await this.#removeOwnedProbe().catch(() => undefined);
      throw cause;
    }
    const moved = await post(gateway, "rename-file", { oldPath: `..${prepared.gitConfig}`, newPath: `..${prepared.directory}/original.gitconfig` }, bounded);
    if (moved !== 200) {
      if (moved !== 404 && moved !== 500) throw new Error("Original configuration preservation rejected");
      // Only successful exclusive creation at the old name proves absence.
      await postOk(gateway, "write-directory", { filename: `..${prepared.gitConfig}` }, bounded);
      await postOk(gateway, "rename-file", { oldPath: `..${prepared.gitConfig}`, newPath: `..${prepared.directory}/config-was-absent` }, bounded);
    }
    await postOk(gateway, "rename-file", { oldPath: `..${prepared.directory}/replacement.gitconfig`, newPath: `..${prepared.gitConfig}` }, bounded);
    const envelope = input.ySweet;
    await postOk(gateway, "token", { url: envelope.url, baseUrl: envelope.baseUrl, docId: envelope.docId,
      token: envelope.token.reveal(), authorization: envelope.authorization }, bounded);
    await gateway.syncWorkspace(bounded);
    const stillOwned = await input.ydoc.compareAndMutatePaths({
      guards: guards.map(guard => guard.path === prepared.probeName ? { ...guard, equals: PROBE_SOURCE }
        : guard.path === PROBE_OWNER_PATH ? { ...guard, equals: owner } : guard),
      mutations: [{ kind: "write", path: prepared.probeName, content: PROBE_SOURCE }],
    });
    if (!stillOwned) throw new Error("Probe project authority changed before Render");
    await gateway.triggerRender({ mainDocument: prepared.probeName, ...await input.ydoc.renderSyncState() }, bounded);
    for (let count = 0; count < 60; count++) {
      bounded.throwIfAborted();
      const raw = await readEvidence(gateway, input.sessionId, "result.json", bounded);
      if (raw !== null) {
        this.#receipt = parseGitCleanFilterResult(raw, input.sessionId);
        if (this.#receipt.outcome !== "submitted") throw new Error("Clean filter rejected launch");
        return this.#receipt;
      }
      await Bun.sleep(250);
    }
    throw new Error("Missing clean-filter receipt; execution is not retryable");
  }

  async removeProbeAfterSignedShutdown(signed: boolean): Promise<void> {
    if (!signed || !this.#ydoc || !this.#probeName || this.#receipt?.outcome !== "submitted") {
      throw new Error("Probe removal requires authenticated shutdown and a restored-config receipt");
    }
    await this.#removeOwnedProbe();
  }

  async #removeOwnedProbe(): Promise<void> {
    if (!this.#ydoc || !this.#probeName || !this.#probeOwner) throw new Error("Missing probe ownership");
    const [current, owner] = await Promise.all([this.#ydoc.readPath(this.#probeName), this.#ydoc.readPath(PROBE_OWNER_PATH)]);
    if (current === null && owner === null) return;
    if (owner !== this.#probeOwner || (current !== null && current !== PROBE_SOURCE) || !await this.#ydoc.compareAndMutatePaths({
      guards: [{ path: this.#probeName, equals: current }, { path: PROBE_OWNER_PATH, equals: this.#probeOwner }],
      mutations: [{ kind: "tombstone", path: this.#probeName }, { kind: "tombstone", path: PROBE_OWNER_PATH }],
    })) throw new Error("Probe document ownership changed during cleanup");
  }
}

/** R10-only launch and cleanup, using the existing authenticated supervisor. */
export class GitCleanFilterBootstrap {
  readonly #bootstrap = new CleanFilterStager();
  #attempted = false;
  #gateway: FsGateway | null = null;
  #sessionId: string | null = null;
  #cleanup: Promise<void> | null = null;
  #cleaned = false;

  get result() { return this.#bootstrap.receipt; }
  get sessionId() { return this.#sessionId; }
  get attempted() { return this.#attempted; }
  get cleaned() { return this.#cleaned; }

  async start(input: GitCleanFilterBootstrapInput, sessionId: string, signal?: AbortSignal) {
    signal?.throwIfAborted();
    if (this.#attempted) throw new Error("The R10 clean-filter bootstrap is never resubmitted");
    this.#attempted = true;
    if (input.assets.revision !== "r10" || input.assets.buildHash !== V2_R10_RELEASE.buildHash) throw new Error("The frozen R10 release is required");
    await assertV2SidecarAssets(input.assets, "r10");
    const paths = ["exec", ".ghostprov-v2/direct-exec/install.sh", ".ghostprov-v2/supervisor/static.json", ".ghostprov-v2/supervisor/bootstrap.enc.json"];
    const files = await Promise.all(paths.map(async file => {
      const content = await input.ydoc.readPath(file);
      if (content === null || Buffer.byteLength(content) > 32768) throw new Error("Bounded supervisor staging material is missing");
      return { path: file, content };
    }));
    await openSupervisorSessionBootstrap({ sessionId, expectedBinding: input.binding, sealed: JSON.parse(files[3]!.content) });
    if (typeof (input.gateway as Partial<FsGateway>).postJson !== "function") throw new Error("Filesystem control unavailable");
    this.#gateway = input.gateway as typeof input.gateway & FsGateway;
    this.#sessionId = sessionId;
    await this.#bootstrap.start({ gateway: this.#gateway, ydoc: input.ydoc, ySweet: input.ySweet, hook: await loadGitCleanFilterHook(), sessionId,
      files: [...input.assets.files, input.assets.manifest, ...files] }, signal);
    return Object.freeze({ sessionId, workspaceRoot: `/codex_workspace/${sessionId}`, jobId: `git-clean-filter-${sessionId}` });
  }

  cleanupAfterSignedShutdown(signed: boolean, signal: AbortSignal): Promise<void> {
    if (!signed || !this.#gateway || !this.#sessionId || this.result?.outcome !== "submitted") {
      return Promise.reject(new Error("Workspace deletion requires signed shutdown and a restored-config receipt"));
    }
    if (this.#cleanup) return this.#cleanup;
    const gateway = this.#gateway;
    const sessionId = this.#sessionId;
    this.#cleanup = (async () => {
      signal.throwIfAborted();
      if (parseGitCleanFilterResult(await readEvidence(gateway, sessionId, "result.json", signal), sessionId).outcome !== "submitted") {
        throw new Error("Clean-filter cleanup authority changed");
      }
      await this.#bootstrap.removeProbeAfterSignedShutdown(true);
      signal.throwIfAborted();
      await gateway.syncWorkspace(signal);
      const initialized = await gateway.postJson("init", {}, signal);
      await initialized.body?.cancel();
      if (initialized.status !== 200) throw Object.assign(new Error("Cleanup filesystem mode unavailable"), { code: "cleanup_filesystem_mode" });
      const cancelled = await gateway.fetchText(`write-file?filename=${encodeURIComponent(`../codex_workspace/${sessionId}/${PREFIX}/cancelled`)}`,
        { method: "POST", body: "cancelled\n", signal });
      if (cancelled.status !== 200) throw new Error("Cleanup cancellation marker rejected");
      const removed = await gateway.postJson("delete-file", { filename: `../codex_workspace/${sessionId}` }, signal);
      await removed.body?.cancel();
      if (removed.status !== 200 || await readEvidence(gateway, sessionId, "result.json", signal) !== null) {
        throw Object.assign(new Error("Execution workspace deletion was not verified"), { code: "cleanup_workspace_unverified" });
      }
      this.#cleaned = true;
    })();
    return this.#cleanup;
  }
}
