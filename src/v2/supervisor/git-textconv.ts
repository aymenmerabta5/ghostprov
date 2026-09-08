import { readFile } from "node:fs/promises";
import {
  GIT_CLEAN_FILTER_HOOK_SHA256, GitCleanFilterBootstrap, loadGitCleanFilterHook, makeGitCleanFilterPlan,
  type FsGateway, type GitCleanFilterBootstrapInput,
} from "./git-clean-filter";

export const GIT_TEXTCONV_INPUT_SHA256 = "176dd2f86f1a4a4e1350023a9070db1a705222d3db02d68c565209468bc78d68";
const sha256 = (value: string) => new Bun.CryptoHasher("sha256").update(value).digest("hex");
let inputSource: Promise<string> | null = null;

export function loadGitTextconvInput(): Promise<string> {
  inputSource ??= (async () => {
    const source = (await readFile(new URL("./git-textconv-input.py", import.meta.url), "utf8")).replaceAll("\r\n", "\n");
    if (Buffer.byteLength(source) > 65536 || sha256(source) !== GIT_TEXTCONV_INPUT_SHA256) throw new Error("Textconv input identity mismatch");
    return source;
  })();
  return inputSource;
}

export function makeGitTextconvDeployment(sessionId: string, core: string, launcher: string, expiresAtUnixMs: number) {
  const base = makeGitCleanFilterPlan(sessionId, core, launcher, expiresAtUnixMs);
  const attributes = `/${base.probeName} diff=ghostprov_textconv -text -ident\n`;
  const config = `[core]\n\tattributesFile = ${base.directory}/attributes\n[diff "ghostprov_textconv"]\n` +
    `\ttextconv = "/usr/bin/python3 ${base.directory}/textconv-input.py ${GIT_TEXTCONV_INPUT_SHA256}"\n` +
    "\tcachetextconv = false\n\tbinary = false\n";
  return Object.freeze({ base, attributes, config, configSha256: sha256(config) });
}

export type GitTextconvDeployment = ReturnType<typeof makeGitTextconvDeployment>;

/** Only the three known Git configuration files change; all other bytes pass through. */
export function translateGitTextconvFile(prepared: GitTextconvDeployment, file: string, content: string): string {
  if (file === "attributes") {
    if (content !== prepared.base.attributes) throw new Error("Unexpected source attributes");
    return prepared.attributes;
  }
  if (file === "replacement.gitconfig") {
    if (content !== prepared.base.config) throw new Error("Unexpected source Git configuration");
    return prepared.config;
  }
  if (file === "plan.json") {
    if (Buffer.byteLength(content) > 8192) throw new Error("Textconv plan exceeded its bound");
    const raw: unknown = JSON.parse(content);
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("Invalid source Git plan");
    const value = raw as Record<string, unknown>;
    const expected = prepared.base.plan;
    if (Object.keys(value).sort().join() !== Object.keys(expected).sort().join() ||
        Object.entries(expected).some(([key, entry]) => key !== "expiresAtUnixMs" && value[key] !== entry) ||
        typeof value.expiresAtUnixMs !== "number" || !Number.isSafeInteger(value.expiresAtUnixMs) || value.expiresAtUnixMs < 1) {
      throw new Error("Source Git plan identity changed");
    }
    if (content !== JSON.stringify({ ...expected, expiresAtUnixMs: value.expiresAtUnixMs })) {
      throw new Error("Source Git plan encoding changed");
    }
    return JSON.stringify({ ...value, configSha256: prepared.configSha256 });
  }
  return content;
}

export function createGitTextconvGateway(original: FsGateway, prepared: GitTextconvDeployment, core: string, relay: string) {
  if (sha256(core) !== GIT_CLEAN_FILTER_HOOK_SHA256 || Buffer.byteLength(relay) > 65536 || sha256(relay) !== GIT_TEXTCONV_INPUT_SHA256) {
    throw new Error("Unverified textconv sources");
  }
  const paths = new Map(["attributes", "replacement.gitconfig", "plan.json"].map(name => [`..${prepared.base.directory}/${name}`, name]));
  const seen = new Set<string>();
  let relayStaged = false;
  let armed = false;
  const gateway = new Proxy(original, {
    get(target, property) {
      if (property === "postJson") return async (operation: string, body: unknown, signal?: AbortSignal) => {
        if (operation === "token") {
          if (armed || !relayStaged || seen.size !== 3) throw new Error("Textconv configuration is incomplete or already armed");
          armed = true; // An ambiguous token submission remains single-use.
        }
        return target.postJson(operation, body, signal);
      };
      if (property === "fetchText") return async (suffix: string, init: RequestInit = {}) => {
        if (!suffix.startsWith("write-file?filename=")) return target.fetchText(suffix, init);
        const url = new URL(suffix, "https://staging.invalid/");
        const filename = url.searchParams.get("filename");
        if (filename === `..${prepared.base.directory}/hook.py`) {
          if (armed || relayStaged || init.method !== "POST" || init.body !== core ||
              url.hash || [...url.searchParams.keys()].length !== 1) throw new Error("Unexpected shared executor staging");
          relayStaged = true;
          const response = await target.fetchText(`write-file?filename=${encodeURIComponent(`..${prepared.base.directory}/textconv-input.py`)}`,
            { ...init, body: relay });
          if (response.status !== 200) throw Object.assign(new Error("Textconv input staging rejected"), { status: response.status });
        }
        const field = filename ? paths.get(filename) : undefined;
        if (field !== undefined) {
          if (armed || seen.has(field) || init.method !== "POST" || typeof init.body !== "string" ||
              url.hash || [...url.searchParams.keys()].length !== 1) throw new Error("Unexpected or repeated textconv configuration write");
          const body = translateGitTextconvFile(prepared, field, init.body);
          seen.add(field);
          return target.fetchText(suffix, { ...init, body });
        }
        return target.fetchText(suffix, init);
      };
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return Object.freeze({ gateway, ready: () => relayStaged && seen.size === 3 && armed });
}

/** Reuse the audited R10 lifecycle and owner record; change only the Git trigger. */
export class GitTextconvBootstrap {
  readonly #delegate = new GitCleanFilterBootstrap();
  #attempted = false;
  get result() { return this.#delegate.result; }
  get sessionId() { return this.#delegate.sessionId; }
  get attempted() { return this.#attempted; }
  get cleaned() { return this.#delegate.cleaned; }

  async start(input: GitCleanFilterBootstrapInput, sessionId: string, signal?: AbortSignal) {
    signal?.throwIfAborted();
    if (this.#attempted) throw new Error("A textconv bootstrap is never resubmitted");
    this.#attempted = true;
    if (typeof (input.gateway as Partial<FsGateway>).postJson !== "function") throw new Error("Filesystem control unavailable");
    const [core, relay, launcher] = await Promise.all([loadGitCleanFilterHook(), loadGitTextconvInput(), input.ydoc.readPath("exec")]);
    if (launcher === null) throw new Error("Missing bounded textconv launcher");
    const prepared = makeGitTextconvDeployment(sessionId, core, launcher, Date.now() + 90000);
    const mapped = createGitTextconvGateway(input.gateway as typeof input.gateway & FsGateway, prepared, core, relay);
    const started = await this.#delegate.start({ ...input, gateway: mapped.gateway }, sessionId, signal);
    if (!mapped.ready()) throw new Error("Textconv deployment proof missing");
    return Object.freeze({ ...started, jobId: `git-textconv-${sessionId}` });
  }

  cleanupAfterSignedShutdown(signed: boolean, signal: AbortSignal): Promise<void> {
    return this.#delegate.cleanupAfterSignedShutdown(signed, signal);
  }
}
