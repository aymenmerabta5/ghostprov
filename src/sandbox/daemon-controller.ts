import { SYSTEM_CLOCK, type Clock } from "../core/ports";

export interface DaemonAssets {
  daemonFile: string;
  daemonSource: string;
  launcherFile: string;
  launcherSource: string;
  mainFile: string;
  mainSource: string;
  trackedFiles: readonly string[];
  expectedBuild: string;
  requiredProtocols: readonly string[];
}

export interface DaemonYDoc {
  readFile(name: string): Promise<string | null>;
  writeFile(name: string, content: string): Promise<void>;
  ensureFiles(names: readonly string[]): Promise<void>;
  renderSyncState(): Promise<{
    clientStateVector: string;
    clientDeleteSetUpdate: string;
  }>;
}

export interface DaemonGateway {
  readFiles(names: string[]): Promise<Map<string, Uint8Array>>;
  syncWorkspace(signal?: AbortSignal): Promise<void>;
  triggerRender(request?: {
    mainDocument: string;
    clientStateVector: string;
    clientDeleteSetUpdate: string;
  }): Promise<void>;
}

export interface DaemonDescriptor {
  build: string;
  pid: number;
  capacity: number;
  protocols: readonly string[];
}

export class DaemonControllerError extends Error {
  constructor(message: string, readonly kind: "timeout" | "protocol" | "sync") {
    super(message);
    this.name = "DaemonControllerError";
  }
}

export interface DaemonControllerOptions {
  ydoc: DaemonYDoc;
  gateway: DaemonGateway;
  assets: DaemonAssets;
  clock?: Clock;
  pollMs?: number;
  timeoutMs?: number;
  freshnessMs?: number;
  cacheMs?: number;
}

interface DaemonStatus {
  daemon_build: string;
  pid: number;
  ts: number;
  slots: number;
  job_protocols: string[];
}

function isMissingBootstrapArchive(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const failure = error as { status?: unknown; operation?: unknown };
  return failure.status === 500 && failure.operation === "zip";
}

export class DaemonController {
  readonly #ydoc: DaemonYDoc;
  readonly #gateway: DaemonGateway;
  readonly #assets: DaemonAssets;
  readonly #clock: Clock;
  readonly #pollMs: number;
  readonly #timeoutMs: number;
  readonly #freshnessMs: number;
  readonly #cacheMs: number;
  #cached: { descriptor: DaemonDescriptor; verifiedAtMs: number } | null = null;
  #ensuring: Promise<DaemonDescriptor> | null = null;

  constructor(options: DaemonControllerOptions) {
    this.#ydoc = options.ydoc;
    this.#gateway = options.gateway;
    this.#assets = options.assets;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#pollMs = options.pollMs ?? 500;
    this.#timeoutMs = options.timeoutMs ?? 60_000;
    this.#freshnessMs = options.freshnessMs ?? 15_000;
    this.#cacheMs = options.cacheMs ?? 2_000;
    if (!Number.isFinite(this.#cacheMs) || this.#cacheMs < 0 || this.#cacheMs >= this.#freshnessMs) {
      throw new RangeError("Daemon readiness cache must be non-negative and shorter than freshness");
    }
  }

  async ensureReady(signal?: AbortSignal): Promise<DaemonDescriptor> {
    if (signal?.aborted) throw new DOMException("Daemon readiness aborted", "AbortError");
    const cached = this.#cached;
    if (cached && this.#clock.now() - cached.verifiedAtMs <= this.#cacheMs) return cached.descriptor;
    if (!this.#ensuring) {
      const tracked = this.#ensureReadyUncached(signal)
        .then(descriptor => {
          this.#cached = { descriptor, verifiedAtMs: this.#clock.now() };
          return descriptor;
        })
        .finally(() => {
          if (this.#ensuring === tracked) this.#ensuring = null;
        });
      this.#ensuring = tracked;
    }
    return this.#ensuring;
  }

  async #ensureReadyUncached(signal?: AbortSignal): Promise<DaemonDescriptor> {
    const [daemonSource, launcherSource, mainSource, initialStatus] = await Promise.all([
      this.#ydoc.readFile(this.#assets.daemonFile),
      this.#ydoc.readFile(this.#assets.launcherFile),
      this.#ydoc.readFile(this.#assets.mainFile),
      this.#readStatus(),
    ]);
    const unchanged = daemonSource === this.#assets.daemonSource &&
      launcherSource === this.#assets.launcherSource &&
      mainSource !== null;
    await this.#ydoc.ensureFiles(this.#assets.trackedFiles);
    await this.#gateway.syncWorkspace(signal);
    const initial = this.#descriptor(initialStatus);
    if (unchanged && initial) return initial;

    if (daemonSource !== this.#assets.daemonSource) {
      await this.#ydoc.writeFile(this.#assets.daemonFile, this.#assets.daemonSource);
    }
    if (launcherSource !== this.#assets.launcherSource) {
      await this.#ydoc.writeFile(this.#assets.launcherFile, this.#assets.launcherSource);
    }
    if (mainSource === null) {
      await this.#ydoc.writeFile(this.#assets.mainFile, this.#assets.mainSource);
    }
    const renderSyncState = await this.#ydoc.renderSyncState();
    await this.#gateway.triggerRender({
      mainDocument: this.#assets.mainFile,
      ...renderSyncState,
    });

    const deadline = this.#clock.now() + this.#timeoutMs;
    for (;;) {
      if (signal?.aborted) throw new DOMException("Daemon readiness aborted", "AbortError");
      const descriptor = this.#descriptor(await this.#readStatus());
      if (descriptor) return descriptor;
      if (this.#clock.now() >= deadline) {
        throw new DaemonControllerError(
          `Daemon did not report build ${this.#assets.expectedBuild} before timeout`,
          "timeout",
        );
      }
      await this.#clock.sleep(Math.min(this.#pollMs, deadline - this.#clock.now()), signal);
    }
  }

  async #readStatus(): Promise<DaemonStatus | null> {
    let files: Map<string, Uint8Array>;
    try {
      files = await this.#gateway.readFiles(["gp_status.json"]);
    } catch (error) {
      if (isMissingBootstrapArchive(error)) return null;
      throw error;
    }
    const bytes = files.get("gp_status.json");
    if (!bytes) return null;
    try {
      const value = JSON.parse(new TextDecoder().decode(bytes));
      if (
        typeof value?.daemon_build !== "string" ||
        typeof value?.pid !== "number" ||
        typeof value?.ts !== "number" ||
        typeof value?.slots !== "number" ||
        !Array.isArray(value?.job_protocols)
      ) return null;
      return value as DaemonStatus;
    } catch {
      return null;
    }
  }

  #descriptor(status: DaemonStatus | null): DaemonDescriptor | null {
    if (!status) return null;
    if (status.daemon_build !== this.#assets.expectedBuild) return null;
    if (this.#clock.now() - status.ts * 1000 > this.#freshnessMs) return null;
    if (!Number.isInteger(status.slots) || status.slots < 1) return null;
    if (!this.#assets.requiredProtocols.every(protocol => status.job_protocols.includes(protocol))) return null;
    return {
      build: status.daemon_build,
      pid: status.pid,
      capacity: status.slots,
      protocols: Object.freeze([...status.job_protocols]),
    };
  }
}
