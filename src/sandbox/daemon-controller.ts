import { SYSTEM_CLOCK, type Clock } from "../core/ports";

export interface DaemonAssets {
  daemonFile: string;
  daemonSource: string;
  launcherFile: string;
  launcherSource: string;
  expectedBuild: string;
  requiredProtocols: readonly string[];
}

export interface DaemonYDoc {
  readFile(name: string): Promise<string | null>;
  writeFile(name: string, content: string): Promise<void>;
}

export interface DaemonGateway {
  readFiles(names: string[]): Promise<Map<string, Uint8Array>>;
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
}

interface DaemonStatus {
  daemon_build: string;
  pid: number;
  ts: number;
  slots: number;
  job_protocols: string[];
}

export class DaemonController {
  readonly #ydoc: DaemonYDoc;
  readonly #gateway: DaemonGateway;
  readonly #assets: DaemonAssets;
  readonly #clock: Clock;
  readonly #pollMs: number;
  readonly #timeoutMs: number;
  readonly #freshnessMs: number;

  constructor(options: DaemonControllerOptions) {
    this.#ydoc = options.ydoc;
    this.#gateway = options.gateway;
    this.#assets = options.assets;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#pollMs = options.pollMs ?? 500;
    this.#timeoutMs = options.timeoutMs ?? 60_000;
    this.#freshnessMs = options.freshnessMs ?? 15_000;
  }

  async ensureReady(signal?: AbortSignal): Promise<DaemonDescriptor> {
    const [daemonSource, launcherSource, initialStatus] = await Promise.all([
      this.#ydoc.readFile(this.#assets.daemonFile),
      this.#ydoc.readFile(this.#assets.launcherFile),
      this.#readStatus(),
    ]);
    const unchanged = daemonSource === this.#assets.daemonSource &&
      launcherSource === this.#assets.launcherSource;
    const initial = this.#descriptor(initialStatus);
    if (unchanged && initial) return initial;

    if (daemonSource !== this.#assets.daemonSource) {
      await this.#ydoc.writeFile(this.#assets.daemonFile, this.#assets.daemonSource);
    }
    if (launcherSource !== this.#assets.launcherSource) {
      await this.#ydoc.writeFile(this.#assets.launcherFile, this.#assets.launcherSource);
    }
    await this.#gateway.triggerRender({
      mainDocument: "main.tex",
      clientStateVector: "AerAuOEF8wI=",
      clientDeleteSetUpdate: "AAA=",
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
    const files = await this.#gateway.readFiles(["gp_status.json"]);
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
