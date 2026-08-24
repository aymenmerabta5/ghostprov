import type { TransportMode } from "../config/multi-sandbox";
import type { PrismIdentityKind } from "../prism/session-manager";
import type { RunHandle } from "../relay/run-handle";
import type { ProvisionedSandbox } from "../prism/protocol";
import type {
  RoutableSandboxRuntime,
  RoutableSandboxSnapshot,
} from "../sandbox/registry";
import { V2PreCommitError } from "./transport/run-handle";

export interface TransportSelectingRuntimeOptions {
  mode: TransportMode;
  identityKind: PrismIdentityKind;
  v2: RoutableSandboxRuntime;
  v1?: RoutableSandboxRuntime;
}

export class TransportSelectingRuntime implements RoutableSandboxRuntime {
  readonly projectId;
  readonly #mode: TransportMode;
  readonly #identityKind: PrismIdentityKind;
  readonly #v2: RoutableSandboxRuntime;
  readonly #v1?: RoutableSandboxRuntime;
  readonly #listeners = new Set<() => void>();
  #v2Started = false;
  #v2Starting: Promise<void> | null = null;
  #v1Started = false;
  #v1Starting: Promise<void> | null = null;
  #stopped = false;
  #fallbackActive = false;
  #suppressChildEvents = 0;

  constructor(options: TransportSelectingRuntimeOptions) {
    if (options.mode === "v1" && !options.v1) {
      throw new TypeError("V1 transport mode requires a V1 runtime");
    }
    if (options.identityKind === "anonymous" && (options.mode === "v1" || options.v1)) {
      throw new TypeError("Anonymous Prism identities must be V2-only");
    }
    if (options.v1 && options.v1.projectId !== options.v2.projectId) {
      throw new TypeError("Transport runtimes belong to different projects");
    }
    this.projectId = options.v2.projectId;
    this.#mode = options.mode;
    this.#identityKind = options.identityKind;
    this.#v2 = options.v2;
    this.#v1 = options.v1;
    this.#v2.subscribe?.(() => this.#childChanged());
    this.#v1?.subscribe?.(() => this.#childChanged());
  }

  async start(): Promise<void> {
    this.#stopped = false;
    this.#suppressChildEvents++;
    try {
      if (this.#mode === "v1") {
        await this.#ensureV1Started();
        return;
      }
      if (this.#v2Started && this.#v2.snapshot().state === "ready") return;
      this.#fallbackActive = false;
      try {
        await this.#ensureV2Started();
      } catch (error) {
        if (this.#stopped) throw error;
        if (
          this.#mode !== "auto" ||
          this.#identityKind !== "authenticated" ||
          !this.#v1
        ) throw error;
        await this.#ensureV1Started();
        this.#fallbackActive = true;
      }
    } finally {
      this.#suppressChildEvents--;
      this.#emit();
    }
  }

  async drain(reason: string): Promise<void> {
    if (this.#v2Starting || this.#v1Starting) {
      await this.stop();
      return;
    }
    const drains: Promise<void>[] = [];
    if (this.#v2Started) drains.push(this.#v2.drain(reason));
    if (this.#v1Started && this.#v1) drains.push(this.#v1.drain(reason));
    await Promise.allSettled(drains);
    this.#stopped = true;
  }

  async stop(): Promise<void> {
    if (this.#stopped) return;
    this.#stopped = true;
    const stops: Promise<void>[] = [];
    if (this.#v2Started || this.#v2Starting) stops.push(this.#v2.stop());
    if ((this.#v1Started || this.#v1Starting) && this.#v1) stops.push(this.#v1.stop());
    await Promise.allSettled(stops);
    this.#v2Started = false;
    this.#v1Started = false;
  }

  async run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<RunHandle> {
    if (this.#stopped) throw new Error(`Sandbox ${this.projectId} is stopped`);
    if (this.#mode === "v1") {
      if (!this.#v1) throw new Error("V1 runtime is unavailable");
      await this.#ensureV1Started();
      return this.#v1.run(body, signal);
    }
    if (this.#fallbackActive && this.#v1) return this.#v1.run(body, signal);
    try {
      return await this.#v2.run(body, signal);
    } catch (error) {
      if (
        this.#mode !== "auto" ||
        this.#identityKind !== "authenticated" ||
        !(error instanceof V2PreCommitError) ||
        !this.#canFallbackFor(error) ||
        !this.#v1
      ) throw error;
      const v2 = this.#v2.snapshot();
      if (v2.active > 0 || v2.quarantined > 0) throw error;
      this.#suppressChildEvents++;
      try {
        await this.#ensureV1Started();
        this.#fallbackActive = true;
        return this.#v1.run(body, signal);
      } finally {
        this.#suppressChildEvents--;
        this.#emit();
      }
    }
  }

  snapshot(): RoutableSandboxSnapshot {
    if (this.#mode === "v1" || (this.#fallbackActive && this.#v1)) {
      return this.#v1!.snapshot();
    }
    return this.#v2.snapshot();
  }

  takeReplacementSandbox(): ProvisionedSandbox | null {
    if (this.#mode === "v1" || (this.#fallbackActive && this.#v1)) {
      return this.#v1?.takeReplacementSandbox?.() ?? null;
    }
    return this.#v2.takeReplacementSandbox?.() ?? null;
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  #ensureV1Started(): Promise<void> {
    if (!this.#v1) return Promise.reject(new Error("V1 runtime is unavailable"));
    if (this.#v1Started) return Promise.resolve();
    if (this.#v1Starting) return this.#v1Starting;
    const starting = this.#v1.start()
      .then(async () => {
        if (this.#stopped) {
          await this.#v1!.stop();
          throw new Error(`Sandbox ${this.projectId} stopped during V1 startup`);
        }
        this.#v1Started = true;
      })
      .finally(() => {
        if (this.#v1Starting === starting) this.#v1Starting = null;
      });
    this.#v1Starting = starting;
    return starting;
  }

  #ensureV2Started(): Promise<void> {
    if (this.#v2Started && this.#v2.snapshot().state === "ready") return Promise.resolve();
    if (this.#v2Starting) return this.#v2Starting;
    const starting = this.#v2.start()
      .then(async () => {
        if (this.#stopped) {
          await this.#v2.stop();
          throw new Error(`Sandbox ${this.projectId} stopped during V2 startup`);
        }
        this.#v2Started = true;
      })
      .finally(() => {
        if (this.#v2Starting === starting) this.#v2Starting = null;
      });
    this.#v2Starting = starting;
    return starting;
  }

  #canFallbackFor(error: V2PreCommitError): boolean {
    return new Set([
      "not_connected",
      "closed",
      "protocol",
      "instance_changed",
      "unavailable",
      "sidecar_unreachable",
    ]).has(error.code);
  }

  #childChanged(): void {
    if (this.#suppressChildEvents > 0) return;
    this.#emit();
  }

  #emit(): void {
    for (const listener of this.#listeners) {
      try { listener(); } catch {}
    }
  }
}
