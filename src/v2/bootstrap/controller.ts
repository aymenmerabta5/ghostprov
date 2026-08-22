import type { Clock, RandomSource } from "../../core/ports";
import { SYSTEM_CLOCK, SYSTEM_RANDOM } from "../../core/ports";
import type { YSweetEnvelope } from "../../prism/protocol";
import type { RenderSyncState } from "../../sandbox/ydoc";
import type { ProjectId } from "../../sandbox/types";
import type { AwarenessChannel } from "../awareness/channel";
import {
  createCredentialRotationKey,
  sealBootstrapCredential,
  sealCredentialRotation,
  type BootstrapBinding,
  type SealedBootstrapCredential,
} from "./crypto";
import {
  base64UrlDecode,
  canonicalJson,
  utf8Text,
  type AwarenessEnvelope,
  type HostFrame,
  type ReliableFrame,
  type SidecarFrame,
} from "../protocol/shared";
import type { V2SidecarAssets } from "../sidecar/assets";

const STATIC_PATH = ".ghostprov-v2/static.json";
const WORKER_PATH = ".ghostprov-v2/worker.json";
const CIPHERTEXT_PATH = ".ghostprov-v2/bootstrap.enc.json";
const LAUNCHER_PATH = ".latexmkrc";
const MAIN_PATH = "main.tex";
const HELLO_PREFIX = "GPV2_BOOTSTRAP ";
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

const INERT_LAUNCHER = [
  "# GhostProv V2 launcher inactive",
  "$pdf_mode = 1;",
  "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
  "",
].join("\n");

const MAIN_SOURCE = [
  "\\documentclass{article}",
  "\\begin{document}",
  "GhostProv V2 sidecar bootstrap.",
  "\\end{document}",
  "",
].join("\n");

export interface V2BootstrapYDoc {
  readPath(path: string): Promise<string | null>;
  writePath(path: string, content: string): Promise<void>;
  tombstonePath(path: string): Promise<boolean>;
  renderSyncState(): Promise<RenderSyncState>;
}

export interface V2BootstrapGateway {
  syncWorkspace(signal?: AbortSignal): Promise<void>;
  triggerRender(request: {
    mainDocument: string;
    clientStateVector: string;
    clientDeleteSetUpdate: string;
  }, signal?: AbortSignal): Promise<void>;
  fetchText(
    suffix: string,
    init?: RequestInit,
  ): Promise<{ status: number; text: string; headers: Headers }>;
}

export type V2BootstrapAwareness = AwarenessChannel<HostFrame, SidecarFrame>;

export interface V2ReadyDescriptor {
  instanceId: string;
  buildHash: string;
  schemaHash: string;
  leaderGeneration: number;
  capacity: number;
}

export type V2BootstrapErrorKind =
  | "deploy"
  | "render"
  | "hello"
  | "crypto"
  | "readiness"
  | "timeout";

export class V2BootstrapError extends Error {
  readonly name = "V2BootstrapError";

  constructor(
    message: string,
    readonly kind: V2BootstrapErrorKind,
    readonly attempt: number,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export interface V2BootstrapControllerOptions {
  projectId: ProjectId;
  workerIndex: number;
  sandboxSessionId: string;
  leaderGeneration: number;
  ySweet: YSweetEnvelope;
  ydoc: V2BootstrapYDoc;
  gateway: V2BootstrapGateway;
  awareness: V2BootstrapAwareness;
  assets: V2SidecarAssets;
  attempts?: number;
  timeoutMs?: number;
  pollMs?: number;
  physicalMax?: number;
  acceptedMax?: number;
  memoryBudgetBytes?: number;
  clock?: Clock;
  random?: RandomSource;
}

interface BootstrapHello {
  version: 1;
  binding: BootstrapBinding;
  instanceId: string;
  publicJwk: JsonWebKey;
}

function exactKeys(
  value: Record<string, unknown>,
  expected: readonly string[],
): boolean {
  const actual = Object.keys(value).sort();
  const sorted = [...expected].sort();
  return actual.length === sorted.length &&
    actual.every((key, index) => key === sorted[index]);
}

function temporaryLauncher(): string {
  return [
    "# GhostProv V2 temporary bootstrap launcher",
    "$pdf_mode = 1;",
    "my $gp_dir = '/tmp/ghostprov-v2';",
    "my $gp_hello = '/tmp/ghostprov-v2/bootstrap-hello';",
    "mkdir $gp_dir, 0700 unless -d $gp_dir;",
    "unlink $gp_hello if -e $gp_hello;",
    "my $gp_pid = fork();",
    "die 'GhostProv V2 fork failed' unless defined $gp_pid;",
    "if ($gp_pid == 0) {",
    "  open STDOUT, '>>', '/tmp/ghostprov-v2/sidecar.log' or die 'stdout';",
    "  open STDERR, '>&STDOUT' or die 'stderr';",
    "  exec 'setsid', '/code/crixet/deno', 'run', '--cached-only', '--unstable-sloppy-imports', '--config=/workspace/.ghostprov-v2/deno.json', \"--cert=$ENV{PROXY_CA_CERT_PATH}\", '--allow-env=OPENAI_BASE_URL,OPENAI_API_KEY,PROXY_CA_CERT_PATH', '--allow-net', '--allow-read=/workspace/.ghostprov-v2,/tmp/ghostprov-v2', '--allow-write=/tmp/ghostprov-v2', '/workspace/.ghostprov-v2/sandbox/v2/main.ts';",
    "  exit 127;",
    "}",
    "for (my $gp_try = 0; $gp_try < 900; $gp_try++) {",
    "  if (open my $gp_fh, '<', $gp_hello) {",
    "    while (my $gp_line = <$gp_fh>) { print $gp_line if $gp_line =~ /^GPV2_BOOTSTRAP /; }",
    "    close $gp_fh;",
    "    last;",
    "  }",
    "  select undef, undef, undef, 0.05;",
    "}",
    "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
    "",
  ].join("\n");
}

function parseHelloCandidate(
  encoded: string,
  expected: BootstrapBinding,
  attempt: number,
): BootstrapHello {
  let value: unknown;
  try {
    value = JSON.parse(utf8Text(base64UrlDecode(encoded)));
  } catch (cause) {
    throw new V2BootstrapError("Sidecar bootstrap hello is malformed", "hello", attempt, { cause });
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new V2BootstrapError("Sidecar bootstrap hello must be an object", "hello", attempt);
  }
  const raw = value as Record<string, unknown>;
  if (!exactKeys(raw, ["version", "binding", "instanceId", "publicJwk"]) || raw.version !== 1) {
    throw new V2BootstrapError("Sidecar bootstrap hello fields are invalid", "hello", attempt);
  }
  if (
    !raw.binding ||
    typeof raw.binding !== "object" ||
    Array.isArray(raw.binding) ||
    canonicalJson(raw.binding) !== canonicalJson(expected)
  ) {
    throw new V2BootstrapError("Sidecar bootstrap hello binding is stale", "hello", attempt);
  }
  if (typeof raw.instanceId !== "string" || !SAFE_ID.test(raw.instanceId)) {
    throw new V2BootstrapError("Sidecar bootstrap instance is invalid", "hello", attempt);
  }
  if (
    !raw.publicJwk ||
    typeof raw.publicJwk !== "object" ||
    Array.isArray(raw.publicJwk) ||
    (raw.publicJwk as JsonWebKey).kty !== "EC" ||
    (raw.publicJwk as JsonWebKey).crv !== "P-256" ||
    typeof (raw.publicJwk as JsonWebKey).x !== "string" ||
    typeof (raw.publicJwk as JsonWebKey).y !== "string" ||
    (raw.publicJwk as JsonWebKey).d !== undefined
  ) {
    throw new V2BootstrapError("Sidecar bootstrap public key is invalid", "hello", attempt);
  }
  return {
    version: 1,
    binding: expected,
    instanceId: raw.instanceId,
    publicJwk: raw.publicJwk as JsonWebKey,
  };
}

function matchesReadiness(
  envelope: AwarenessEnvelope<SidecarFrame>,
  binding: BootstrapBinding,
  instanceId: string,
): boolean {
  return envelope.protocol === "gp-awareness-v2" &&
    envelope.schemaVersion === 1 &&
    envelope.projectId === binding.projectId &&
    envelope.sessionId === binding.sandboxSessionId &&
    envelope.leaderGeneration === binding.leaderGeneration &&
    envelope.buildHash === binding.buildHash &&
    envelope.schemaHash === binding.schemaHash &&
    envelope.instanceId === instanceId &&
    envelope.peerId === "sidecar" &&
    !envelope.draining &&
    Number.isSafeInteger(envelope.credits) &&
    envelope.credits > 0 &&
    envelope.credits <= 128;
}

function matchesBoundInstance(
  envelope: AwarenessEnvelope<SidecarFrame>,
  binding: BootstrapBinding,
  instanceId: string,
): boolean {
  return envelope.protocol === "gp-awareness-v2" &&
    envelope.schemaVersion === 1 &&
    envelope.projectId === binding.projectId &&
    envelope.sessionId === binding.sandboxSessionId &&
    envelope.leaderGeneration === binding.leaderGeneration &&
    envelope.buildHash === binding.buildHash &&
    envelope.schemaHash === binding.schemaHash &&
    envelope.instanceId === instanceId &&
    envelope.peerId === "sidecar";
}

function validWorkerMarker(
  content: string,
  workerIndex: number,
  buildHash: string,
): boolean {
  try {
    const value = JSON.parse(content) as Record<string, unknown>;
    return exactKeys(value, ["protocol", "workerIndex", "buildHash", "createdAtMs"]) &&
      value.protocol === "gp-awareness-v2" &&
      value.workerIndex === workerIndex &&
      value.buildHash === buildHash &&
      Number.isSafeInteger(value.createdAtMs) &&
      (value.createdAtMs as number) >= 0;
  } catch {
    return false;
  }
}

function safeReason(value: string): string {
  const normalized = value.replace(/[\r\n\0]/g, " ").trim().slice(0, 256);
  return normalized || "shutdown";
}

export class V2BootstrapController {
  readonly #projectId: ProjectId;
  readonly #workerIndex: number;
  readonly #sandboxSessionId: string;
  readonly #leaderGeneration: number;
  readonly #ySweet: YSweetEnvelope;
  readonly #ydoc: V2BootstrapYDoc;
  readonly #gateway: V2BootstrapGateway;
  readonly #awareness: V2BootstrapAwareness;
  readonly #assets: V2SidecarAssets;
  readonly #attempts: number;
  readonly #timeoutMs: number;
  readonly #pollMs: number;
  readonly #physicalMax: number;
  readonly #acceptedMax: number;
  readonly #memoryBudgetBytes: number;
  readonly #clock: Clock;
  readonly #random: RandomSource;
  #ready: V2ReadyDescriptor | null = null;
  #readyBinding: BootstrapBinding | null = null;
  #rotationKey: string | null = null;
  #ensuring: Promise<V2ReadyDescriptor> | null = null;
  #revision = 0;
  #hostSequence = 0;
  #sidecarAck = { epoch: 1, sequence: -1 };

  constructor(options: V2BootstrapControllerOptions) {
    const attempts = options.attempts ?? 3;
    const timeoutMs = options.timeoutMs ?? 45_000;
    const pollMs = options.pollMs ?? 100;
    const physicalMax = options.physicalMax ?? 128;
    const acceptedMax = options.acceptedMax ?? 256;
    const memoryBudgetBytes = options.memoryBudgetBytes ?? 512 * 1024 * 1024;
    if (!Number.isSafeInteger(options.workerIndex) || options.workerIndex < 1) {
      throw new RangeError("workerIndex must be a positive integer");
    }
    if (!SAFE_ID.test(options.sandboxSessionId)) {
      throw new TypeError("sandboxSessionId is invalid");
    }
    if (!Number.isSafeInteger(options.leaderGeneration) || options.leaderGeneration < 1) {
      throw new RangeError("leaderGeneration must be a positive integer");
    }
    if (!Number.isInteger(attempts) || attempts < 1 || attempts > 3) {
      throw new RangeError("attempts must be an integer from 1 through 3");
    }
    if (!Number.isFinite(timeoutMs) || timeoutMs < 1) {
      throw new RangeError("timeoutMs must be positive");
    }
    if (!Number.isFinite(pollMs) || pollMs < 1 || pollMs > timeoutMs) {
      throw new RangeError("pollMs must be positive and no greater than timeoutMs");
    }
    if (!Number.isSafeInteger(physicalMax) || physicalMax < 1 || physicalMax > 128) {
      throw new RangeError("physicalMax must be an integer from 1 through 128");
    }
    if (!Number.isSafeInteger(acceptedMax) || acceptedMax < physicalMax) {
      throw new RangeError("acceptedMax must be at least physicalMax");
    }
    if (!Number.isSafeInteger(memoryBudgetBytes) || memoryBudgetBytes < 64 * 1024 * 1024) {
      throw new RangeError("memoryBudgetBytes must be at least 64 MiB");
    }
    if (options.ySweet.docId !== options.projectId) {
      throw new TypeError("Y-Sweet credential belongs to another project");
    }
    if (options.ySweet.authorization !== "full") {
      throw new TypeError("V2 bootstrap requires full Y-Sweet authorization");
    }
    if (options.awareness.projectId !== options.projectId) {
      throw new TypeError("Awareness channel belongs to another project");
    }
    this.#projectId = options.projectId;
    this.#workerIndex = options.workerIndex;
    this.#sandboxSessionId = options.sandboxSessionId;
    this.#leaderGeneration = options.leaderGeneration;
    this.#ySweet = options.ySweet;
    this.#ydoc = options.ydoc;
    this.#gateway = options.gateway;
    this.#awareness = options.awareness;
    this.#assets = options.assets;
    this.#attempts = attempts;
    this.#timeoutMs = timeoutMs;
    this.#pollMs = pollMs;
    this.#physicalMax = physicalMax;
    this.#acceptedMax = acceptedMax;
    this.#memoryBudgetBytes = memoryBudgetBytes;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
  }

  ensureReady(signal?: AbortSignal): Promise<V2ReadyDescriptor> {
    if (signal?.aborted) {
      return Promise.reject(new DOMException("V2 bootstrap aborted", "AbortError"));
    }
    if (this.#ready) return Promise.resolve(this.#ready);
    if (this.#ensuring) return this.#ensuring;
    const ensuring = this.#ensureReady(signal).finally(() => {
      if (this.#ensuring === ensuring) this.#ensuring = null;
    });
    this.#ensuring = ensuring;
    return ensuring;
  }

  async sealCredentialRotation(
    ySweet: YSweetEnvelope,
    rotationId: string,
  ): Promise<string> {
    const descriptor = this.#ready;
    const binding = this.#readyBinding;
    const rotationKey = this.#rotationKey;
    if (!descriptor || !binding || !rotationKey) {
      throw new V2BootstrapError("Sidecar is not ready for credential rotation", "readiness", 1);
    }
    const sealed = await sealCredentialRotation({
      rotationKey,
      binding: {
        projectId: binding.projectId,
        sandboxSessionId: binding.sandboxSessionId,
        leaderGeneration: binding.leaderGeneration,
        buildHash: binding.buildHash,
        schemaHash: binding.schemaHash,
        instanceId: descriptor.instanceId,
        rotationId,
      },
      credential: {
        url: ySweet.url,
        baseUrl: ySweet.baseUrl,
        docId: ySweet.docId,
        token: ySweet.token.reveal(),
        authorization: ySweet.authorization,
      },
    });
    const serialized = canonicalJson(sealed);
    if (serialized.length > 10_000) {
      throw new V2BootstrapError("Encrypted credential rotation is too large", "crypto", 1);
    }
    return serialized;
  }

  async ping(signal?: AbortSignal): Promise<number> {
    const descriptor = this.#ready;
    const binding = this.#readyBinding;
    if (!descriptor || !binding) {
      throw new V2BootstrapError("Sidecar is not ready for ping", "readiness", 1);
    }
    const sentAtMs = this.#clock.now();
    const frame = this.#controlFrame({ kind: "ping", sentAtMs });
    const seen: AwarenessEnvelope<SidecarFrame>[] = [];
    const unsubscribe = this.#awareness.subscribe(update => seen.push(update.envelope));
    try {
      this.#publishControl(binding, [frame], false);
      const deadline = this.#clock.now() + this.#timeoutMs;
      for (;;) {
        for (const envelope of seen) {
          if (
            !matchesBoundInstance(envelope, binding, descriptor.instanceId) ||
            envelope.ack.epoch !== frame.epoch ||
            envelope.ack.sequence < frame.sequence
          ) continue;
          const pong = envelope.frames.find(candidate => (
            candidate.kind === "pong" && candidate.sentAtMs === sentAtMs
          ));
          if (!pong) continue;
          this.#sidecarAck = { epoch: pong.epoch, sequence: pong.sequence };
          return Math.max(0, this.#clock.now() - sentAtMs);
        }
        if (signal?.aborted) throw new DOMException("V2 ping aborted", "AbortError");
        if (this.#clock.now() >= deadline) {
          throw new V2BootstrapError("Sidecar ping timed out", "timeout", 1);
        }
        await this.#clock.sleep(Math.min(this.#pollMs, deadline - this.#clock.now()), signal);
      }
    } finally {
      unsubscribe();
    }
  }

  async drainAndStop(reason: string, signal?: AbortSignal): Promise<void> {
    const descriptor = this.#ready;
    const binding = this.#readyBinding;
    if (!descriptor || !binding) {
      await this.#cleanupTransient(false);
      this.#awareness.clear();
      return;
    }
    const frame = this.#controlFrame({
      kind: "drain",
      reason: safeReason(reason),
    });
    const seen: AwarenessEnvelope<SidecarFrame>[] = [];
    const unsubscribe = this.#awareness.subscribe(update => seen.push(update.envelope));
    try {
      this.#publishControl(binding, [frame], true);
      const deadline = this.#clock.now() + this.#timeoutMs;
      for (;;) {
        const drained = seen.find(envelope => (
          matchesBoundInstance(envelope, binding, descriptor.instanceId) &&
          envelope.ack.epoch === frame.epoch &&
          envelope.ack.sequence >= frame.sequence &&
          envelope.frames.some(candidate => (
            candidate.kind === "drained" && candidate.active === 0 && candidate.queued === 0
          ))
        ));
        if (drained) break;
        if (signal?.aborted) throw new DOMException("V2 drain aborted", "AbortError");
        if (this.#clock.now() >= deadline) {
          throw new V2BootstrapError("Sidecar did not prove drain completion", "timeout", 1);
        }
        await this.#clock.sleep(Math.min(this.#pollMs, deadline - this.#clock.now()), signal);
      }
      this.#awareness.clear();
      this.#ready = null;
      this.#readyBinding = null;
      this.#rotationKey = null;
      await this.#cleanupTransient(true);
    } finally {
      unsubscribe();
    }
  }

  async #ensureReady(signal?: AbortSignal): Promise<V2ReadyDescriptor> {
    try {
      await this.#deployAssets();
      await this.#awareness.connect(signal);
    } catch (cause) {
      if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
      if (cause instanceof V2BootstrapError) throw cause;
      throw new V2BootstrapError("V2 sidecar asset deployment failed", "deploy", 1, { cause });
    }

    let lastError: V2BootstrapError | null = null;
    for (let attempt = 1; attempt <= this.#attempts; attempt++) {
      if (signal?.aborted) throw new DOMException("V2 bootstrap aborted", "AbortError");
      const binding: BootstrapBinding = {
        projectId: this.#projectId,
        sandboxSessionId: this.#sandboxSessionId,
        leaderGeneration: this.#leaderGeneration,
        buildHash: this.#assets.buildHash,
        schemaHash: this.#assets.schemaHash,
        nonce: this.#random.uuid(),
      };
      this.#rotationKey = null;
      try {
        const descriptor = await this.#attempt(binding, attempt, signal);
        await this.#cleanupTransient(true);
        this.#ready = descriptor;
        this.#readyBinding = binding;
        return descriptor;
      } catch (cause) {
        await this.#cleanupTransient(false);
        if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
        lastError = cause instanceof V2BootstrapError
          ? cause
          : new V2BootstrapError("V2 bootstrap attempt failed", "deploy", attempt, { cause });
      }
    }
    throw lastError ?? new V2BootstrapError("V2 bootstrap failed", "timeout", this.#attempts);
  }

  async #deployAssets(): Promise<void> {
    for (const asset of this.#assets.files) {
      if (await this.#ydoc.readPath(asset.path) !== asset.content) {
        await this.#ydoc.writePath(asset.path, asset.content);
      }
    }
    const existingMarker = await this.#ydoc.readPath(WORKER_PATH);
    if (existingMarker === null) {
      await this.#ydoc.writePath(WORKER_PATH, canonicalJson({
        protocol: "gp-awareness-v2",
        workerIndex: this.#workerIndex,
        buildHash: this.#assets.buildHash,
        createdAtMs: Math.floor(this.#clock.now()),
      }));
    } else if (!validWorkerMarker(existingMarker, this.#workerIndex, this.#assets.buildHash)) {
      throw new V2BootstrapError("Existing GhostProv worker marker does not match", "deploy", 1);
    }
    if (await this.#ydoc.readPath(MAIN_PATH) === null) {
      await this.#ydoc.writePath(MAIN_PATH, MAIN_SOURCE);
    }
  }

  async #attempt(
    binding: BootstrapBinding,
    attempt: number,
    signal?: AbortSignal,
  ): Promise<V2ReadyDescriptor> {
    try {
      await this.#ydoc.writePath(STATIC_PATH, canonicalJson({
        version: 1,
        binding,
        physicalMax: this.#physicalMax,
        acceptedMax: this.#acceptedMax,
        memoryBudgetBytes: this.#memoryBudgetBytes,
      }));
      await this.#ydoc.writePath(LAUNCHER_PATH, temporaryLauncher());
      await this.#gateway.syncWorkspace(signal);
    } catch (cause) {
      throw new V2BootstrapError("Unable to stage V2 bootstrap files", "deploy", attempt, { cause });
    }

    try {
      await this.#gateway.triggerRender({
        mainDocument: MAIN_PATH,
        ...await this.#ydoc.renderSyncState(),
      }, signal);
    } catch (cause) {
      throw new V2BootstrapError("Unable to trigger V2 sidecar render", "render", attempt, { cause });
    }

    const hello = await this.#waitForHello(binding, attempt, signal);
    const rotationKey = createCredentialRotationKey();
    let sealed: SealedBootstrapCredential;
    try {
      sealed = await sealBootstrapCredential({
        binding,
        sidecarPublicJwk: hello.publicJwk,
        credential: {
          url: this.#ySweet.url,
          baseUrl: this.#ySweet.baseUrl,
          docId: this.#ySweet.docId,
          token: this.#ySweet.token.reveal(),
          authorization: this.#ySweet.authorization,
          rotationKey,
        },
      });
    } catch (cause) {
      throw new V2BootstrapError("Unable to encrypt V2 bootstrap credential", "crypto", attempt, { cause });
    }

    const readiness: AwarenessEnvelope<SidecarFrame>[] = [];
    const unsubscribe = this.#awareness.subscribe(update => readiness.push(update.envelope));
    try {
      await this.#ydoc.writePath(CIPHERTEXT_PATH, canonicalJson(sealed));
      await this.#gateway.syncWorkspace(signal);
      const descriptor = await this.#waitForReadiness(
        binding,
        hello.instanceId,
        readiness,
        attempt,
        signal,
      );
      this.#rotationKey = rotationKey;
      return descriptor;
    } catch (cause) {
      if (cause instanceof V2BootstrapError) throw cause;
      if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
      throw new V2BootstrapError("Unable to publish V2 bootstrap ciphertext", "deploy", attempt, { cause });
    } finally {
      unsubscribe();
    }
  }

  async #waitForHello(
    binding: BootstrapBinding,
    attempt: number,
    signal?: AbortSignal,
  ): Promise<BootstrapHello> {
    const deadline = this.#clock.now() + this.#timeoutMs;
    let lastError: V2BootstrapError | null = null;
    for (;;) {
      if (signal?.aborted) throw new DOMException("V2 bootstrap aborted", "AbortError");
      let response: Awaited<ReturnType<V2BootstrapGateway["fetchText"]>>;
      try {
        response = await this.#gateway.fetchText("logs", { method: "GET", signal });
      } catch (cause) {
        lastError = new V2BootstrapError("Unable to read bootstrap render logs", "render", attempt, { cause });
        response = { status: 0, text: "", headers: new Headers() };
      }
      if (response.status >= 200 && response.status < 300) {
        for (const line of response.text.split(/\r?\n/)) {
          const index = line.indexOf(HELLO_PREFIX);
          if (index < 0) continue;
          const encoded = line.slice(index + HELLO_PREFIX.length).trim().split(/\s/, 1)[0]!;
          try {
            return parseHelloCandidate(encoded, binding, attempt);
          } catch (cause) {
            lastError = cause as V2BootstrapError;
          }
        }
      }
      if (this.#clock.now() >= deadline) {
        throw lastError ?? new V2BootstrapError(
          "Timed out waiting for the V2 bootstrap hello",
          "timeout",
          attempt,
        );
      }
      await this.#clock.sleep(Math.min(this.#pollMs, deadline - this.#clock.now()), signal);
    }
  }

  async #waitForReadiness(
    binding: BootstrapBinding,
    instanceId: string,
    readiness: readonly AwarenessEnvelope<SidecarFrame>[],
    attempt: number,
    signal?: AbortSignal,
  ): Promise<V2ReadyDescriptor> {
    const deadline = this.#clock.now() + this.#timeoutMs;
    for (;;) {
      const envelope = readiness.find(candidate => matchesReadiness(candidate, binding, instanceId));
      if (envelope) {
        return {
          instanceId,
          buildHash: binding.buildHash,
          schemaHash: binding.schemaHash,
          leaderGeneration: binding.leaderGeneration,
          capacity: envelope.credits,
        };
      }
      if (signal?.aborted) throw new DOMException("V2 bootstrap aborted", "AbortError");
      if (this.#clock.now() >= deadline) {
        throw new V2BootstrapError(
          "Sidecar readiness did not match the bootstrap binding and instance",
          "readiness",
          attempt,
        );
      }
      await this.#clock.sleep(Math.min(this.#pollMs, deadline - this.#clock.now()), signal);
    }
  }

  async #cleanupTransient(required: boolean): Promise<void> {
    try {
      await this.#ydoc.tombstonePath(CIPHERTEXT_PATH);
      await this.#ydoc.writePath(LAUNCHER_PATH, INERT_LAUNCHER);
      await this.#gateway.syncWorkspace();
    } catch (cause) {
      if (required) {
        throw new V2BootstrapError("Unable to restore the inert V2 launcher", "deploy", 1, { cause });
      }
    }
  }

  #controlFrame(frame: HostFrame): ReliableFrame<HostFrame> {
    return {
      ...frame,
      epoch: 1,
      sequence: this.#hostSequence++,
    } as ReliableFrame<HostFrame>;
  }

  #publishControl(
    binding: BootstrapBinding,
    frames: readonly ReliableFrame<HostFrame>[],
    draining: boolean,
  ): void {
    this.#awareness.publish({
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: binding.schemaHash,
      buildHash: binding.buildHash,
      projectId: this.#projectId,
      sessionId: binding.sandboxSessionId,
      peerId: "host-bootstrap",
      instanceId: `host-${binding.nonce}`,
      leaderGeneration: binding.leaderGeneration,
      epoch: 1,
      revision: this.#revision++,
      ack: this.#sidecarAck,
      credits: 0,
      active: 0,
      queued: 0,
      draining,
      frames,
    });
  }
}
