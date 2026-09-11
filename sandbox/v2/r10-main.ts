import { diagnosticLog } from "./diagnostic-log.ts";
import type { BootstrapBinding } from "../../src/v2/bootstrap/crypto.ts";
import { protocolBindingFromBootstrap } from "../../src/v2/bootstrap/binding.ts";
import { RequestAssembler } from "../../src/v2/sidecar/assembler.ts";
import {
  ClaimJournal,
  V2_JOURNAL_MAX_BYTES,
  V2_JOURNAL_MAX_RECORDS,
} from "../../src/v2/sidecar/journal.ts";
import { AdmissionController } from "../../src/v2/sidecar/admission.ts";
import { DirectResponsesClient } from "../../src/v2/sidecar/r10-upstream.ts";
import { SidecarEngine } from "../../src/v2/sidecar/r10-engine.ts";
import {
  phaseZeroBootstrap,
  resolveSidecarBootstrapHelloPath,
} from "./bootstrap.ts";
import { DenoAwarenessPeer } from "./r10-awareness-peer.ts";
import { DenoJournalStorage } from "./journal-storage.ts";
import { SidecarResourceSampler } from "../../src/v2/diagnostics/sidecar-sampler.ts";
import { SidecarDiagnosticsObserver } from "../../src/v2/diagnostics/r10-sidecar-observer.ts";
import { OutputBackpressure } from "../../src/v2/sidecar/output-sink.ts";
import { DenoSidecarOwnershipLease } from "./ownership-lease.ts";
import {
  SIDECAR_OWNERSHIP_ROOT,
  sidecarJournalRoot,
} from "./storage-paths.ts";
import {
  AtomicBytePermitPool,
} from "../../src/v2/memory/atomic-byte-permits.ts";
import type { V2TransportProfile } from "../../src/v2/protocol/shared.ts";
import { deriveV2SidecarEventMemorySizing } from "../../src/v2/memory/event-memory-sizing.ts";
import {
  runR10Supervisor,
  runR10SupervisorInstaller,
} from "./r10-supervisor.ts";
import { canonicalSandboxWorkspacePath } from "./workspace-root.ts";
import { finishR10SupervisorProcess } from "./r10-supervisor-receipt.ts";
import {
  V2_TERMINAL_SPOOL_CAPACITY,
  V2_TERMINAL_SPOOL_RESERVATION_BYTES,
  V2_TERMINAL_SPOOL_TOTAL_BYTES,
} from "../../src/v2/sidecar/capacity.ts";

const MAX_RESPONSE_EVENT_BYTES = V2_TERMINAL_SPOOL_RESERVATION_BYTES;
const SIDECAR_STATIC_RELATIVE_PATH = ".ghostprov-v2/static.json" as const;
const SIDECAR_CREDENTIAL_RELATIVE_PATH = ".ghostprov-v2/bootstrap.enc.json" as const;

function requiredWorkspacePath(relativePath: string): string {
  const path = canonicalSandboxWorkspacePath(
    Deno.env.get("GHOSTPROV_WORKSPACE_ROOT") ?? "",
    relativePath,
  );
  if (!path) throw new Error("Canonical sandbox workspace root is invalid");
  return path;
}

interface StaticSidecarConfig {
  version: 1;
  binding: BootstrapBinding;
  physicalMax: number;
  acceptedMax: number;
  initialActivePerSandbox: number;
  maxActivePerSandbox: number;
  memoryBudgetBytes: number;
  transportProfile: V2TransportProfile;
  diagnostics: { enabled: boolean; intervalMs: number };
  batching: boolean;
  /**
   * R9 sandbox-relay: how long phase-zero waits for the sealed bootstrap
   * credential after publishing the hello. The relay handshake (exec/agent
   * roundtrip) cannot fit the historical 30 s window, so the host configures
   * an explicit bound here. Omission selects the historical 30 s default.
   */
  bootstrapTimeoutMs?: number;
}

/** Explicit bound for the phase-zero credential wait: 30 s .. 10 min. */
const MIN_BOOTSTRAP_TIMEOUT_MS = 30_000;
const MAX_BOOTSTRAP_TIMEOUT_MS = 600_000;

function readConfig(value: unknown): StaticSidecarConfig {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Static sidecar config is invalid");
  }
  const raw = value as Record<string, unknown>;
  if (
    raw.version !== 1 ||
    !raw.binding ||
    typeof raw.binding !== "object" ||
    !Number.isInteger(raw.physicalMax) ||
    (raw.physicalMax as number) < 1 ||
    (raw.physicalMax as number) > 128 ||
    !Number.isInteger(raw.acceptedMax) ||
    (raw.acceptedMax as number) < (raw.physicalMax as number) ||
    !Number.isInteger(raw.initialActivePerSandbox) ||
    (raw.initialActivePerSandbox as number) < 1 ||
    !Number.isInteger(raw.maxActivePerSandbox) ||
    (raw.maxActivePerSandbox as number) < (raw.initialActivePerSandbox as number) ||
    (raw.maxActivePerSandbox as number) > (raw.physicalMax as number) ||
    (raw.maxActivePerSandbox as number) > V2_TERMINAL_SPOOL_CAPACITY ||
    !Number.isInteger(raw.memoryBudgetBytes) ||
    (raw.memoryBudgetBytes as number) < 64 * 1024 * 1024 ||
    (raw.transportProfile !== "standard" && raw.transportProfile !== "bulk-v1") ||
    !raw.diagnostics || typeof raw.diagnostics !== "object" ||
    typeof (raw.diagnostics as any).enabled !== "boolean" ||
    !Number.isInteger((raw.diagnostics as any).intervalMs) ||
    (raw.diagnostics as any).intervalMs < 100 || (raw.diagnostics as any).intervalMs > 60_000 ||
    typeof raw.batching !== "boolean" ||
    (
      raw.bootstrapTimeoutMs !== undefined &&
      (
        !Number.isSafeInteger(raw.bootstrapTimeoutMs) ||
        (raw.bootstrapTimeoutMs as number) < MIN_BOOTSTRAP_TIMEOUT_MS ||
        (raw.bootstrapTimeoutMs as number) > MAX_BOOTSTRAP_TIMEOUT_MS
      )
    )
  ) throw new Error("Static sidecar config fields are invalid");
  return {
    version: 1,
    binding: raw.binding as BootstrapBinding,
    physicalMax: raw.physicalMax as number,
    acceptedMax: raw.acceptedMax as number,
    initialActivePerSandbox: raw.initialActivePerSandbox as number,
    maxActivePerSandbox: raw.maxActivePerSandbox as number,
    memoryBudgetBytes: raw.memoryBudgetBytes as number,
    transportProfile: raw.transportProfile as V2TransportProfile,
    diagnostics: raw.diagnostics as StaticSidecarConfig["diagnostics"],
    batching: raw.batching as boolean,
    ...(raw.bootstrapTimeoutMs === undefined
      ? {}
      : { bootstrapTimeoutMs: raw.bootstrapTimeoutMs as number }),
  };
}

class SidecarLifecycleError extends Error {
  readonly name = "SidecarLifecycleError";
  constructor(message: string, readonly code: string, options?: ErrorOptions) {
    super(message, options);
  }
}

const CLEANUP_TIMEOUT_MS = 15_000;
let fatalPhase = "config";

async function settleCleanup(
  operation: () => void | Promise<void>,
  deadline: number,
  label: string,
): Promise<void> {
  const remaining = Math.max(0, deadline - performance.now());
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      Promise.resolve().then(operation).then(
        () => ({ kind: "complete" as const }),
        error => ({ kind: "failed" as const, error }),
      ),
      new Promise<{ kind: "timeout" }>(resolve => {
        timer = setTimeout(() => resolve({ kind: "timeout" }), remaining);
        (timer as any).unref?.();
      }),
    ]);
    if (result.kind === "timeout") {
      throw new SidecarLifecycleError(
        `${label} exceeded the sidecar cleanup deadline`,
        "cleanup_timeout",
      );
    }
    if (result.kind === "failed") throw result.error;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function run(): Promise<void> {
  diagnosticLog("sidecar").record("phase", { phase: "config" }, true);
  const config = readConfig(JSON.parse(
    await Deno.readTextFile(requiredWorkspacePath(SIDECAR_STATIC_RELATIVE_PATH)),
  ));
  const binding = protocolBindingFromBootstrap(config.binding, "r2");
  fatalPhase = "environment";
  diagnosticLog("sidecar").record("phase", { phase: "environment" }, true);
  const baseUrl = Deno.env.get("OPENAI_BASE_URL");
  const apiKey = Deno.env.get("OPENAI_API_KEY");
  if (!baseUrl || !apiKey) throw new Error("Required upstream environment is missing");
  // Reject a stale/delayed launcher before it can take the global ownership
  // lock and transiently exclude the correctly bound candidate.
  const launchIdentity = resolveSidecarBootstrapHelloPath({
    launchNonce: config.binding.nonce,
    pid: Deno.pid,
    args: Deno.args,
    root: SIDECAR_OWNERSHIP_ROOT,
  });
  fatalPhase = "ownership";
  diagnosticLog("sidecar").record("phase", { phase: "ownership" }, true);

  const lifecycle = new AbortController();
  let resolveShutdown!: (reason: string) => void;
  const shutdown = new Promise<string>(resolve => { resolveShutdown = resolve; });
  let signalReason: string | null = null;
  const requestSignal = (reason: string) => {
    if (signalReason) return;
    signalReason = reason;
    lifecycle.abort(new DOMException(`Sidecar received ${reason}`, "AbortError"));
    resolveShutdown(reason);
  };
  const onTerm = () => requestSignal("SIGTERM");
  const onInt = () => requestSignal("SIGINT");
  Deno.addSignalListener("SIGTERM", onTerm);
  try {
    Deno.addSignalListener("SIGINT", onInt);
  } catch (error) {
    Deno.removeSignalListener("SIGTERM", onTerm);
    throw error;
  }

  const instanceId = crypto.randomUUID();
  let ownership: DenoSidecarOwnershipLease | null = null;
  let diagnostics: SidecarResourceSampler | null = null;
  let outputBackpressure: OutputBackpressure | null = null;
  let journal: ClaimJournal | null = null;
  let engine: SidecarEngine | null = null;
  let peer: DenoAwarenessPeer | null = null;
  let atomicBytePermits: AtomicBytePermitPool | null = null;
  let maintenanceTimer: ReturnType<typeof setInterval> | null = null;
  let maintenanceTask: Promise<void> | null = null;
  let fatalCause: Error | null = null;
  let failure: unknown = null;

  try {
    // Ownership precedes the bootstrap hello so overlapping local launches
    // cannot race the shared journal or advertise a non-owning instance.
    ownership = await DenoSidecarOwnershipLease.acquire({
      instanceId,
      root: SIDECAR_OWNERSHIP_ROOT,
    });
    fatalPhase = "bootstrap";
    diagnosticLog("sidecar").record("phase", { phase: "bootstrap" }, true);
    const bootstrap = await phaseZeroBootstrap({
      binding: config.binding,
      instanceId,
      encryptedPath: requiredWorkspacePath(SIDECAR_CREDENTIAL_RELATIVE_PATH),
      helloPath: launchIdentity.helloPath,
      beforeHelloPublished: () => ownership!.promoteProcessIdentity(
        config.binding.nonce,
        launchIdentity.processStart,
      ),
      timeoutMs: config.bootstrapTimeoutMs,
      signal: lifecycle.signal,
    });
    if (bootstrap.instanceId !== instanceId) {
      throw new SidecarLifecycleError("Bootstrap changed the owned instance", "instance_mismatch");
    }

    fatalPhase = "storage";

    diagnosticLog("sidecar").record("phase", { phase: "storage" }, true);
    const storage = new DenoJournalStorage({
      // R1 and R2 journal schemas differ. Keeping the R2 journal disjoint
      // preserves an exact R2 -> frozen-R1 rollback while the global lock
      // above still prevents either revision from executing concurrently.
      root: sidecarJournalRoot(binding.protocolRevision),
      rootBoundary: SIDECAR_OWNERSHIP_ROOT,
      assertExclusiveOwnership: () => ownership!.assertOwned(),
      withExclusiveOwnership: operation => ownership!.withOwnership(operation),
      maxTerminalSpoolBytes: V2_TERMINAL_SPOOL_RESERVATION_BYTES,
      maxTerminalBytes: V2_TERMINAL_SPOOL_TOTAL_BYTES,
      maxActiveSpools: V2_TERMINAL_SPOOL_CAPACITY,
      maxJournalBytes: V2_JOURNAL_MAX_BYTES,
      maxJournalRecords: V2_JOURNAL_MAX_RECORDS,
    });
    diagnostics = new SidecarResourceSampler(config.diagnostics);
    diagnostics.start();
    const diagnosticObserver = new SidecarDiagnosticsObserver({
      enabled: config.diagnostics.enabled,
      sampler: diagnostics,
    });
    outputBackpressure = new OutputBackpressure({
      highFrames: 256,
      highBytes: 256 * 1024,
      lowFrames: 64,
      lowBytes: 64 * 1024,
      hardFrames: 1_024,
      hardBytes: 1_024 * 1024,
    });
    const eventMemory = deriveV2SidecarEventMemorySizing({
      totalBytes: config.memoryBudgetBytes,
      responseMaxBytes: MAX_RESPONSE_EVENT_BYTES,
    });
    const { ordinaryBytes } = eventMemory;
    atomicBytePermits = new AtomicBytePermitPool({
      ordinaryBytes,
      jumboBytes: eventMemory.jumboBytes,
      maxWaiters: config.acceptedMax,
    });
    journal = new ClaimJournal({
      storage,
      flushIntervalMs: 2,
      maxBatch: 64,
      maxJournalBytes: V2_JOURNAL_MAX_BYTES,
      maxJournalRecords: V2_JOURNAL_MAX_RECORDS,
      assertExclusiveOwnership: () => ownership!.assertOwned(),
      withExclusiveOwnership: operation => ownership!.withOwnership(operation),
    });
    const admission = new AdmissionController({
      physicalMax: Math.min(config.physicalMax, config.maxActivePerSandbox),
      acceptedMax: config.acceptedMax,
      hostCeiling: Math.min(config.physicalMax, config.maxActivePerSandbox),
      memoryBudgetBytes: ordinaryBytes,
    });
    const upstream = new DirectResponsesClient({
      baseUrl,
      apiKey: { reveal: () => apiKey },
      maxEventBytes: MAX_RESPONSE_EVENT_BYTES,
      atomicBytePermits,
    });
    let resolveDrain!: (reason: string) => void;
    const drainRequested = new Promise<string>(resolve => { resolveDrain = resolve; });
    let drainLatched = false;
    const requestDrain = (reason: string, cause?: Error) => {
      if (cause && !fatalCause) fatalCause = cause;
      if (drainLatched) return;
      drainLatched = true;
      resolveDrain(`awareness:${reason}`);
    };

    engine = new SidecarEngine({
      binding,
      assembler: new RequestAssembler({
        maxReservedBytes: ordinaryBytes,
        atomicBytePermits,
      }),
      journal,
      admission,
      upstream,
      onDrainRequested: reason => requestDrain(reason),
      onFatalError: error => requestDrain("journal_error", error),
      assertOwnership: () => ownership!.assertOwned(),
      assertExecutionFence: frame => {
        if (!peer) throw new Error("R2 Awareness peer is not initialized");
        peer.assertExecutionFence(frame);
      },
      diagnostics: diagnosticObserver,
      outputBackpressure,
      atomicBytePermits,
      deferAcceptUntilStarted: true,
      batching: config.batching,
      emit: (frame, priority, streamId) => peer?.enqueue(frame, priority, streamId),
    });
    peer = new DenoAwarenessPeer({
      credential: bootstrap.credential,
      binding,
      transportProfile: config.transportProfile,
      instanceId,
      engine,
      diagnostics: diagnosticObserver,
      outputBackpressure,
      initialActivePerSandbox: config.initialActivePerSandbox,
      maxActivePerSandbox: config.maxActivePerSandbox,
      onAcceptError: (error, frame) => requestDrain(`accept_${frame.kind}`, error),
      onEngineError: error => requestDrain("engine_error", error),
    });

    maintenanceTimer = setInterval(() => {
      if (maintenanceTask) return;
      const task = ownership!.withOwnership(async () => {
        engine!.maintenance();
      }).catch(error => {
        requestDrain(
          "ownership_lost",
          error instanceof Error ? error : new Error("Sidecar ownership was lost"),
        );
      });
      maintenanceTask = task;
      void task.finally(() => {
        if (maintenanceTask === task) maintenanceTask = null;
      });
    }, 60_000);
    (maintenanceTimer as any).unref?.();

    // Scan-only recovery makes readiness independent of replay delivery.
    fatalPhase = "engine_start";
    diagnosticLog("sidecar").record("phase", { phase: "engine_start" }, true);
    await engine.start(lifecycle.signal);
    fatalPhase = "awareness_connect";
    diagnosticLog("sidecar").record("phase", { phase: "awareness_connect" }, true);
    await peer.connect(lifecycle.signal);
    peer.publishEngineState();
    fatalPhase = "runtime";
    diagnosticLog("sidecar").record("phase", { phase: "runtime" }, true);
    const reason = await Promise.race([shutdown, drainRequested]);
    peer.freezeInbound();
    await engine.drain(reason);
    if (engine.snapshot().forcedDrain) {
      fatalCause ??= new SidecarLifecycleError(
        "Sidecar could not settle forced drain cancellation",
        "forced_drain",
      );
    }
    if (fatalCause) throw fatalCause;
  } catch (error) {
    const expectedSignalAbort = signalReason !== null &&
      error instanceof DOMException && error.name === "AbortError";
    if (!expectedSignalAbort || fatalCause !== null) failure = fatalCause ?? error;
  } finally {
    const cleanupErrors: unknown[] = [];
    const cleanupDeadline = performance.now() + CLEANUP_TIMEOUT_MS;
    const cleanup = async (operation: () => void | Promise<void>) => {
      try {
        await settleCleanup(operation, cleanupDeadline, "Sidecar cleanup operation");
      } catch (error) {
        cleanupErrors.push(error);
      }
    };
    if (maintenanceTimer !== null) clearInterval(maintenanceTimer);
    if (maintenanceTask) await cleanup(() => maintenanceTask!);
    if (peer) await cleanup(() => peer!.closeGracefully());
    outputBackpressure?.close();
    if (journal) await cleanup(() => journal!.close());
    if (atomicBytePermits) {
      await cleanup(() => {
        atomicBytePermits!.close();
        if (atomicBytePermits!.snapshot().activePermits !== 0) {
          throw new SidecarLifecycleError(
            "Sidecar atomic byte permits leaked during shutdown",
            "event_memory_leak",
          );
        }
      });
    }
    if (diagnostics) await cleanup(() => diagnostics!.close());
    if (ownership) await cleanup(() => ownership!.release());
    await cleanup(() => Deno.removeSignalListener("SIGTERM", onTerm));
    await cleanup(() => Deno.removeSignalListener("SIGINT", onInt));
    if (cleanupErrors.length) {
      failure = failure === null
        ? new AggregateError(cleanupErrors, "Sidecar cleanup failed")
        : new AggregateError([failure, ...cleanupErrors], "Sidecar failed and cleanup was incomplete");
    }
  }
  if (failure === null && fatalCause !== null) failure = fatalCause;
  if (failure !== null) throw failure;
}

const installerNonce = Deno.args.length === 1
  ? /^--ghostprov-install-supervisor=([A-Za-z0-9][A-Za-z0-9._:-]{0,127})$/.exec(Deno.args[0]!)?.[1]
  : undefined;
const supervisorMode = Deno.args.length === 2 &&
  /^--ghostprov-supervisor-nonce=/.test(Deno.args[0]!) &&
  /^--ghostprov-process-start=/.test(Deno.args[1]!);
const runtimeMode = installerNonce ? "supervisor_install" : supervisorMode ? "supervisor" : "sidecar";

try {
  if (installerNonce || supervisorMode) await finishR10SupervisorProcess(
    () => installerNonce ? runR10SupervisorInstaller(installerNonce) : runR10Supervisor(),
    () => diagnosticLog("supervisor").close(),
    () => Deno.exit(0),
  );
  else await run();
} catch (error) {
  const wrapper = error instanceof Error ? error : new Error("Unknown sidecar failure");
  let value = wrapper;
  const seen = new Set<Error>();
  while (value instanceof AggregateError && !seen.has(value) && seen.size < 6) {
    seen.add(value);
    const primary = value.errors.find((candidate): candidate is Error => candidate instanceof Error);
    if (!primary) break;
    value = primary;
  }
  const code = "code" in value && typeof (value as { code?: unknown }).code === "string"
    ? String((value as { code: string }).code).slice(0, 64)
    : "fatal";
  const permission = /Requires (read|write|env|net|sys|run|ffi) access/i
    .exec(value.message)?.[1]?.toLowerCase() ?? null;
  diagnosticLog(runtimeMode === "sidecar" ? "sidecar" : "supervisor").record("fatal", { error_name: value.name, error_code: code, phase: fatalPhase, permission }, true);
  console.error(JSON.stringify({
    event: runtimeMode === "sidecar"
      ? "ghostprov_v2_sidecar_fatal"
      : "ghostprov_v2_supervisor_fatal",
    error_name: value.name.slice(0, 64),
    error_code: code,
    cause_name: value.cause instanceof Error ? value.cause.name.slice(0, 64) : null,
    phase: runtimeMode === "sidecar" ? fatalPhase : runtimeMode,
    permission,
  }));
  Deno.exit(1);
}
