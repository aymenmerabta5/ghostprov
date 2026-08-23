import type { BootstrapBinding } from "../../src/v2/bootstrap/crypto.ts";
import { protocolBindingFromBootstrap } from "../../src/v2/bootstrap/binding.ts";
import { RequestAssembler } from "../../src/v2/sidecar/assembler.ts";
import { ClaimJournal } from "../../src/v2/sidecar/journal.ts";
import { AdmissionController } from "../../src/v2/sidecar/admission.ts";
import { DirectResponsesClient } from "../../src/v2/sidecar/upstream.ts";
import { SidecarEngine } from "../../src/v2/sidecar/engine.ts";
import { phaseZeroBootstrap } from "./bootstrap.ts";
import { DenoAwarenessPeer } from "./awareness-peer.ts";
import { DenoJournalStorage } from "./journal-storage.ts";
import { SidecarResourceSampler } from "../../src/v2/diagnostics/sidecar-sampler.ts";
import { SidecarDiagnosticsObserver } from "../../src/v2/diagnostics/sidecar-observer.ts";
import { OutputBackpressure } from "../../src/v2/sidecar/output-sink.ts";

interface StaticSidecarConfig {
  version: 1;
  binding: BootstrapBinding;
  physicalMax: number;
  acceptedMax: number;
  memoryBudgetBytes: number;
  diagnostics: { enabled: boolean; intervalMs: number };
  batching: boolean;
}

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
    !Number.isInteger(raw.memoryBudgetBytes) ||
    (raw.memoryBudgetBytes as number) < 64 * 1024 * 1024 ||
    !raw.diagnostics || typeof raw.diagnostics !== "object" ||
    typeof (raw.diagnostics as any).enabled !== "boolean" ||
    !Number.isInteger((raw.diagnostics as any).intervalMs) ||
    (raw.diagnostics as any).intervalMs < 100 || (raw.diagnostics as any).intervalMs > 60_000 ||
    typeof raw.batching !== "boolean"
  ) throw new Error("Static sidecar config fields are invalid");
  return {
    version: 1,
    binding: raw.binding as BootstrapBinding,
    physicalMax: raw.physicalMax as number,
    acceptedMax: raw.acceptedMax as number,
    memoryBudgetBytes: raw.memoryBudgetBytes as number,
    diagnostics: raw.diagnostics as StaticSidecarConfig["diagnostics"],
    batching: raw.batching as boolean,
  };
}

async function run(): Promise<void> {
  const config = readConfig(JSON.parse(
    await Deno.readTextFile("/workspace/.ghostprov-v2/static.json"),
  ));
  const bootstrap = await phaseZeroBootstrap({
    binding: config.binding,
    encryptedPath: "/workspace/.ghostprov-v2/bootstrap.enc.json",
  });
  const binding = protocolBindingFromBootstrap(config.binding);
  const baseUrl = Deno.env.get("OPENAI_BASE_URL");
  const apiKey = Deno.env.get("OPENAI_API_KEY");
  if (!baseUrl || !apiKey) throw new Error("Required upstream environment is missing");

  const storage = new DenoJournalStorage();
  const diagnostics = new SidecarResourceSampler(config.diagnostics);
  diagnostics.start();
  const diagnosticObserver = new SidecarDiagnosticsObserver({ enabled: config.diagnostics.enabled, sampler: diagnostics });
  const outputBackpressure = new OutputBackpressure({ highFrames:256,highBytes:256*1024,lowFrames:64,lowBytes:64*1024,hardFrames:1024,hardBytes:1024*1024 });
  const journal = new ClaimJournal({
    storage,
    flushIntervalMs: 2,
    maxBatch: 64,
  });
  const admission = new AdmissionController({
    physicalMax: config.physicalMax,
    acceptedMax: config.acceptedMax,
    hostCeiling: config.physicalMax,
    memoryBudgetBytes: config.memoryBudgetBytes,
  });
  const upstream = new DirectResponsesClient({
    baseUrl,
    apiKey: { reveal: () => apiKey },
  });
  let resolveAwarenessDrain!: (reason: string) => void;
  const awarenessDrain = new Promise<string>(resolve => {
    resolveAwarenessDrain = resolve;
  });
  let awarenessDrainRequested = false;
  let peer: DenoAwarenessPeer | null = null;
  const engine = new SidecarEngine({
    binding,
    assembler: new RequestAssembler({
      maxReservedBytes: config.memoryBudgetBytes,
    }),
    journal,
    admission,
    upstream,
    onDrainRequested: reason => {
      if (awarenessDrainRequested) return;
      awarenessDrainRequested = true;
      resolveAwarenessDrain(`awareness:${reason}`);
    },
    diagnostics: diagnosticObserver,
    outputBackpressure,
    batching: config.batching,
    emit: (frame, priority, streamId) => {
      peer?.enqueue(frame, priority, streamId);
    },
  });
  peer = new DenoAwarenessPeer({
    credential: bootstrap.credential,
    binding,
    instanceId: bootstrap.instanceId,
    engine,
    diagnostics: diagnosticObserver,
    outputBackpressure,
  });

  await engine.start();
  await peer.connect();

  let resolveSignal!: (reason: string) => void;
  const shutdownSignal = new Promise<string>(resolve => {
    resolveSignal = resolve;
  });
  const onTerm = () => resolveSignal("SIGTERM");
  const onInt = () => resolveSignal("SIGINT");
  Deno.addSignalListener("SIGTERM", onTerm);
  Deno.addSignalListener("SIGINT", onInt);
  try {
    const reason = await Promise.race([shutdownSignal, awarenessDrain]);
    await engine.drain(reason);
    await peer.close();
  } finally {
    await diagnostics.close();
    outputBackpressure.close();
    Deno.removeSignalListener("SIGTERM", onTerm);
    Deno.removeSignalListener("SIGINT", onInt);
  }
}

try {
  await run();
} catch {
  console.error("GhostProv V2 sidecar failed");
  Deno.exit(1);
}
