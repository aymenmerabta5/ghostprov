import { resolveGhostProvApiKey } from "../src/http/api-key";
import {
  DEFAULT_CANDIDATE_PORT,
  PROTECTED_SERVICE_PORT,
} from "../src/core/ports";

interface RuntimeSnapshot {
  state: string;
  capacity: number;
  active: number;
  quarantined: number;
}

interface RouterSnapshot {
  capacity: number;
  active: number;
  available: number;
  queued: number;
  runtimes: RuntimeSnapshot[];
}

interface ProofResponse {
  id: string;
  status: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function outputText(response: any): string {
  const parts: string[] = [];
  for (const item of Array.isArray(response?.output) ? response.output : []) {
    if (item?.type !== "message") continue;
    for (const content of Array.isArray(item.content) ? item.content : []) {
      if (content?.type === "output_text" && typeof content.text === "string") parts.push(content.text);
    }
  }
  return parts.join("\n").trim();
}

export function validateProofResponse(raw: string, expected: string): ProofResponse {
  let response: any;
  try { response = JSON.parse(raw); }
  catch { throw new Error("Live proof received invalid JSON"); }
  if (typeof response?.id !== "string" || !response.id) throw new Error("Live proof response is missing an ID");
  if (response.status !== "completed") throw new Error(`Live proof response ${response.id} is ${String(response.status)}`);
  const actual = outputText(response);
  if (actual !== expected) {
    throw new Error(`Live proof response ${response.id} returned ${JSON.stringify(actual.slice(0, 160))}, expected ${expected}`);
  }
  return { id: response.id, status: response.status };
}

export function percentile(values: readonly number[], fraction: number): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.max(0, Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1));
  return sorted[index];
}

async function sandboxSnapshot(root: string, apiKey: string): Promise<RouterSnapshot> {
  const response = await fetch(`${root}/__sandboxes`, {
    headers: { authorization: `Bearer ${apiKey}` },
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error(`Sandbox monitoring failed: ${response.status}`);
  return response.json() as Promise<RouterSnapshot>;
}

async function waitReady(root: string, apiKey: string): Promise<RouterSnapshot> {
  const deadline = Date.now() + 5 * 60_000;
  for (;;) {
    const snapshot = await sandboxSnapshot(root, apiKey);
    if (snapshot.runtimes.length >= 4 && snapshot.runtimes.every(runtime => runtime.state === "ready")) return snapshot;
    if (Date.now() >= deadline) throw new Error(`Candidate runtimes did not become ready: ${JSON.stringify(snapshot.runtimes)}`);
    await sleep(1_000);
  }
}

async function runStage(options: {
  candidate: URL;
  candidateRoot: string;
  model: string;
  timeoutMs: number;
  concurrency: number;
  capacity: number;
  apiKey: string;
}): Promise<{ summary: Record<string, unknown>; ids: string[] }> {
  const { candidate, candidateRoot, model, timeoutMs, concurrency, capacity, apiKey } = options;
  const peakRuntimeActive = [0, 0, 0, 0];
  let runtimeActiveAtPeak = [0, 0, 0, 0];
  let peakPhysicalActive = 0;
  let queuePeak = 0;
  let quarantinePeak = 0;
  let monitoring = true;
  let monitorError: unknown;
  const sample = async () => {
    const snapshot = await sandboxSnapshot(candidateRoot, apiKey);
    const active = snapshot.runtimes.map(runtime => runtime.active);
    const physicalActive = active.reduce((sum, value) => sum + value, 0);
    if (physicalActive > peakPhysicalActive) {
      peakPhysicalActive = physicalActive;
      runtimeActiveAtPeak = active;
    }
    for (let index = 0; index < Math.min(peakRuntimeActive.length, active.length); index++) {
      peakRuntimeActive[index] = Math.max(peakRuntimeActive[index], active[index]);
    }
    queuePeak = Math.max(queuePeak, snapshot.queued);
    quarantinePeak = Math.max(
      quarantinePeak,
      snapshot.runtimes.reduce((sum, runtime) => sum + runtime.quarantined, 0),
    );
  };
  const monitor = (async () => {
    while (monitoring) {
      try { await sample(); }
      catch (error) { monitorError = error; return; }
      await sleep(25);
    }
    try { await sample(); } catch (error) { monitorError = error; }
  })();

  const stageStarted = performance.now();
  let results: Array<{ id: string; durationMs: number }>;
  try {
    results = await Promise.all(Array.from({ length: concurrency }, async (_, index) => {
      const expected = `RAMP_${concurrency}_${index}`;
      const started = performance.now();
      const response = await fetch(`${candidate.origin}${candidate.pathname.replace(/\/$/, "")}/responses`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${apiKey}`,
          "x-ghostprov-client-id": `load-${index % 8}`,
        },
        body: JSON.stringify({
          model,
          store: false,
          input: `Reply with exactly ${expected}`,
          reasoning: { effort: concurrency === capacity ? "medium" : "low" },
        }),
        signal: AbortSignal.timeout(timeoutMs),
      });
      const raw = await response.text();
      if (!response.ok) throw new Error(`Ramp ${concurrency} request ${index} failed (${response.status}): ${raw.slice(0, 300)}`);
      const proof = validateProofResponse(raw, expected);
      return { id: proof.id, durationMs: performance.now() - started };
    }));
  } finally {
    monitoring = false;
    await monitor;
  }
  if (monitorError) throw monitorError;

  const ids = results.map(result => result.id);
  const uniqueResponseIds = new Set(ids).size;
  if (uniqueResponseIds !== concurrency) throw new Error(`Ramp ${concurrency} produced duplicate response IDs`);
  if (quarantinePeak !== 0) throw new Error(`Ramp ${concurrency} created ${quarantinePeak} quarantined lanes`);
  if (queuePeak !== 0) throw new Error(`Ramp ${concurrency} queued work despite available physical capacity`);
  const targetPhysicalActive = Math.min(concurrency, capacity);
  const minimumObservedActive = Math.ceil(targetPhysicalActive * 0.98);
  if (peakPhysicalActive < minimumObservedActive) {
    throw new Error(`Ramp ${concurrency} reached ${peakPhysicalActive}/${targetPhysicalActive} physical lanes`);
  }
  if (peakRuntimeActive.some(active => active > 30)) throw new Error(`Ramp ${concurrency} oversubscribed a runtime`);

  const durations = results.map(result => result.durationMs);
  return {
    ids,
    summary: {
      concurrency,
      durationMs: Math.round(performance.now() - stageStarted),
      requestP50Ms: Math.round(percentile(durations, 0.50)),
      requestP95Ms: Math.round(percentile(durations, 0.95)),
      requestMaxMs: Math.round(Math.max(...durations)),
      peakPhysicalActive,
      targetPhysicalActive,
      observedOccupancyPercent: Math.round((peakPhysicalActive / targetPhysicalActive) * 10_000) / 100,
      runtimeActiveAtPeak,
      peakRuntimeActive,
      queuePeak,
      quarantinePeak,
      uniqueResponseIds,
    },
  };
}

export async function main(env: Record<string, string | undefined> = process.env): Promise<void> {
  if (env.GP_LIVE_VERIFY !== "1") throw new Error("Set GP_LIVE_VERIFY=1 to run the multi-sandbox load ramp");
  const candidate = new URL(
    env.OPENAI_BASE_URL ?? `http://127.0.0.1:${DEFAULT_CANDIDATE_PORT}/v1`,
  );
  if (Number(candidate.port) === PROTECTED_SERVICE_PORT) {
    throw new Error(`Live multi-sandbox proof refuses protected port ${PROTECTED_SERVICE_PORT}`);
  }
  if (Number(candidate.port) !== DEFAULT_CANDIDATE_PORT) {
    throw new Error(`Live multi-sandbox proof is hard-fenced to port ${DEFAULT_CANDIDATE_PORT}`);
  }
  const candidateRoot = `${candidate.protocol}//${candidate.host}`;
  const model = env.GP_LIVE_MODEL ?? "gpt-5.6-sol";
  const timeoutMs = Number(env.GP_LIVE_TIMEOUT_MS ?? 20 * 60_000);
  const apiKey = resolveGhostProvApiKey(env.GHOSTPROV_API_KEY);
  if (!Number.isFinite(timeoutMs) || timeoutMs < 1) throw new Error("GP_LIVE_TIMEOUT_MS must be positive");

  const ready = await waitReady(candidateRoot, apiKey);
  const stages: Record<string, unknown>[] = [];
  const allIds = new Set<string>();
  for (const concurrency of [16, 32, 64, 96, 120]) {
    const stage = await runStage({
      candidate,
      candidateRoot,
      model,
      timeoutMs,
      concurrency,
      capacity: ready.capacity,
      apiKey,
    });
    for (const id of stage.ids) {
      if (allIds.has(id)) throw new Error(`Duplicate response ID across stages: ${id}`);
      allIds.add(id);
    }
    stages.push(stage.summary);
  }
  console.log(JSON.stringify({
    projects: ready.runtimes.length,
    capacity: ready.capacity,
    totalResponses: allIds.size,
    stages,
  }, null, 2));
}

if (import.meta.main) await main();
