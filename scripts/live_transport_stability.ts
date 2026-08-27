
import "dotenv/config";
import WebSocket from "ws";
import { writeFile, rename } from "node:fs/promises";
import path from "node:path";

const ROOT = "http://127.0.0.1:18888";
const CONCURRENCY = Number(process.env.GP_TEST_CONCURRENCY ?? 8);
const WAVES = Number(process.env.GP_TEST_WAVES ?? 5);
const TARGET_BYTES = Number(process.env.GP_TEST_REQUEST_BYTES ?? Math.floor(2.9 * 1024 * 1024));
const TARGET_BYTES_LIST = process.env.GP_TEST_REQUEST_BYTES_LIST?.split(",")
  .map(value => Number(value.trim())) ?? null;
const START_STAGGER_MS = Number(process.env.GP_TEST_START_STAGGER_MS ?? 0);
const START_BURST = Number(process.env.GP_TEST_START_BURST ?? 1);
const REPORT_DIR = process.env.GP_TEST_REPORT_DIR;
if (process.env.GP_LIVE_TEST_STABILITY !== "1") throw new Error("GP_LIVE_TEST_STABILITY=1 required");
if (!REPORT_DIR) throw new Error("GP_TEST_REPORT_DIR required");
if (!Number.isInteger(CONCURRENCY) || CONCURRENCY < 1 || CONCURRENCY > 16) throw new Error("concurrency must be 1..16");
if (!Number.isInteger(WAVES) || WAVES < 1 || WAVES > 20) throw new Error("waves must be 1..20");
if (!Number.isInteger(TARGET_BYTES) || TARGET_BYTES < 128 * 1024 || TARGET_BYTES > 3 * 1024 * 1024) throw new Error("request bytes out of test bound");
if (TARGET_BYTES_LIST && (
  TARGET_BYTES_LIST.length !== CONCURRENCY ||
  TARGET_BYTES_LIST.some(value => !Number.isInteger(value) || value < 128 * 1024 || value > 3 * 1024 * 1024)
)) throw new Error("request byte list must exactly match concurrency and test bounds");
if (!Number.isInteger(START_STAGGER_MS) || START_STAGGER_MS < 0 || START_STAGGER_MS > 2_000) throw new Error("start stagger out of test bound");
if (!Number.isInteger(START_BURST) || START_BURST < 1 || START_BURST > 8) throw new Error("start burst out of test bound");
const apiKey = process.env.GHOSTPROV_API_KEY;
if (!apiKey) throw new Error("GHOSTPROV_API_KEY missing");
const headers = { authorization: "Bearer " + apiKey };

let seed = 0x6d2b79f5;
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
function entropy(length: number): string {
  const chunks: string[] = [];
  for (let offset = 0; offset < length; offset += 8192) {
    const count = Math.min(8192, length - offset);
    const chars = new Array<string>(count);
    for (let index = 0; index < count; index++) {
      seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5;
      chars[index] = alphabet[(seed >>> 0) & 63]!;
    }
    chunks.push(chars.join(""));
  }
  return chunks.join("");
}
const empty = {
  type: "response.create",
  stream_id: "probe-w00-r00",
  model: "gpt-5.6-sol",
  store: false,
  stream: true,
  service_tier: "fast",
  max_output_tokens: 16,
  input: "Reply with OK.",
  x_ghostprov_transport_probe: "",
};
const fixedBytes = Buffer.byteLength(JSON.stringify(empty));
const requestSizes = TARGET_BYTES_LIST ?? Array.from({ length: CONCURRENCY }, () => TARGET_BYTES);
const paddings = requestSizes.map(bytes => entropy(bytes - fixedBytes));

function percentile(values: number[], q: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(sorted.length * q) - 1)] ?? 0;
}
function compactFleet(value: any) {
  return {
    atMs: performance.now(),
    capacity: value.capacity,
    active: value.active,
    available: value.available,
    queued: value.queued,
    ready: value.ready,
    runtimes: (value.runtimes ?? []).map((r: any) => ({
      projectId: r.projectId,
      state: r.state,
      incarnation: r.incarnation,
      reason: r.availabilityReason,
      capacity: r.capacity,
      active: r.active,
      available: r.available,
      peerAgeMs: r.sidecarPeerAgeMs,
      rttMs: r.ackEwmaMs,
      host: r.diagnostics?.host ? {
        windowFrames: r.diagnostics.host.windowFrames,
        schedulerFrames: r.diagnostics.host.schedulerFrames,
        ackProgressAgeMs: r.diagnostics.host.ackProgressAgeMs,
        retransmissionAttempt: r.diagnostics.host.retransmissionAttempt,
        retransmissionsTotal: r.diagnostics.host.retransmissionsTotal,
        stallEventsTotal: r.diagnostics.host.stallEventsTotal,
        uniqueAcknowledgedBytesTotal: r.diagnostics.host.uniqueAcknowledgedBytesTotal,
      } : null,
    })),
  };
}
async function fleetSnapshot() {
  const response = await fetch(ROOT + "/__sandboxes", { headers });
  if (!response.ok) throw new Error("fleet snapshot failed " + response.status);
  return compactFleet(await response.json());
}
async function waitIdle() {
  let stable = 0;
  const deadline = performance.now() + 60_000;
  while (performance.now() < deadline) {
    const snapshot = await fleetSnapshot();
    stable = snapshot.ready === 8 && snapshot.active === 0 && snapshot.queued === 0 ? stable + 1 : 0;
    if (stable >= 3) return;
    await Bun.sleep(250);
  }
  throw new Error("fleet did not become idle and fully ready");
}
async function runOne(wave: number, request: number) {
  const streamId = "probe-w" + String(wave).padStart(2, "0") + "-r" + String(request).padStart(2, "0");
  const body = { ...empty, stream_id: streamId, x_ghostprov_transport_probe: paddings[request] };
  const encoded = JSON.stringify(body);
  if (Buffer.byteLength(encoded) !== requestSizes[request]) throw new Error("request size drifted");
  const started = performance.now();
  const socket = new WebSocket("ws://127.0.0.1:18888/v1/responses", {
    headers: { authorization: "Bearer " + apiKey },
  });
  return await new Promise<any>((resolve, reject) => {
    let firstAt: number | null = null;
    let lastAt = started;
    let maxGapMs = 0;
    let messages = 0;
    let responseBytes = 0;
    const timer = setTimeout(() => {
      socket.terminate();
      reject(new Error("request " + wave + "/" + request + " timed out"));
    }, 60_000);
    const finish = (result: any) => {
      clearTimeout(timer);
      socket.close();
      resolve(result);
    };
    socket.once("open", () => socket.send(encoded));
    socket.on("message", raw => {
      const now = performance.now();
      firstAt ??= now;
      maxGapMs = Math.max(maxGapMs, now - lastAt);
      lastAt = now;
      messages++;
      responseBytes += raw.length;
      let event: any;
      try { event = JSON.parse(raw.toString()); } catch { return; }
      if (["error", "response.failed", "response.incomplete", "response.completed"].includes(event.type)) {
        finish({
          terminal: event.type,
          code: event.error?.code ?? null,
          firstMs: (firstAt ?? now) - started,
          totalMs: now - started,
          maxGapMs,
          messages,
          responseBytes,
        });
      }
    });
    socket.once("error", error => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

const report: any = { schema: 1, startedAt: new Date().toISOString(), concurrency: CONCURRENCY, waves: WAVES, requestBytes: TARGET_BYTES_LIST ?? TARGET_BYTES, results: [], samples: [] };
for (let wave = 0; wave < WAVES; wave++) {
  await waitIdle();
  let monitoring = true;
  const monitor = (async () => {
    while (monitoring) {
      try { report.samples.push(await fleetSnapshot()); } catch {}
      await Bun.sleep(250);
    }
  })();
  const results = await Promise.all(Array.from({ length: CONCURRENCY }, async (_, index) => {
    if (START_STAGGER_MS > 0) {
      await Bun.sleep(Math.max(0, index - START_BURST + 1) * START_STAGGER_MS);
    }
    return runOne(wave, index);
  }));
  monitoring = false;
  await monitor;
  const failures = results.filter(value => value.terminal !== "error" || value.code !== "unknown_parameter");
  if (failures.length) throw new Error("unexpected terminal in wave " + wave);
  report.results.push(results);
  console.log(JSON.stringify({
    wave,
    completed: results.length,
    firstP95Ms: percentile(results.map(value => value.firstMs), 0.95),
    totalP95Ms: percentile(results.map(value => value.totalMs), 0.95),
    maxGapMs: Math.max(...results.map(value => value.maxGapMs)),
  }));
  await Bun.sleep(500);
}
report.completedAt = new Date().toISOString();
const all = report.results.flat();
report.summary = {
  requests: all.length,
  expectedErrors: all.filter((value: any) => value.terminal === "error" && value.code === "unknown_parameter").length,
  firstP50Ms: percentile(all.map((value: any) => value.firstMs), 0.5),
  firstP95Ms: percentile(all.map((value: any) => value.firstMs), 0.95),
  totalP50Ms: percentile(all.map((value: any) => value.totalMs), 0.5),
  totalP95Ms: percentile(all.map((value: any) => value.totalMs), 0.95),
  maxMs: Math.max(...all.map((value: any) => value.totalMs)),
};
const target = path.join(REPORT_DIR, "transport-stability-" + Date.now() + ".json");
const temporary = target + "." + process.pid + ".tmp";
await writeFile(temporary, JSON.stringify(report, null, 2));
await rename(temporary, target);
console.log(JSON.stringify({ report: target, summary: report.summary }, null, 2));
