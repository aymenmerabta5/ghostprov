import type { RunHandle } from "../src/relay/run-handle";
import { sha256Hex } from "../src/v2/protocol/shared";
import { createBrowserlessSystem } from "../src/server/system";
import { resolveLiveV2Options } from "./live_v2_options";

const PRIMARY = "http://127.0.0.1:8787";

function percentile(values: readonly number[], fraction: number): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)] ?? 0;
}

async function health(root: string): Promise<void> {
  const response = await fetch(`${root}/health`, { signal: AbortSignal.timeout(10_000) });
  const body = await response.json().catch(() => null) as any;
  if (!response.ok || body?.ok !== true) throw new Error(`${root} is not healthy`);
}

async function execute(
  count: number,
  start: (index: number) => Promise<RunHandle>,
  snapshots: () => readonly { active: number }[],
): Promise<{
  ids: string[];
  firstEventP95Ms: number;
  totalMs: number;
  peakActive: number[];
}> {
  const peakActive: number[] = [];
  let monitoring = true;
  const monitor = (async () => {
    while (monitoring) {
      const current = snapshots();
      for (let index = 0; index < current.length; index++) {
        peakActive[index] = Math.max(peakActive[index] ?? 0, current[index]!.active);
      }
      await Bun.sleep(5);
    }
  })();
  const stageStarted = performance.now();
  try {
    const results = await Promise.all(Array.from({ length: count }, async (_, index) => {
      const started = performance.now();
      const handle = await start(index);
      let firstEventMs: number | null = null;
      const consuming = (async () => {
        for await (const _event of handle.events()) {
          firstEventMs ??= performance.now() - started;
        }
      })();
      const final = await handle.final;
      await consuming;
      if (typeof final.id !== "string" || !final.id) throw new Error("Live V2 response ID is missing");
      return {
        id: final.id,
        firstEventMs: firstEventMs ?? performance.now() - started,
      };
    }));
    return {
      ids: results.map(result => result.id),
      firstEventP95Ms: percentile(results.map(result => result.firstEventMs), 0.95),
      totalMs: performance.now() - stageStarted,
      peakActive,
    };
  } finally {
    monitoring = false;
    await monitor;
  }
}

export async function main(
  env: Record<string, string | undefined> = process.env,
): Promise<void> {
  const options = resolveLiveV2Options(env, { requireScale: true });
  if (options.projectCount !== 4) {
    throw new Error("Fleet canary requires GP_LIVE_PROJECT_COUNT=4");
  }
  const projectIds = (env.PRISM_PROJECT_IDS ?? "").split(",").map(value => value.trim()).filter(Boolean);
  if (projectIds.length !== 4) throw new Error("Fleet canary requires exactly four PRISM_PROJECT_IDS UUIDs");
  if (!env.PRISM_COOKIE_FILE?.trim()) throw new Error("PRISM_COOKIE_FILE is required");
  await health(PRIMARY);
  const model = env.GP_LIVE_MODEL ?? "gpt-5.6-sol";
  const system = await createBrowserlessSystem({
    ...env,
    GP_CANDIDATE: "1",
    PORT: String(options.port),
  });
  const summaries: Record<string, unknown>[] = [];
  try {
    await system.start();
    await system.fleet.waitForReady(4);
    const runtimes = system.registry.runtimes();
    const snapshots = () => system.registry.snapshots();
    const body = (marker: string) => ({
      model,
      store: false,
      stream: true,
      input: `Reply with exactly ${marker}`,
      reasoning: { effort: "low" },
    });

    const direct128 = await execute(
      128,
      index => runtimes[0]!.run(body(`V2_DIRECT_128_${index}`)),
      snapshots,
    );
    if (direct128.peakActive[0] > 128) throw new Error("Direct stage exceeded 128 active calls");
    if (direct128.firstEventP95Ms >= 4_000) throw new Error("Direct 128 first-event p95 exceeded 4 seconds");
    summaries.push({ stage: "direct-128", ...direct128, ids: direct128.ids.length });

    const accepted256 = await execute(
      256,
      index => runtimes[0]!.run(body(`V2_ACCEPTED_256_${index}`)),
      snapshots,
    );
    if (accepted256.peakActive[0] > 128) throw new Error("Accepted stage exceeded 128 active calls");
    summaries.push({ stage: "accepted-256", ...accepted256, ids: accepted256.ids.length });

    const routed512 = await execute(
      512,
      index => system.router.startRelay(body(`V2_ROUTED_512_${index}`), {
        clientId: `client-${index % 32}`,
        source: "internal",
      }),
      snapshots,
    );
    if (routed512.peakActive.some(active => active > 128)) {
      throw new Error("Routed stage exceeded a per-sandbox 128-call cap");
    }
    if (routed512.firstEventP95Ms >= 7_000) {
      throw new Error("Routed 512 first-event p95 exceeded 7 seconds");
    }
    summaries.push({ stage: "routed-512", ...routed512, ids: routed512.ids.length });

    const allIds = [...direct128.ids, ...accepted256.ids, ...routed512.ids];
    if (new Set(allIds).size !== allIds.length) throw new Error("Live V2 stages returned duplicate response IDs");

    const large = "ghostprov-live-".repeat(Math.ceil((4 * 1024 * 1024) / 15))
      .slice(0, 4 * 1024 * 1024);
    const largeHandle = await runtimes[0]!.run({ model, store: false, stream: true, input: large });
    const consumeLarge = Array.fromAsync(largeHandle.events());
    const largeFinal = await largeHandle.final;
    await consumeLarge;
    const output = (largeFinal.output as any[])
      ?.flatMap(item => item.content ?? [])
      .find(item => item.type === "output_text")?.text;
    if (typeof output !== "string" || await sha256Hex(output) !== await sha256Hex(large)) {
      throw new Error("Live V2 4 MiB integrity check failed");
    }
    summaries.push({ stage: "4mib-integrity", sha256: await sha256Hex(large) });

    if (options.soak) {
      for (let cycle = 0; cycle < 5; cycle++) {
        const handle = await system.router.startRelay(body(`V2_SOAK_${cycle}`));
        await Promise.all([handle.final, Array.fromAsync(handle.events())]);
      }
      summaries.push({ stage: "soak", cycles: 5 });
    }
  } finally {
    await system.close();
  }
  await health(PRIMARY);
  console.log(JSON.stringify({
    live: true,
    projectCount: 4,
    summaries,
    projectsDeleted: 0,
    primary8787Healthy: true,
  }, null, 2));
}

if (import.meta.main) await main();
