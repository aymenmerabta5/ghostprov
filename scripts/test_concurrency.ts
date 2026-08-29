/** Concurrency test: fire N parallel requests at /v1/responses, report timings. */
import { resolveGhostProvApiKey } from "../src/http/api-key";

if (process.env.GP_LIVE_VERIFY !== "1") {
  throw new Error("Set GP_LIVE_VERIFY=1 to run the paid candidate concurrency test");
}
const N = Number(process.argv[2] ?? "2");
if (!Number.isSafeInteger(N) || N < 1 || N > 40) throw new RangeError("Concurrency must be 1 through 40");
const SERVER = process.env.GP_BASE ?? "http://127.0.0.1:18888";
if (!/^http:\/\/(?:127\.0\.0\.1|localhost|\[::1\]):(?:18888)(?:\/)?$/.test(SERVER)) {
  throw new Error("GP_BASE must target the isolated loopback candidate on port 18888");
}
const API_KEY = resolveGhostProvApiKey(process.env.GHOSTPROV_API_KEY);

function body(i: number): string {
  return JSON.stringify({
    model: "gpt-5.6-sol",
    input: `Reply with exactly: CONC_${i}_OK`,
    stream: false,
    store: false,
    service_tier: "fast",
    reasoning: { effort: "low" },
  });
}

async function one(i: number): Promise<{ i: number; ms: number; status: number; text: string | null; err?: string }> {
  const t0 = Date.now();
  try {
    const r = await fetch(`${SERVER}/v1/responses`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${API_KEY}`,
      },
      body: body(i),
    });
    const j: any = await r.json().catch(() => null);
    const ms = Date.now() - t0;
    let text: string | null = null;
    try {
      for (const item of j?.output ?? []) {
        for (const c of item?.content ?? []) {
          if (c?.type === "output_text") text = (text ? text + " " : "") + c.text;
        }
      }
    } catch {}
    return { i, ms, status: r.status, text };
  } catch (e: any) {
    return { i, ms: Date.now() - t0, status: 0, text: null, err: String(e?.message ?? e).slice(0, 120) };
  }
}

console.log(`firing ${N} parallel requests…`);
const t0 = Date.now();
const results = await Promise.all(Array.from({ length: N }, (_, k) => one(k + 1)));
const wall = Date.now() - t0;

for (const r of results) {
  console.log(`  req${r.i}: ${r.status} in ${r.ms}ms  text=${JSON.stringify(r.text)}${r.err ? " err=" + r.err : ""}`);
}
const ok = results.filter((r) => r.status === 200);
console.log(`WALL: ${wall}ms  ok=${ok.length}/${N}  avg=${Math.round(results.reduce((s, r) => s + r.ms, 0) / N)}ms`);
process.exit(0);
