if (process.env.GP_LIVE_VERIFY !== "1") {
  throw new Error("Set GP_LIVE_VERIFY=1 to run the multi-sandbox load ramp");
}

const candidate = new URL(process.env.OPENAI_BASE_URL ?? "http://127.0.0.1:18787/v1");
if (candidate.port === "8787") throw new Error("Live multi-sandbox proof refuses protected port 8787");
const candidateRoot = `${candidate.protocol}//${candidate.host}`;
const primaryRoot = "http://127.0.0.1:8787";
const model = process.env.GP_LIVE_MODEL ?? "gpt-5.6-sol";
const timeoutMs = Number(process.env.GP_LIVE_TIMEOUT_MS ?? 20 * 60_000);

async function health(root: string): Promise<any> {
  const response = await fetch(`${root}/health`, { signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`${root} health failed: ${response.status}`);
  return response.json();
}

async function waitReady(): Promise<any> {
  const deadline = Date.now() + 5 * 60_000;
  for (;;) {
    const response = await fetch(`${candidateRoot}/__sandboxes`);
    const snapshot = await response.json() as any;
    const runtimes = Array.isArray(snapshot.runtimes) ? snapshot.runtimes : [];
    if (runtimes.length >= 4 && runtimes.every((runtime: any) => runtime.state === "ready")) return snapshot;
    if (Date.now() >= deadline) throw new Error(`Candidate runtimes did not become ready: ${JSON.stringify(runtimes)}`);
    await new Promise(resolve => setTimeout(resolve, 1_000));
  }
}

async function runStage(concurrency: number): Promise<any> {
  const started = performance.now();
  const requests = Array.from({ length: concurrency }, (_, index) => fetch(`${candidate.origin}${candidate.pathname.replace(/\/$/, "")}/responses`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer live-proof", "x-ghostprov-client-id": `load-${index % 8}` },
    body: JSON.stringify({ model, store: false, input: `Reply with exactly RAMP_${concurrency}_${index}` }),
    signal: AbortSignal.timeout(timeoutMs),
  }).then(async response => ({ status: response.status, body: await response.text() })));
  const results = await Promise.all(requests);
  const failed = results.filter(result => result.status !== 200);
  if (failed.length) throw new Error(`Ramp ${concurrency} failed ${failed.length} requests: ${failed[0].body.slice(0, 300)}`);
  const snapshot = await (await fetch(`${candidateRoot}/__sandboxes`)).json() as any;
  return {
    concurrency,
    durationMs: Math.round(performance.now() - started),
    runtimeActive: snapshot.runtimes?.map((runtime: any) => runtime.active),
    quarantined: snapshot.runtimes?.reduce((sum: number, runtime: any) => sum + runtime.quarantined, 0),
  };
}

const primaryBefore = await health(primaryRoot);
const ready = await waitReady();
const stages = [];
for (const concurrency of [16, 32, 64, 96, 120]) stages.push(await runStage(concurrency));
const primaryAfter = await health(primaryRoot);
if (!primaryBefore.ok || !primaryAfter.ok) throw new Error("Protected port 8787 lost health during candidate proof");
console.log(JSON.stringify({
  projects: ready.runtimes.length,
  capacity: ready.capacity,
  stages,
  primary8787HealthyBefore: primaryBefore.ok,
  primary8787HealthyAfter: primaryAfter.ok,
}, null, 2));
