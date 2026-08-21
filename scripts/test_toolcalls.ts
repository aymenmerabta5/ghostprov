/**
 * Wire-level tool-call integrity test against localhost:8787/v1/responses.
 *
 * Turn 1: force a function call via tools[], capture RAW SSE frames and assert:
 *   - ordered Responses-API events arrive verbatim (created → deltas → done → completed)
 *   - concatenated response.function_call_arguments.delta == output_item.done arguments
 *   - == the arguments inside the final response.completed output[]
 *   - exactly one response.completed; zero gp_* private keys leak to the client
 * Turn 2: replay the loop like Codex does (append function_call_output) and
 *   assert the model answers normally — proves the multi-request agent loop.
 */
const BASE = process.env.GP_BASE ?? "http://localhost:8787";
const MODEL = process.env.GP_MODEL ?? "gpt-5.6-luna";

interface Frame { event: string; data: string; }

async function stream(body: Record<string, unknown>): Promise<Frame[]> {
  const res = await fetch(`${BASE}/v1/responses`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ...body, stream: true, store: false }),
  });
  if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
  const text = await res.text(); // small responses; read fully then split
  const frames: Frame[] = [];
  for (const chunk of text.split("\n\n")) {
    let event = "message";
    const dataLines: string[] = [];
    for (const line of chunk.split("\n")) {
      if (line.startsWith(":")) continue; // keepalive comment
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
    }
    if (dataLines.length) frames.push({ event, data: dataLines.join("\n") });
  }
  return frames;
}

function countEvent(frames: Frame[], ev: string): number {
  return frames.filter((f) => f.event === ev).length;
}

let failures = 0;
function check(name: string, ok: boolean, detail = ""): void {
  console.log(` ${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`);
  if (!ok) failures++;
}

// ---------------------------------------------------------------- turn 1 ---
console.log(`turn 1: forcing function call (model=${MODEL})…`);
const t1 = await stream({
  model: MODEL,
  input: "You must call the get_weather tool for the city Paris. Do not answer in words.",
  tools: [{
    type: "function",
    name: "get_weather",
    description: "Get current weather for a city",
    parameters: {
      type: "object",
      properties: { city: { type: "string" } },
      required: ["city"],
    },
  }],
});

const types = [...new Set(t1.map((f) => f.event))];
console.log(`events seen (${t1.length} frames):`, types.join(", "));
check("stream non-empty", t1.length > 3);
check("response.created present", countEvent(t1, "response.created") === 1);
check("exactly one response.completed", countEvent(t1, "response.completed") === 1);
check("completed is last frame", t1[t1.length - 1].event === "response.completed");

const addedFc = t1.find((f) => {
  try { return JSON.parse(f.data)?.item?.type === "function_call"; } catch { return false; }
});
check("output_item.added(function_call)", !!addedFc);

const deltaArgs = t1
  .filter((f) => f.event === "response.function_call_arguments.delta")
  .map((f) => { try { return JSON.parse(f.data)?.delta ?? ""; } catch { return ""; } })
  .join("");
check("argument deltas arrived", deltaArgs.length > 0, `${deltaArgs.length} chars`);

const doneFcFrame = t1.find((f) => {
  try { return f.event === "response.output_item.done" && JSON.parse(f.data)?.item?.type === "function_call"; } catch { return false; }
});
const doneItem = doneFcFrame ? JSON.parse(doneFcFrame.data).item : null;
check("output_item.done(function_call)", !!doneItem);

if (doneItem) {
  const argsStr = doneItem.arguments ?? "";
  check("deltas === done.arguments", deltaArgs === argsStr,
    `delta=${deltaArgs.length}B done=${argsStr.length}B`);
  let parsed: any = null;
  try { parsed = JSON.parse(argsStr); } catch {}
  check("done.arguments valid JSON", parsed !== null, argsStr.slice(0, 120));
}

const completedFrame = t1[t1.length - 1];
const completed = JSON.parse(completedFrame.data);
const fcOut = (completed.response?.output ?? []).find((o: any) => o.type === "function_call");
check("completed.output has function_call", !!fcOut);
if (fcOut && doneItem) {
  check("completed arguments match deltas", fcOut.arguments === deltaArgs,
    `final=${String(fcOut.arguments).length}B`);
  check("completed.call_id matches", fcOut.call_id === doneItem.call_id);
}
check("completed.status=completed", completed.response?.status === "completed");

const leaked = t1.some((f) => /"gp_(sse|ack|total|frames)"/.test(f.data));
check("no gp_* private keys leaked", !leaked);

if (!fcOut) {
  console.log("\nNo function call produced — cannot continue to turn 2.");
  process.exit(1);
}

// ---------------------------------------------------------------- turn 2 ---
console.log("\nturn 2: replaying agent loop (function_call_output)…");
const t2 = await stream({
  model: MODEL,
  input: [
    { role: "user", content: "You must call the get_weather tool for the city Paris. Do not answer in words." },
    fcOut,
    {
      type: "function_call_output",
      call_id: fcOut.call_id,
      output: JSON.stringify({ city: "Paris", temp_c: 18, sky: "cloudy" }),
    },
  ],
  tools: [{
    type: "function",
    name: "get_weather",
    description: "Get current weather for a city",
    parameters: {
      type: "object",
      properties: { city: { type: "string" } },
      required: ["city"],
    },
  }],
});
const txt2 = t2
  .filter((f) => f.event === "response.output_text.delta")
  .map((f) => { try { return JSON.parse(f.data)?.delta ?? ""; } catch { return ""; } })
  .join("");
check("turn2 completed once", countEvent(t2, "response.completed") === 1);
check("turn2 answered in text", txt2.trim().length > 0, txt2.slice(0, 100));
console.log(`\nturn2 answer: ${txt2.slice(0, 160)}`);
if (!txt2.trim()) {
  const types2 = [...new Set(t2.map((f) => f.event))];
  console.log("turn2 events:", types2.join(", "));
  for (const f of t2.slice(0, 12)) {
    console.log(`  [${f.event}] ${f.data.slice(0, 220)}`);
  }
}

console.log(`\n${failures === 0 ? "ALL CHECKS PASSED" : `${failures} CHECK(S) FAILED`}`);
process.exit(failures === 0 ? 0 : 1);
