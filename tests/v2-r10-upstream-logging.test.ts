import { describe, expect, it } from "bun:test";
import { DiagnosticLog } from "../sandbox/v2/diagnostic-log";
import { DirectResponsesClient, UpstreamExecutionError } from "../src/v2/sidecar/r10-upstream";
import { parseDiagnosticLine } from "../src/v2/diagnostics/sandbox-log-schema";

function fixture() {
  const records: Record<string, unknown>[] = [];
  const log = new DiagnosticLog("sidecar", { append: bytes => { records.push(parseDiagnosticLine(new TextDecoder().decode(bytes).trimEnd())); }, rotate() {}, close() {} });
  return { log, records };
}
function response(events: Array<[string, unknown]>) {
  return new Response(events.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join(""),
    { headers: { "content-type": "text/event-stream" } });
}

describe("R10 upstream logging", () => {
  it("samples routine records while retaining counts, first text, tool readiness and backpressure timings", async () => {
    const { log, records } = fixture();
    let now = 0; let fetches = 0;
    const events: Array<[string, unknown]> = [
      ["response.created", { type: "response.created", response: { id: "resp_logging" } }],
      ...Array.from({ length: 96 }, () => ["response.output_text.delta", { type: "response.output_text.delta", delta: "private text" }] as [string, unknown]),
      ["response.output_item.added", { type: "response.output_item.added", item: { type: "function_call", name: "private_tool", arguments: "", call_id: "call_private" } }],
      ["response.function_call_arguments.done", { type: "response.function_call_arguments.done", arguments: '{"secret":"private arguments"}' }],
      ["response.completed", { type: "response.completed", response: { id: "resp_logging", object: "response", status: "completed", output: [], usage: { input_tokens: 10, output_tokens: 4, input_tokens_details: { cached_tokens: 9 } } } }],
    ];
    const client = new DirectResponsesClient({ baseUrl: "https://private.invalid/v1", apiKey: { reveal: () => "private-key" }, diagnosticLog: log, now: () => now,
      fetch: async (_url, init) => {
        fetches++; now += 400;
        const body = JSON.parse(await new Response(init?.body).text());
        expect(body).toMatchObject({ stream: true, store: true, previous_response_id: "resp_exact" });
        return response(events);
      } });
    const received: string[] = [];
    const terminal = await client.execute({ diagnosticOrdinal: 7,
      body: new TextEncoder().encode(JSON.stringify({ model: "model", input: "private prompt", previous_response_id: "resp_exact", stream: false, store: false })),
      beforeFetch: async () => { now += 5; }, onPrelude: async () => {},
      onEvent: async event => { received.push(event.event); now += received.length === 6 ? 60 : 1; } });
    expect(fetches).toBe(1); expect(terminal.responseId).toBe("resp_logging");
    expect(received).not.toContain("response.completed");
    const summary = records.find(record => record.event === "upstream_terminal")!;
    expect(summary).toMatchObject({ request: 7, events: events.length, input_tokens: 10, output_tokens: 4, cached_tokens: 9, event_delivery_max_ms: 60 });
    expect(typeof summary.first_text_ms).toBe("number"); expect(typeof summary.first_tool_ms).toBe("number"); expect(typeof summary.tool_ready_ms).toBe("number");
    expect(records.filter(record => record.event === "upstream_event").length).toBeLessThan(16);
    expect(records.some(record => record.event === "event_delivery" && record.duration_ms === 60)).toBe(true);
    expect(JSON.stringify(records)).not.toContain("private");
    expect(log.snapshot().dropped).toBe(0);
  });

  it("logs a definitive 429 once without sleeping, replaying, or recording its body", async () => {
    const { log, records } = fixture(); let fetches = 0;
    const client = new DirectResponsesClient({ baseUrl: "https://offline.invalid/v1", apiKey: { reveal: () => "offline" }, diagnosticLog: log,
      fetch: async () => { fetches++; return Response.json({ error: { message: "private rate-limit detail", type: "rate_limit_error", code: "rate_limit_exceeded" } }, { status: 429 }); } });
    await expect(client.execute({ diagnosticOrdinal: 2, body: new TextEncoder().encode('{"model":"test","input":"private"}'),
      beforeFetch: async () => {}, onPrelude: async () => {}, onEvent: async () => {} })).rejects.toBeInstanceOf(UpstreamExecutionError);
    expect(fetches).toBe(1);
    expect(records.filter(record => record.event === "upstream_error")).toHaveLength(1);
    expect(records.find(record => record.event === "upstream_error")).toMatchObject({ status: 429, error_code: "rate_limit" });
    expect(JSON.stringify(records)).not.toContain("private");
  });
});
