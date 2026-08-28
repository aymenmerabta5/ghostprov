import { DirectResponsesClient } from "../../src/v2/sidecar/upstream.ts";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function completeSse(): Response {
  const final = {
    id: "resp_deno_retry",
    object: "response",
    status: "completed",
    output: [],
  };
  return new Response(
    `event: response.created\ndata: ${JSON.stringify({
      type: "response.created",
      response: { id: final.id },
    })}\n\nevent: response.completed\ndata: ${JSON.stringify({
      type: "response.completed",
      response: final,
    })}\n\n`,
    { status: 200, headers: { "content-type": "text/event-stream" } },
  );
}

Deno.test("R7 retries one definitive invalid_json from immutable validated text", async () => {
  const body = new TextEncoder().encode(JSON.stringify({
    model: "gpt-5.6-sol",
    input: [{ role: "user", content: "deno canonical body" }],
    stream: true,
  }));
  let calls = 0;
  let boundaries = 0;
  const client = new DirectResponsesClient({
    baseUrl: "https://openai.invalid/v1",
    apiKey: { reveal: () => "test-key" },
    fetch: async (_url, init) => {
      calls++;
      if (calls === 1) {
        assert(init?.body instanceof Blob, "first R7 body must be an immutable Blob");
        return Response.json({
          error: {
            message: "Invalid body: failed to parse JSON value.",
            type: "invalid_request_error",
            code: "invalid_json",
          },
        }, { status: 400 });
      }
      assert(typeof init?.body === "string", "retry body must be immutable text");
      body.fill(0);
      const parsed = JSON.parse(init.body) as Record<string, unknown>;
      assert(parsed.model === "gpt-5.6-sol", "retry body did not preserve the model");
      return completeSse();
    },
  });

  const terminal = await client.execute({
    body,
    bodyIsCanonicalStream: true,
    beforeFetch: async () => { boundaries++; },
    onPrelude: async () => {},
    onEvent: async () => {},
  });

  assert(terminal.status === "completed", "retry did not complete");
  assert(calls === 2, `expected two fetches, received ${calls}`);
  assert(boundaries === 1, `execution boundary repeated ${boundaries} times`);
});
