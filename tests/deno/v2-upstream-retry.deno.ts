import { DirectResponsesClient } from "../../src/v2/sidecar/upstream.ts";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

Deno.test("R8 normalizes stored streaming policy and never replays invalid_json", async () => {
  const input = {
    model: "gpt-5.6-sol",
    input: [{ role: "user", content: "deno canonical body" }],
    stream: false,
    store: false,
    previous_response_id: "resp_deno_parent",
    vendor_extension: { retain: "verbatim" },
  };
  const body = new TextEncoder().encode(JSON.stringify(input));
  const original = body.slice();
  let calls = 0;
  let boundaries = 0;
  const forwarded: Record<string, unknown>[] = [];
  const client = new DirectResponsesClient({
    baseUrl: "https://openai.invalid/v1",
    apiKey: { reveal: () => "test-key" },
    fetch: async (_url, init) => {
      calls++;
      assert(typeof init?.body === "string", "R8 policy repair must serialize immutable text");
      forwarded.push(JSON.parse(init.body) as Record<string, unknown>);
      return Response.json({
        error: {
          message: "Invalid body: failed to parse JSON value.",
          type: "invalid_request_error",
          code: "invalid_json",
        },
      }, { status: 400 });
    },
  });

  let error: unknown;
  try {
    await client.execute({
      body,
      bodyIsCanonicalStream: true,
      beforeFetch: async () => { boundaries++; },
      onPrelude: async () => {},
      onEvent: async () => {},
    });
  } catch (cause) {
    error = cause;
  }

  assert(error instanceof Error, "expected the definitive provider rejection");
  assert("code" in error && error.code === "invalid_json", "unexpected provider error code");
  assert(calls === 1, `expected one fetch, received ${calls}`);
  assert(boundaries === 1, `execution boundary repeated ${boundaries} times`);
  assert(forwarded.length === 1, "normalized body was not forwarded exactly once");
  const normalized = forwarded[0]!;
  assert(normalized.stream === true, "R8 did not force stream:true");
  assert(normalized.store === true, "R8 did not force store:true");
  assert(
    normalized.previous_response_id === input.previous_response_id,
    "R8 did not preserve previous_response_id",
  );
  assert(
    JSON.stringify(normalized.vendor_extension) === JSON.stringify(input.vendor_extension),
    "R8 did not preserve extension fields",
  );
  assert(
    body.every((value, index) => value === original[index]),
    "R8 mutated caller-owned request bytes",
  );
});
