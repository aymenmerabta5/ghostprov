import { describe, expect, it } from "bun:test";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { parseFrames } from "../src/relay/sse";
import { createGhostProvApp } from "../src/server/app";
import { parseFrame } from "../src/v2/protocol/validate";
import {
  PROVIDER_ERROR_MESSAGE_MAX_BYTES,
  createProviderErrorEnvelope,
  parseProviderErrorEnvelope,
} from "../src/v2/provider-error";
import { SidecarEngine as R10SidecarEngine } from "../src/v2/sidecar/r10-engine";
import { normalizeR10ProviderErrorEnvelope } from "../src/v2/sidecar/r10-provider-error";
import { DirectResponsesClient } from "../src/v2/sidecar/upstream";
import { DirectResponsesClient as R10DirectResponsesClient } from "../src/v2/sidecar/r10-upstream";
import { V2CommittedError, V2RunHandle } from "../src/v2/transport/run-handle";
import { completedUpstream, SidecarHarness, TEST_R2_BINDING } from "./support/fake-v2-sidecar";

const BODY = { model: "gpt-5.6-sol", input: "rate-limit isolation", stream: true, store: true };
const TPM_MESSAGE = "Rate limit reached for gpt-5.6-sol in organization org-Example123 " +
  "on tokens per min (TPM): Limit 40000000, Used 40000000, Requested 609930. " +
  "Please try again in 914ms. Visit https://platform.openai.com/account/rate-limits. ";

describe.each([
  ["R10 engine with frozen base client", R10SidecarEngine, DirectResponsesClient],
  ["R10", R10SidecarEngine, R10DirectResponsesClient],
] as const)("%s long provider rate-limit isolation", (_revision, engineClass, Client) => {
  for (const origin of ["http", "sse"] as const) {
    for (const identity of ["code", "type"] as const) {
      for (const [messageCase, messageBytes] of [["plain", 700], ["plain", 12_000], ["redaction boundary", 2_048]] as const) {
        it(`keeps a ${messageBytes}-byte ${messageCase} ${origin} ${identity} rejection definitive and accepts the next request`, async () => {
          const message = messageCase === "plain"
            ? TPM_MESSAGE.repeat(Math.ceil(messageBytes / TPM_MESSAGE.length)).slice(0, messageBytes)
            : TPM_MESSAGE.padEnd(2_032, " ") + "authorization: fake";
          const providerValue = { error: {
            message,
            type: identity === "type" ? "rate_limit_exceeded" : "rate_limit_error",
            param: null,
            ...(identity === "code" ? { code: "rate_limit_exceeded" } : {}),
          }, api_key: "fixture-secret-must-not-cross" };
          let fetches = 0;
          const client = new Client({
            baseUrl: "https://provider.invalid/v1",
            apiKey: { reveal: () => "fixture-key" },
            fetch: async () => {
              fetches++;
              const headers = {
                "retry-after": "0.914",
                "x-ratelimit-limit-tokens": "40000000",
                "x-ratelimit-remaining-tokens": "0",
                "set-cookie": "fixture-cookie-must-not-cross",
              };
              return origin === "http"
                ? Response.json(providerValue, { status: 429, headers })
                : new Response(`event: error\ndata: ${JSON.stringify(providerValue)}\n\n`, {
                    status: 200,
                    headers: { ...headers, "content-type": "text/event-stream" },
                  });
            },
          });
          const pool = new AtomicBytePermitPool({ ordinaryBytes: 10 * 1024 * 1024, jumboBytes: 1 });
          const fatals: Error[] = [];
          const drains: string[] = [];
          let executions = 0;
          const harness = new SidecarHarness({
            engineClass,
            binding: TEST_R2_BINDING,
            atomicBytePermits: pool,
            onFatalError: error => fatals.push(error),
            onDrainRequested: reason => drains.push(reason),
            behavior: input => ++executions === 1 ? client.execute(input) : completedUpstream(input),
          });
          try {
            await harness.sendRequest(BODY);
            await harness.settle();
            const errors = harness.frames.filter(frame => frame.kind === "response_error");
            expect(errors).toHaveLength(1);
            const error = errors[0]!;
            expect(error).toMatchObject({
              code: "rate_limit_exceeded",
              ambiguous: false,
              rateLimit: { retryAfterMs: 914, limitTokens: 40_000_000, remainingTokens: 0 },
              providerError: { origin, status: 429 },
            });
            expect(parseFrame({ ...error, epoch: 1, sequence: 1 }, "sidecar", "r2"))
              .toMatchObject(error);
            expect(error.message.length).toBeLessThanOrEqual(512);
            expect(error.providerError!.error.message).toBe(messageCase === "plain"
              ? message.slice(0, PROVIDER_ERROR_MESSAGE_MAX_BYTES)
              : TPM_MESSAGE.padEnd(2_032, " ") + "authorization: ");
            expect(parseProviderErrorEnvelope(error.providerError)).toBe(error.providerError!);
            if (messageBytes > 8 * 1024) expect(error.providerError!.payload.truncated).toBe(true);
            else if (messageCase === "plain") expect(JSON.parse(error.providerError!.payload.text).error.message).toBe(message);
            expect(JSON.stringify(error)).not.toContain("fixture-secret-must-not-cross");
            expect(JSON.stringify(error)).not.toContain("fixture-cookie-must-not-cross");
            expect(harness.journal.status(harness.requestId)).toBe("provider_error");
            expect(harness.journal.canRetry(harness.requestId)).toBe(false);
            expect(harness.engine.snapshot()).toMatchObject({ ready: true, active: 0, ambiguous: 0, draining: false });
            expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
            expect(fetches).toBe(1);
            expect(fatals).toEqual([]);
            expect(drains).toEqual([]);

            harness.requestId = "req-after-long-rate-limit";
            await harness.sendRequest({ ...BODY, input: "next" });
            await harness.settle();
            expect(executions).toBe(2);
            expect(fetches).toBe(1);
            expect(harness.frames).toContainEqual(expect.objectContaining({
              kind: "response_terminal", requestId: harness.requestId, terminalStatus: "completed",
            }));
            expect(harness.engine.snapshot()).toMatchObject({ ready: true, active: 0, ambiguous: 0, draining: false });
            expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
            expect(fatals).toEqual([]);
            expect(drains).toEqual([]);
          } finally {
            pool.close();
          }
        });
      }
    }
  }
});

describe("public long provider rate-limit errors", () => {
  for (const origin of ["http", "sse"] as const) {
    it(`returns the bounded ${origin} status and envelope once without relay retry`, async () => {
      const providerError = await normalizeR10ProviderErrorEnvelope(await createProviderErrorEnvelope({
        origin,
        status: 429,
        headers: new Headers({ "retry-after": "0.914" }),
        value: { error: {
          message: TPM_MESSAGE.padEnd(2_032, " ") + "authorization: fake",
          type: "rate_limit_error",
          code: "rate_limit_exceeded",
          param: null,
        } },
      }));
      expect(parseProviderErrorEnvelope(providerError)).toBe(providerError);
      const handle = new V2RunHandle({ expectUpstreamPrelude: true });
      let relayStarts = 0;
      const router = {
        snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
        startRelay: async () => { relayStarts++; return handle; },
      };
      const pending = Promise.resolve(createGhostProvApp({ router }).request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(BODY),
      }));
      if (origin === "sse") {
        handle.acceptUpstreamPrelude({ status: 200, contentType: "text/event-stream", upstreamRequestId: null });
        await pending;
      }
      const rejected = new V2CommittedError(
        providerError.error.message, "rate_limit_exceeded", false, { retryAfterMs: 914 }, providerError,
      );
      handle.fail(rejected);
      handle.fail(rejected);
      const response = await pending;
      if (origin === "http") {
        expect(response.status).toBe(429);
        expect(response.headers.get("retry-after")).toBe("0.914");
        expect(await response.json()).toEqual({ error: providerError.error });
      } else {
        expect(response.status).toBe(200);
        const frames = parseFrames(await response.text(), 0).frames;
        expect(frames).toHaveLength(1);
        expect(frames[0]!.event).toBe("response.failed");
        expect(JSON.parse(frames[0]!.data)).toEqual({
          type: "response.failed", status: 429, headers: { "retry-after": "0.914" },
          response: { status: "failed", error: providerError.error },
        });
      }
      expect(relayStarts).toBe(1);
    });
  }
});
