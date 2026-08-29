import { describe, expect, it } from "bun:test";
import {
  apiKeyAuthorized,
  apiKeyMatches,
  createAuthenticatedTenantScope,
  resolveGhostProvApiKey,
} from "../src/http/api-key";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";

const API_KEY = "test-api-key-0123456789abcdef-0123456789";

function router(): RouterLike {
  const authorizationScope = createAuthenticatedTenantScope();
  return {
    responseProvenanceAuthorizationScope: () => authorizationScope,
    async startRelay() { throw new Error("authentication test must not relay"); },
    snapshot: () => ({
      capacity: 1,
      active: 0,
      available: 1,
      queued: 0,
      runtimes: [],
      resources: {},
    }),
  };
}

describe("GhostProv inbound API-key authentication", () => {
  it("uses constant-size comparison and exact standard header forms", () => {
    expect(() => resolveGhostProvApiKey(undefined)).toThrow("required");
    expect(() => resolveGhostProvApiKey("short-key")).toThrow("at least 32");
    expect(resolveGhostProvApiKey(API_KEY)).toBe(API_KEY);
    expect(apiKeyMatches(API_KEY, API_KEY)).toBe(true);
    expect(apiKeyMatches("wrong", API_KEY)).toBe(false);
    expect(apiKeyMatches(null, API_KEY)).toBe(false);
    expect(apiKeyAuthorized({ authorization: `Bearer ${API_KEY}` }, API_KEY, "openai"))
      .toBe(true);
    expect(apiKeyAuthorized({ xApiKey: API_KEY }, API_KEY, "openai")).toBe(false);
    expect(apiKeyAuthorized({ xApiKey: API_KEY }, API_KEY, "anthropic")).toBe(true);
    expect(apiKeyAuthorized({ authorization: `bearer ${API_KEY}` }, API_KEY, "anthropic"))
      .toBe(true);
    expect(apiKeyAuthorized({ authorization: `Bearer ${API_KEY},wrong` }, API_KEY, "openai"))
      .toBe(false);
    expect(apiKeyAuthorized({ xApiKey: [API_KEY, API_KEY] }, API_KEY, "anthropic"))
      .toBe(false);
  });

  it("protects every v1 route while leaving health telemetry available", async () => {
    const app = createGhostProvApp({ router: router(), apiKey: API_KEY });
    expect((await app.request("/health")).status).toBe(200);
    expect((await app.request("/__sandboxes")).status).toBe(401);
    expect((await app.request("/__sandboxes", {
      headers: { authorization: `Bearer ${API_KEY}` },
    })).status).toBe(200);

    const openAiAttempts: Array<Record<string, string> | undefined> = [
      undefined,
      { authorization: "Bearer wrong" },
      { "x-api-key": API_KEY },
    ];
    for (const headers of openAiAttempts) {
      const response = await app.request("/v1/models", { headers });
      expect(response.status).toBe(401);
      expect(await response.json()).toEqual({
        error: {
          message: "Invalid API key",
          type: "authentication_error",
          param: null,
          code: "invalid_api_key",
        },
      });
    }

    const authorized = await app.request("/v1/models", {
      headers: { authorization: `Bearer ${API_KEY}` },
    });
    expect(authorized.status).toBe(200);
  });

  it("returns Anthropic authentication envelopes and accepts either Anthropic key form", async () => {
    const app = createGhostProvApp({ router: router(), apiKey: API_KEY });
    const missing = await app.request("/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json", "anthropic-version": "2023-06-01" },
      body: "{}",
    });
    expect(missing.status).toBe(401);
    expect(await missing.json()).toMatchObject({
      type: "error",
      error: { type: "authentication_error", message: "Invalid API key" },
      request_id: expect.stringMatching(/^req_/),
    });

    const anthropicAttempts: Array<Record<string, string>> = [
      { "x-api-key": API_KEY },
      { authorization: `Bearer ${API_KEY}` },
    ];
    for (const headers of anthropicAttempts) {
      const response = await app.request("/v1/messages", {
        method: "POST",
        headers: {
          ...headers,
          "content-type": "application/json",
          "anthropic-version": "2023-06-01",
        },
        body: "{}",
      });
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({
        type: "error",
        error: { type: "invalid_request_error", message: "model is required" },
      });
    }
  });

  it("rejects unauthorized network requests before body-memory admission", async () => {
    let admissions = 0;
    const value = router();
    value.acquireIncomingBody = async () => {
      admissions++;
      throw new Error("unauthorized body must not be admitted");
    };
    const app = createGhostProvApp({ router: value, apiKey: API_KEY });
    const server = createGhostProvServer({
      app,
      router: value,
      apiKey: API_KEY,
      port: 0,
      candidate: true,
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    try {
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("missing test address");
      const response = await fetch(`http://127.0.0.1:${address.port}/v1/responses`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
      });
      expect(response.status).toBe(401);
      expect(admissions).toBe(0);
    } finally {
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  });
});
