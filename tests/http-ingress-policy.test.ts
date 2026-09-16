import { describe, expect, it } from "bun:test";
import { request as httpRequest } from "node:http";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import { STANDALONE_SEARCH_MAX_REQUEST_BYTES } from "../src/search/limits";

const API_KEY = "ingress-policy-offline-key";
type Admission = { bytes: number; multiplier?: number; maxBytes?: number };
async function fixture(rejectAdmission: boolean, apiKey: string | null = API_KEY) {
  const admissions: Admission[] = [];
  let releases = 0;
  const scope = createAuthenticatedTenantScope();
  const router: RouterLike = {
    responseProvenanceAuthorizationScope: () => scope,
    startRelay: async () => { throw new Error("Invalid offline requests must not relay"); },
    snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
    acquireIncomingBody: async (bytes, _signal, options) => {
      admissions.push({ bytes, multiplier: options?.reservationMultiplier, maxBytes: options?.maxBytes });
      if (rejectAdmission) throw new Error("Offline probe stops before allocation");
      let released = false;
      return { release() { if (!released) { released = true; releases++; } } };
    },
  };
  const app = createGhostProvApp({ router, apiKey: apiKey ?? undefined,
    standaloneSearch: async () => { throw new Error("Invalid offline search must not relay"); },
  });
  const server = createGhostProvServer({ app, router, apiKey: apiKey ?? undefined, port: 0, candidate: true });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing offline listener address");
  return {
    admissions, releases: () => releases,
    async send(path: string, options: {
      method?: string; headers?: Record<string, string>; body?: string; declared?: number;
    } = {}) {
      return new Promise<{ status: number; headers: import("node:http").IncomingHttpHeaders; body: string }>((resolve, reject) => {
        const req = httpRequest({
          host: "127.0.0.1", port: address.port, path,
          method: options.method ?? "POST",
          headers: {
            connection: "close",
            ...(options.headers?.["transfer-encoding"] ? {} : {
              "content-length": String(options.declared ?? Buffer.byteLength(options.body ?? "")),
            }),
            ...options.headers,
          },
        }, res => {
          const chunks: Buffer[] = [];
          res.on("data", chunk => chunks.push(Buffer.from(chunk)));
          res.on("error", reject);
          res.on("end", () => {
            resolve({ status: res.statusCode!, headers: res.headers, body: Buffer.concat(chunks).toString() });
            req.destroy();
          });
        });
        req.on("error", reject);
        req.setTimeout(1500, () => req.destroy(new Error("Offline ingress response timed out")));
        if (options.body !== undefined) req.end(options.body);
        else req.flushHeaders();
      });
    },
    async close() {
      server.closeAllConnections();
      await new Promise<void>(resolve => server.close(() => resolve()));
    },
  };
}

describe("HTTP ingress admission boundary", () => {
  it("responds to bodyless routes and unsupported methods without waiting for or reserving declared bodies", async () => {
    const f = await fixture(true);
    try {
      const cases = [
        ["/health", "POST", 404], ["/", "POST", 404], ["/unknown", "POST", 404],
        ["/?next=/v1/responses", "POST", 404], ["/%68ealth", "POST", 404],
        ["/v1/images/variations", "POST", 501], ["/v1/audio/speech", "POST", 501],
        ["/v1/audio/transcriptions", "POST", 501], ["/v1/audio/translations", "POST", 501],
        ["/v1/responses", "PUT", 404], ["/v1/messages", "PUT", 405],
        ["/v1/%6dessages/count_tokens", "OPTIONS", 405], ["/health", "OPTIONS", 404],
        ["/v1%2fresponses", "POST", 404], ["/v1/%256dessages", "POST", 404],
        ["/%ZZhealth", "POST", 404],
      ] as const;
      for (const [path, method, status] of cases) {
        const result = await f.send(path, {
          method, declared: 64 * 1024 ** 2,
          headers: { authorization: `Bearer ${API_KEY}`, "anthropic-version": "2023-06-01" },
        });
        expect({ path, status: result.status }).toEqual({ path, status });
      }
      for (const path of ["/health", "/unknown", "/"]) {
        expect((await f.send(path, { declared: 64 * 1024 ** 2 })).status).toBe(404);
        expect((await f.send(path, { headers: { "transfer-encoding": "chunked" } })).status).toBe(404);
      }
      expect(f.admissions).toEqual([]);
    } finally { await f.close(); }
  });

  it("authenticates Hono's encoded route aliases before ingress admission with the correct protocol envelope", async () => {
    const f = await fixture(true);
    try {
      for (const path of ["/v1/responses", "/%76%31/responses", "/%5f%5fsandboxes"]) {
        const result = await f.send(path, { declared: 64 * 1024 ** 2 });
        expect(result.status).toBe(401);
        expect(JSON.parse(result.body)).toMatchObject({ error: { code: "invalid_api_key" } });
      }
      const result = await f.send("/%76%31/%6dessages", { declared: 64 * 1024 ** 2 });
      expect(result.status).toBe(401);
      expect(JSON.parse(result.body)).toMatchObject({ type: "error", error: { type: "authentication_error" } });
      expect((await f.send("/%76%31/responses", { headers: { "transfer-encoding": "chunked" } })).status).toBe(401);
      expect(f.admissions).toEqual([]);
    } finally { await f.close(); }
  });

  it("enforces effective Anthropic/search limits before buffering encoded paths", async () => {
    const f = await fixture(true);
    try {
      for (const [path, declared] of [
        ["/v1/%6dessages", 32 * 1024 ** 2 + 1],
        ["/v1/messages/%63ount_tokens", 32 * 1024 ** 2 + 1],
        ["/%76%31/alpha/%73earch", STANDALONE_SEARCH_MAX_REQUEST_BYTES + 1],
      ] as const) {
        expect((await f.send(path, { declared,
          headers: { authorization: `Bearer ${API_KEY}`, "anthropic-version": "2023-06-01" },
        })).status).toBe(413);
      }
      expect(f.admissions).toEqual([]);
    } finally { await f.close(); }
  });

  it("preserves canonical and encoded authorized body routes and exact reservation policies", async () => {
    const f = await fixture(false);
    try {
      for (const path of ["/v1/responses", "/%76%31/responses", "/v1/images/generations", "/v1/alpha/search"]) {
        const result = await f.send(path, { body: "null", headers: {
          authorization: `Bearer ${API_KEY}`, "content-type": "application/json",
        } });
        expect(result.status).toBe(400);
      }
      for (const path of ["/v1/messages", "/v1/%6dessages", "/v1/messages/count_tokens"]) {
        const result = await f.send(path, { body: "{}", headers: {
          "x-api-key": API_KEY, "content-type": "application/json", "anthropic-version": "2023-06-01",
        } });
        expect(result.status).toBe(400);
        expect(JSON.parse(result.body)).toMatchObject({ type: "error", error: { message: "model is required" } });
      }
      const edited = await f.send("/v1/images/%65dits", { body: "--offline--\r\n", headers: {
        authorization: `Bearer ${API_KEY}`, "content-type": "multipart/form-data; boundary=offline",
      } });
      expect(edited.status).toBe(400);
      expect(f.admissions).toHaveLength(8);
      expect(f.admissions.slice(0, 7).every(a => a.multiplier === 2)).toBe(true);
      expect(f.admissions.at(-1)?.multiplier).toBe(5);
      expect(f.admissions[5]?.maxBytes).toBe(32 * 1024 ** 2);
      expect(f.releases()).toBe(8);
      expect((await f.send("/health", { method: "GET" })).status).toBe(200);
      const head = await f.send("/health", { method: "HEAD" });
      expect(head.status).toBe(200); expect(head.body).toBe("");
      expect(f.admissions).toHaveLength(8);
    } finally { await f.close(); }
  });

  it("preserves the explicit keyless library seam without admitting public bodies", async () => {
    const f = await fixture(false, null);
    try {
      expect((await f.send("/v1/responses", { body: "null", headers: { "content-type": "application/json" } })).status).toBe(400);
      expect(f.admissions).toHaveLength(1);
      expect(f.releases()).toBe(1);
      expect((await f.send("/health", { declared: 64 * 1024 ** 2 })).status).toBe(404);
      expect(f.admissions).toHaveLength(1);
    } finally { await f.close(); }
  });
});
