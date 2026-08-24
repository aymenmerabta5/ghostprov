import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type { Clock, FetchLike } from "../src/core/ports";
import {
  PrismSessionError,
  PrismSessionManager,
  type PrismIdentityKind,
} from "../src/prism/session-manager";

const roots: string[] = [];
const managers: PrismSessionManager[] = [];

afterEach(async () => {
  await Promise.allSettled(managers.splice(0).map(manager => manager.close()));
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

async function stateFiles(): Promise<{ cookieFile: string; identityFile: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-session-"));
  roots.push(root);
  const cookieFile = path.join(root, "cookies.json");
  return { cookieFile, identityFile: `${cookieFile}.identity.json` };
}

function json(status: number, body: unknown, headers?: HeadersInit): Response {
  return Response.json(body, { status, headers });
}

function authState(kind: PrismIdentityKind, id: string): Record<string, unknown> {
  const user = { id, is_anonymous: kind === "anonymous" };
  return { session: { user }, user };
}

function scripted(
  responses: Response[],
  calls: string[],
): FetchLike {
  return async input => {
    const url = new URL(String(input));
    calls.push(url.pathname);
    const response = responses.shift();
    if (!response) throw new Error(`missing response for ${url.pathname}`);
    return response;
  };
}

async function manager(options: {
  mode: "authenticated" | "auto" | "anonymous";
  allowAnonymous?: boolean;
  responses: Response[];
  calls?: string[];
  cookieFile?: string;
  identityFile?: string;
  fetch?: FetchLike;
  clock?: Clock;
  revalidateTimeoutMs?: number;
}): Promise<PrismSessionManager> {
  const files = options.cookieFile
    ? { cookieFile: options.cookieFile, identityFile: options.identityFile ?? `${options.cookieFile}.identity.json` }
    : await stateFiles();
  const value = await PrismSessionManager.create({
    origin: "https://prism.test",
    mode: options.mode,
    allowAnonymous: options.allowAnonymous ?? false,
    cookieFile: files.cookieFile,
    identityFile: files.identityFile,
    fetch: options.fetch ?? scripted(options.responses, options.calls ?? []),
    clock: options.clock,
    lockTimeoutMs: 20,
    revalidateTimeoutMs: options.revalidateTimeoutMs,
  });
  managers.push(value);
  return value;
}

describe("PrismSessionManager", () => {
  it("requires a signed-in user in authenticated mode", async () => {
    const value = await manager({
      mode: "authenticated",
      responses: [json(200, { session: null, user: null })],
    });

    await expect(value.resolve()).rejects.toMatchObject({
      name: "PrismSessionError",
      kind: "auth_required",
    });
  });

  it("creates one backend-owned guest only in explicit anonymous mode", async () => {
    const calls: string[] = [];
    const files = await stateFiles();
    const value = await manager({
      mode: "anonymous",
      calls,
      cookieFile: files.cookieFile,
      identityFile: files.identityFile,
      responses: [
        json(200, { session: null, user: null }),
        json(200, { session: { prism_session_token: "body-secret", user: { id: "guest-1", is_anonymous: true } } }, {
          "set-cookie": "prism_session_token=cookie-secret; Path=/; HttpOnly",
        }),
        json(200, authState("anonymous", "guest-1")),
      ],
    });

    const identity = await value.resolve();

    expect(identity.kind).toBe("anonymous");
    expect(identity.fingerprint).toMatch(/^[a-f0-9]{64}$/);
    expect(calls).toEqual(["/auth/session", "/api/auth/anonymous-session", "/auth/session"]);
    expect(JSON.stringify(identity)).not.toContain("guest-1");
    expect(JSON.stringify(identity)).not.toContain("cookie-secret");
    const metadata = JSON.parse(await readFile(files.identityFile, "utf8"));
    expect(metadata).toEqual({ version: 1, kind: "anonymous", fingerprint: identity.fingerprint });
    if (process.platform !== "win32") expect((await stat(files.identityFile)).mode & 0o777).toBe(0o600);
  });

  it("does not create a guest in auto mode without explicit permission", async () => {
    const calls: string[] = [];
    const value = await manager({
      mode: "auto",
      allowAnonymous: false,
      calls,
      responses: [json(200, { session: null, user: null })],
    });

    await expect(value.resolve()).rejects.toMatchObject({ kind: "auth_required" });
    expect(calls).toEqual(["/auth/session"]);
  });

  it("does not replace a lost guest identity silently", async () => {
    const calls: string[] = [];
    const files = await stateFiles();
    await writeFile(files.identityFile, JSON.stringify({
      version: 1,
      kind: "anonymous",
      fingerprint: "a".repeat(64),
    }), { mode: 0o600 });
    const value = await manager({
      mode: "anonymous",
      calls,
      cookieFile: files.cookieFile,
      identityFile: files.identityFile,
      responses: [json(200, { session: null, user: null })],
    });

    await expect(value.resolve()).rejects.toMatchObject({ kind: "guest_identity_lost" });
    expect(calls).toEqual(["/auth/session"]);
  });

  it("resolves an anonymous-session conflict by refreshing auth state", async () => {
    const calls: string[] = [];
    const value = await manager({
      mode: "anonymous",
      calls,
      responses: [
        json(200, { session: null, user: null }),
        json(409, { error: "already exists" }),
        json(200, authState("anonymous", "guest-race")),
      ],
    });

    await expect(value.resolve()).resolves.toMatchObject({ kind: "anonymous" });
    expect(calls).toEqual(["/auth/session", "/api/auth/anonymous-session", "/auth/session"]);
  });

  it("rejects identity mode and fingerprint changes", async () => {
    const files = await stateFiles();
    await writeFile(files.identityFile, JSON.stringify({
      version: 1,
      kind: "authenticated",
      fingerprint: "b".repeat(64),
    }), { mode: 0o600 });
    const value = await manager({
      mode: "authenticated",
      cookieFile: files.cookieFile,
      identityFile: files.identityFile,
      responses: [json(200, authState("authenticated", "different-user"))],
    });

    try {
      await value.resolve();
      throw new Error("expected identity mismatch");
    } catch (error) {
      expect(error).toBeInstanceOf(PrismSessionError);
      expect(error).toMatchObject({ kind: "mode_mismatch" });
    }
  });

  it("revalidates the same persisted and current identity and refreshes its resolution time", async () => {
    const calls: string[] = [];
    let now = 100;
    const clock: Clock = {
      now: () => now,
      sleep: async () => undefined,
      waitUntil: async () => undefined,
    };
    const value = await manager({
      mode: "authenticated",
      calls,
      clock,
      responses: [
        json(200, authState("authenticated", "user-1")),
        json(200, authState("authenticated", "user-1")),
      ],
    });
    const initial = await value.resolve();
    now = 200;

    const refreshed = await value.revalidate();

    expect(refreshed).toEqual({ ...initial, resolvedAtMs: 200 });
    expect(await value.resolve()).toBe(refreshed);
    expect(calls).toEqual(["/auth/session", "/auth/session"]);
  });

  it("singleflights concurrent revalidation into one auth fetch", async () => {
    let calls = 0;
    let start!: () => void;
    const started = new Promise<void>(resolve => { start = resolve; });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const value = await manager({
      mode: "authenticated",
      responses: [],
      fetch: async () => {
        calls += 1;
        if (calls === 1) return json(200, authState("authenticated", "user-1"));
        start();
        await gate;
        return json(200, authState("authenticated", "user-1"));
      },
    });
    await value.resolve();

    const first = value.revalidate();
    const second = value.revalidate();
    expect(second).toBe(first);
    await started;
    expect(calls).toBe(2);
    release();

    const [left, right] = await Promise.all([first, second]);
    expect(left).toBe(right);
    expect(calls).toBe(2);
  });

  it("lets one caller abort its revalidation wait without cancelling the shared fetch", async () => {
    let calls = 0;
    let start!: () => void;
    const started = new Promise<void>(resolve => { start = resolve; });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let sharedSignal: AbortSignal | null | undefined;
    const value = await manager({
      mode: "authenticated",
      responses: [],
      fetch: async (_input, init) => {
        calls += 1;
        if (calls === 1) return json(200, authState("authenticated", "user-1"));
        sharedSignal = init?.signal;
        start();
        await gate;
        return json(200, authState("authenticated", "user-1"));
      },
    });
    await value.resolve();
    const controller = new AbortController();

    const cancelled = value.revalidate(controller.signal);
    const survivor = value.revalidate();
    await started;
    controller.abort();

    await expect(cancelled).rejects.toMatchObject({ name: "AbortError" });
    expect(sharedSignal?.aborted).toBe(false);
    release();
    await expect(survivor).resolves.toMatchObject({ kind: "authenticated" });
    expect(calls).toBe(2);
  });

  it("aborts and awaits the shared revalidation when closed", async () => {
    let calls = 0;
    let start!: () => void;
    const started = new Promise<void>(resolve => { start = resolve; });
    let seeAbort!: () => void;
    const abortSeen = new Promise<void>(resolve => { seeAbort = resolve; });
    let finishAbort!: () => void;
    const abortFinished = new Promise<void>(resolve => { finishAbort = resolve; });
    const value = await manager({
      mode: "authenticated",
      responses: [],
      fetch: async (_input, init) => {
        calls += 1;
        if (calls === 1) return json(200, authState("authenticated", "user-1"));
        start();
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            seeAbort();
            void abortFinished.then(() => reject(new DOMException("aborted", "AbortError")));
          }, { once: true });
        });
      },
    });
    await value.resolve();
    const outcome = value.revalidate().then(
      () => null,
      error => error,
    );
    await started;

    let closeSettled = false;
    const closing = value.close().then(() => { closeSettled = true; });
    await abortSeen;
    await Promise.resolve();
    expect(closeSettled).toBe(false);
    finishAbort();
    await closing;

    expect(await outcome).toMatchObject({ name: "AbortError" });
    await expect(value.revalidate()).rejects.toMatchObject({ kind: "protocol" });
  });

  it("times out and aborts a hung candidate request without mutating live cookies", async () => {
    let calls = 0;
    let abortSeen!: () => void;
    const aborted = new Promise<void>(resolve => { abortSeen = resolve; });
    const value = await manager({
      mode: "authenticated",
      responses: [],
      revalidateTimeoutMs: 25,
      fetch: async (_input, init) => {
        calls += 1;
        if (calls === 1) return json(200, authState("authenticated", "user-1"));
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            abortSeen();
            reject(new DOMException("aborted", "AbortError"));
          }, { once: true });
        });
      },
    });
    const identity = await value.resolve();
    await identity.account.setCookie("prism_session_token=live; Path=/");

    await expect(value.revalidate()).rejects.toMatchObject({ kind: "timeout" });
    await aborted;
    expect(await identity.account.cookieHeader("https://prism.test/auth/session"))
      .toContain("prism_session_token=live");
  });

  it("rejects missing and mismatched identities during revalidation", async () => {
    const missing = await manager({
      mode: "authenticated",
      responses: [
        json(200, authState("authenticated", "user-1")),
        json(200, { session: null, user: null }, {
          "set-cookie": "prism_session_token=rejected-missing; Path=/",
        }),
      ],
    });
    const missingIdentity = await missing.resolve();
    await missingIdentity.account.setCookie("prism_session_token=accepted; Path=/");
    await expect(missing.revalidate()).rejects.toMatchObject({ kind: "auth_required" });
    expect(await missingIdentity.account.cookieHeader("https://prism.test/auth/session"))
      .toContain("prism_session_token=accepted");
    expect(await missingIdentity.account.cookieHeader("https://prism.test/auth/session"))
      .not.toContain("rejected-missing");

    const mismatched = await manager({
      mode: "authenticated",
      responses: [
        json(200, authState("authenticated", "user-1")),
        json(200, authState("authenticated", "user-2"), {
          "set-cookie": "prism_session_token=rejected-mismatch; Path=/",
        }),
      ],
    });
    const mismatchedIdentity = await mismatched.resolve();
    await mismatchedIdentity.account.setCookie("prism_session_token=accepted; Path=/");
    await expect(mismatched.revalidate()).rejects.toMatchObject({ kind: "mode_mismatch" });
    expect(await mismatchedIdentity.account.cookieHeader("https://prism.test/auth/session"))
      .toContain("prism_session_token=accepted");
    expect(await mismatchedIdentity.account.cookieHeader("https://prism.test/auth/session"))
      .not.toContain("rejected-mismatch");
  }, 30_000);

  it("retries a candidate commit that conflicts with a concurrent live Set-Cookie rotation", async () => {
    let authCalls = 0;
    const authCookies: Array<string | null> = [];
    let start!: () => void;
    const started = new Promise<void>(resolve => { start = resolve; });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const value = await manager({
      mode: "authenticated",
      responses: [],
      fetch: async (input, init) => {
        const pathname = new URL(String(input)).pathname;
        if (pathname === "/rotate") {
          return json(200, {}, {
            "set-cookie": "prism_session_token=concurrent-winner; Path=/",
          });
        }
        expect(pathname).toBe("/auth/session");
        authCalls += 1;
        authCookies.push(new Headers(init?.headers).get("cookie"));
        if (authCalls === 2) {
          start();
          await gate;
          return json(200, authState("authenticated", "user-1"), {
            "set-cookie": "prism_session_token=discarded-candidate; Path=/",
          });
        }
        return json(200, authState("authenticated", "user-1"));
      },
    });
    const identity = await value.resolve();
    await identity.account.setCookie("prism_session_token=base; Path=/");

    const revalidating = value.revalidate();
    await started;
    await identity.account.request("/rotate");
    release();
    await expect(revalidating).resolves.toMatchObject({ kind: "authenticated" });

    expect(authCalls).toBe(3);
    expect(authCookies[2]).toContain("prism_session_token=concurrent-winner");
    const live = await identity.account.cookieHeader("https://prism.test/auth/session");
    expect(live).toContain("prism_session_token=concurrent-winner");
    expect(live).not.toContain("discarded-candidate");
  }, 30_000);

  it("never creates a replacement anonymous identity during revalidation", async () => {
    const calls: string[] = [];
    const value = await manager({
      mode: "anonymous",
      calls,
      responses: [
        json(200, { session: null, user: null }),
        json(200, {}),
        json(200, authState("anonymous", "guest-1")),
        json(200, { session: null, user: null }),
      ],
    });
    await value.resolve();

    await expect(value.revalidate()).rejects.toMatchObject({ kind: "guest_identity_lost" });
    expect(calls).toEqual([
      "/auth/session",
      "/api/auth/anonymous-session",
      "/auth/session",
      "/auth/session",
    ]);
  });
});
