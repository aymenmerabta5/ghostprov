import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type { FetchLike } from "../src/core/ports";
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
    fetch: scripted(options.responses, options.calls ?? []),
    lockTimeoutMs: 20,
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
});
