import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { CookieJar } from "tough-cookie";
import {
  AccountAuthError,
  AccountTransportError,
  PrismAccountSession,
} from "../src/prism/account-session";

const tempRoots: string[] = [];

afterEach(async () => {
  await Promise.all(tempRoots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

async function tempFile(): Promise<string> {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-account-"));
  tempRoots.push(root);
  return path.join(root, "cookies.json");
}

function setCookieResponse(cookie: string, status = 200): Response {
  return new Response("{}", { status, headers: { "set-cookie": cookie } });
}

describe("PrismAccountSession", () => {
  it("applies matching cookies and persists Set-Cookie rotation atomically", async () => {
    const file = await tempFile();
    const seen: Array<string | null> = [];
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=old; prism-did=device",
      cookieFile: file,
      fetch: async (_url, init) => {
        seen.push(new Headers(init?.headers).get("cookie"));
        return setCookieResponse("prism_session_token=new; Path=/; HttpOnly");
      },
    });

    await session.request("/api/y", { method: "POST" });

    expect(seen[0]).toContain("prism_session_token=old");
    const persisted = JSON.parse(await readFile(file, "utf8"));
    expect(JSON.stringify(persisted)).toContain("prism_session_token");
    expect(JSON.stringify(persisted)).toContain("new");
    expect(session.snapshot()).toEqual(expect.objectContaining({
      origin: "https://prism.test",
      cookieCount: 2,
      cookieFile: file,
    }));
    expect(JSON.stringify(session.snapshot())).not.toContain("old");
    expect(JSON.stringify(session.snapshot())).not.toContain("new");
    if (process.platform !== "win32") expect((await stat(file)).mode & 0o777).toBe(0o600);
  });

  it("hot reloads a newer serialized cookie jar", async () => {
    const file = await tempFile();
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=old",
      cookieFile: file,
      fetch: async () => new Response("{}"),
    });
    await session.flush();
    const before = (await stat(file)).mtimeMs;

    const replacement = new CookieJar();
    await replacement.setCookie("prism_session_token=external; Path=/", "https://prism.test/");
    await new Promise(resolve => setTimeout(resolve, 20));
    await writeFile(file, JSON.stringify(replacement.serializeSync()));
    expect((await stat(file)).mtimeMs).toBeGreaterThanOrEqual(before);

    await session.reloadIfChanged();
    expect(await session.cookieHeader("https://prism.test/api/y")).toContain("prism_session_token=external");
    expect(await session.cookieHeader("https://prism.test/api/y")).not.toContain("old");
  });

  it("serializes concurrent cookie persistence without corrupting the jar", async () => {
    const file = await tempFile();
    let call = 0;
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "session=base",
      cookieFile: file,
      fetch: async () => setCookieResponse(`${++call === 1 ? "left=1" : "right=2"}; Path=/`),
    });

    await Promise.all([session.request("/api/left"), session.request("/api/right")]);
    await session.flush();

    const serialized = JSON.parse(await readFile(file, "utf8"));
    const jar = CookieJar.deserializeSync(serialized);
    const header = await jar.getCookieString("https://prism.test/");
    expect(header).toContain("left=1");
    expect(header).toContain("right=2");
  });

  it("honors cookie domain, path, and expiry rules", async () => {
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "root=1",
      fetch: async () => new Response("{}"),
    });
    await session.setCookie("api=2; Path=/api");
    await session.setCookie("expired=3; Max-Age=0; Path=/");

    expect(await session.cookieHeader("https://prism.test/api/y")).toContain("api=2");
    expect(await session.cookieHeader("https://prism.test/s/sandboxes/proxy/heartbeat")).not.toContain("api=2");
    expect(await session.cookieHeader("https://prism.test/")).not.toContain("expired=3");
  });

  it("classifies account endpoint auth failures but leaves sandbox auth to its runtime", async () => {
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "session=value",
      fetch: async () => new Response("unauthorized", { status: 401 }),
    });

    await expect(session.request("/api/y")).rejects.toBeInstanceOf(AccountAuthError);
    expect((await session.request("/s/sandboxes/proxy/heartbeat")).status).toBe(401);
  });

  it("never includes cookie material in transport errors", async () => {
    const secret = "prism_session_token=super-secret";
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: secret,
      fetch: async () => { throw new Error(`network failed ${secret}`); },
    });

    try {
      await session.request("/api/y", { method: "POST" });
      throw new Error("expected transport error");
    } catch (error) {
      expect(error).toBeInstanceOf(AccountTransportError);
      expect(String(error)).not.toContain("super-secret");
    }
  });
});
