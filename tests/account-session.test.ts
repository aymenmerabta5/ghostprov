import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
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

function setCookiesResponse(cookies: readonly string[], status = 200): Response {
  const headers = new Headers();
  for (const cookie of cookies) headers.append("set-cookie", cookie);
  return new Response("{}", { status, headers });
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
  }, 30_000);

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
    const future = new Date(before + 2_000);
    await utimes(file, future, future);
    expect((await stat(file)).mtimeMs).toBeGreaterThanOrEqual(before);

    await session.reloadIfChanged();
    expect(await session.cookieHeader("https://prism.test/api/y")).toContain("prism_session_token=external");
    expect(await session.cookieHeader("https://prism.test/api/y")).not.toContain("old");
  }, 30_000);

  it("force reloads a serialized cookie jar whose mtime did not change", async () => {
    const file = await tempFile();
    const initial = new CookieJar();
    await initial.setCookie("prism_session_token=old; Path=/", "https://prism.test/");
    await writeFile(file, JSON.stringify(initial.serializeSync()));
    const pinnedSeconds = Math.floor(Date.now() / 1_000);
    await utimes(file, pinnedSeconds, pinnedSeconds);
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async () => new Response("{}"),
    });
    const original = await stat(file);

    const replacement = new CookieJar();
    await replacement.setCookie("prism_session_token=external; Path=/", "https://prism.test/");
    await writeFile(file, JSON.stringify(replacement.serializeSync()));
    await utimes(file, pinnedSeconds, pinnedSeconds);
    expect((await stat(file)).mtimeMs).toBe(original.mtimeMs);

    expect(await session.reload()).toBe(false);
    expect(await session.cookieHeader("https://prism.test/api/y")).toContain("prism_session_token=old");
    expect(await session.reload({ force: true })).toBe(true);
    expect(await session.cookieHeader("https://prism.test/api/y")).toContain("prism_session_token=external");
    expect(await session.cookieHeader("https://prism.test/api/y")).not.toContain("old");
  });

  it("loads a same-mtime cookie candidate without changing the live jar before commit", async () => {
    const file = await tempFile();
    const initial = new CookieJar();
    await initial.setCookie("prism_session_token=live; Path=/", "https://prism.test/");
    await writeFile(file, JSON.stringify(initial.serializeSync()));
    const pinnedSeconds = Math.floor(Date.now() / 1_000);
    await utimes(file, pinnedSeconds, pinnedSeconds);
    const seen: Array<string | null> = [];
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async (_input, init) => {
        seen.push(new Headers(init?.headers).get("cookie"));
        return new Response("{}");
      },
    });
    const replacement = new CookieJar();
    await replacement.setCookie("prism_session_token=candidate; Path=/", "https://prism.test/");
    await writeFile(file, JSON.stringify(replacement.serializeSync()));
    await utimes(file, pinnedSeconds, pinnedSeconds);

    const candidate = await session.createCookieCandidate();
    await candidate.request("/auth/session");
    expect(seen[0]).toContain("prism_session_token=candidate");
    expect(await session.cookieHeader("https://prism.test/auth/session"))
      .toContain("prism_session_token=live");

    expect(await candidate.commit()).toBe(true);
    expect(await session.cookieHeader("https://prism.test/auth/session"))
      .toContain("prism_session_token=candidate");
  });

  it("rejects and reloads a candidate after an external same-mtime file rewrite", async () => {
    const file = await tempFile();
    const initial = new CookieJar();
    await initial.setCookie("prism_session_token=initial; Path=/", "https://prism.test/");
    await writeFile(file, JSON.stringify(initial.serializeSync()));
    const pinnedSeconds = Math.floor(Date.now() / 1_000);
    await utimes(file, pinnedSeconds, pinnedSeconds);
    const originalMtime = (await stat(file)).mtimeMs;
    const seen: Array<string | null> = [];
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async (_input, init) => {
        seen.push(new Headers(init?.headers).get("cookie"));
        return setCookieResponse("candidate_rotation=verified; Path=/");
      },
    });

    const candidate = await session.createCookieCandidate();
    await candidate.request("/auth/session");

    const replacement = new CookieJar();
    await replacement.setCookie("prism_session_token=external; Path=/", "https://prism.test/");
    await writeFile(file, JSON.stringify(replacement.serializeSync()));
    await utimes(file, pinnedSeconds, pinnedSeconds);
    expect((await stat(file)).mtimeMs).toBe(originalMtime);

    expect(await candidate.commit()).toBe(false);
    const live = await session.cookieHeader("https://prism.test/auth/session");
    expect(live).toContain("prism_session_token=initial");
    expect(live).not.toContain("external");
    expect(live).not.toContain("candidate_rotation");

    const persisted = CookieJar.deserializeSync(JSON.parse(await readFile(file, "utf8")));
    const persistedHeader = await persisted.getCookieString("https://prism.test/auth/session");
    expect(persistedHeader).toContain("prism_session_token=external");
    expect(persistedHeader).not.toContain("candidate_rotation");

    const retried = await session.createCookieCandidate();
    await retried.request("/auth/session");
    expect(seen.at(-1)).toContain("prism_session_token=external");
    expect(await retried.commit()).toBe(true);
    const verified = await session.cookieHeader("https://prism.test/auth/session");
    expect(verified).toContain("prism_session_token=external");
    expect(verified).toContain("candidate_rotation=verified");
  });

  it("preserves a live cookie rotation persisted concurrently with candidate commit", async () => {
    const file = await tempFile();
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=initial",
      cookieFile: file,
      fetch: async () => setCookieResponse("candidate_rotation=discarded; Path=/"),
    });
    await session.flush();
    const candidate = await session.createCookieCandidate();
    await candidate.request("/auth/session");

    const livePersistence = session.setCookie("prism_session_token=live-rotated; Path=/");
    const candidateCommit = candidate.commit();
    const [, committed] = await Promise.all([livePersistence, candidateCommit]);
    expect(committed).toBe(false);

    const live = await session.cookieHeader("https://prism.test/auth/session");
    expect(live).toContain("prism_session_token=live-rotated");
    expect(live).not.toContain("candidate_rotation");
    const persisted = CookieJar.deserializeSync(JSON.parse(await readFile(file, "utf8")));
    const persistedHeader = await persisted.getCookieString("https://prism.test/auth/session");
    expect(persistedHeader).toContain("prism_session_token=live-rotated");
    expect(persistedHeader).not.toContain("candidate_rotation");
  });

  it("does not overwrite a rotation persisted by another account session", async () => {
    const file = await tempFile();
    const seed = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=initial",
      cookieFile: file,
      fetch: async () => new Response("{}"),
    });
    await seed.flush();
    const pinnedSeconds = Math.floor(Date.now() / 1_000);
    await utimes(file, pinnedSeconds, pinnedSeconds);
    const candidateSession = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async () => setCookieResponse("candidate_rotation=discarded; Path=/"),
    });
    const writerSession = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async () => new Response("{}"),
    });
    const candidate = await candidateSession.createCookieCandidate();
    await candidate.request("/auth/session");

    await writerSession.setCookie("prism_session_token=other-session; Path=/");
    await utimes(file, pinnedSeconds, pinnedSeconds);
    expect(await candidate.commit()).toBe(false);

    const stillVerified = await candidateSession.cookieHeader("https://prism.test/auth/session");
    expect(stillVerified).toContain("prism_session_token=initial");
    expect(stillVerified).not.toContain("other-session");
    const persisted = CookieJar.deserializeSync(JSON.parse(await readFile(file, "utf8")));
    const persistedHeader = await persisted.getCookieString("https://prism.test/auth/session");
    expect(persistedHeader).toContain("prism_session_token=other-session");
    expect(persistedHeader).not.toContain("candidate_rotation");
  });

  it("rejects a stale writer after a candidate commits a newer file generation", async () => {
    const file = await tempFile();
    const seed = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=initial",
      cookieFile: file,
      fetch: async () => new Response("{}"),
    });
    await seed.flush();
    const candidateSession = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async () => setCookieResponse("candidate_rotation=committed; Path=/"),
    });
    const staleWriter = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async () => new Response("{}"),
    });
    const candidate = await candidateSession.createCookieCandidate();
    await candidate.request("/auth/session");
    expect(await candidate.commit()).toBe(true);

    await expect(staleWriter.setCookie("writer_rotation=stale; Path=/"))
      .rejects.toThrow("cookie file changed");
    const persisted = CookieJar.deserializeSync(JSON.parse(await readFile(file, "utf8")));
    const persistedHeader = await persisted.getCookieString("https://prism.test/auth/session");
    expect(persistedHeader).toContain("candidate_rotation=committed");
    expect(persistedHeader).not.toContain("writer_rotation");
    const staleLive = await staleWriter.cookieHeader("https://prism.test/auth/session");
    expect(staleLive).not.toContain("writer_rotation");
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

  it("keeps the newer same-cookie response while merging an independent older cookie", async () => {
    const file = await tempFile();
    let olderStarted!: () => void;
    const started = new Promise<void>(resolve => { olderStarted = resolve; });
    let releaseOlder!: () => void;
    const olderGate = new Promise<void>(resolve => { releaseOlder = resolve; });
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=initial",
      cookieFile: file,
      fetch: async input => {
        if (new URL(String(input)).pathname === "/api/older") {
          olderStarted();
          await olderGate;
          return setCookiesResponse([
            "prism_session_token=older; Path=/",
            "prism-did=older-device; Path=/",
          ]);
        }
        return setCookieResponse("prism_session_token=newer; Path=/");
      },
    });
    await session.flush();

    const older = session.request("/api/older");
    await started;
    await session.request("/api/newer");
    releaseOlder();
    await older;

    const live = await session.cookieHeader("https://prism.test/api/check");
    expect(live).toContain("prism_session_token=newer");
    expect(live).not.toContain("prism_session_token=older");
    expect(live).toContain("prism-did=older-device");
    const persisted = CookieJar.deserializeSync(JSON.parse(await readFile(file, "utf8")));
    const persistedHeader = await persisted.getCookieString("https://prism.test/api/check");
    expect(persistedHeader).toContain("prism_session_token=newer");
    expect(persistedHeader).toContain("prism-did=older-device");
  });

  it("does not let an older response resurrect a cookie deleted by a newer response", async () => {
    let olderStarted!: () => void;
    const started = new Promise<void>(resolve => { olderStarted = resolve; });
    let releaseOlder!: () => void;
    const olderGate = new Promise<void>(resolve => { releaseOlder = resolve; });
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "token=initial",
      fetch: async input => {
        if (new URL(String(input)).pathname.endsWith("older")) {
          olderStarted();
          await olderGate;
          return setCookieResponse("token=resurrected; Path=/");
        }
        return setCookieResponse("token=; Max-Age=0; Path=/");
      },
    });

    const older = session.request("/api/older");
    await started;
    await session.request("/api/newer");
    releaseOlder();
    await older;
    expect(await session.cookieHeader("https://prism.test/")).not.toContain("token=");
  });

  it("canonicalizes Domain and default or relative Path for response ordering", async () => {
    let olderStarted!: () => void;
    const started = new Promise<void>(resolve => { olderStarted = resolve; });
    let releaseOlder!: () => void;
    const olderGate = new Promise<void>(resolve => { releaseOlder = resolve; });
    const session = await PrismAccountSession.create({
      origin: "https://app.prism.test",
      fetch: async input => {
        if (new URL(String(input)).pathname.endsWith("older")) {
          olderStarted();
          await olderGate;
          return setCookieResponse("token=older; Domain=.APP.PRISM.TEST; Path=relative");
        }
        return setCookieResponse("token=newer");
      },
    });

    const older = session.request("/api/older");
    await started;
    await session.request("/api/newer");
    releaseOlder();
    await older;
    const header = await session.cookieHeader("https://app.prism.test/api/check");
    expect(header).toContain("token=newer");
    expect(header).not.toContain("token=older");
  });

  it("keeps same-name cookies with distinct effective paths and header order", async () => {
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      fetch: async input => {
        const pathname = new URL(String(input)).pathname;
        if (pathname.startsWith("/left/")) return setCookieResponse("lane=left");
        if (pathname.startsWith("/right/")) return setCookieResponse("lane=right");
        return setCookiesResponse(["ordered=first; Path=/", "ordered=second; Path=/"]);
      },
    });

    await Promise.all([
      session.request("/left/run"),
      session.request("/right/run"),
    ]);
    await session.request("/ordered");
    expect(await session.cookieHeader("https://prism.test/left/check")).toContain("lane=left");
    expect(await session.cookieHeader("https://prism.test/right/check")).toContain("lane=right");
    const root = await session.cookieHeader("https://prism.test/");
    expect(root).toContain("ordered=second");
    expect(root).not.toContain("ordered=first");
  });

  it("orders concurrent cookie rotations inside a candidate before commit", async () => {
    let olderStarted!: () => void;
    const started = new Promise<void>(resolve => { olderStarted = resolve; });
    let releaseOlder!: () => void;
    const olderGate = new Promise<void>(resolve => { releaseOlder = resolve; });
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "token=initial",
      fetch: async input => {
        if (new URL(String(input)).pathname.endsWith("older")) {
          olderStarted();
          await olderGate;
          return setCookieResponse("token=older; Path=/");
        }
        return setCookieResponse("token=newer; Path=/");
      },
    });
    const candidate = await session.createCookieCandidate();
    const older = candidate.request("/auth/older");
    await started;
    await candidate.request("/auth/newer");
    releaseOlder();
    await older;
    expect(await candidate.commit()).toBe(true);
    const live = await session.cookieHeader("https://prism.test/");
    expect(live).toContain("token=newer");
    expect(live).not.toContain("token=older");
  });

  it("bounds mutation history behind a hung request with a conservative barrier", async () => {
    let started!: () => void;
    const requestStarted = new Promise<void>(resolve => { started = resolve; });
    let release!: () => void;
    const responseGate = new Promise<void>(resolve => { release = resolve; });
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "guard=initial",
      fetch: async () => {
        started();
        await responseGate;
        return setCookieResponse("guard=stale; Path=/");
      },
    });
    const pending = session.request("/api/hung");
    await requestStarted;
    for (let index = 0; index < 1_025; index++) {
      await session.setCookie(`expired-${index}=; Max-Age=0; Path=/`);
    }
    await session.setCookie("guard=current; Path=/");
    release();
    await pending;

    const live = await session.cookieHeader("https://prism.test/");
    expect(live).toContain("guard=current");
    expect(live).not.toContain("guard=stale");
  }, 30_000);

  it("does not let an older in-flight response undo a successful live rotation", async () => {
    const file = await tempFile();
    let started!: () => void;
    const requestStarted = new Promise<void>(resolve => { started = resolve; });
    let release!: () => void;
    const responseGate = new Promise<void>(resolve => { release = resolve; });
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=initial",
      cookieFile: file,
      fetch: async () => {
        started();
        await responseGate;
        return setCookieResponse("prism_session_token=stale-response; Path=/");
      },
    });
    await session.flush();

    const pending = session.request("/auth/session");
    await requestStarted;
    await session.setCookie("prism_session_token=live-rotated; Path=/");
    release();
    await pending;

    const live = await session.cookieHeader("https://prism.test/auth/session");
    expect(live).toContain("prism_session_token=live-rotated");
    expect(live).not.toContain("stale-response");
    const persisted = CookieJar.deserializeSync(JSON.parse(await readFile(file, "utf8")));
    const persistedHeader = await persisted.getCookieString("https://prism.test/auth/session");
    expect(persistedHeader).toContain("prism_session_token=live-rotated");
    expect(persistedHeader).not.toContain("stale-response");
  });

  it("discards Set-Cookie from a request sent with a jar replaced by a candidate", async () => {
    const file = await tempFile();
    let start!: () => void;
    const started = new Promise<void>(resolve => { start = resolve; });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const session = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=old",
      cookieFile: file,
      fetch: async input => {
        if (new URL(String(input)).pathname === "/slow") {
          start();
          await gate;
          return setCookieResponse("prism_session_token=stale-response; Path=/");
        }
        return new Response("{}");
      },
    });
    await session.flush();

    const slow = session.request("/slow");
    await started;
    const replacement = new CookieJar();
    await replacement.setCookie("prism_session_token=verified-candidate; Path=/", "https://prism.test/");
    await writeFile(file, JSON.stringify(replacement.serializeSync()));
    const candidate = await session.createCookieCandidate();
    await candidate.request("/auth/session");
    expect(await candidate.commit()).toBe(true);

    release();
    await slow;
    const live = await session.cookieHeader("https://prism.test/auth/session");
    expect(live).toContain("prism_session_token=verified-candidate");
    expect(live).not.toContain("stale-response");
  });

  it("does not let an in-flight response overwrite a newer external file generation", async () => {
    const file = await tempFile();
    const seed = await PrismAccountSession.create({
      origin: "https://prism.test",
      initialCookieHeader: "prism_session_token=initial",
      cookieFile: file,
      fetch: async () => new Response("{}"),
    });
    await seed.flush();
    let started!: () => void;
    const requestStarted = new Promise<void>(resolve => { started = resolve; });
    let release!: () => void;
    const responseGate = new Promise<void>(resolve => { release = resolve; });
    const requester = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async () => {
        started();
        await responseGate;
        return setCookieResponse("stale_response=discarded; Path=/");
      },
    });
    const writer = await PrismAccountSession.create({
      origin: "https://prism.test",
      cookieFile: file,
      fetch: async () => new Response("{}"),
    });

    const pending = requester.request("/auth/session");
    await requestStarted;
    await writer.setCookie("prism_session_token=external-newer; Path=/");
    release();
    await pending;

    const persisted = CookieJar.deserializeSync(JSON.parse(await readFile(file, "utf8")));
    const persistedHeader = await persisted.getCookieString("https://prism.test/auth/session");
    expect(persistedHeader).toContain("prism_session_token=external-newer");
    expect(persistedHeader).not.toContain("stale_response");
    const requesterLive = await requester.cookieHeader("https://prism.test/auth/session");
    expect(requesterLive).toContain("prism_session_token=initial");
    expect(requesterLive).not.toContain("stale_response");
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
