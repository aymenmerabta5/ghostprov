import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { createStartupArtifactExpectations, StartupArtifactCapture, type ArtifactExpectation, type StartupArtifactExpectations, type StartupArtifactWorker } from "../scripts/lib/startup-artifact-capture";
import type { BootstrapBinding } from "../src/v2/bootstrap/crypto";
import type { V2BootstrapGateway } from "../src/v2/bootstrap/controller";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import { directSupervisorLaunchFiles, DIRECT_SUPERVISOR_INSTALL_PATH, DIRECT_SUPERVISOR_LAUNCH_PATH } from "../src/v2/supervisor/r10-direct-launcher";
import { supervisorStaticConfig, SUPERVISOR_STATIC_PATH } from "../src/v2/supervisor/r10-launcher";

const MiB = 1024 * 1024;
const assets = await loadV2SidecarAssets({ revision: "r10" });
const sessions = ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333"] as const;
const binding: BootstrapBinding = { projectId: "44444444-4444-4444-8444-444444444444", sandboxSessionId: "fixture-sandbox",
  leaderGeneration: 1, nonce: "fixture-supervisor", buildHash: assets.buildHash, schemaHash: assets.schemaHash, protocolRevision: "r2", schemaVersion: 2 };
const hash = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");
const contents = ["fixture-bundle", "fixture-static", "fixture-exec", "fixture-installer"] as const;
const expectations: StartupArtifactExpectations = Object.freeze([
  { label: "bundle", path: assets.entrypointPath, expectedBytes: contents[0].length, expectedSha256: hash(contents[0]) },
  { label: "supervisor_static", path: SUPERVISOR_STATIC_PATH, expectedBytes: contents[1].length, expectedSha256: hash(contents[1]) },
  { label: "exec", path: DIRECT_SUPERVISOR_LAUNCH_PATH, expectedBytes: contents[2].length, expectedSha256: hash(contents[2]) },
  { label: "installer", path: DIRECT_SUPERVISOR_INSTALL_PATH, expectedBytes: contents[3].length, expectedSha256: hash(contents[3]) },
]);

class ManualClock {
  now = 0;
  readonly waiters = new Set<{ at: number; finish(): void }>();
  sleep = (ms: number, signal?: AbortSignal): Promise<void> => {
    if (signal?.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
      const waiter = { at: this.now + ms, finish: () => { cleanup(); resolve(); } };
      const abort = () => { cleanup(); reject(signal?.reason); };
      const cleanup = () => { this.waiters.delete(waiter); signal?.removeEventListener("abort", abort); };
      this.waiters.add(waiter); signal?.addEventListener("abort", abort, { once: true });
    });
  };
  advance(ms: number): void {
    this.now += ms;
    for (const waiter of [...this.waiters]) if (waiter.at <= this.now) waiter.finish();
  }
}
async function flush() { for (let count = 0; count < 150; count++) await Promise.resolve(); }
function body(sessionId: string, expected: ArtifactExpectation, content: string = contents[expectations.findIndex(value => value.label === expected.label)]!): Record<string, unknown> {
  return { session_id: sessionId, path: `/codex_workspace/${sessionId}/${expected.path}`,
    relative_path: `${sessionId}/${expected.path}`, bytes: Buffer.byteLength(content), body_b64: Buffer.from(content).toString("base64") };
}
interface Request { sessionId: string; expected: ArtifactExpectation; init: RequestInit }
function fixture(options: {
  clock?: ManualClock;
  response?: (request: Request) => Response | Promise<Response>;
  phase?: () => string;
  onGateway?: (expected: ArtifactExpectation, snapshot: ReturnType<StartupArtifactCapture["snapshot"]>) => void;
  requestUrl?: (url: URL) => string;
  detachOnAbort?: boolean;
  record?: (row: Record<string, unknown>) => void;
} = {}) {
  const clock = options.clock ?? new ManualClock(), abort = new AbortController(), rows: Record<string, unknown>[] = [];
  const capture = new StartupArtifactCapture({ signal: abort.signal, nowMs: () => clock.now, sleep: clock.sleep,
    record: row => { rows.push(row); options.record?.(row); } });
  const calls: Array<{ at: number; sessionId: string; label: string }> = [];
  const gateway: Pick<V2BootstrapGateway, "fetchText"> = {
    async fetchText(suffix, init = {}) {
      const url = new URL(suffix, "https://backend.invalid/"), sessionId = url.searchParams.get("session_id")!;
      const expected = expectations.find(value => value.path === url.searchParams.get("path"))!;
      options.onGateway?.(expected, capture.snapshot());
      calls.push({ at: clock.now, sessionId, label: expected.label });
      const pending = capture.wrapAccountRequest(options.requestUrl?.(url) ?? url.href, init,
        async () => options.response ? options.response({ sessionId, expected, init }) : Response.json(body(sessionId, expected)));
      let response: Response;
      if (options.detachOnAbort) {
        let detach = () => {};
        const cancelled = new Promise<never>((_resolve, reject) => {
          const stop = () => reject(init.signal?.reason);
          init.signal?.addEventListener("abort", stop, { once: true });
          detach = () => init.signal?.removeEventListener("abort", stop);
          if (init.signal?.aborted) stop();
        });
        try { response = await Promise.race([pending, cancelled]); }
        finally { detach(); }
      } else response = await pending;
      return { status: response.status, text: await response.text(), headers: response.headers };
    },
  };
  const accept = (worker: 1 | 2 = 1, sessionId: string = sessions[worker - 1], expected = expectations) => capture.accept({ worker, sessionId, gateway,
    expectations: expected, phase: options.phase ?? (() => "readiness_pending") });
  return { clock, abort, capture, rows, calls, gateway, accept };
}
async function finishSchedule(value: ReturnType<typeof fixture>, initial: Promise<void>): Promise<void> {
  await initial; await flush();
  value.clock.advance(5000); await flush();
  value.clock.advance(15000); await flush();
  await value.capture.settle();
}

describe("startup artifact expectations", () => {
  it("derives exactly four public artifacts from the registered release and effective timeout", () => {
    const actual = createStartupArtifactExpectations(assets, binding, 91_234);
    const generated = directSupervisorLaunchFiles({ entrypointPath: assets.entrypointPath, supervisorNonce: binding.nonce });
    const expectedContents = [assets.files.find(value => value.path === assets.entrypointPath)!.content,
      supervisorStaticConfig({ binding, entrypointPath: assets.entrypointPath, connectTimeoutMs: 91_234 }), generated.launcher, generated.installer];
    expect(actual.map(value => value.label)).toEqual(["bundle", "supervisor_static", "exec", "installer"]);
    actual.forEach((value, index) => {
      expect(value.expectedBytes).toBe(Buffer.byteLength(expectedContents[index]!));
      expect(value.expectedSha256).toBe(hash(expectedContents[index]!));
      expect(Object.keys(value).sort()).toEqual(["expectedBytes", "expectedSha256", "label", "path"]);
      expect(Object.isFrozen(value)).toBe(true);
    });
    expect(Object.isFrozen(actual)).toBe(true);
    expect(JSON.stringify(actual)).not.toContain("bootstrap.enc.json");
  });

  it("rejects unregistered paths, mismatched bindings, duplicate bundles, and invalid hashes", () => {
    expect(() => createStartupArtifactExpectations({ ...assets, revision: "r9" }, binding, 90000)).toThrow();
    expect(() => createStartupArtifactExpectations(assets, { ...binding, buildHash: "a".repeat(64) }, 90000)).toThrow();
    expect(() => createStartupArtifactExpectations({ ...assets, entrypointPath: assets.entrypointPath + "\n" }, binding, 90000)).toThrow();
    expect(() => createStartupArtifactExpectations({ ...assets, files: [...assets.files, assets.files.find(value => value.path === assets.entrypointPath)!] }, binding, 90000)).toThrow();
    expect(() => createStartupArtifactExpectations({ ...assets, files: assets.files.map(value => ({ ...value, sha256: "f".repeat(64) })) }, binding, 90000)).toThrow();
    expect(() => createStartupArtifactExpectations(assets, binding, 0)).toThrow();
  });
});

describe("startup artifact capture", () => {
  it("samples at acceptance and exactly five and twenty seconds with reservations before every gateway call", async () => {
    const value = fixture({ onGateway: (expected, snapshot) => {
      expect(snapshot.memory.ordinaryUsedBytes).toBeGreaterThanOrEqual(expected.label === "bundle" ? 16 * MiB : MiB);
      expect(snapshot.memory.activePermits).toBeGreaterThan(0);
    } });
    const run = value.accept();
    await finishSchedule(value, run.initial);
    expect(value.calls.map(call => call.at)).toEqual([0, 0, 0, 0, 5000, 5000, 5000, 5000, 20000, 20000, 20000, 20000]);
    expect(value.rows.every(row => row.outcome === "matched" && row.equal === true)).toBe(true);
    expect(value.capture.snapshot()).toMatchObject({ sessions: 1, completedSessions: 1, expectedRows: 12, rowCount: 12, complete: true, errors: 0, activeReads: 0, accountOwners: 0,
      memory: { ordinaryCapacityBytes: 48 * MiB, ordinaryUsedBytes: 0, activePermits: 0, waiters: 0 } });
    expect(value.clock.waiters.size).toBe(0);
  });

  it("captures at most one original session per worker and rejects duplicates without throwing", async () => {
    const value = fixture();
    const first = value.accept(1), second = value.accept(2);
    await expect(value.accept(1, sessions[2]).initial).resolves.toBeUndefined();
    await expect(value.accept(2, sessions[0]).done).resolves.toBeUndefined();
    await expect(value.capture.accept({ worker: 3, sessionId: sessions[2], gateway: value.gateway,
      expectations, phase: () => "accepted" }).done).resolves.toBeUndefined();
    await finishSchedule(value, Promise.all([first.initial, second.initial]).then(() => {}));
    expect(value.capture.snapshot()).toMatchObject({ sessions: 2, rowCount: 24, rejected: 3, errors: 3, complete: false });
    expect(value.capture.snapshot().peakReservedBytes).toBeLessThanOrEqual(48 * MiB);
  });

  it("preserves worker one and worker eight identities throughout all checkpoints", async () => {
    const value = fixture(), first = value.accept(1);
    const eighth = value.capture.accept({ worker: 8, sessionId: sessions[1], gateway: value.gateway,
      expectations, phase: () => "readiness_pending" });
    await finishSchedule(value, Promise.all([first.initial, eighth.initial]).then(() => {}));
    expect(value.rows.filter(row => row.worker === 1)).toHaveLength(12);
    expect(value.rows.filter(row => row.worker === 8)).toHaveLength(12);
    expect(value.rows.every(row => row.worker === 1 || row.worker === 8)).toBe(true);
    expect(value.capture.snapshot()).toMatchObject({ sessions: 2, rowCount: 24, rejected: 0, errors: 0, complete: true });
    expect(value.capture.snapshot().memory.ordinaryCapacityBytes).toBe(48 * MiB);
  });

  it.each([0, 9, 1.5])("rejects invalid worker identity %s without dispatch or relabeling", async worker => {
    const value = fixture();
    await value.capture.accept({ worker: worker as StartupArtifactWorker, sessionId: sessions[0], gateway: value.gateway,
      expectations, phase: () => "accepted" }).done;
    expect(value.calls).toHaveLength(0);
    expect(value.rows).toHaveLength(0);
    expect(value.capture.snapshot()).toMatchObject({ sessions: 0, rejected: 1, errors: 1, complete: false });
    await value.capture.settle();
  });

  it("anchors followups and elapsed metadata to the acknowledgement before persistence delay", async () => {
    const value = fixture(); value.clock.advance(2000);
    const run = value.capture.accept({ worker: 1, sessionId: sessions[0], acceptedAtMs: 0, gateway: value.gateway,
      expectations, phase: () => "accepted" });
    await run.initial; await flush();
    value.clock.advance(3000); await flush(); value.clock.advance(15000); await run.done;
    expect(value.calls.map(call => call.at)).toEqual([2000, 2000, 2000, 2000, 5000, 5000, 5000, 5000, 20000, 20000, 20000, 20000]);
    expect(value.rows.filter(row => row.checkpoint === 0).every(row => row.atMs === 2000)).toBe(true);
    expect(value.capture.snapshot().complete).toBe(true);
    await value.capture.settle();
  });

  it.each([-1, 1, Number.NaN, Number.POSITIVE_INFINITY])("rejects an invalid acknowledgement timestamp %s without dispatch", async acceptedAtMs => {
    const value = fixture();
    await value.capture.accept({ worker: 1, sessionId: sessions[0], acceptedAtMs, gateway: value.gateway,
      expectations, phase: () => "accepted" }).done;
    expect(value.calls).toHaveLength(0);
    expect(value.capture.snapshot()).toMatchObject({ sessions: 0, rejected: 1, complete: false });
    await value.capture.settle();
  });

  it.each(["missing", "empty", "mismatch"] as const)("retains %s as a complete observation without publishing response content", async outcome => {
    const value = fixture({ phase: () => "untrusted-secret-phase", response: ({ sessionId, expected }) => outcome === "missing"
      ? new Response("untrusted-secret-error-body", { status: 404 })
      : Response.json({ ...body(sessionId, expected, outcome === "empty" ? "" : "untrusted-secret-artifact"), ignored: "untrusted-secret-field" }) });
    await finishSchedule(value, value.accept().initial);
    expect(value.rows.every(row => row.outcome === outcome && row.lifecyclePhase === "other")).toBe(true);
    expect(value.capture.snapshot().complete).toBe(true);
    const serialized = JSON.stringify(value.capture.snapshot());
    for (const forbidden of [...sessions, "backend.invalid", "untrusted-secret", "body_b64", "bootstrap.enc.json", "codex_workspace", "fixture-static"]) {
      expect(serialized).not.toContain(forbidden);
    }
  });

  it("allows ordinary account requests outside an artifact scope without capturing them", async () => {
    const value = fixture(), response = new Response("ordinary body");
    let calls = 0;
    expect(await value.capture.wrapAccountRequest("https://ordinary.invalid/path", {}, async () => { calls++; return response; })).toBe(response);
    expect(calls).toBe(1); expect(value.capture.snapshot().rowCount).toBe(0);
    await value.capture.settle();
  });

  it("never dispatches a credential path supplied as an artifact expectation", async () => {
    const value = fixture();
    const unsafe = expectations.map(value => value.label === "supervisor_static" ? { ...value, path: ".ghostprov-v2/supervisor/bootstrap.enc.json" } : value) as unknown as StartupArtifactExpectations;
    await expect(value.accept(1, sessions[0], unsafe).done).resolves.toBeUndefined();
    expect(value.calls).toHaveLength(0);
    expect(value.capture.snapshot()).toMatchObject({ sessions: 0, rejected: 1, errors: 1, complete: false });
  });

  it.each(["session", "path", "duplicate_path"] as const)("rejects an account request with a changed %s before the account call", async change => {
    let nextCalls = 0;
    const value = fixture({ requestUrl: url => {
      if (change === "session") url.searchParams.set("session_id", sessions[2]);
      else if (change === "path") url.searchParams.set("path", ".ghostprov-v2/supervisor/bootstrap.enc.json");
      else url.searchParams.append("path", url.searchParams.get("path")!);
      return url.href;
    }, response: ({ sessionId, expected }) => { nextCalls++; return Response.json(body(sessionId, expected)); } });
    await finishSchedule(value, value.accept().initial);
    expect(nextCalls).toBe(0);
    expect(value.rows.every(row => row.outcome === "request_scope_mismatch")).toBe(true);
    expect(value.capture.snapshot().memory.ordinaryUsedBytes).toBe(0);
  });

  it.each([
    ["wrong_session", "identity_mismatch"], ["wrong_path", "identity_mismatch"], ["wrong_relative", "identity_mismatch"],
    ["negative_bytes", "invalid_response"], ["byte_mismatch", "invalid_encoding"], ["noncanonical_base64", "invalid_encoding"],
    ["newline_base64", "invalid_encoding"], ["decoded_overflow", "decoded_overflow"], ["invalid_json", "invalid_response"],
  ] as const)("rejects %s response evidence", async (mutation, outcome) => {
    const value = fixture({ response: ({ sessionId, expected }) => {
      if (mutation === "invalid_json") return new Response("not json");
      const raw = body(sessionId, expected);
      if (mutation === "wrong_session") raw.session_id = sessions[2];
      if (mutation === "wrong_path") raw.path = `${raw.path}suffix`;
      if (mutation === "wrong_relative") raw.relative_path = expected.path;
      if (mutation === "negative_bytes") raw.bytes = -1;
      if (mutation === "byte_mismatch") raw.bytes = 1;
      if (mutation === "noncanonical_base64") { raw.bytes = 1; raw.body_b64 = "Zh=="; }
      if (mutation === "newline_base64") raw.body_b64 = `${raw.body_b64}\n`;
      if (mutation === "decoded_overflow") raw.bytes = expected.label === "bundle" ? MiB + 1 : 16 * 1024 + 1;
      return Response.json(raw);
    } });
    await finishSchedule(value, value.accept().initial);
    expect(value.rows.every(row => row.outcome === outcome && row.status === 200)).toBe(true);
    expect(value.capture.snapshot()).toMatchObject({ complete: false, errors: 12, activeReads: 0, accountOwners: 0 });
  });

  it.each(["header", "stream"] as const)("enforces the artifact wire ceiling at the %s boundary", async kind => {
    let cancelled = 0;
    const value = fixture({ response: ({ expected }) => {
      const maximum = expected.label === "bundle" ? 2 * MiB : 64 * 1024;
      const stream = new ReadableStream<Uint8Array>({
        start(controller) { if (kind === "stream") controller.enqueue(new Uint8Array(maximum + 1)); },
        cancel() { cancelled++; },
      });
      return new Response(stream, { headers: kind === "header" ? { "content-length": String(maximum + 1) } : {} });
    } });
    await finishSchedule(value, value.accept().initial);
    expect(value.rows.every(row => row.outcome === "wire_overflow")).toBe(true);
    expect(cancelled).toBe(12);
    expect(value.capture.snapshot().memory.activePermits).toBe(0);
  });

  it("accepts the exact decoded byte ceilings with optional relative paths omitted", async () => {
    const value = fixture({ response: ({ sessionId, expected }) => {
      const raw = body(sessionId, expected, "x".repeat(expected.label === "bundle" ? MiB : 16 * 1024));
      delete raw.relative_path;
      return Response.json(raw);
    } });
    await finishSchedule(value, value.accept().initial);
    expect(value.rows.every(row => row.outcome === "mismatch" && row.bytes === (row.artifact === "bundle" ? MiB : 16 * 1024))).toBe(true);
    expect(value.capture.snapshot()).toMatchObject({ complete: true, errors: 0, activeReads: 0, accountOwners: 0 });
  });

  it("retains the permit for a late account owner after the public deadline and skips overlapping batches", async () => {
    const late = Promise.withResolvers<Response>();
    const value = fixture({ detachOnAbort: true, response: ({ sessionId, expected }) => expected.label === "bundle" ? late.promise : Response.json(body(sessionId, expected)) });
    const run = value.accept();
    await flush(); value.clock.advance(5000); await run.initial; await flush();
    expect(value.capture.snapshot()).toMatchObject({ activeReads: 1, accountOwners: 1, memory: { ordinaryUsedBytes: 16 * MiB, activePermits: 1 } });
    value.clock.advance(15000); await run.done;
    expect(value.calls).toHaveLength(4);
    expect(value.rows.filter(row => row.outcome === "previous_read_pending")).toHaveLength(8);
    expect(value.rows.find(row => row.artifact === "bundle" && row.checkpoint === 0)).toMatchObject({ outcome: "timeout", durationMs: 5000 });
    expect(value.capture.snapshot().complete).toBe(false);
    late.resolve(Response.json(body(sessions[0], expectations[0]))); await flush();
    expect(value.capture.snapshot()).toMatchObject({ activeReads: 0, accountOwners: 0, memory: { ordinaryUsedBytes: 0, activePermits: 0 } });
    await value.capture.settle();
    expect(value.clock.waiters.size).toBe(0);
  });

  it("accepts a four-second artifact read within the five-second allowance", async () => {
    const response = Promise.withResolvers<Response>();
    let firstBundle = true;
    const value = fixture({ response: ({ sessionId, expected }) => {
      if (expected.label === "bundle" && firstBundle) { firstBundle = false; return response.promise; }
      return Response.json(body(sessionId, expected));
    } });
    const run = value.accept();
    try {
      await flush(); value.clock.advance(4000); await flush();
      response.resolve(Response.json(body(sessions[0], expectations[0])));
      await run.initial; await flush();
      expect(value.rows.find(row => row.artifact === "bundle" && row.checkpoint === 0)).toMatchObject({
        outcome: "matched", equal: true, atMs: 0, durationMs: 4000,
      });
      value.clock.advance(1000); await flush(); value.clock.advance(15000); await run.done;
      expect(value.capture.snapshot()).toMatchObject({ complete: true, errors: 0, rowCount: 12, activeReads: 0, accountOwners: 0 });
    } finally {
      value.abort.abort(); response.resolve(Response.json(body(sessions[0], expectations[0])));
      await run.done; await flush(); await value.capture.settle();
    }
    expect(value.clock.waiters.size).toBe(0);
  });

  it("ends a read started at twenty seconds at the twenty-five-second horizon", async () => {
    const late = Promise.withResolvers<Response>();
    const clock = new ManualClock();
    const value = fixture({ clock, detachOnAbort: true, response: ({ sessionId, expected }) =>
      clock.now >= 20000 && expected.label === "bundle" ? late.promise : Response.json(body(sessionId, expected)) });
    const run = value.accept();
    let done = false; void run.done.then(() => { done = true; });
    try {
      await run.initial; await flush(); clock.advance(5000); await flush(); clock.advance(15000); await flush();
      clock.advance(4999); await flush();
      expect(done).toBe(false);
      expect(value.rows).toHaveLength(11);
      clock.advance(1); await run.done;
      expect(done).toBe(true);
      expect(value.rows.find(row => row.artifact === "bundle" && row.checkpoint === 20)).toMatchObject({
        outcome: "timeout", atMs: 20000, durationMs: 5000,
      });
      expect(value.capture.snapshot()).toMatchObject({ complete: false, errors: 1, rowCount: 12, activeReads: 1, accountOwners: 1,
        memory: { ordinaryCapacityBytes: 48 * MiB, ordinaryUsedBytes: 16 * MiB } });
      expect(value.calls).toHaveLength(12);
    } finally {
      value.abort.abort(); late.resolve(Response.json(body(sessions[0], expectations[0])));
      await run.done; await flush(); await value.capture.settle();
    }
    expect(value.capture.snapshot()).toMatchObject({ activeReads: 0, accountOwners: 0, memory: { ordinaryUsedBytes: 0, activePermits: 0 } });
    expect(clock.waiters.size).toBe(0);
  });

  it.each([
    { checkpoint: 0, overrunMs: 0 }, { checkpoint: 0, overrunMs: 1 },
    { checkpoint: 20, overrunMs: 0 }, { checkpoint: 20, overrunMs: 1 },
  ] as const)("rejects a response at an expired read deadline before its timeout callback runs (%j)", async ({ checkpoint, overrunMs }) => {
    const late = Promise.withResolvers<Response>(), clock = new ManualClock();
    const value = fixture({ clock, response: ({ sessionId, expected }) => clock.now === checkpoint * 1000 && expected.label === "bundle"
      ? late.promise : Response.json(body(sessionId, expected)) });
    const run = value.accept();
    try {
      if (checkpoint === 20) {
        await run.initial; await flush(); clock.advance(5000); await flush(); clock.advance(15000);
      }
      await flush();
      // Simulate an occupied event loop: wall time advances, timers are not dispatched.
      clock.now = checkpoint * 1000 + 5000 + overrunMs;
      late.resolve(Response.json(body(sessions[0], expectations[0])));
      if (checkpoint === 0) await run.initial; else await run.done;
      expect(value.rows.find(row => row.artifact === "bundle" && row.checkpoint === checkpoint)).toMatchObject({
        outcome: "timeout", equal: null, atMs: checkpoint * 1000, durationMs: 5000 + overrunMs,
      });
      expect(value.capture.snapshot().complete).toBe(false);
    } finally {
      value.abort.abort(); late.resolve(Response.json(body(sessions[0], expectations[0])));
      await run.done; await flush(); await value.capture.settle();
    }
    expect(value.capture.snapshot()).toMatchObject({ activeReads: 0, accountOwners: 0, memory: { ordinaryUsedBytes: 0, activePermits: 0 } });
    expect(clock.waiters.size).toBe(0);
  });

  it("retains a cancelled response body owner until its underlying cancellation settles", async () => {
    const cancel = Promise.withResolvers<void>();
    const value = fixture({ detachOnAbort: true, response: ({ sessionId, expected }) => expected.label === "bundle"
      ? new Response(new ReadableStream<Uint8Array>({ cancel: () => cancel.promise })) : Response.json(body(sessionId, expected)) });
    const run = value.accept(); await flush(); value.abort.abort(); await run.initial; await run.done;
    expect(value.capture.snapshot()).toMatchObject({ activeReads: 1, accountOwners: 1, memory: { ordinaryUsedBytes: 16 * MiB } });
    cancel.resolve(); await flush();
    expect(value.capture.snapshot()).toMatchObject({ activeReads: 0, accountOwners: 0, complete: false, memory: { ordinaryUsedBytes: 0 } });
    await value.capture.settle();
  });

  it("caps the shared memory pool and waiter queue across independent capture instances", async () => {
    const clock = new ManualClock(), release = Promise.withResolvers<void>();
    const values = Array.from({ length: 3 }, () => fixture({ clock, response: async ({ sessionId, expected }) => { await release.promise; return Response.json(body(sessionId, expected)); } }));
    const runs = [values[0]!.accept(1), values[0]!.accept(2), values[1]!.accept(1), values[1]!.accept(2), values[2]!.accept(1)];
    try {
      await flush();
      const memory = values[0]!.capture.snapshot().memory;
      expect(memory.ordinaryCapacityBytes).toBe(48 * MiB);
      expect(memory.ordinaryUsedBytes).toBeLessThanOrEqual(48 * MiB);
      expect(memory.waiters).toBe(8);
      expect(values[2]!.capture.snapshot().errors).toBeGreaterThan(0);
    } finally {
      values.forEach(value => value.abort.abort()); release.resolve();
      await Promise.all(runs.map(run => run.done)); await flush();
      await Promise.all(values.map(value => value.capture.settle()));
    }
    expect(values[0]!.capture.snapshot().memory).toMatchObject({ ordinaryUsedBytes: 0, activePermits: 0, waiters: 0 });
    expect(clock.waiters.size).toBe(0);
  });

  it("handles cancellation before capture and observer exceptions without rejecting the launch promises", async () => {
    const value = fixture({ record: () => { throw new Error("untrusted-secret-record-error"); } });
    value.abort.abort(new Error("untrusted-secret-abort"));
    const run = value.accept();
    await expect(run.initial).resolves.toBeUndefined(); await expect(run.done).resolves.toBeUndefined();
    expect(value.calls).toHaveLength(0);
    expect(value.capture.snapshot()).toMatchObject({ rowCount: 12, recordErrors: 12, errors: 12, complete: false });
    expect(JSON.stringify(value.capture.snapshot())).not.toContain("untrusted-secret");
    await value.capture.settle();
  });

  it("does not run a late scheduled checkpoint after the capture horizon", async () => {
    const value = fixture(), run = value.accept(); await run.initial; await flush();
    value.clock.advance(25000); await run.done;
    expect(value.calls).toHaveLength(4);
    expect(value.rows.filter(row => row.outcome === "horizon_expired")).toHaveLength(8);
    expect(value.capture.snapshot().complete).toBe(false);
    await value.capture.settle();
  });
});
