import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { sseFrame } from "../src/relay/sse";
import {
  MultiSandboxRouter,
  RouterCapacityTimeoutError,
} from "../src/router/router";
import {
  RESPONSE_PROVENANCE_LIMITS,
  ResponseProvenanceCommitError,
  ResponseProvenanceRegistry,
  ResponseProvenanceScopeError,
} from "../src/router/provenance";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { ResourceGovernor } from "../src/router/resource-governor";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type SandboxRunOptions,
} from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { V2PreCommitError } from "../src/v2/transport/run-handle";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolveValue, rejectValue) => {
    resolve = resolveValue;
    reject = rejectValue;
  });
  return { promise, resolve, reject };
}

function response(id: string, status: "completed" | "failed" | "incomplete" = "completed") {
  return { id, object: "response", status, output: [] };
}

class ProvenanceRuntime implements RoutableSandboxRuntime {
  incarnation = 1;
  active = 0;
  executions = 0;
  failStarts = 0;
  readonly bodies: Record<string, unknown>[] = [];
  readonly responseIds: string[] = [];
  readonly responseStatuses: Array<"completed" | "failed" | "incomplete"> = [];
  readonly pending: Array<{
    final: ReturnType<typeof response>;
    gate: ReturnType<typeof deferred<ReturnType<typeof response>>>;
  }> = [];
  readonly listeners = new Set<() => void>();

  constructor(
    readonly projectId: ProjectId,
    readonly label: string,
  ) {}

  async start(): Promise<void> {}
  async drain(): Promise<void> {}
  async stop(): Promise<void> {}

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  notify(): void {
    for (const listener of this.listeners) listener();
  }

  replaceIncarnation(): void {
    this.incarnation++;
    this.notify();
  }

  snapshot() {
    return {
      projectId: this.projectId,
      incarnation: this.incarnation,
      state: "ready",
      capacity: 4,
      active: this.active,
      available: 4 - this.active,
      quarantined: 0,
      heartbeatAgeMs: 0,
      credentialExpiresInMs: 60_000,
      ackEwmaMs: 1,
      completionEwmaMs: 10,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    } as const;
  }

  async run(
    body: Record<string, unknown>,
    _signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    this.executions++;
    this.bodies.push(body);
    if (this.failStarts > 0) {
      this.failStarts--;
      throw new V2PreCommitError("synthetic safe precommit failure", "test_precommit");
    }
    this.active++;
    options?.onAdmissionReflected?.();
    options?.onExecutionRiskReflected?.();
    const id = this.responseIds.shift() ?? `resp_${this.label}_${this.executions}`;
    const status = this.responseStatuses.shift() ?? "completed";
    const final = response(id, status);
    const gate = deferred<ReturnType<typeof response>>();
    this.pending.push({ final, gate });
    const terminalEvent = `response.${status}`;
    const frame = {
      event: terminalEvent,
      data: JSON.stringify({ type: terminalEvent, response: final }),
    };
    return {
      async *events() { yield frame; },
      final: gate.promise.finally(() => {
        this.active--;
        this.notify();
      }),
      stats: {
        bytes: new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength,
        firstByteMs: 0,
        frames: 1,
      },
    };
  }

  completeNext(): ReturnType<typeof response> {
    const pending = this.pending.shift();
    if (!pending) throw new Error("runtime has no pending response");
    pending.gate.resolve(pending.final);
    return pending.final;
  }
}

function governor(): ResourceGovernor {
  return new ResourceGovernor({
    probe: {
      sample: () => ({
        totalBytes: 16 * 1024 ** 3,
        freeBytes: 12 * 1024 ** 3,
        rssBytes: 256 * 1024 ** 2,
        heapBytes: 64 * 1024 ** 2,
        externalBytes: 8 * 1024 ** 2,
        logicalProcessors: 8,
      }),
    },
  });
}

function routerSystem(randomValues: number[] = [0], queueTimeoutMs = 1_000) {
  const first = new ProvenanceRuntime(
    projectId("00000000-0000-4000-8000-000000000001"),
    "first",
  );
  const second = new ProvenanceRuntime(
    projectId("00000000-0000-4000-8000-000000000002"),
    "second",
  );
  let randomIndex = 0;
  const logged: unknown[] = [];
  const authorizationScope = createAuthenticatedTenantScope();
  const router = new MultiSandboxRouter({
    registry: new SandboxRegistry([first, second]),
    governor: governor(),
    authorizationScope,
    queueTimeoutMs,
    routeAttemptTimeoutMs: Math.min(queueTimeoutMs, 100),
    logger: {
      emit: event => { logged.push(event); },
      snapshot: () => ({
        queuedEvents: 0,
        queuedBytes: 0,
        droppedSamples: 0,
        droppedProgress: 0,
        droppedTerminals: 0,
        sinkErrors: 0,
      }),
      close: async () => {},
    },
    random: {
      next: () => randomValues[Math.min(randomIndex++, randomValues.length - 1)] ?? 0,
      uuid: () => `trace-${randomIndex}`,
    },
  });
  return { router, first, second, logged, authorizationScope };
}

async function flush(turns = 20): Promise<void> {
  for (let index = 0; index < turns; index++) await Promise.resolve();
}

describe("ResponseProvenanceRegistry", () => {
  it("keeps scope/project/incarnation binding immutable under poisoning and races", async () => {
    let now = 1;
    const registry = new ResponseProvenanceRegistry(() => now);
    const firstScope = createAuthenticatedTenantScope();
    const otherScope = createAuthenticatedTenantScope();
    const first = projectId("00000000-0000-4000-8000-000000000001");
    const other = projectId("00000000-0000-4000-8000-000000000002");

    expect(registry.record("resp_bound", firstScope, {
      projectId: first,
      incarnation: 7,
    })).toBe(true);
    expect(registry.record("resp_bound", firstScope, {
      projectId: first,
      incarnation: 7,
    })).toBe(true);
    expect(registry.record("resp_bound", firstScope, {
      projectId: other,
      incarnation: 7,
    })).toBe(false);
    expect(registry.record("resp_bound", firstScope, {
      projectId: first,
      incarnation: 8,
    })).toBe(false);
    expect(registry.record("resp_bound", otherScope, {
      projectId: first,
      incarnation: 7,
    })).toBe(false);
    expect(registry.lookup("resp_bound", otherScope)).toEqual({
      kind: "scope_mismatch",
    });
    expect(registry.lookup("resp_bound", firstScope)).toEqual({
      kind: "known",
      provenance: { projectId: first, incarnation: 7 },
    });

    const raced = await Promise.all([
      Promise.resolve().then(() => registry.record("resp_race", firstScope, {
        projectId: first,
        incarnation: 1,
      })),
      Promise.resolve().then(() => registry.record("resp_race", otherScope, {
        projectId: other,
        incarnation: 2,
      })),
    ]);
    expect(raced).toEqual([true, false]);
    expect(registry.lookup("resp_race", firstScope)).toEqual({
      kind: "known",
      provenance: { projectId: first, incarnation: 1 },
    });
    expect(registry.lookup("resp_race", otherScope)).toEqual({
      kind: "scope_mismatch",
    });

    now += 1;
    expect(registry.snapshot()).toMatchObject({ records: 2 });
  });

  it("enforces ID, reservation, count, byte, and absolute TTL ceilings with oldest-hint eviction", () => {
    let now = 0;
    const registry = new ResponseProvenanceRegistry(() => now);
    const scope = createAuthenticatedTenantScope();
    const location = {
      projectId: projectId("00000000-0000-4000-8000-000000000001"),
      incarnation: 1,
    };

    expect(registry.record(
      "x".repeat(RESPONSE_PROVENANCE_LIMITS.maxResponseIdBytes + 1),
      scope,
      location,
    )).toBe(false);
    expect(registry.snapshot()).toMatchObject({
      records: 0,
      bytes: 0,
      reservedRecords: 0,
      reservedBytes: 0,
    });

    const reservation = registry.reserve();
    expect(reservation).not.toBeNull();
    expect(registry.snapshot()).toMatchObject({
      records: 0,
      reservedRecords: 1,
      reservedBytes: RESPONSE_PROVENANCE_LIMITS.maxRecordBytes,
    });
    registry.release(reservation);
    expect(registry.snapshot()).toMatchObject({ reservedRecords: 0, reservedBytes: 0 });

    for (let index = 0; index < RESPONSE_PROVENANCE_LIMITS.maxRecords; index++) {
      expect(registry.record(`r${index}`, scope, location)).toBe(true);
    }
    expect(registry.record("record-overflow", scope, location)).toBe(true);
    expect(registry.lookup("r0", scope).kind).toBe("unknown");
    expect(registry.lookup("r1", scope).kind).toBe("known");
    expect(registry.lookup("record-overflow", scope).kind).toBe("known");
    expect(registry.snapshot()).toMatchObject({
      records: RESPONSE_PROVENANCE_LIMITS.maxRecords,
      maxRecords: RESPONSE_PROVENANCE_LIMITS.maxRecords,
      maxBytes: RESPONSE_PROVENANCE_LIMITS.maxBytes,
      reservedRecords: 0,
    });

    registry.clear();
    for (let index = 0; index < 2_000; index++) {
      if (!registry.record(
        `${String(index).padStart(5, "0")}-${"z".repeat(500)}`,
        scope,
        location,
      )) break;
    }
    const byteBounded = registry.snapshot();
    expect(byteBounded.bytes).toBeLessThanOrEqual(RESPONSE_PROVENANCE_LIMITS.maxBytes);
    expect(byteBounded.records).toBeLessThan(2_000);
    expect(registry.record("byte-overflow", scope, location)).toBe(true);
    expect(registry.lookup(`00000-${"z".repeat(500)}`, scope).kind).toBe("unknown");
    expect(registry.lookup("byte-overflow", scope).kind).toBe("known");

    registry.clear();
    now = 0;
    expect(registry.record("resp_ttl_old", scope, location)).toBe(true);
    now = 1;
    expect(registry.record("resp_ttl_new", scope, location)).toBe(true);
    now = RESPONSE_PROVENANCE_LIMITS.ttlMs - 1;
    expect(registry.lookup("resp_ttl_old", scope).kind).toBe("known");
    now = RESPONSE_PROVENANCE_LIMITS.ttlMs;
    expect(registry.lookup("resp_ttl_old", scope)).toEqual({ kind: "unknown" });
    expect(registry.lookup("resp_ttl_new", scope).kind).toBe("known");
    now = RESPONSE_PROVENANCE_LIMITS.ttlMs + 1;
    expect(registry.lookup("resp_ttl_new", scope)).toEqual({ kind: "unknown" });
    expect(registry.snapshot()).toMatchObject({ records: 0, bytes: 0 });
  });
});

describe("MultiSandboxRouter response provenance", () => {
  it("routes an unknown provider continuation normally", async () => {
    const { router, first, second } = routerSystem([0.999]);
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "unknown continuation",
        previous_response_id: "resp_not_locally_known",
      });
      expect(first.executions).toBe(0);
      expect(second.executions).toBe(1);
      second.completeNext();
      await handle.final;
    } finally {
      while (first.pending.length > 0) first.completeNext();
      while (second.pending.length > 0) second.completeNext();
      await router.close();
    }
  });

  it("does not index failed or incomplete authoritative finals", async () => {
    const { router, first, second } = routerSystem([0, 0.999]);
    first.responseIds.push("resp_failed_not_portable");
    first.responseStatuses.push("failed");
    second.responseIds.push("resp_incomplete_not_portable");
    second.responseStatuses.push("incomplete");
    try {
      const failed = await router.startRelay({ model: "gpt-5.6-sol", input: "failed" });
      first.completeNext();
      await failed.final;
      const incomplete = await router.startRelay({ model: "gpt-5.6-sol", input: "incomplete" });
      second.completeNext();
      await incomplete.final;

      expect(router.lookupResponseProvenance("resp_failed_not_portable"))
        .toEqual({ kind: "unknown" });
      expect(router.lookupResponseProvenance("resp_incomplete_not_portable"))
        .toEqual({ kind: "unknown" });
    } finally {
      while (first.pending.length > 0) first.completeNext();
      while (second.pending.length > 0) second.completeNext();
      await router.close();
    }
  });

  it("normalizes once, commits provenance before final/terminal exposure, and pins continuations", async () => {
    const { router, first, second, logged } = routerSystem([0, 0.999]);
    const input = Object.freeze({
      model: "gpt-5.6-sol",
      input: "first",
      stream: false,
      store: false,
    });
    try {
      const handle = await router.startRelay(input);
      expect(first.bodies).toHaveLength(1);
      expect(first.bodies[0]).toMatchObject({ stream: true, store: true });
      expect(input).toMatchObject({ stream: false, store: false });
      const terminal = handle.events().next();
      await flush();
      expect(await Promise.race([
        terminal.then(() => "settled" as const),
        Promise.resolve("pending" as const),
      ])).toBe("pending");
      expect(router.lookupResponseProvenance("resp_first_1")).toEqual({ kind: "unknown" });

      const firstFinal = first.completeNext();
      await expect(handle.final).resolves.toBe(firstFinal);
      await expect(terminal).resolves.toMatchObject({
        done: false,
        value: { event: "response.completed" },
      });
      expect(router.lookupResponseProvenance(firstFinal.id)).toEqual({
        kind: "known",
        provenance: { projectId: first.projectId, incarnation: 1 },
      });
      expect(JSON.stringify(logged)).not.toContain(firstFinal.id);

      const continuation = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "continue",
        previous_response_id: firstFinal.id,
      });
      expect(first.executions).toBe(2);
      expect(second.executions).toBe(0);
      first.completeNext();
      await continuation.final;
    } finally {
      while (first.pending.length > 0) first.completeNext();
      while (second.pending.length > 0) second.completeNext();
      await router.close();
    }
  });

  it("never fails a known continuation over to another project or incarnation", async () => {
    const { router, first, second } = routerSystem([0, 0.999, 0.999], 300);
    try {
      const original = await router.startRelay({ model: "gpt-5.6-sol", input: "seed" });
      const originalFinal = first.completeNext();
      await original.final;

      first.failStarts = 1;
      const retrying = router.startRelay({
        model: "gpt-5.6-sol",
        input: "retry safely",
        previous_response_id: originalFinal.id,
      });
      await flush(50);
      expect(second.executions).toBe(0);
      const retryHandle = await retrying;
      expect(first.executions).toBe(3);
      expect(second.executions).toBe(0);
      first.completeNext();
      await retryHandle.final;

      first.replaceIncarnation();
      await expect(router.startRelay({
        model: "gpt-5.6-sol",
        input: "old incarnation",
        previous_response_id: originalFinal.id,
      })).rejects.toBeInstanceOf(RouterCapacityTimeoutError);
      expect(second.executions).toBe(0);
    } finally {
      while (first.pending.length > 0) first.completeNext();
      while (second.pending.length > 0) second.completeNext();
      await router.close();
    }
  });

  it("fails closed on response-ID scope/binding conflicts without poisoning the first record", async () => {
    const { router, first, second } = routerSystem([0, 0.999]);
    const firstScope = createAuthenticatedTenantScope();
    const secondScope = createAuthenticatedTenantScope();
    first.responseIds.push("resp_collision");
    second.responseIds.push("resp_collision");
    try {
      const firstHandle = await router.startRelay(
        { model: "gpt-5.6-sol", input: "first" },
        { authorizationScope: firstScope },
      );
      first.completeNext();
      await firstHandle.final;

      await expect(router.startRelay({
        model: "gpt-5.6-sol",
        input: "cross-scope continuation",
        previous_response_id: "resp_collision",
      }, {
        authorizationScope: secondScope,
      })).rejects.toBeInstanceOf(ResponseProvenanceScopeError);
      expect(first.executions).toBe(1);
      expect(second.executions).toBe(0);

      const conflicting = await router.startRelay(
        { model: "gpt-5.6-sol", input: "second" },
        { authorizationScope: secondScope },
      );
      second.completeNext();
      await expect(conflicting.final).rejects.toBeInstanceOf(ResponseProvenanceCommitError);
      expect(router.lookupResponseProvenance(
        "resp_collision",
        firstScope,
      )).toEqual({
        kind: "known",
        provenance: { projectId: first.projectId, incarnation: 1 },
      });
      expect(router.lookupResponseProvenance(
        "resp_collision",
        secondScope,
      )).toEqual({ kind: "scope_mismatch" });
      expect(first.executions).toBe(1);
      expect(second.executions).toBe(1);
    } finally {
      while (first.pending.length > 0) first.completeNext();
      while (second.pending.length > 0) second.completeNext();
      await router.close();
    }
  });
});
