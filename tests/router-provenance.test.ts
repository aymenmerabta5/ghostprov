import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { sseFrame } from "../src/relay/sse";
import {
  type IncomingResourceLease,
  MultiSandboxRouter,
} from "../src/router/router";
import {
  RESPONSE_PROVENANCE_LIMITS,
  ResponseProvenanceCommitError,
  ResponseProvenanceObsoleteError,
  ResponseProvenanceRegistry,
  ResponseProvenanceScopeError,
} from "../src/router/provenance";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { ResourceGovernor, type ResourceLease } from "../src/router/resource-governor";
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

function recordProvenance(
  registry: ResponseProvenanceRegistry,
  responseId: unknown,
  scope: ReturnType<typeof createAuthenticatedTenantScope>,
  provenance: { readonly projectId: ProjectId; readonly incarnation: number },
): boolean {
  const reservation = registry.reserve();
  return reservation !== null && registry.commit(reservation, responseId, scope, provenance);
}

function detachedIncomingLease(): IncomingResourceLease {
  return {
    release: () => undefined,
    replace: async workload => {
      let reservedBytes = workload.reservedBytes;
      let active = true;
      let lease!: ResourceLease;
      lease = {
        get reservedBytes() { return active ? reservedBytes : 0; },
        replace: async replacement => {
          if (!active) throw new Error("detached lease is released");
          reservedBytes = replacement.reservedBytes;
          return lease;
        },
        release: () => { active = false; },
      };
      return lease;
    },
  };
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
    readonly capacity = 4,
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
      capacity: this.capacity,
      active: this.active,
      available: this.capacity - this.active,
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

function routerSystem(
  randomValues: number[] = [0],
  queueTimeoutMs = 1_000,
) {
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
  it("exposes only a bounded per-project retirement fence and prunes it at TTL", () => {
    let now = 1;
    const registry = new ResponseProvenanceRegistry(() => now);
    const scope = createAuthenticatedTenantScope();
    const first = projectId("00000000-0000-4000-8000-000000000001");
    const second = projectId("00000000-0000-4000-8000-000000000002");
    expect(recordProvenance(registry, "resp_retirement_fence", scope, {
      projectId: first,
      incarnation: 1,
    })).toBe(true);
    expect(registry.bindingsForProject(first)).toBe(1);
    expect(registry.bindingsForProject(second)).toBe(0);
    now += RESPONSE_PROVENANCE_LIMITS.ttlMs;
    expect(registry.bindingsForProject(first)).toBe(0);
  });

  it("keeps scope/project/incarnation binding immutable under poisoning and races", async () => {
    let now = 1;
    const registry = new ResponseProvenanceRegistry(() => now);
    const firstScope = createAuthenticatedTenantScope();
    const otherScope = createAuthenticatedTenantScope();
    const first = projectId("00000000-0000-4000-8000-000000000001");
    const other = projectId("00000000-0000-4000-8000-000000000002");

    expect(recordProvenance(registry, "resp_bound", firstScope, {
      projectId: first,
      incarnation: 7,
    })).toBe(true);
    expect(recordProvenance(registry, "resp_bound", firstScope, {
      projectId: first,
      incarnation: 7,
    })).toBe(true);
    expect(recordProvenance(registry, "resp_bound", firstScope, {
      projectId: other,
      incarnation: 7,
    })).toBe(false);
    expect(recordProvenance(registry, "resp_bound", firstScope, {
      projectId: first,
      incarnation: 8,
    })).toBe(false);
    expect(recordProvenance(registry, "resp_bound", otherScope, {
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
      Promise.resolve().then(() => recordProvenance(registry, "resp_race", firstScope, {
        projectId: first,
        incarnation: 1,
      })),
      Promise.resolve().then(() => recordProvenance(registry, "resp_race", otherScope, {
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

    expect(recordProvenance(registry,
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
      expect(recordProvenance(registry, `r${index}`, scope, location)).toBe(true);
    }
    expect(recordProvenance(registry, "record-overflow", scope, location)).toBe(true);
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
      if (!recordProvenance(registry,
        `${String(index).padStart(5, "0")}-${"z".repeat(500)}`,
        scope,
        location,
      )) break;
    }
    const byteBounded = registry.snapshot();
    expect(byteBounded.bytes).toBeLessThanOrEqual(RESPONSE_PROVENANCE_LIMITS.maxBytes);
    expect(byteBounded.records).toBeLessThan(2_000);
    expect(recordProvenance(registry, "byte-overflow", scope, location)).toBe(true);
    expect(registry.lookup(`00000-${"z".repeat(500)}`, scope).kind).toBe("unknown");
    expect(registry.lookup("byte-overflow", scope).kind).toBe("known");

    registry.clear();
    now = 0;
    expect(recordProvenance(registry, "resp_ttl_old", scope, location)).toBe(true);
    now = 1;
    expect(recordProvenance(registry, "resp_ttl_new", scope, location)).toBe(true);
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
    const { router, first, second, authorizationScope } = routerSystem([0, 0.999]);
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

      expect(router.lookupResponseProvenance("resp_failed_not_portable", authorizationScope))
        .toEqual({ kind: "unknown" });
      expect(router.lookupResponseProvenance("resp_incomplete_not_portable", authorizationScope))
        .toEqual({ kind: "unknown" });
    } finally {
      while (first.pending.length > 0) first.completeNext();
      while (second.pending.length > 0) second.completeNext();
      await router.close();
    }
  });

  it("normalizes once, commits provenance before final/terminal exposure, and pins continuations", async () => {
    const { router, first, second, logged, authorizationScope } = routerSystem([0, 0.999]);
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
      expect(router.lookupResponseProvenance("resp_first_1", authorizationScope))
        .toEqual({ kind: "unknown" });

      const firstFinal = first.completeNext();
      await expect(handle.final).resolves.toBe(firstFinal);
      await expect(terminal).resolves.toMatchObject({
        done: false,
        value: { event: "response.completed" },
      });
      expect(router.lookupResponseProvenance(firstFinal.id, authorizationScope)).toEqual({
        kind: "known",
        provenance: { projectId: first.projectId, incarnation: 1 },
      });
      expect(router.lookupResponseProvenance(firstFinal.id))
        .toEqual({ kind: "scope_mismatch" });
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

  it("queues at metadata pressure without spinning or capping the 4,096-job ingress queue", async () => {
    const reservationCapacity = Math.min(
      RESPONSE_PROVENANCE_LIMITS.maxRecords,
      Math.floor(
        RESPONSE_PROVENANCE_LIMITS.maxBytes /
          RESPONSE_PROVENANCE_LIMITS.maxRecordBytes,
      ),
    );
    const runtime = new ProvenanceRuntime(
      projectId("00000000-0000-4000-8000-000000000001"),
      "pressure",
      reservationCapacity + 1,
    );
    const authorizationScope = createAuthenticatedTenantScope();
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([runtime]),
      governor: governor(),
      authorizationScope,
      queueTimeoutMs: 2_000,
      random: { next: () => 0, uuid: () => "trace-pressure" },
    });
    try {
      const starts = Array.from({ length: reservationCapacity + 1 }, (_, index) => (
        router.startRelay(
          { model: "gpt-5.6-sol", input: `pressure-${index}` },
          { incomingLease: detachedIncomingLease() },
        )
      ));
      const admitted = await Promise.all(starts.slice(0, reservationCapacity));
      await flush(50);

      expect(runtime.executions).toBe(reservationCapacity);
      expect(router.snapshot()).toMatchObject({
        queued: 1,
        provenance: { reservedRecords: reservationCapacity },
      });
      // Repeated microtask turns cannot rescan/spin or execute without a fresh
      // reservation. The terminal below is the sole capacity wake.
      await flush(100);
      expect(runtime.executions).toBe(reservationCapacity);

      runtime.completeNext();
      await admitted[0]!.final;
      const last = await starts.at(-1)!;
      expect(runtime.executions).toBe(reservationCapacity + 1);
      expect(router.snapshot().queued).toBe(0);

      while (runtime.pending.length > 0) runtime.completeNext();
      await Promise.all([
        ...admitted.slice(1).map(handle => handle.final),
        last.final,
      ]);
    } finally {
      while (runtime.pending.length > 0) runtime.completeNext();
      await router.close();
    }
  });

  it("never fails a known continuation over and rejects an obsolete incarnation immediately", async () => {
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
      const obsoleteStartedAt = performance.now();
      await expect(router.startRelay({
        model: "gpt-5.6-sol",
        input: "old incarnation",
        previous_response_id: originalFinal.id,
      })).rejects.toBeInstanceOf(ResponseProvenanceObsoleteError);
      expect(performance.now() - obsoleteStartedAt).toBeLessThan(50);
      expect(second.executions).toBe(0);
    } finally {
      while (first.pending.length > 0) first.completeNext();
      while (second.pending.length > 0) second.completeNext();
      await router.close();
    }
  });

  it("rejects a queued continuation as soon as its pinned incarnation is replaced", async () => {
    const { router, first, second } = routerSystem([0], 1_000);
    try {
      const seed = await router.startRelay({ model: "gpt-5.6-sol", input: "seed" });
      const seedFinal = first.completeNext();
      await seed.final;

      first.active = first.capacity;
      first.notify();
      const pending = router.startRelay({
        model: "gpt-5.6-sol",
        input: "queued old incarnation",
        previous_response_id: seedFinal.id,
      });
      await flush();
      expect(router.snapshot().queued).toBe(1);

      const replacedAt = performance.now();
      first.replaceIncarnation();
      await expect(pending).rejects.toBeInstanceOf(ResponseProvenanceObsoleteError);
      expect(performance.now() - replacedAt).toBeLessThan(50);
      expect(router.snapshot().queued).toBe(0);
      expect(second.executions).toBe(0);
    } finally {
      first.active = 0;
      while (first.pending.length > 0) first.completeNext();
      while (second.pending.length > 0) second.completeNext();
      await router.close();
    }
  });

  it("accepts only the exact configured authorization-scope object", async () => {
    const { router, first, second, authorizationScope } = routerSystem();
    const otherScope = createAuthenticatedTenantScope();
    try {
      await expect(router.startRelay(
        { model: "gpt-5.6-sol", input: "foreign scope" },
        { authorizationScope: otherScope },
      )).rejects.toBeInstanceOf(ResponseProvenanceScopeError);
      expect(router.lookupResponseProvenance("resp_unknown", otherScope))
        .toEqual({ kind: "scope_mismatch" });
      expect(router.responseProvenanceAuthorizationScope()).toBe(authorizationScope);
      expect(first.executions + second.executions).toBe(0);
    } finally {
      await router.close();
    }
  });

  it("releases provenance and disposes a committed handle when final access throws", async () => {
    const { router, first, second, authorizationScope } = routerSystem([0]);
    let discarded = 0;
    first.run = async (_body, _signal, options): Promise<RunHandle> => {
      first.executions++;
      options?.onAdmissionReflected?.();
      options?.onExecutionRiskReflected?.();
      return {
        async *events() {},
        discardUnconsumedEvents: () => { discarded++; },
        get final(): Promise<unknown> {
          throw new Error("synthetic final accessor failure");
        },
        stats: { bytes: 0, firstByteMs: null, frames: 0 },
      };
    };
    try {
      await expect(router.startRelay(
        { model: "gpt-5.6-sol", input: "throwing committed handle" },
        { authorizationScope },
      )).rejects.toMatchObject({ committed: true, ambiguous: true });
      await flush();
      expect(discarded).toBe(1);
      expect(router.snapshot().provenance).toMatchObject({
        reservedRecords: 0,
        reservedBytes: 0,
      });
      expect(second.executions).toBe(0);
    } finally {
      await router.close();
    }
  });

  it("keeps cleanup ownership when committed metadata and prelude accessors throw", async () => {
    const { router, first, authorizationScope } = routerSystem([0]);
    const terminal = deferred<ReturnType<typeof response>>();
    let discarded = 0;
    first.run = async (_body, _signal, options): Promise<RunHandle> => {
      first.executions++;
      options?.onAdmissionReflected?.();
      options?.onExecutionRiskReflected?.();
      return {
        async *events() {},
        discardUnconsumedEvents: () => { discarded++; },
        final: terminal.promise,
        get upstreamPrelude(): Promise<never> {
          throw new Error("synthetic upstream prelude accessor failure");
        },
        get stats(): RunHandle["stats"] {
          throw new Error("synthetic stats accessor failure");
        },
      };
    };
    try {
      const outcome = await Promise.race([
        router.startRelay(
          { model: "gpt-5.6-sol", input: "throwing committed metadata" },
          { authorizationScope },
        ).then(
          () => ({ kind: "handle" as const }),
          error => ({ kind: "error" as const, error }),
        ),
        Bun.sleep(100).then(() => ({ kind: "timeout" as const })),
      ]);
      expect(outcome).toMatchObject({
        kind: "error",
        error: { committed: true, ambiguous: true },
      });
      expect(discarded).toBe(1);
      expect(router.snapshot().provenance).toMatchObject({
        reservedRecords: 0,
        reservedBytes: 0,
      });
      terminal.resolve(response("resp_discarded_metadata"));
      await expect(router.awaitIdle(1_000)).resolves.toBeUndefined();
    } finally {
      terminal.resolve(response("resp_discarded_metadata"));
      await router.close();
    }
  });

  it("does not let a router without composition authority adopt request scopes", async () => {
    const runtime = new ProvenanceRuntime(
      projectId("00000000-0000-4000-8000-000000000001"),
      "unscoped",
    );
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([runtime]),
      governor: governor(),
    });
    const scope = createAuthenticatedTenantScope();
    try {
      await expect(router.startRelay(
        { model: "gpt-5.6-sol", input: "cannot adopt scope" },
        { authorizationScope: scope },
      )).rejects.toBeInstanceOf(ResponseProvenanceScopeError);
      expect(router.lookupResponseProvenance("resp_unknown", scope))
        .toEqual({ kind: "scope_mismatch" });
      expect(runtime.executions).toBe(0);
    } finally {
      await router.close();
    }
  });

  it("fails closed on response-ID scope/binding conflicts without poisoning the first record", async () => {
    const { router, first, second, authorizationScope } = routerSystem([0, 0.999]);
    const secondScope = createAuthenticatedTenantScope();
    first.responseIds.push("resp_collision");
    second.responseIds.push("resp_collision");
    try {
      const firstHandle = await router.startRelay(
        { model: "gpt-5.6-sol", input: "first" },
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
      );
      second.completeNext();
      await expect(conflicting.final).rejects.toBeInstanceOf(ResponseProvenanceCommitError);
      expect(router.lookupResponseProvenance(
        "resp_collision",
        authorizationScope,
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
