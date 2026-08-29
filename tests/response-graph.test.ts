import { describe, expect, it } from "bun:test";
import {
  PreviousResponseNotFoundError,
  ResponseGraph,
  ResponseGraphCapacityError,
  SharedByteBudget,
  SYNTHETIC_WARMUP_ID_PREFIX,
  releaseResponseGraphPayload,
  releaseResponseGraphResolution,
  type ResponseCreateEvent,
  type ResponseGraphResolveOptions,
  type ResolvedRelayRequest,
} from "../src/ws/response-graph";

import {
  createAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../src/http/api-key";

const user = (text: string) => ({
  type: "message",
  role: "user",
  content: [{ type: "input_text", text }],
});

const alias = (label: string) => `${SYNTHETIC_WARMUP_ID_PREFIX}${label}`;
const defaultAuthorizationScope = createAuthenticatedTenantScope();

function request(input: unknown, extra: Partial<ResponseCreateEvent> = {}): ResponseCreateEvent {
  return { type: "response.create", model: "gpt-5.6-sol", input, ...extra };
}

function graphOptions(
  options: Partial<ResponseGraphResolveOptions> = {},
): ResponseGraphResolveOptions {
  return {
    ...options,
    authorizationScope: options.authorizationScope ?? defaultAuthorizationScope,
  };
}

function resolveGraph(
  graph: ResponseGraph,
  event: ResponseCreateEvent,
  options?: Partial<ResponseGraphResolveOptions>,
) {
  return graph.resolve(event, graphOptions(options));
}

function recordWarmup(
  graph: ResponseGraph,
  id: string,
  event: ResponseCreateEvent,
  options?: Partial<ResponseGraphResolveOptions>,
): void {
  graph.recordWarmup(id, event, graphOptions(options));
}

describe("ResponseGraph persisted generated lane", () => {
  it.each([
    { label: "omitted", event: request([user("omitted")]) },
    { label: "false", event: request([user("false")], { store: false }) },
    { label: "true", event: request([user("true")], { store: true }) },
  ])("normalizes client store:$label to upstream store:true", ({ event }) => {
    const graph = new ResponseGraph();
    Object.assign(event, {
      stream_id: "public-lane",
      stream: true,
      generate: true,
      previous_response_id: "resp_provider_parent_exact",
      instructions: "current instructions",
      include: ["file_search_call.results"],
      vendor_extension: { exact: true },
      background: null,
    });

    const resolved = resolveGraph(graph, event);

    expect(resolved.relayBody).toMatchObject({
      store: true,
      stream: true,
      previous_response_id: "resp_provider_parent_exact",
      input: event.input,
      instructions: "current instructions",
      include: ["file_search_call.results"],
      vendor_extension: { exact: true },
      background: null,
    });
    expect(resolved.relayBody).not.toHaveProperty("type");
    expect(resolved.relayBody).not.toHaveProperty("stream_id");
    expect(resolved.relayBody).not.toHaveProperty("generate");
    expect(resolved.relayBody.input).toBe(event.input);
    expect(resolved.relayBody.include).not.toContain("reasoning.encrypted_content");
    expect(resolved.materializedLocally).toBe(false);
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });

    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
  });

  it("preserves ordinary input shape, identity, omission, and provider lineage exactly", () => {
    const graph = new ResponseGraph();
    const objectInput = { type: "custom_input", nested: { exact: true } };
    const cases: ResponseCreateEvent[] = [
      request("plain string", { previous_response_id: "resp_string_parent" }),
      request(objectInput, { previous_response_id: "resp_object_parent" }),
      request(null, { previous_response_id: null }),
      { type: "response.create", model: "gpt-5.6-sol" },
    ];

    const resolved = cases.map(event => resolveGraph(graph, event).relayBody);
    expect(resolved[0]!.input).toBe("plain string");
    expect(resolved[0]!.previous_response_id).toBe("resp_string_parent");
    expect(resolved[1]!.input).toBe(objectInput);
    expect(resolved[1]!.previous_response_id).toBe("resp_object_parent");
    expect(resolved[2]).toHaveProperty("input", null);
    expect(resolved[2]).toHaveProperty("previous_response_id", null);
    expect(resolved[3]).not.toHaveProperty("input");
    expect(resolved[3]).not.toHaveProperty("previous_response_id");
  });

  it("does not recursively normalize or freeze deeply nested ordinary input", () => {
    const graph = new ResponseGraph();
    let input: Record<string, unknown> = { leaf: true };
    for (let depth = 0; depth < 25_000; depth++) input = { next: input };
    const event = request(input, {
      store: false,
      previous_response_id: "resp_deep_parent_exact",
    });

    const resolved = resolveGraph(graph, event, { inputOwnership: "owned" });

    expect(resolved.relayBody.input).toBe(input);
    expect(resolved.relayBody.previous_response_id).toBe("resp_deep_parent_exact");
    expect(resolved.relayBody).toMatchObject({ stream: true, store: true });
    expect(Object.isFrozen(input)).toBe(false);
  });

  it("requires an exact authenticated server scope", () => {
    const graph = new ResponseGraph();
    const event = request("scope");
    expect(() => (graph.resolve as (event: ResponseCreateEvent) => unknown)(event))
      .toThrow("authorizationScope");
    expect(() => graph.resolve(event, {
      authorizationScope: undefined as unknown as AuthenticatedTenantScope,
    })).toThrow(TypeError);
    expect(() => graph.resolve(event, {
      authorizationScope: {} as AuthenticatedTenantScope,
    })).toThrow(TypeError);
  });

  it("passes an unknown provider ID through unchanged with only the current delta", () => {
    const graph = new ResponseGraph();
    const delta = [{ type: "function_call_output", call_id: "call_1", output: "42" }];
    const resolved = resolveGraph(graph, request(delta, {
      store: false,
      previous_response_id: "resp_unknown_but_provider_owned",
    }));

    expect(resolved.relayBody.previous_response_id).toBe("resp_unknown_but_provider_owned");
    expect(resolved.relayBody.input).toBe(delta);
    expect(resolved.relayBody.store).toBe(true);
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
  });

  it("does not replay a large image or generated output into provider continuation", () => {
    const graph = new ResponseGraph();
    const imageUrl = `data:image/png;base64,${"A".repeat(2 * 1024 * 1024)}`;
    resolveGraph(graph, request([{
      role: "user",
      content: [
        { type: "input_text", text: "inspect" },
        { type: "input_image", image_url: imageUrl },
      ],
    }], { store: false }));

    const continuation = resolveGraph(graph, request("follow up", {
      store: false,
      previous_response_id: "resp_image_provider",
    }));
    const encoded = JSON.stringify(continuation.relayBody);

    expect(continuation.relayBody.previous_response_id).toBe("resp_image_provider");
    expect(continuation.relayBody.input).toBe("follow up");
    expect(encoded).not.toContain(imageUrl);
    expect(encoded).not.toContain("provider output");
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
  });

  it("does not inject encrypted reasoning for replay and redacts unrequested provider fields", () => {
    const graph = new ResponseGraph();
    const hidden = resolveGraph(graph, request("think", { store: false }));
    expect(hidden.relayBody).not.toHaveProperty("include");

    const final = {
      id: "resp_reasoning",
      output: [{ type: "reasoning", id: "rs_1", encrypted_content: "secret", summary: [] }],
    };
    expect(graph.sanitizeOutbound(final, hidden) as unknown).toEqual({
      id: "resp_reasoning",
      output: [{ type: "reasoning", id: "rs_1", summary: [] }],
    });
    const visible = resolveGraph(graph, request([], {
      include: ["reasoning.encrypted_content"],
    }));
    expect(visible.relayBody.include).toEqual(["reasoning.encrypted_content"]);
    expect(graph.sanitizeOutbound(final, visible)).toBe(final);
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
  });

  it("redacts encrypted content iteratively from deeply nested provider output", () => {
    const graph = new ResponseGraph();
    const resolved = resolveGraph(graph, request("deep redaction"));
    let nested: Record<string, unknown> = {
      encrypted_content: "secret",
      visible: true,
    };
    for (let depth = 0; depth < 20_000; depth++) nested = { next: nested };
    const final = { output: nested };

    const sanitized = graph.sanitizeOutbound(final, resolved) as typeof final;
    let cursor = sanitized.output;
    for (let depth = 0; depth < 20_000; depth++) {
      cursor = cursor.next as Record<string, unknown>;
    }
    expect(cursor).toEqual({ visible: true });
    expect(final).not.toBe(sanitized);

    const noSecret = { output: [{ visible: true }] };
    expect(graph.sanitizeOutbound(noSecret, resolved)).toBe(noSecret);
  });

  it("keeps ordinary generated graph nodes and bytes at zero under sustained use", () => {
    const shared = new SharedByteBudget(2 * 1024 * 1024);
    const graph = new ResponseGraph({
      maxNodes: 8,
      maxBytes: 1024 * 1024,
      sharedBudget: shared,
    });

    for (let index = 0; index < 500; index++) {
      const resolved = resolveGraph(graph, request(`delta-${index}`, {
        store: index % 2 === 0,
        previous_response_id: index === 0 ? undefined : `resp_provider_${index - 1}`,
      }), { inputOwnership: "owned" });
      expect(resolved.relayBody.input).toBe(`delta-${index}`);
    }

    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(shared.used).toBe(0);
  });
});

describe("ResponseGraph synthetic warmup aliases", () => {
  it("admits only explicit generate:false requests into local state", () => {
    const graph = new ResponseGraph();
    expect(() => recordWarmup(graph, alias("generated"), request("not a warmup")))
      .toThrow("generate:false");
    expect(() => recordWarmup(graph, "resp_provider_id", request("warmup", {
      generate: false,
    }))).toThrow("Warmup alias IDs");
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
  });

  it("accumulates exact warmup deltas over an optional provider ancestor", () => {
    const shared = new SharedByteBudget(1024 * 1024);
    const graph = new ResponseGraph({ sharedBudget: shared });
    const root = alias("root");
    const child = alias("child");

    recordWarmup(graph, root, request([user("warmup one")], {
      generate: false,
      store: false,
      previous_response_id: "resp_provider_ancestor",
    }));
    recordWarmup(graph, child, request([
      { type: "function_call_output", call_id: "warm_call", output: "warmup two" },
    ], {
      generate: false,
      store: true,
      model: undefined,
      previous_response_id: root,
    }));

    const generatedDelta = [user("generate now")];
    const resolved = resolveGraph(graph, request(generatedDelta, {
      store: false,
      model: undefined,
      stream_id: "branch",
      previous_response_id: child,
    }));

    expect(resolved.relayBody).toMatchObject({
      store: true,
      stream: true,
      model: "gpt-5.6-sol",
      previous_response_id: "resp_provider_ancestor",
      input: [
        user("warmup one"),
        { type: "function_call_output", call_id: "warm_call", output: "warmup two" },
        ...generatedDelta,
      ],
    });
    expect(JSON.stringify(resolved.relayBody)).not.toContain(SYNTHETIC_WARMUP_ID_PREFIX);
    expect(resolved.materializedLocally).toBe(true);
    expect(graph.stats().nodes).toBe(2);

    const providerTerminal = {
      type: "response.completed",
      response: {
        id: "resp_generated_child",
        previous_response_id: "resp_provider_ancestor",
      },
    };
    expect(graph.sanitizeOutbound(providerTerminal, resolved)).toMatchObject({
      response: { previous_response_id: child },
    });
    releaseResponseGraphResolution(resolved);
    expect(graph.stats().nodes).toBe(2);
    expect(shared.used).toBe(graph.stats().bytes);
    graph.clear();
    expect(shared.used).toBe(0);
  });

  it.each([undefined, false, true])(
    "caches generate:false warmup independent of client store %p",
    store => {
      const graph = new ResponseGraph();
      const id = alias(`store-${String(store)}`);
      const event = request(`warm-${String(store)}`, {
        generate: false,
        ...(store === undefined ? {} : { store }),
      });

      recordWarmup(graph, id, event);

      expect(graph.stats().nodes).toBe(1);
      const resolved = resolveGraph(graph, request("child", {
        previous_response_id: id,
      }));
      expect(resolved.relayBody).toMatchObject({
        store: true,
        input: [user(`warm-${String(store)}`), user("child")],
      });
      expect(resolved.relayBody).not.toHaveProperty("previous_response_id");
      releaseResponseGraphResolution(resolved);
      graph.clear();
    },
  );

  it("keeps one immutable alias reusable for two generated forks", () => {
    const graph = new ResponseGraph();
    const root = alias("fork-root");
    recordWarmup(graph, root, request([user("shared warmup")], {
      generate: false,
      previous_response_id: "resp_provider_before_fork",
    }));

    const left = resolveGraph(graph, request([user("left")], {
      store: false,
      previous_response_id: root,
    }));
    const right = resolveGraph(graph, request([user("right")], {
      store: true,
      previous_response_id: root,
    }));

    expect(left.relayBody).toMatchObject({
      previous_response_id: "resp_provider_before_fork",
      input: [user("shared warmup"), user("left")],
      store: true,
    });
    expect(right.relayBody).toMatchObject({
      previous_response_id: "resp_provider_before_fork",
      input: [user("shared warmup"), user("right")],
      store: true,
    });
    expect(graph.stats().nodes).toBe(1);
    releaseResponseGraphResolution(left);
    releaseResponseGraphResolution(right);
    expect(graph.stats().nodes).toBe(1);

    const third = resolveGraph(graph, request("third", { previous_response_id: root }));
    expect(third.relayBody.input).toEqual([user("shared warmup"), user("third")]);
    releaseResponseGraphResolution(third);
    graph.clear();
  });

  it("materializes a deeply nested generated delta without recursively freezing it", () => {
    const graph = new ResponseGraph();
    const root = alias("deep-generated-delta");
    recordWarmup(graph, root, request("warmup", { generate: false }));
    let input: Record<string, unknown> = { leaf: true };
    for (let depth = 0; depth < 25_000; depth++) input = { next: input };

    const resolved = resolveGraph(graph, request(input, {
      previous_response_id: root,
    }), { inputOwnership: "owned" });

    const materialized = resolved.relayBody.input as unknown[];
    expect(materialized.at(-1)).toBe(input);
    expect(Object.isFrozen(input)).toBe(false);
    releaseResponseGraphPayload(resolved);
    graph.clear();
  });

  it("binds warmup aliases and provider ancestors to the trusted authorization scope", () => {
    const graph = new ResponseGraph();
    const root = alias("scoped-root");
    const trustedScope = createAuthenticatedTenantScope();
    const otherScope = createAuthenticatedTenantScope();
    const clientControlledScope = "tenant:client-controlled";
    recordWarmup(graph, root, request("scoped warmup", {
      generate: false,
      stream_id: clientControlledScope,
      previous_response_id: "resp_provider_scoped",
      client_metadata: { authorization_scope: clientControlledScope },
    }), { authorizationScope: trustedScope });

    expect(() => resolveGraph(graph, request("wrong-scope child", {
      previous_response_id: root,
    }), { authorizationScope: otherScope })).toThrow(PreviousResponseNotFoundError);
    expect(() => recordWarmup(graph, alias("wrong-scope-parent"), request("warm", {
      generate: false,
      previous_response_id: root,
    }), { authorizationScope: otherScope })).toThrow(PreviousResponseNotFoundError);

    const resolved = resolveGraph(graph, request("trusted child", {
      previous_response_id: root,
    }), { authorizationScope: trustedScope });
    expect(resolved.authorizationScope).toBe(trustedScope);
    expect(resolved.relayBody).toMatchObject({
      previous_response_id: "resp_provider_scoped",
      input: [user("scoped warmup"), user("trusted child")],
      store: true,
    });
    expect(graph.stats().nodes).toBe(1);
    releaseResponseGraphResolution(resolved);
    graph.clear();
  });

  it("keeps provider IDs opaque across scopes instead of authorizing them locally", () => {
    const graph = new ResponseGraph();
    const providerId = "resp_provider_opaque";
    const serverScope = createAuthenticatedTenantScope();
    const resolved = resolveGraph(graph, request("delta", {
      store: false,
      stream_id: "client-controlled-scope",
      previous_response_id: providerId,
      client_metadata: { authorization_scope: "client-controlled-scope" },
    }), { authorizationScope: serverScope });

    expect(resolved.authorizationScope).toBe(serverScope);
    expect(resolved).not.toHaveProperty("parentId");
    expect(resolved.relayBody).toMatchObject({
      previous_response_id: providerId,
      input: "delta",
      store: true,
    });
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
  });

  it("never forwards a missing or evicted synthetic alias as a provider ID", () => {
    const graph = new ResponseGraph();
    const missing = alias("missing");
    expect(() => resolveGraph(graph, request("child", {
      previous_response_id: missing,
    }))).toThrow(PreviousResponseNotFoundError);
    expect(() => recordWarmup(graph, alias("child-of-missing"), request("warm", {
      generate: false,
      previous_response_id: missing,
    }))).toThrow(PreviousResponseNotFoundError);

    const retained = alias("retained");
    recordWarmup(graph, retained, request("warm", { generate: false }));
    graph.evict(retained);
    expect(() => resolveGraph(graph, request("child", {
      previous_response_id: retained,
    }))).toThrow(PreviousResponseNotFoundError);
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
  });

  it("evicts least-recently-used alias leaves and clears all retained bytes", () => {
    const shared = new SharedByteBudget(64 * 1024);
    const graph = new ResponseGraph({
      maxNodes: 2,
      maxBytes: shared.limit,
      sharedBudget: shared,
    });
    const first = alias("lru-first");
    const second = alias("lru-second");
    const third = alias("lru-third");
    recordWarmup(graph, first, request("first", { generate: false }));
    recordWarmup(graph, second, request("second", { generate: false }));

    const touch = resolveGraph(graph, request("touch", { previous_response_id: first }));
    releaseResponseGraphResolution(touch);
    recordWarmup(graph, third, request("third", { generate: false }));

    expect(graph.stats().nodes).toBe(2);
    expect(() => resolveGraph(graph, request([], { previous_response_id: second })))
      .toThrow(PreviousResponseNotFoundError);
    const retained = resolveGraph(graph, request([], { previous_response_id: first }));
    releaseResponseGraphResolution(retained);
    graph.clear();
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(shared.used).toBe(0);
  });

  it("cascades explicit root eviction through accumulated aliases", () => {
    const graph = new ResponseGraph();
    const root = alias("cascade-root");
    const child = alias("cascade-child");
    recordWarmup(graph, root, request("root", { generate: false }));
    recordWarmup(graph, child, request("child", {
      generate: false,
      previous_response_id: root,
    }));
    expect(graph.stats().nodes).toBe(2);

    graph.evict(root);

    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(() => resolveGraph(graph, request("next", { previous_response_id: child })))
      .toThrow(PreviousResponseNotFoundError);
  });

  it("rejects oversized warmup state without retaining unaccounted bytes", () => {
    const shared = new SharedByteBudget(2048);
    const graph = new ResponseGraph({
      maxNodes: 4,
      maxBytes: 2048,
      sharedBudget: shared,
    });

    expect(() => recordWarmup(graph, alias("oversized"), request("x".repeat(4096), {
      generate: false,
      store: false,
    }))).toThrow(ResponseGraphCapacityError);
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(shared.used).toBe(0);
  });

  it("charges retained containers instead of their compact wire encoding", () => {
    const shared = new SharedByteBudget(1024 * 1024);
    const graph = new ResponseGraph({
      maxBytes: shared.limit,
      maxRetainedItems: 2_000,
      sharedBudget: shared,
    });
    const input = Array.from({ length: 1_000 }, () => ({}));
    const wireBytes = Buffer.byteLength(JSON.stringify(input));

    recordWarmup(graph, alias("container-accounting"), request(input, {
      generate: false,
    }), { inputOwnership: "owned" });

    expect(graph.stats().bytes).toBeGreaterThan(wireBytes * 20);
    expect(shared.used).toBe(graph.stats().bytes);
    graph.clear();
    expect(shared.used).toBe(0);
  });

  it("rejects pathological retained item and depth graphs before admission", () => {
    const shared = new SharedByteBudget(64 * 1024 * 1024);
    const graph = new ResponseGraph({ sharedBudget: shared });
    const distinctEmptyObjects = Array.from({ length: 200_000 }, () => ({}));

    expect(() => recordWarmup(graph, alias("too-many-items"), request(
      distinctEmptyObjects,
      { generate: false },
    ), { inputOwnership: "owned" })).toThrow("retained item limit");

    let nested: Record<string, unknown> = {};
    for (let depth = 0; depth < 80; depth++) nested = { child: nested };
    expect(() => recordWarmup(graph, alias("too-deep"), request(nested, {
      generate: false,
    }), { inputOwnership: "owned" })).toThrow("retained depth limit");
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(shared.used).toBe(0);
  });

  it("charges only bounded reference arrays while a warmup lineage is materialized", () => {
    const shared = new SharedByteBudget(8 * 1024 * 1024);
    const graph = new ResponseGraph({
      maxNodes: 8,
      maxBytes: 4 * 1024 * 1024,
      sharedBudget: shared,
    });
    const root = alias("materialized-root");
    recordWarmup(graph, root, request([user("x".repeat(1024 * 1024))], {
      generate: false,
    }));
    const storedBytes = shared.used;

    const resolved = resolveGraph(graph, request([user("child")], {
      previous_response_id: root,
    }));
    expect(shared.used).toBeGreaterThan(storedBytes);
    expect(shared.used - storedBytes).toBeLessThan(4096);

    releaseResponseGraphPayload(resolved);
    expect(shared.used).toBe(storedBytes);
    releaseResponseGraphResolution(resolved);
    expect(shared.used).toBe(storedBytes);
    graph.clear();
    expect(shared.used).toBe(0);
  });

  it("keeps retired warmup bytes charged until an active materialization releases", () => {
    const shared = new SharedByteBudget(8 * 1024 * 1024);
    const graph = new ResponseGraph({
      maxNodes: 2,
      maxBytes: 4 * 1024 * 1024,
      sharedBudget: shared,
    });
    const root = alias("clear-while-materialized");
    recordWarmup(graph, root, request([user("x".repeat(1024 * 1024))], {
      generate: false,
    }));
    const resolved = resolveGraph(graph, request("child", { previous_response_id: root }));

    graph.clear();
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(shared.used).toBeGreaterThan(1024 * 1024);
    expect(() => recordWarmup(graph, root, request("replacement", { generate: false })))
      .toThrow("already cached");

    releaseResponseGraphPayload(resolved);
    expect(shared.used).toBe(0);
    expect(resolved.relayBody).toEqual({});
    releaseResponseGraphResolution(resolved);
    recordWarmup(graph, root, request("replacement", { generate: false }));
    expect(graph.stats().nodes).toBe(1);
    graph.clear();
    expect(shared.used).toBe(0);
  });

  it("makes released warmup lineage payloads unreachable after router handoff", async () => {
    const shared = new SharedByteBudget(1024 * 1024);
    const graph = new ResponseGraph({ sharedBudget: shared });
    const root = alias("reachability");
    let resolved!: ResolvedRelayRequest;
    const weak = (() => {
      let payload: { marker: string } | null = { marker: "collect me" };
      const reference = new WeakRef(payload);
      recordWarmup(graph, root, request([payload], { generate: false }), {
        inputOwnership: "owned",
      });
      resolved = resolveGraph(graph, request("generated", {
        previous_response_id: root,
      }));
      graph.clear();
      payload = null;
      return reference;
    })();

    releaseResponseGraphPayload(resolved);
    expect(resolved.relayBody).toEqual({});
    expect(shared.used).toBe(0);

    let collected = false;
    for (let attempt = 0; attempt < 10 && !collected; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 0));
      Bun.gc(true);
      collected = weak.deref() === undefined;
    }
    expect(collected).toBe(true);
    releaseResponseGraphResolution(resolved);
  });

  it("releases warmup lineage pins at router handoff before provider final", () => {
    const shared = new SharedByteBudget(1024 * 1024);
    const graph = new ResponseGraph({
      maxNodes: 1,
      maxBytes: shared.limit,
      sharedBudget: shared,
    });
    const first = alias("handoff-first");
    const second = alias("handoff-second");
    recordWarmup(graph, first, request("first", { generate: false }));
    const resolved = resolveGraph(graph, request("generated child", {
      previous_response_id: first,
    }));

    // startRelay has accepted/encoded the request, but its final may remain
    // pending for an arbitrarily long provider generation.
    releaseResponseGraphPayload(resolved);
    recordWarmup(graph, second, request("second", { generate: false }));

    expect(graph.stats().nodes).toBe(1);
    expect(() => resolveGraph(graph, request("evicted", { previous_response_id: first })))
      .toThrow(PreviousResponseNotFoundError);
    const retained = resolveGraph(graph, request("retained", { previous_response_id: second }));
    releaseResponseGraphResolution(retained);
    releaseResponseGraphResolution(resolved);
    graph.clear();
    expect(shared.used).toBe(0);
  });
});
