import { describe, expect, it } from "bun:test";
import {
  PreviousResponseNotFoundError,
  ResponseGraph,
  SharedByteBudget,
  releaseResponseGraphMaterialization,
  releaseResponseGraphPayload,
  type ResponseCreateEvent,
} from "../src/ws/response-graph";

const user = (text: string) => ({
  type: "message",
  role: "user",
  content: [{ type: "input_text", text }],
});

const assistant = (text: string) => ({
  id: `msg_${text}`,
  type: "message",
  role: "assistant",
  status: "completed",
  content: [{ type: "output_text", text, annotations: [] }],
});

function request(input: unknown, extra: Partial<ResponseCreateEvent> = {}): ResponseCreateEvent {
  return { type: "response.create", model: "gpt-5.6-sol", store: false, input, ...extra };
}

describe("ResponseGraph", () => {
  it("reconstructs incremental store:false continuation without leaking websocket fields", () => {
    const graph = new ResponseGraph();
    const first = graph.resolve(request([user("first")], { stream_id: "main", instructions: "initial" }));
    graph.record(first, { id: "resp_1", model: "gpt-5.6-sol", output: [assistant("one")] });

    const nextInput = [{ type: "function_call_output", call_id: "call_1", output: "42" }];
    const next = graph.resolve(request(nextInput, {
      model: undefined,
      stream_id: "main",
      previous_response_id: "resp_1",
    }));

    expect(next.relayBody).toMatchObject({ model: "gpt-5.6-sol", store: false, stream: true });
    expect(next.relayBody).not.toHaveProperty("type");
    expect(next.relayBody).not.toHaveProperty("stream_id");
    expect(next.relayBody).not.toHaveProperty("previous_response_id");
    expect(next.relayBody).not.toHaveProperty("instructions");
    expect(next.relayBody.input).toEqual([user("first"), assistant("one"), ...nextInput]);
    expect(next.relayBody.include).toContain("reasoning.encrypted_content");
    releaseResponseGraphMaterialization(next.relayBody);
  });

  it("supports two cross-lane forks from the same cached parent", () => {
    const graph = new ResponseGraph();
    const root = graph.resolve(request([user("root")]));
    graph.record(root, { id: "resp_root", output: [assistant("root answer")] });

    const left = graph.resolve(request([user("left")], { stream_id: "left", previous_response_id: "resp_root" }));
    const right = graph.resolve(request([user("right")], { stream_id: "right", previous_response_id: "resp_root" }));

    expect(left.relayBody.input).toEqual([user("root"), assistant("root answer"), user("left")]);
    expect(right.relayBody.input).toEqual([user("root"), assistant("root answer"), user("right")]);
    expect(graph.stats().nodes).toBe(1);
    releaseResponseGraphMaterialization(left.relayBody);
    releaseResponseGraphMaterialization(right.relayBody);
  });

  it("shares frozen JSON values without allowing caller or transport mutation", () => {
    const graph = new ResponseGraph();
    const source = [user("immutable")];
    const root = graph.resolve(request(source));
    (source[0]!.content[0] as { text: string }).text = "caller-mutated";
    expect(root.relayBody.input).toEqual([user("immutable")]);
    expect(Object.isFrozen(root.relayBody.input)).toBe(true);
    expect(Object.isFrozen((root.relayBody.input as any[])[0])).toBe(true);

    const final = { id: "immutable-root", output: [assistant("original-output")] };
    graph.record(root, final);
    final.output[0]!.content[0]!.text = "final-mutated";
    const continuation = graph.resolve(request([user("next")], {
      previous_response_id: "immutable-root",
    }));
    expect(JSON.stringify(continuation.relayBody.input)).toContain("original-output");
    expect(JSON.stringify(continuation.relayBody.input)).not.toContain("caller-mutated");
    expect(JSON.stringify(continuation.relayBody.input)).not.toContain("final-mutated");
    releaseResponseGraphMaterialization(continuation.relayBody);
  });

  it("rejects an unknown store:false previous_response_id", () => {
    const graph = new ResponseGraph();
    expect(() => graph.resolve(request([], { previous_response_id: "resp_missing" }))).toThrow(
      PreviousResponseNotFoundError,
    );
  });

  it("passes an unknown store:true previous_response_id to persisted upstream state", () => {
    const graph = new ResponseGraph();
    const resolved = graph.resolve(request([], { store: true, previous_response_id: "resp_persisted" }));
    expect(resolved.relayBody.previous_response_id).toBe("resp_persisted");
  });

  it("retains encrypted reasoning internally and redacts it unless requested", () => {
    const graph = new ResponseGraph();
    const resolved = graph.resolve(request([user("think")]));
    const final: any = {
      id: "resp_reasoning",
      output: [{ type: "reasoning", id: "rs_1", encrypted_content: "secret", summary: [] }],
    };
    graph.record(resolved, final);

    expect(graph.sanitizeOutbound(final, resolved)).toEqual({
      id: "resp_reasoning",
      output: [{ type: "reasoning", id: "rs_1", summary: [] }],
    });

    const continuation = graph.resolve(request([user("continue")], { previous_response_id: "resp_reasoning" }));
    expect(JSON.stringify(continuation.relayBody.input)).toContain("secret");
    releaseResponseGraphMaterialization(continuation.relayBody);

    const visible = graph.resolve(request([], { include: ["reasoning.encrypted_content"] }));
    expect(graph.sanitizeOutbound(final, visible)).toBe(final);
  });

  it("reuses unchanged streaming objects and copy-on-writes only redacted paths", () => {
    const graph = new ResponseGraph();
    const hidden = graph.resolve(request([]));
    const plain = {
      type: "response.output_text.delta",
      delta: "token",
      nested: { annotations: [] },
    };
    expect(graph.sanitizeOutbound(plain, hidden)).toBe(plain);

    const encrypted = {
      type: "response.reasoning.done",
      item: {
        id: "rs_copy_on_write",
        encrypted_content: "secret",
        summary: [],
      },
      unchanged: { marker: true },
    };
    const sanitized = graph.sanitizeOutbound(encrypted, hidden);
    expect(sanitized).not.toBe(encrypted);
    expect(sanitized.item).not.toBe(encrypted.item);
    expect(sanitized.unchanged).toBe(encrypted.unchanged);
    expect(sanitized.item).not.toHaveProperty("encrypted_content");
    expect(encrypted.item.encrypted_content).toBe("secret");
  });

  it("rewrites continuation IDs without mutating the terminal object", () => {
    const graph = new ResponseGraph();
    const root = graph.resolve(request([user("root")]));
    graph.record(root, { id: "resp_parent", output: [] });
    const continuation = graph.resolve(request([], {
      previous_response_id: "resp_parent",
    }));
    const terminal = {
      type: "response.completed",
      response: { id: "resp_child", previous_response_id: null as string | null },
    };
    const outbound = graph.sanitizeOutbound(terminal, continuation);
    expect(outbound).not.toBe(terminal);
    expect(outbound.response).not.toBe(terminal.response);
    expect(outbound.response.previous_response_id).toBe("resp_parent");
    expect(terminal.response.previous_response_id).toBeNull();
    releaseResponseGraphMaterialization(continuation.relayBody);
  });

  it("evicts least-recently-used independent leaves within count and shared byte budgets", () => {
    const shared = new SharedByteBudget(10_000);
    const graph = new ResponseGraph({ maxNodes: 2, maxBytes: 10_000, sharedBudget: shared });
    for (let index = 1; index <= 3; index++) {
      const resolved = graph.resolve(request([user(String(index))]));
      graph.record(resolved, { id: `resp_${index}`, output: [assistant(String(index))] });
    }
    expect(graph.stats().nodes).toBe(2);
    expect(() => graph.resolve(request([], { previous_response_id: "resp_1" }))).toThrow(
      PreviousResponseNotFoundError,
    );
    const latest = graph.resolve(request([], { previous_response_id: "resp_3" }));
    expect(latest.parentId).toBe("resp_3");
    releaseResponseGraphMaterialization(latest.relayBody);
    graph.clear();
    expect(shared.used).toBe(0);
  });

  it("cascades explicit eviction through descendants while preserving other forks", () => {
    const graph = new ResponseGraph();
    const root = graph.resolve(request([user("root")]));
    graph.record(root, { id: "root", output: [assistant("root")] });
    const left = graph.resolve(request([user("left")], { previous_response_id: "root" }));
    graph.record(left, { id: "left", output: [assistant("left")] });
    const right = graph.resolve(request([user("right")], { previous_response_id: "root" }));
    graph.record(right, { id: "right", output: [assistant("right")] });

    graph.evict("left");
    expect(() => graph.resolve(request([], { previous_response_id: "left" }))).toThrow();
    const retained = graph.resolve(request([], { previous_response_id: "right" }));
    expect(retained.parentId).toBe("right");
    releaseResponseGraphMaterialization(retained.relayBody);
  });

  it("never evicts the parent required by a continuation being recorded", () => {
    const graph = new ResponseGraph({ maxNodes: 1, maxBytes: 10_000, sharedBudget: new SharedByteBudget(10_000) });
    const root = graph.resolve(request([user("root")]));
    graph.record(root, { id: "root", output: [assistant("root")] });
    const child = graph.resolve(request([user("child")], { previous_response_id: "root" }));
    expect(() => graph.record(child, {
      id: "child",
      output: [assistant("child")],
    })).toThrow("capacity");

    const retained = graph.resolve(request([], { previous_response_id: "root" }));
    expect(retained.parentId).toBe("root");
    releaseResponseGraphMaterialization(retained.relayBody);
    expect(() => graph.resolve(request([], { previous_response_id: "child" }))).toThrow(
      PreviousResponseNotFoundError,
    );
  });

  it("retains a continuation parent from payload handoff through child recording", () => {
    const graph = new ResponseGraph({
      maxNodes: 2,
      maxBytes: 10_000,
      sharedBudget: new SharedByteBudget(10_000),
    });
    const root = graph.resolve(request([user("root")]));
    graph.record(root, { id: "root", output: [assistant("root")] });
    const child = graph.resolve(request([user("child")], { previous_response_id: "root" }));

    // This models the current router-handoff release before the child settles.
    releaseResponseGraphPayload(child);
    const pressureOne = graph.resolve(request([user("pressure-one")]));
    graph.record(pressureOne, { id: "pressure-one", output: [] });
    const pressureTwo = graph.resolve(request([user("pressure-two")]));
    graph.record(pressureTwo, { id: "pressure-two", output: [] });
    graph.record(child, { id: "child", output: [assistant("child")] });

    const grandchild = graph.resolve(request([user("grandchild")], {
      previous_response_id: "child",
    }));
    expect(grandchild.relayBody.input).toEqual([
      user("root"),
      assistant("root"),
      user("child"),
      assistant("child"),
      user("grandchild"),
    ]);
    releaseResponseGraphMaterialization(grandchild.relayBody);
  });

  it("reports cache admission failure instead of silently publishing an unusable id", () => {
    const graph = new ResponseGraph({
      maxNodes: 1,
      maxBytes: 10_000,
      sharedBudget: new SharedByteBudget(10_000),
    });
    const root = graph.resolve(request([user("root")]));
    graph.record(root, { id: "root", output: [assistant("root")] });
    const child = graph.resolve(request([user("child")], { previous_response_id: "root" }));
    expect(() => graph.record(child, {
      id: "child",
      output: [assistant("child")],
    })).toThrow("capacity");
  });

  it("does not evict valid local history when shared cache admission cannot succeed", () => {
    const shared = new SharedByteBudget(5_000);
    const graph = new ResponseGraph({ maxNodes: 8, maxBytes: 5_000, sharedBudget: shared });
    const competing = new ResponseGraph({ maxNodes: 8, maxBytes: 5_000, sharedBudget: shared });
    const root = graph.resolve(request([user("root")]));
    graph.record(root, { id: "root", output: [assistant("a".repeat(1_000))] });
    const other = competing.resolve(request([user("other")]));
    competing.record(other, {
      id: "other",
      output: [{ type: "message", content: "b".repeat(1_500) }],
    });

    const impossible = graph.resolve(request([user("impossible")]));
    expect(() => graph.record(impossible, {
      id: "impossible",
      output: [assistant("c".repeat(2_000))],
    })).toThrow("capacity");

    expect(graph.stats().nodes).toBe(1);
    competing.clear();
    const retained = graph.resolve(request([], { previous_response_id: "root" }));
    expect(retained.parentId).toBe("root");
    releaseResponseGraphMaterialization(retained.relayBody);
    graph.clear();
    expect(shared.used).toBe(0);
  });

  it("retains only byte-accounted deltas across a deep continuation chain", () => {
    const MiB = 1024 * 1024;
    Bun.gc(true);
    const before = process.memoryUsage().heapUsed;
    const shared = new SharedByteBudget(256 * MiB);
    const graph = new ResponseGraph({
      maxNodes: 256,
      maxBytes: 256 * MiB,
      sharedBudget: shared,
    });
    let previousResponseId: string | undefined;

    for (let index = 0; index < 80; index++) {
      const resolved = graph.resolve(request([
        user(`${index}:` + "x".repeat(32 * 1024)),
      ], previousResponseId ? { previous_response_id: previousResponseId } : {}));
      previousResponseId = `resp_${index}`;
      graph.record(resolved, {
        id: previousResponseId,
        model: "gpt-5.6-sol",
        output: [],
      });
    }

    Bun.gc(true);
    const retainedHeap = Math.max(0, process.memoryUsage().heapUsed - before);
    expect(graph.stats().bytes).toBeLessThan(3 * MiB);
    expect(retainedHeap).toBeLessThan(25 * MiB);
    graph.clear();
    expect(shared.used).toBe(0);
  });

  it("pre-reserves sixteen large continuation lineages before cloning", () => {
    const MiB = 1024 * 1024;
    const shared = new SharedByteBudget(70 * MiB);
    const graph = new ResponseGraph({
      maxNodes: 32,
      maxBytes: 8 * MiB,
      sharedBudget: shared,
    });
    const root = graph.resolve(request([user("root")]));
    graph.record(root, {
      id: "resp_large_root",
      model: "gpt-5.6-sol",
      output: [assistant("x".repeat(2 * MiB))],
    });
    const storedBytes = shared.used;
    expect(storedBytes * 17).toBeLessThanOrEqual(shared.limit);
    expect(storedBytes * 18).toBeGreaterThan(shared.limit);

    const active = Array.from({ length: 16 }, (_, index) => graph.resolve(request(
      [user(`child-${index}`)],
      { previous_response_id: "resp_large_root", stream_id: `lane-${index}` },
    )));
    expect(shared.used).toBeLessThanOrEqual(shared.limit);
    expect(() => graph.resolve(request([user("overflow")], {
      previous_response_id: "resp_large_root",
    }))).toThrow("materialization capacity");

    for (const resolved of active) {
      releaseResponseGraphMaterialization(resolved.relayBody);
    }
    expect(shared.used).toBe(storedBytes);
    graph.clear();
    expect(shared.used).toBe(0);
  });
});
