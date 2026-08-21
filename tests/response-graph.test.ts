import { describe, expect, it } from "vitest";
import {
  PreviousResponseNotFoundError,
  ResponseGraph,
  SharedByteBudget,
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
    const final = {
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

    const visible = graph.resolve(request([], { include: ["reasoning.encrypted_content"] }));
    expect(graph.sanitizeOutbound(final, visible)).toEqual(final);
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
    expect(graph.resolve(request([], { previous_response_id: "resp_3" })).parentId).toBe("resp_3");
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
    expect(graph.resolve(request([], { previous_response_id: "right" })).parentId).toBe("right");
  });

  it("never evicts the parent required by a continuation being recorded", () => {
    const graph = new ResponseGraph({ maxNodes: 1, maxBytes: 10_000, sharedBudget: new SharedByteBudget(10_000) });
    const root = graph.resolve(request([user("root")]));
    graph.record(root, { id: "root", output: [assistant("root")] });
    const child = graph.resolve(request([user("child")], { previous_response_id: "root" }));
    graph.record(child, { id: "child", output: [assistant("child")] });

    expect(graph.resolve(request([], { previous_response_id: "root" })).parentId).toBe("root");
    expect(() => graph.resolve(request([], { previous_response_id: "child" }))).toThrow(
      PreviousResponseNotFoundError,
    );
  });
});
