import { describe, expect, it } from "bun:test";
import type { WebSocket } from "ws";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { V2RunHandle } from "../src/v2/transport/run-handle";
import { ResponseGraph } from "../src/ws/response-graph";
import { ResponsesWebSocketSession } from "../src/ws/session";

class FakeSocket {
  readonly OPEN = 1;
  readonly CONNECTING = 0;
  readyState = this.OPEN;
  bufferedAmount = 0;
  readonly sent: string[] = [];

  send(data: string, callback: (error?: Error) => void): void {
    this.sent.push(data);
    queueMicrotask(() => callback());
  }

  close(): void { this.readyState = 3; }
  terminate(): void { this.readyState = 3; }
}

async function flush(): Promise<void> {
  await new Promise<void>(resolve => setImmediate(resolve));
  for (let index = 0; index < 100; index++) await Promise.resolve();
}

describe("Responses WebSocket reasoning summaries", () => {
  it.each(["concise", "detailed", "auto"] as const)(
    "keeps a quiet request pending and forwards %s summary events without changing lineage or effort",
    async summary => {
      const socket = new FakeSocket();
      const graph = new ResponseGraph();
      const handle = new V2RunHandle({});
      const bodies: Record<string, unknown>[] = [];
      const signals: AbortSignal[] = [];
      const request = {
        model: "gpt-5.6-sol",
        stream: true,
        store: false,
        input: [{ role: "user", content: "Continue the earlier analysis." }],
        previous_response_id: "resp_previous_exact_provider_lineage",
        reasoning: { effort: "max", summary },
        include: ["reasoning.encrypted_content"],
      };
      const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
        authorizationScope: createAuthenticatedTenantScope(),
        graph,
        startRelay: async (body, options) => {
          bodies.push(body);
          signals.push(options.signal);
          return handle;
        },
      });
      const text = 'Checking the earlier result.\nPreserving "exact" values: λ.';
      const item = {
        id: "rs_summary_exact",
        type: "reasoning",
        summary: [{ type: "summary_text", text }],
      };
      const final = {
        id: "resp_summary_exact",
        object: "response",
        model: request.model,
        status: "completed",
        previous_response_id: request.previous_response_id,
        reasoning: request.reasoning,
        output: [item],
      };
      const summaryEvents = [
        {
          type: "response.created",
          sequence_number: 0,
          response: { id: final.id, status: "in_progress" },
        },
        {
          type: "response.output_item.added",
          sequence_number: 1,
          output_index: 0,
          item: { id: item.id, type: "reasoning", summary: [] },
        },
        {
          type: "response.reasoning_summary_part.added",
          sequence_number: 2,
          item_id: item.id,
          output_index: 0,
          summary_index: 0,
          part: { type: "summary_text", text: "" },
        },
        {
          type: "response.reasoning_summary_text.delta",
          sequence_number: 3,
          item_id: item.id,
          output_index: 0,
          summary_index: 0,
          delta: "Checking the earlier result.\n",
        },
        {
          type: "response.reasoning_summary_text.delta",
          sequence_number: 4,
          item_id: item.id,
          output_index: 0,
          summary_index: 0,
          delta: 'Preserving "exact" values: λ.',
        },
        {
          type: "response.reasoning_summary_text.done",
          sequence_number: 5,
          item_id: item.id,
          output_index: 0,
          summary_index: 0,
          text,
        },
        {
          type: "response.reasoning_summary_part.done",
          sequence_number: 6,
          item_id: item.id,
          output_index: 0,
          summary_index: 0,
          part: { type: "summary_text", text },
        },
        {
          type: "response.output_item.done",
          sequence_number: 7,
          output_index: 0,
          item,
        },
      ];
      const terminal = { type: "response.completed", sequence_number: 8, response: final };

      try {
        session.receive(JSON.stringify({ type: "response.create", ...request }));
        await flush();
        expect(bodies).toEqual([{ ...request, store: true }]);
        expect(signals).toHaveLength(1);
        expect(signals[0]!.aborted).toBe(false);

        // Hold the first provider event across scheduler turns. This exercises
        // pending behavior without shortening or simulating production timers.
        await flush();
        expect(session.isIdle()).toBe(false);
        expect(socket.readyState).toBe(socket.OPEN);
        expect(socket.sent).toEqual([]);
        expect(handle.stats.firstByteMs).toBeNull();
        expect(bodies).toHaveLength(1);
        expect(signals[0]!.aborted).toBe(false);

        for (const event of summaryEvents) {
          await handle.acceptEvent({ event: event.type, data: JSON.stringify(event) });
        }
        await flush();
        expect(socket.sent.map(value => JSON.parse(value))).toEqual(summaryEvents);
        expect(session.isIdle()).toBe(false);
        expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
        expect(bodies).toEqual([{ ...request, store: true }]);
        expect(signals[0]!.aborted).toBe(false);

        await handle.acceptEvent({ event: terminal.type, data: JSON.stringify(terminal) });
        await flush();
        expect(socket.sent.map(value => JSON.parse(value))).toEqual(summaryEvents);
        expect(session.isIdle()).toBe(false);

        handle.settle(final);
        await flush();
        expect(socket.sent.map(value => JSON.parse(value))).toEqual([...summaryEvents, terminal]);
        expect(session.isIdle()).toBe(true);
        expect(socket.readyState).toBe(socket.OPEN);
        expect(bodies).toEqual([{ ...request, store: true }]);
        expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
      } finally {
        handle.settle(final);
        session.close();
        await flush();
      }
    },
  );
});
