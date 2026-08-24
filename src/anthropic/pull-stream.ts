import type { RunHandle } from "../relay/run-handle";
import type { SseFrame } from "../relay/sse";
import { AnthropicStreamTranslator, anthropicSseFrame } from "./stream";
import type { AnthropicSseEvent } from "./stream-types";

type PendingRead =
  | { kind: "next"; result: IteratorResult<SseFrame> }
  | { kind: "error"; error: unknown };

export type AnthropicPullStreamOptions = {
  handle: RunHandle;
  translator: AnthropicStreamTranslator;
  keepAliveMs: number;
  abortController: AbortController;
  onClose(): void;
};

function timeout(ms: number): {
  promise: Promise<{ kind: "keepalive" }>;
  cancel(): void;
} {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return {
    promise: new Promise(resolve => {
      timer = setTimeout(() => resolve({ kind: "keepalive" }), ms);
      timer.unref?.();
    }),
    cancel() {
      if (timer) clearTimeout(timer);
      timer = undefined;
    },
  };
}

/**
 * Adapt one relay iterator to a downstream-pull-driven Anthropic SSE stream.
 * A keepalive never starts a second iterator read while the first is pending.
 */
export function createAnthropicPullStream(
  options: AnthropicPullStreamOptions,
): ReadableStream<Uint8Array> {
  if (!Number.isFinite(options.keepAliveMs) || options.keepAliveMs < 1) {
    throw new RangeError("keepAliveMs must be positive");
  }

  const encoder = new TextEncoder();
  const iterator = options.handle.events();
  const output: string[] = options.translator.start().map(anthropicSseFrame);
  let pendingRead: Promise<PendingRead> | null = null;
  let upstreamDone = false;
  let closed = false;
  let iteratorReturned = false;

  const cleanup = (): void => {
    if (closed) return;
    closed = true;
    options.onClose();
  };

  const returnIterator = (): void => {
    if (iteratorReturned) return;
    iteratorReturned = true;
    try {
      const returned = iterator.return?.(undefined);
      void returned?.catch(() => undefined);
    } catch {
      // The linked relay signal remains the authoritative cancellation path.
    }
  };

  const append = (events: AnthropicSseEvent[]): "error" | "message_stop" | null => {
    let terminal: "error" | "message_stop" | null = null;
    for (const event of events) {
      output.push(anthropicSseFrame(event));
      if (event.type === "error" || event.type === "message_stop") terminal = event.type;
    }
    return terminal;
  };

  const startRead = (): Promise<PendingRead> => {
    if (!pendingRead) {
      pendingRead = Promise.resolve(iterator.next()).then(
        result => ({ kind: "next" as const, result }),
        error => ({ kind: "error" as const, error }),
      );
    }
    return pendingRead;
  };

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (closed) return;
      for (;;) {
        if (output.length) {
          controller.enqueue(encoder.encode(output.shift()!));
          return;
        }
        if (upstreamDone) {
          cleanup();
          controller.close();
          return;
        }

        const keepalive = timeout(options.keepAliveMs);
        const winner = await Promise.race([startRead(), keepalive.promise]);
        keepalive.cancel();
        if (closed) return;
        if (winner.kind === "keepalive") {
          controller.enqueue(encoder.encode(": keepalive\n\n"));
          return;
        }

        pendingRead = null;
        if (winner.kind === "error") {
          append(options.translator.fail(winner.error));
          upstreamDone = true;
          returnIterator();
          continue;
        }
        if (winner.result.done) {
          upstreamDone = true;
          try {
            append(options.translator.finish(await options.handle.final));
          } catch (error) {
            append(options.translator.fail(error));
          }
          continue;
        }
        const terminal = append(options.translator.push(winner.result.value));
        if (terminal) {
          upstreamDone = true;
          if (terminal === "error" && !options.abortController.signal.aborted) {
            options.abortController.abort();
          }
          returnIterator();
        }
      }
    },
    cancel(reason) {
      if (!options.abortController.signal.aborted) options.abortController.abort(reason);
      cleanup();
      returnIterator();
    },
  });
}
