import type { RunHandle } from "../relay/run-handle";
import type { SseFrame } from "../relay/sse";
import { AnthropicStreamTranslator, anthropicSseFrame } from "./stream";
import type { AnthropicSseEvent } from "./stream-types";

type PendingRead =
  | { kind: "next"; result: IteratorResult<SseFrame> }
  | { kind: "error"; error: unknown };

type FinalRead =
  | { kind: "final"; value: unknown }
  | { kind: "error"; error: unknown };

type ClosedRead = { kind: "closed" };

export type AnthropicPullStreamOptions = {
  handle: RunHandle;
  translator: AnthropicStreamTranslator;
  keepAliveMs: number;
  abortController: AbortController;
  onClose(): void;
};

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
  let pendingFinal: Promise<FinalRead> | null = null;
  let iteratorDone = false;
  let upstreamDone = false;
  let closed = false;
  let iteratorReturned = false;
  let activeTimer: ReturnType<typeof setTimeout> | undefined;
  let resolveClosed!: (value: ClosedRead) => void;
  const closedRead = new Promise<ClosedRead>(resolve => { resolveClosed = resolve; });

  const clearKeepAlive = (): void => {
    if (activeTimer !== undefined) clearTimeout(activeTimer);
    activeTimer = undefined;
  };

  const cleanup = (): void => {
    if (closed) return;
    closed = true;
    clearKeepAlive();
    resolveClosed({ kind: "closed" });
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
      try {
        pendingRead = Promise.resolve(iterator.next()).then(
          result => ({ kind: "next" as const, result }),
          error => ({ kind: "error" as const, error }),
        );
      } catch (error) {
        pendingRead = Promise.resolve({ kind: "error", error });
      }
    }
    return pendingRead;
  };

  const keepAlive = (): Promise<{ kind: "keepalive" }> => {
    clearKeepAlive();
    return new Promise(resolve => {
      activeTimer = setTimeout(() => {
        activeTimer = undefined;
        resolve({ kind: "keepalive" });
      }, options.keepAliveMs);
      activeTimer.unref?.();
    });
  };

  const startFinal = (): Promise<FinalRead> => {
    if (!pendingFinal) {
      try {
        pendingFinal = Promise.resolve(options.handle.final).then(
          value => ({ kind: "final" as const, value }),
          error => ({ kind: "error" as const, error }),
        );
      } catch (error) {
        pendingFinal = Promise.resolve({ kind: "error", error });
      }
    }
    return pendingFinal;
  };

  const fail = (error: unknown): void => {
    append(options.translator.fail(error));
    upstreamDone = true;
    clearKeepAlive();
    if (!options.abortController.signal.aborted) options.abortController.abort();
    returnIterator();
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

        if (iteratorDone) {
          const final = await Promise.race([startFinal(), keepAlive(), closedRead]);
          clearKeepAlive();
          if (final.kind === "closed" || closed) return;
          if (final.kind === "keepalive") {
            controller.enqueue(encoder.encode(": keepalive\n\n"));
            return;
          }
          pendingFinal = null;
          if (final.kind === "error") fail(final.error);
          else {
            append(options.translator.finish(final.value));
            upstreamDone = true;
          }
          continue;
        }

        const winner = await Promise.race([startRead(), keepAlive(), closedRead]);
        clearKeepAlive();
        if (winner.kind === "closed" || closed) return;
        if (winner.kind === "keepalive") {
          controller.enqueue(encoder.encode(": keepalive\n\n"));
          return;
        }

        pendingRead = null;
        if (winner.kind === "error") {
          fail(winner.error);
          continue;
        }
        if (winner.result.done) {
          iteratorDone = true;
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
      cleanup();
      if (!options.abortController.signal.aborted) options.abortController.abort(reason);
      returnIterator();
    },
  });
}
