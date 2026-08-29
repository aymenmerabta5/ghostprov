import type { RunHandle } from "../relay/run-handle";
import type { SseFrame } from "../relay/sse";
import { AnthropicStreamTranslator, anthropicSseFrame } from "./stream";
import type { AnthropicSseEvent } from "./stream-types";
import {
  releaseAtomicBytePermit,
  transferAtomicBytePermit,
} from "../v2/memory/permit-carrier";

type PendingRead =
  | { kind: "next"; result: IteratorResult<SseFrame> }
  | { kind: "error"; error: unknown };

type FinalRead =
  | { kind: "final"; value: unknown }
  | { kind: "error"; error: unknown };

type ClosedRead = { kind: "closed" };
type AbortedRead = { kind: "aborted"; reason: unknown };

const AUTHORITATIVE_FINAL_EVENTS = new Set([
  "response.completed",
  "response.failed",
  "response.incomplete",
]);

function finalTerminalEvent(value: unknown): string | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const status = (value as Record<string, unknown>).status;
  return status === "completed" || status === "failed" || status === "incomplete"
    ? `response.${status}`
    : null;
}

interface PendingOutput {
  data: string;
  /** Present only on the last translated output derived from one owner. */
  owner?: object;
}

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
  const output: PendingOutput[] = options.translator.start().map(event => ({
    data: anthropicSseFrame(event),
  }));
  let pendingRead: Promise<PendingRead> | null = null;
  let pendingFinal: Promise<FinalRead> | null = null;
  let iteratorDone = false;
  let observedTerminalEvent: string | null = null;
  let upstreamDone = false;
  let closed = false;
  let iteratorReturned = false;
  let activeTimer: ReturnType<typeof setTimeout> | undefined;
  let deliveredOwner: object | null = null;
  let dequeuedFrame: SseFrame | null = null;
  let finalReleaseWatched = false;
  let streamController: ReadableStreamDefaultController<Uint8Array> | null = null;
  let internalFailureAbort = false;
  let resolveClosed!: (value: ClosedRead) => void;
  const closedRead = new Promise<ClosedRead>(resolve => { resolveClosed = resolve; });
  let resolveAborted!: (value: AbortedRead) => void;
  const abortedRead = new Promise<AbortedRead>(resolve => { resolveAborted = resolve; });
  let onAbort!: () => void;

  const clearKeepAlive = (): void => {
    if (activeTimer !== undefined) clearTimeout(activeTimer);
    activeTimer = undefined;
  };

  const releaseOutputOwners = (): void => {
    releaseAtomicBytePermit(deliveredOwner);
    deliveredOwner = null;
    releaseAtomicBytePermit(dequeuedFrame);
    dequeuedFrame = null;
    for (const item of output) releaseAtomicBytePermit(item.owner);
    output.length = 0;
  };

  const finalDescriptor = Object.getOwnPropertyDescriptor(options.handle, "final");
  const passiveFinal = finalDescriptor && "value" in finalDescriptor
    ? finalDescriptor.value as Promise<unknown>
    : null;
  const releaseFinalWhenSettled = (allowAccessor = false): void => {
    if (finalReleaseWatched) return;
    if (!passiveFinal && !allowAccessor) return;
    finalReleaseWatched = true;
    void Promise.resolve(passiveFinal ?? options.handle.final).then(
      releaseAtomicBytePermit,
      () => undefined,
    );
  };

  const cleanup = (): void => {
    if (closed) return;
    closed = true;
    clearKeepAlive();
    releaseOutputOwners();
    options.abortController.signal.removeEventListener("abort", onAbort);
    resolveClosed({ kind: "closed" });
    options.onClose();
  };

  const returnIterator = (): void => {
    if (iteratorReturned) return;
    iteratorReturned = true;
    options.handle.discardUnconsumedEvents?.();
    try {
      const returned = iterator.return?.(undefined);
      void returned?.catch(() => undefined);
    } catch {
      // The linked relay signal remains the authoritative cancellation path.
    }
  };

  const append = (
    events: AnthropicSseEvent[],
    source?: unknown,
  ): "error" | "message_stop" | null => {
    let terminal: "error" | "message_stop" | null = null;
    const translated: PendingOutput[] = [];
    for (const event of events) {
      translated.push({ data: anthropicSseFrame(event) });
      if (event.type === "error" || event.type === "message_stop") terminal = event.type;
    }
    if (source !== undefined) {
      if (translated.length > 0) {
        const owner = {};
        transferAtomicBytePermit(source, owner, "anthropic-sse-output");
        translated[translated.length - 1]!.owner = owner;
      } else {
        releaseAtomicBytePermit(source);
      }
    }
    output.push(...translated);
    return terminal;
  };

  const startRead = (): Promise<PendingRead> => {
    if (!pendingRead) {
      try {
        pendingRead = Promise.resolve(iterator.next()).then(
          result => {
            if (!result.done) {
              if (closed) releaseAtomicBytePermit(result.value);
              else dequeuedFrame = result.value;
            }
            return { kind: "next" as const, result };
          },
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

  const abortInternally = (reason?: unknown): void => {
    if (options.abortController.signal.aborted) return;
    internalFailureAbort = true;
    try { options.abortController.abort(reason); } finally { internalFailureAbort = false; }
  };

  const fail = (error: unknown): void => {
    append(options.translator.fail(error));
    upstreamDone = true;
    releaseFinalWhenSettled(Boolean(iteratorDone || pendingFinal));
    clearKeepAlive();
    abortInternally();
    returnIterator();
  };

  onAbort = () => {
    resolveAborted({
      kind: "aborted",
      reason: options.abortController.signal.reason,
    });
    if (internalFailureAbort || closed) return;
    releaseFinalWhenSettled(Boolean(iteratorDone || pendingFinal));
    returnIterator();
    cleanup();
    try { streamController?.close(); } catch {
      // A simultaneous downstream cancel may already have closed the stream.
    }
  };
  options.abortController.signal.addEventListener("abort", onAbort, { once: true });
  if (options.abortController.signal.aborted) onAbort();

  return new ReadableStream<Uint8Array>({
    start(controller) {
      streamController = controller;
      if (closed) controller.close();
    },
    async pull(controller) {
      if (closed) return;
      // A new pull proves the prior translated chunk left the stream queue.
      releaseAtomicBytePermit(deliveredOwner);
      deliveredOwner = null;
      for (;;) {
        if (output.length) {
          const item = output.shift()!;
          try {
            controller.enqueue(encoder.encode(item.data));
            deliveredOwner = item.owner ?? null;
          } catch (error) {
            releaseAtomicBytePermit(item.owner);
            fail(error);
            cleanup();
            controller.error(error);
          }
          return;
        }
        if (upstreamDone) {
          cleanup();
          controller.close();
          return;
        }

        if (iteratorDone) {
          const final = await Promise.race([
            startFinal(),
            keepAlive(),
            closedRead,
            abortedRead,
          ]);
          clearKeepAlive();
          if (final.kind === "closed" || closed) return;
          if (final.kind === "aborted") {
            releaseFinalWhenSettled(true);
            returnIterator();
            cleanup();
            controller.close();
            return;
          }
          if (final.kind === "keepalive") {
            controller.enqueue(encoder.encode(": keepalive\n\n"));
            return;
          }
          pendingFinal = null;
          if (final.kind === "error") fail(final.error);
          else {
            if (
              observedTerminalEvent !== null &&
              finalTerminalEvent(final.value) !== observedTerminalEvent
            ) {
              releaseAtomicBytePermit(final.value);
              fail(new Error("Relay terminal disagrees with its authoritative final"));
              continue;
            }
            try {
              const translated = await options.translator.finishAsync(final.value);
              if (closed || options.abortController.signal.aborted) {
                releaseAtomicBytePermit(final.value);
                returnIterator();
                if (!closed) {
                  cleanup();
                  controller.close();
                }
                return;
              }
              append(translated, final.value);
              upstreamDone = true;
            } catch (error) {
              releaseAtomicBytePermit(final.value);
              fail(error);
            }
          }
          continue;
        }

        const winner = await Promise.race([
          startRead(),
          keepAlive(),
          closedRead,
          abortedRead,
        ]);
        clearKeepAlive();
        if (winner.kind === "closed" || closed) return;
        if (winner.kind === "aborted") {
          releaseFinalWhenSettled(Boolean(iteratorDone || pendingFinal));
          returnIterator();
          cleanup();
          controller.close();
          return;
        }
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
        if (AUTHORITATIVE_FINAL_EVENTS.has(winner.result.value.event)) {
          // The transport transcript can expose its terminal event just before
          // router provenance/prefix commits make handle.final authoritative.
          // Discard this duplicate representation and rebuild the Anthropic
          // terminal solely from the exact correlated final value below.
          releaseAtomicBytePermit(winner.result.value);
          if (dequeuedFrame === winner.result.value) dequeuedFrame = null;
          observedTerminalEvent = winner.result.value.event;
          iteratorDone = true;
          returnIterator();
          continue;
        }
        let terminal: "error" | "message_stop" | null;
        try {
          const translated = await options.translator.pushAsync(winner.result.value);
          if (closed || options.abortController.signal.aborted) {
            releaseAtomicBytePermit(winner.result.value);
            if (dequeuedFrame === winner.result.value) dequeuedFrame = null;
            returnIterator();
            if (!closed) {
              cleanup();
              controller.close();
            }
            return;
          }
          terminal = append(
            translated,
            winner.result.value,
          );
          if (dequeuedFrame === winner.result.value) dequeuedFrame = null;
        } catch (error) {
          releaseAtomicBytePermit(winner.result.value);
          if (dequeuedFrame === winner.result.value) dequeuedFrame = null;
          fail(error);
          continue;
        }
        if (terminal) {
          upstreamDone = true;
          releaseFinalWhenSettled(true);
          if (terminal === "error" && !options.abortController.signal.aborted) {
            abortInternally();
          }
          returnIterator();
        }
      }
    },
    cancel(reason) {
      releaseFinalWhenSettled(Boolean(iteratorDone || pendingFinal));
      cleanup();
      if (!options.abortController.signal.aborted) options.abortController.abort(reason);
      returnIterator();
    },
  }, { highWaterMark: 0 });
}
