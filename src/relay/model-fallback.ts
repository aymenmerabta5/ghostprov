import type { RunHandle } from "./run-handle";
import type { SseFrame } from "./sse";
import { parseProviderErrorEnvelope } from "../v2/provider-error";
import { releaseAtomicBytePermit } from "../v2/memory/permit-carrier";
import { V2CommittedError } from "../v2/transport/run-handle";
import { canFallbackFromModel, FALLBACK_UPSTREAM_MODEL } from "./model-policy";

export interface ModelFallbackOptions {
  readonly requestedModel: string;
  readonly signal?: AbortSignal;
  readonly startFallback: (error: V2CommittedError) => Promise<RunHandle>;
}

export function isDefinitiveModelNotFound(
  error: unknown,
  requestedModel: string,
): error is V2CommittedError {
  if (
    !canFallbackFromModel(requestedModel) ||
    !(error instanceof V2CommittedError) || error.ambiguous !== false ||
    error.code !== "model_not_found"
  ) return false;
  const envelope = parseProviderErrorEnvelope(error.providerError);
  return envelope !== null && envelope.error.code === error.code &&
    (envelope.model === null || envelope.model === requestedModel) &&
    (envelope.error.param === null || envelope.error.param === "model");
}

function eligible(
  error: unknown,
  primary: RunHandle,
  options: ModelFallbackOptions,
): error is V2CommittedError {
  return !options.signal?.aborted && primary.stats.frames === 0 && primary.stats.bytes === 0 &&
    isDefinitiveModelNotFound(error, options.requestedModel);
}

/**
 * Holds at most the first permit-bearing event while choosing a handle. Only a
 * definitive, structured model denial before every relay event can select the
 * one fallback; its errors are returned directly without another decision.
 */
export function wrapModelFallbackHandle(
  primary: RunHandle,
  options: ModelFallbackOptions,
): { handle: RunHandle; decision: Promise<void> } {
  let current = primary;
  let iterator: AsyncGenerator<SseFrame> | undefined;
  let prefix: SseFrame | undefined;
  let firstFinished = false;
  let firstDone = false;
  let firstFailed = false;
  let firstError: unknown;
  let sawPrimaryFrame = false;
  let claimed = false;
  let discarded = false;
  let draining = false;
  let selected = false;
  let terminalSettled = false;
  let eventsFinished = false;
  let cancelled = options.signal?.aborted ?? false;
  let modelFallback: { readonly from: string; readonly to: string } | undefined;

  const cleanupAbortListener = (): void => {
    // Final proof may precede delivery. An unconsumed first frame must remain
    // abort-disposable until its own memory lifetime ends.
    if (terminalSettled && (discarded || cancelled || eventsFinished ||
      (firstFinished && prefix === undefined))) {
      options.signal?.removeEventListener("abort", onAbort);
    }
  };

  const releasePrefix = (): void => {
    if (!prefix) return;
    releaseAtomicBytePermit(prefix);
    prefix = undefined;
    cleanupAbortListener();
  };
  const closeIterator = async (): Promise<void> => {
    try { await iterator?.return(undefined); } catch { /* Final is authoritative. */ }
  };
  const discardNative = (handle: RunHandle): boolean => {
    if (!handle.discardUnconsumedEvents) return false;
    try {
      handle.discardUnconsumedEvents();
      return true;
    } catch {
      return false;
    }
  };

  // A single eager pull makes final-only callers eligible too. V2 ends the
  // iterator on failure, so iterator completion alone is never failure proof.
  const first = (async (): Promise<void> => {
    try {
      iterator = primary.events();
      const next = await iterator.next();
      firstDone = next.done === true;
      if (!next.done) {
        sawPrimaryFrame = true;
        prefix = next.value;
      }
    } catch (error) {
      firstFailed = true;
      firstError = error;
    } finally {
      firstFinished = true;
      cleanupAbortListener();
    }
  })();
  const terminal = primary.final.then(
    () => ({ ok: true as const }),
    error => ({ ok: false as const, error }),
  );
  // Observing the source rejection does not expose its prelude to callers.
  void primary.upstreamPrelude?.catch(() => undefined);

  const selection = (async (): Promise<void> => {
    const winner = await Promise.race([
      first.then(() => "event" as const),
      terminal.then(() => "final" as const),
    ]);
    if (winner === "event" && sawPrimaryFrame) return;
    const result = await terminal;
    if (result.ok || !eligible(result.error, primary, options)) return;
    // Closing V2 failure proof also closes this pending pull. Check it before
    // disposal so a simultaneously delivered frame always forbids fallback.
    await first;
    if (sawPrimaryFrame || firstFailed || !eligible(result.error, primary, options)) return;
    discardNative(primary);
    await closeIterator();
    if (options.signal?.aborted) return;
    modelFallback = Object.freeze({ from: options.requestedModel, to: FALLBACK_UPSTREAM_MODEL });
    current = await options.startFallback(result.error);
    iterator = undefined;
  })();

  const drainDiscarded = async (): Promise<void> => {
    if (draining || !selected || !discarded || claimed) return;
    draining = true;
    if (current !== primary && discardNative(current)) return;
    try {
      if (current === primary) {
        await first;
        releasePrefix();
        if (firstDone || firstFailed) return;
      } else {
        iterator = current.events();
      }
      // Returning a claimed live V2 iterator would close its producer. A
      // final-only caller instead drains one frame at a time without retaining
      // output; cancellation may close it because the request is aborting.
      while (!cancelled) {
        const next = await iterator!.next();
        if (next.done) return;
        releaseAtomicBytePermit(next.value);
      }
    } catch { /* The selected final carries the authoritative error. */ }
    finally {
      releasePrefix();
      await closeIterator();
    }
  };
  const onAbort = (): void => {
    cancelled = true;
    releasePrefix();
    if (!claimed) discarded = true;
    void drainDiscarded();
  };
  options.signal?.addEventListener("abort", onAbort, { once: true });
  if (cancelled) discarded = true;

  const decision = selection.then(() => {
    selected = true;
    void drainDiscarded();
  }, error => {
    releasePrefix();
    discardNative(primary);
    void closeIterator();
    throw error;
  });
  const final = decision.then(() => current.final);
  void decision.catch(() => undefined);
  const onTerminal = (): void => {
    terminalSettled = true;
    cleanupAbortListener();
  };
  void final.then(
    onTerminal,
    onTerminal,
  );

  const upstreamPrelude = primary.upstreamPrelude
    ? decision.then(async () => {
      if (current.upstreamPrelude) return current.upstreamPrelude;
      await current.final;
      throw new Error("Selected response has no upstream prelude");
    })
    : undefined;
  void upstreamPrelude?.catch(() => undefined);

  // Consumers capture stats before selection. Keep that object stable while
  // every field follows the selected handle, including its later mutations.
  const stats: RunHandle["stats"] = {
    get bytes() { return current.stats.bytes; },
    get firstByteMs() { return current.stats.firstByteMs; },
    get frames() { return current.stats.frames; },
    get diagnostics() { return current.stats.diagnostics; },
  };
  const handle: RunHandle = {
    async *events() {
      if (discarded) return;
      if (claimed) throw new Error("Model fallback events may only be consumed once");
      claimed = true;
      try {
        await decision;
        if (discarded || cancelled) return;
        if (current === primary) {
          await first;
          if (discarded || cancelled) return;
          if (firstFailed) throw firstError;
          if (prefix) {
            const frame = prefix;
            prefix = undefined;
            try { yield frame; } finally { releaseAtomicBytePermit(frame); }
          }
          if (firstDone) return;
        } else {
          iterator = current.events();
        }
        while (!cancelled) {
          const next = await iterator!.next();
          if (next.done) return;
          try {
            if (cancelled) return;
            yield next.value;
          } finally {
            releaseAtomicBytePermit(next.value);
          }
        }
      } finally {
        eventsFinished = true;
        releasePrefix();
        cleanupAbortListener();
        // A final-only drain owns its iterator until proof completes.
        if (!draining) await closeIterator();
      }
    },
    discardUnconsumedEvents() {
      if (claimed) return;
      discarded = true;
      releasePrefix();
      cleanupAbortListener();
      void drainDiscarded();
    },
    final,
    get rateLimit() { return current.rateLimit; },
    upstreamPrelude,
    get modelFallback() { return modelFallback; },
    stats,
  };
  return { handle, decision };
}
