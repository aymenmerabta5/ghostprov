import type { AuthenticatedTenantScope } from "./api-key";
import {
  AnthropicContinuationIndex,
  type AnthropicContinuationIndexOptions,
  type AnthropicContinuationSnapshot,
  type PreparedAnthropicContinuation,
} from "../anthropic/continuation";
import type {
  ResponsesInputItem,
  ResponsesRequestBody,
} from "../anthropic/request-types";
import { BoundedPreparationScheduler } from "../relay/preparation-scheduler";

const PREPARE_MAX_QUEUED = 64;
const PREPARE_MAX_QUEUED_BYTES = 256 * 1024 * 1024;
const MAX_PROVIDER_RESPONSE_ID_BYTES = 256;
const MAX_REPLAY_ITEMS = 4_096;

export function responsesContinuationPreparationCostBytes(
  retainedBytes: number,
  reservedBytes = 0,
  snapshotBytes = retainedBytes,
): number {
  if (!Number.isSafeInteger(retainedBytes) || retainedBytes < 0 ||
    !Number.isSafeInteger(reservedBytes) || reservedBytes < 0 ||
    !Number.isSafeInteger(snapshotBytes) || snapshotBytes < 0) {
    throw new RangeError("continuation preparation bytes must be non-negative safe integers");
  }
  const total = retainedBytes + snapshotBytes;
  if (!Number.isSafeInteger(total)) {
    throw new RangeError("continuation preparation bytes exceed the safe integer range");
  }
  return Math.max(
    1,
    snapshotBytes,
    total - Math.min(total, reservedBytes),
  );
}

export type ResponsesContinuationToken = Pick<
  PreparedAnthropicContinuation,
  "decision" | "match" | "inputDigest" | "settingsDigest" |
    "settingsCanonicalBytes" | "settingsCanonicalNodes" | "scopeDigest"
>;

export type ResponsesContinuationPreparation = Readonly<{
  token: ResponsesContinuationToken;
  fullRelayBody: Record<string, unknown>;
  relayBody: Record<string, unknown>;
}>;

function boundedProviderResponseId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 &&
    value.length <= MAX_PROVIDER_RESPONSE_ID_BYTES &&
    Buffer.byteLength(value, "utf8") <= MAX_PROVIDER_RESPONSE_ID_BYTES;
}

function replayItems(finalValue: unknown): {
  responseId: string;
  items: readonly ResponsesInputItem[];
} | null {
  if (!finalValue || typeof finalValue !== "object" || Array.isArray(finalValue)) return null;
  const final = finalValue as Record<string, unknown>;
  if (final.status !== "completed" || !boundedProviderResponseId(final.id) ||
    !Array.isArray(final.output) || final.output.length === 0 ||
    final.output.length > MAX_REPLAY_ITEMS) return null;
  for (const value of final.output) {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
      typeof (value as Record<string, unknown>).type !== "string" ||
      !(value as Record<string, unknown>).type) return null;
  }
  return {
    responseId: final.id,
    // Exact future input matching is the validator. Unsupported client rewrites
    // cannot hit this digest and therefore fall back to full-history relay.
    items: final.output as ResponsesInputItem[],
  };
}

/**
 * App-scoped, content-free prefix index for native Responses HTTP clients that
 * resend complete input arrays. Explicit caller continuation IDs always bypass
 * this optimization and remain byte-for-byte authoritative.
 */
export class ResponsesHttpContinuationIndex {
  readonly #index: AnthropicContinuationIndex;
  readonly #scheduler = new BoundedPreparationScheduler<ResponsesContinuationPreparation>({
    maxQueued: PREPARE_MAX_QUEUED,
    maxQueuedBytes: PREPARE_MAX_QUEUED_BYTES,
  });
  readonly #shutdown = new AbortController();

  constructor(options: AnthropicContinuationIndexOptions = {}) {
    this.#index = new AnthropicContinuationIndex(options);
  }

  prepare(
    body: Record<string, unknown>,
    scope: AuthenticatedTenantScope,
    signal: AbortSignal,
    retainedBytes: number,
  ): Promise<ResponsesContinuationPreparation | null> {
    if (Object.hasOwn(body, "previous_response_id") ||
      !Array.isArray(body.input) || body.input.length === 0 ||
      signal.aborted || this.#shutdown.signal.aborted) return Promise.resolve(null);

    const linked = new AbortController();
    const abortFromRequest = () => linked.abort(signal.reason);
    const abortFromShutdown = () => linked.abort(this.#shutdown.signal.reason);
    signal.addEventListener("abort", abortFromRequest, { once: true });
    this.#shutdown.signal.addEventListener("abort", abortFromShutdown, { once: true });

    const preparation = this.#scheduler.prepare(async () => {
      const prepared = await this.#index.prepareAsync(
        body as ResponsesRequestBody,
        scope,
        linked.signal,
      );
      return {
        token: {
          decision: prepared.decision,
          match: prepared.match,
          inputDigest: prepared.inputDigest,
          settingsDigest: prepared.settingsDigest,
          settingsCanonicalBytes: prepared.settingsCanonicalBytes,
          settingsCanonicalNodes: prepared.settingsCanonicalNodes,
          scopeDigest: prepared.scopeDigest,
        },
        fullRelayBody: prepared.fullRelayBody,
        relayBody: prepared.relayBody,
      };
    }, retainedBytes, linked.signal);
    return preparation.finally(() => {
      signal.removeEventListener("abort", abortFromRequest);
      this.#shutdown.signal.removeEventListener("abort", abortFromShutdown);
    });
  }

  async commit(
    token: ResponsesContinuationToken | null,
    scope: AuthenticatedTenantScope,
    finalValue: unknown,
    signal?: AbortSignal,
  ): Promise<boolean> {
    if (!token || token.decision === "empty_suffix" || signal?.aborted ||
      this.#shutdown.signal.aborted) {
      return false;
    }
    const linked = new AbortController();
    const abortFromRequest = () => linked.abort(signal?.reason);
    const abortFromShutdown = () => linked.abort(this.#shutdown.signal.reason);
    signal?.addEventListener("abort", abortFromRequest, { once: true });
    this.#shutdown.signal.addEventListener("abort", abortFromShutdown, { once: true });
    try {
      const replay = replayItems(finalValue);
      if (!replay) return false;
      return await this.#index.commitAsync(
        token,
        scope,
        replay.responseId,
        replay.items,
        linked.signal,
      );
    } catch {
      // Metadata reuse is optional and cannot replace a valid provider response.
      return false;
    } finally {
      signal?.removeEventListener("abort", abortFromRequest);
      this.#shutdown.signal.removeEventListener("abort", abortFromShutdown);
    }
  }

  compareDelete(token: ResponsesContinuationToken): boolean {
    const match = token.match;
    return match !== null && this.#index.compareDelete(match);
  }

  beginShutdown(): void {
    this.#index.beginShutdown();
    this.#shutdown.abort(new DOMException("Continuation preparation stopped", "AbortError"));
    this.#scheduler.close();
  }

  clear(): void {
    this.#index.clear();
  }

  snapshot(): AnthropicContinuationSnapshot {
    return this.#index.snapshot();
  }
}
