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

type PreparedResponsesContinuation = PreparedAnthropicContinuation;

export type ResponsesContinuationToken = Pick<
  PreparedResponsesContinuation,
  "decision" | "match" | "inputDigest" | "settingsDigest" | "scopeDigest"
>;

export type ResponsesContinuationPreparation = Readonly<{
  token: ResponsesContinuationToken;
  relayBody: Record<string, unknown>;
}>;

export type ResponsesContinuationOptions = AnthropicContinuationIndexOptions;

function boundedProviderResponseId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 &&
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

  constructor(options: ResponsesContinuationOptions = {}) {
    this.#index = new AnthropicContinuationIndex(options);
  }

  prepare(
    body: Record<string, unknown>,
    scope: AuthenticatedTenantScope,
    signal: AbortSignal,
    retainedBytes: number,
  ): Promise<ResponsesContinuationPreparation | null> {
    if (Object.hasOwn(body, "previous_response_id") ||
      !Array.isArray(body.input) || body.input.length === 0) return Promise.resolve(null);
    const typed = body as ResponsesRequestBody;
    return this.#scheduler.prepare(async () => {
      const prepared = await this.#index.prepareAsync(typed, scope, signal);
      return {
        token: {
          decision: prepared.decision,
          match: prepared.match,
          inputDigest: prepared.inputDigest,
          settingsDigest: prepared.settingsDigest,
          scopeDigest: prepared.scopeDigest,
        },
        relayBody: prepared.relayBody,
      };
    }, retainedBytes, signal);
  }

  commit(
    token: ResponsesContinuationToken | null,
    scope: AuthenticatedTenantScope,
    finalValue: unknown,
  ): void {
    if (!token || token.decision === "empty_suffix") return;
    try {
      const replay = replayItems(finalValue);
      if (!replay) return;
      this.#index.commit(
        token,
        scope,
        replay.responseId,
        replay.items,
      );
    } catch {
      // Metadata reuse is optional and cannot replace a valid provider response.
    }
  }

  compareDelete(token: ResponsesContinuationToken): boolean {
    const match = token.match;
    return match !== null && this.#index.compareDelete(match);
  }

  beginShutdown(): void {
    this.#scheduler.close();
    this.#index.beginShutdown();
  }

  clear(): void {
    this.#index.clear();
  }

  snapshot(): AnthropicContinuationSnapshot {
    return this.#index.snapshot();
  }
}
