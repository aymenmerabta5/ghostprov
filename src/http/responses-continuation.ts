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
// Codex changes these tracing labels between otherwise identical turns. They
// are forwarded unchanged, but do not describe model execution settings.
const TRACE_METADATA_KEYS = new Set([
  "session_id", "thread_id", "turn_id", "root_turn_id", "x-codex-window-id",
  "x-codex-installation-id", "x-codex-turn-metadata",
]);
const MAX_TRACE_METADATA_BYTES = 32 * 1024;

function tracingMetadata(value: unknown): Readonly<Record<string, string>> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const snapshot: Record<string, string> = Object.create(null);
  let bytes = 0;
  let count = 0;
  let enumerated = 0;
  for (const key in value) {
    if (++enumerated > TRACE_METADATA_KEYS.size) return null;
    if (!Object.hasOwn(value, key)) continue;
    if (!TRACE_METADATA_KEYS.has(key) || ++count > TRACE_METADATA_KEYS.size) return null;
    const property = Object.getOwnPropertyDescriptor(value, key);
    if (!property || !("value" in property) || typeof property.value !== "string" ||
        property.value.length > MAX_TRACE_METADATA_BYTES) return null;
    bytes += Buffer.byteLength(key) + Buffer.byteLength(property.value);
    if (bytes > MAX_TRACE_METADATA_BYTES) return null;
    snapshot[key] = property.value;
  }
  return count > 0 ? Object.freeze(snapshot) : null;
}

/** An additional lossless replay spelling used by the installed Codex client. */
function compactReplay(items: readonly ResponsesInputItem[]): ResponsesInputItem[] | null {
  let changed = false;
  let visited = 0;
  const projected: ResponsesInputItem[] = [];
  for (const value of items) {
    if (++visited > 8_192) return null;
    const item = value as unknown as Record<string, unknown>;
    let copy = item;
    let itemChanged = false;
    if ((item.type === "message" || item.type === "reasoning" || item.type === "function_call") &&
        item.status === "completed") {
      copy = { ...item };
      delete copy.status;
      itemChanged = true;
    }
    if (item.type === "message" && item.role === "assistant" && Array.isArray(item.content)) {
      if (item.content.length > MAX_REPLAY_ITEMS) return null;
      let content: unknown[] | null = null;
      for (let index = 0; index < item.content.length; index++) {
        if (++visited > 8_192) return null;
        const part = item.content[index];
        if (!part || typeof part !== "object" || Array.isArray(part)) continue;
        const block = part as Record<string, unknown>;
        if (block.type !== "output_text") continue;
        let next = block;
        for (const key of ["annotations", "logprobs"] as const) {
          if (Array.isArray(block[key]) && block[key].length === 0) {
            if (next === block) next = { ...block };
            delete next[key];
          }
        }
        if (next !== block) {
          content ??= item.content.slice();
          content[index] = next;
        }
      }
      if (content) {
        if (copy === item) copy = { ...item };
        copy.content = content;
        itemChanged = true;
      }
    }
    changed ||= itemChanged;
    projected.push((itemChanged ? copy : item) as unknown as ResponsesInputItem);
  }
  return changed ? projected : null;
}

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
      const metadata = tracingMetadata(body.client_metadata);
      const matchingBody = metadata === null ? body : { ...body };
      if (metadata !== null) delete matchingBody.client_metadata;
      const prepared = await this.#index.prepareAsync(
        matchingBody as ResponsesRequestBody,
        scope,
        linked.signal,
      );
      const fullRelayBody = metadata === null ? prepared.fullRelayBody : {
        ...prepared.fullRelayBody, client_metadata: metadata,
      };
      const relayBody = prepared.relayBody === prepared.fullRelayBody
        ? fullRelayBody
        : metadata === null ? prepared.relayBody : { ...prepared.relayBody, client_metadata: metadata };
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
        fullRelayBody,
        relayBody,
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
      const committed = await this.#index.commitAsync(
        token,
        scope,
        replay.responseId,
        replay.items,
        linked.signal,
      );
      if (committed) {
        const compact = compactReplay(replay.items);
        if (compact) {
          // Both spellings share the existing record/byte/TTL budget. Await the
          // alternate commit before a public terminal can expose this lineage.
          await this.#index.commitAsync(token, scope, replay.responseId, compact, linked.signal);
        }
      }
      return committed;
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
