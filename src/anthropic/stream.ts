import type { SseFrame } from "../relay/sse";
import { normalizeApiError } from "../http/errors";
import { createHash } from "node:crypto";
import {
  AnthropicRequestError,
  anthropicErrorEvent,
  anthropicErrorType,
} from "./errors";
import {
  resolveAnthropicRequestLimits,
  type AnthropicRequestLimits,
} from "./request-validation";
import { responseToAnthropic, type AnthropicResponseContext } from "./response";
import { responseStopSequences } from "./response-stop";
import { invalidUpstream, record } from "./response-validation";
import { usageFromResponses } from "./response-usage";
import {
  createStreamBlock,
  streamBlockIdentity,
  type StreamBlock,
  type StreamBlockKind,
} from "./stream-block-state";
import { AnthropicStreamBudget } from "./stream-budget";
import { finalStreamDescriptors, terminalForStreamItem } from "./stream-final";
import {
  resolveAnthropicStreamLimits,
  type AnthropicStreamLimits,
  type AnthropicStreamSnapshot,
} from "./stream-limits";
import { ResponsesStreamProtocol } from "./stream-protocol";
import { assertUnicodeWellFormed, UnicodeStopFilter } from "./stream-stop-filter";
import type { AnthropicContentBlockDelta, AnthropicSseEvent } from "./stream-types";
import type {
  AnthropicResponseContentBlock,
  AnthropicThinkingBlock,
  AnthropicToolUseBlock,
} from "./types";

export { anthropicSseFrame } from "./stream-types";
export { DEFAULT_ANTHROPIC_STREAM_LIMITS } from "./stream-limits";
export type {
  AnthropicContentBlockDelta,
  AnthropicContentBlockDeltaEvent,
  AnthropicContentBlockStartEvent,
  AnthropicContentBlockStopEvent,
  AnthropicMessageDeltaEvent,
  AnthropicMessageStartEvent,
  AnthropicMessageStopEvent,
  AnthropicSseEvent,
} from "./stream-types";
export type { AnthropicStreamLimits, AnthropicStreamSnapshot } from "./stream-limits";

const MALFORMED_EVENT_MESSAGE = "Responses stream returned a malformed event.";
const FAILED_EVENT_MESSAGE = "Responses stream failed.";

function malformedEvent(): never {
  throw invalidUpstream(MALFORMED_EVENT_MESSAGE);
}

function stringField(value: Record<string, unknown>, key: string): string {
  if (typeof value[key] !== "string") malformedEvent();
  return value[key] as string;
}

function indexField(value: Record<string, unknown>, key: string): number {
  const candidate = value[key];
  if (!Number.isSafeInteger(candidate) || Number(candidate) < 0) malformedEvent();
  return Number(candidate);
}

function optionalIndexField(value: Record<string, unknown>, key: string): number | null {
  if (value[key] === undefined) return null;
  return indexField(value, key);
}

function outputItemId(item: Record<string, unknown>): string | null {
  return typeof item.id === "string" && item.id.length > 0 ? item.id : null;
}

/**
 * Pure, pull-safe Responses-frame translator.
 *
 * The retained state is bounded to block metadata, pending data for a block that
 * is waiting behind an earlier block, counters, and at most the longest stop
 * sequence minus one character of ordinary text. It never retains prior frames.
 */
export class AnthropicStreamTranslator {
  readonly #context: AnthropicResponseContext;
  readonly #limits: AnthropicRequestLimits;
  readonly #streamLimits: AnthropicStreamLimits;
  readonly #stopSequences: string[];
  readonly #protocol: ResponsesStreamProtocol;
  readonly #textFilter: UnicodeStopFilter<StreamBlock>;
  readonly #reasoningFilters = new Map<StreamBlock, UnicodeStopFilter<StreamBlock>>();
  readonly #budget: AnthropicStreamBudget;
  readonly #blocksByIdentity = new Map<string, StreamBlock>();
  readonly #blocksByPosition = new Map<string, StreamBlock>();
  #blocks: StreamBlock[] = [];
  #matchedStop: string | null = null;
  #started = false;
  #terminal = false;
  #finishing = false;
  #drainCursor = 0;
  #nextBlockIndex = 0;

  constructor(context: AnthropicResponseContext) {
    this.#stopSequences = [...responseStopSequences(context.stopSequences)];
    try {
      this.#limits = resolveAnthropicRequestLimits(context.limits);
      this.#streamLimits = resolveAnthropicStreamLimits(context.streamLimits);
    } catch {
      throw invalidUpstream("Anthropic response context is malformed.");
    }
    this.#protocol = new ResponsesStreamProtocol(this.#streamLimits);
    this.#budget = new AnthropicStreamBudget(this.#streamLimits);
    this.#context = { ...context, stopSequences: this.#stopSequences };
    this.#textFilter = new UnicodeStopFilter(this.#stopSequences);
  }

  start(): AnthropicSseEvent[] {
    if (this.#started || this.#terminal) return [];
    this.#started = true;
    const usage = usageFromResponses(undefined, this.#context.estimatedInputTokens);
    return [{
      type: "message_start",
      message: {
        id: `msg_${this.#context.requestId}`,
        type: "message",
        role: "assistant",
        model: this.#context.requestedModel,
        content: [],
        stop_details: null,
        stop_reason: null,
        stop_sequence: null,
        container: null,
        usage,
      },
    }];
  }

  push(frame: SseFrame): AnthropicSseEvent[] {
    if (this.#terminal) return [];
    const events = this.#started ? [] : this.start();
    try {
      if (!frame || typeof frame.event !== "string" || typeof frame.data !== "string") {
        malformedEvent();
      }
      this.#budget.countWire(frame.data);
      let parsed: unknown;
      try {
        parsed = JSON.parse(frame.data);
      } catch {
        malformedEvent();
      }
      const event = record(parsed);
      if (!event || typeof event.type !== "string" || event.type !== frame.event) {
        malformedEvent();
      }
      const acceptedSuffix = this.#protocol.accept(event);
      this.#countStreamedPayload(event.type, event);
      if (acceptedSuffix !== null && acceptedSuffix.value.length > 0) {
        this.#budget.countPayload(acceptedSuffix.category, acceptedSuffix.value);
      }
      events.push(...this.#translate(event.type, event));
      return events;
    } catch (error) {
      events.push(...this.#emitError(error));
      return events;
    }
  }

  finish(finalValue: unknown): AnthropicSseEvent[] {
    if (this.#terminal) return [];
    const events = this.#started ? [] : this.start();
    try {
      events.push(...this.#complete(finalValue));
      return events;
    } catch (error) {
      events.push(...this.#emitError(error));
      return events;
    }
  }

  fail(error: unknown): AnthropicSseEvent[] {
    if (this.#terminal) return [];
    const events = this.#started ? [] : this.start();
    events.push(...this.#emitError(error));
    return events;
  }

  snapshot(): AnthropicStreamSnapshot {
    const protocol = this.#protocol.snapshot();
    return {
      ...this.#budget.snapshot(),
      events: protocol.events,
      identities: protocol.identities,
      retainedBlocks: this.#blocks.length,
      retainedIdentities: protocol.retainedIdentities,
      retainedTextCodePoints: this.#textFilter.retainedCodePoints(),
      retainedReasoningCodePoints: Array.from(this.#reasoningFilters.values())
        .reduce((total, filter) => total + filter.retainedCodePoints(), 0),
    };
  }

  #countStreamedPayload(type: string, event: Record<string, unknown>): void {
    let category: "text" | "reasoning" | "function" | null = null;
    if (type === "response.output_text.delta") category = "text";
    else if (type === "response.reasoning_summary_text.delta" ||
      type === "response.reasoning_text.delta") category = "reasoning";
    else if (type === "response.function_call_arguments.delta") category = "function";
    if (category === null) return;

    this.#budget.countPayload(category, stringField(event, "delta"));
  }

  #translate(type: string, event: Record<string, unknown>): AnthropicSseEvent[] {
    const events: AnthropicSseEvent[] = [];
    if (type === "response.created") return events;
    if (type === "response.failed" || type === "error") {
      return this.#emitError(invalidUpstream(FAILED_EVENT_MESSAGE));
    }
    if (type === "response.completed" || type === "response.incomplete") {
      if (!("response" in event)) malformedEvent();
      return this.#complete(event.response);
    }
    if (this.#context.maxTokensZero) return events;

    if (type === "response.output_item.added") {
      this.#onOutputItemAdded(event, events);
    } else if (type === "response.content_part.added") {
      this.#onContentPartAdded(event, events);
    } else if (type === "response.reasoning_summary_part.added") {
      this.#onReasoningPartAdded(event, events);
    } else if (type === "response.reasoning_summary_text.delta") {
      this.#onReasoningDelta(event, events);
    } else if (type === "response.reasoning_summary_text.done") {
      this.#onReasoningSummaryDone(event, events);
    } else if (type === "response.output_text.delta") {
      this.#onTextDelta(event, events);
    } else if (type === "response.output_text.done") {
      this.#onTextDone(event, events);
    } else if (type === "response.function_call_arguments.delta") {
      this.#onArgumentsDelta(event, events);
    } else if (type === "response.function_call_arguments.done") {
      this.#onArgumentsDone(event, events);
    } else if (type === "response.output_item.done") {
      this.#onOutputItemDone(event, events);
    }
    return events;
  }

  #onOutputItemAdded(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    const item = record(event.item);
    if (!item || typeof item.type !== "string") malformedEvent();
    const itemId = outputItemId(item);
    if (item.type === "reasoning") {
      this.#endOrdinarySegment(events);
      if (this.#matchedStop) return;
      const block = this.#block("thinking", itemId, outputIndex, null);
      block.forceStart = true;
      this.#drain(events);
    } else if (item.type === "function_call") {
      this.#endOrdinarySegment(events);
      if (this.#matchedStop) return;
      const block = this.#block("tool_use", itemId, outputIndex, null);
      this.#setToolMetadata(block, item);
      this.#drain(events);
    }
  }

  #onContentPartAdded(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    const contentIndex = indexField(event, "content_index");
    const itemId = stringField(event, "item_id");
    const part = record(event.part);
    if (!part || typeof part.type !== "string") malformedEvent();
    if (part.type !== "output_text" || this.#matchedStop) return;
    this.#block("text", itemId, outputIndex, contentIndex);
  }

  #onReasoningPartAdded(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    optionalIndexField(event, "summary_index");
    const itemId = stringField(event, "item_id");
    this.#endOrdinarySegment(events);
    if (this.#matchedStop) return;
    const block = this.#block("thinking", itemId, outputIndex, null);
    block.forceStart = true;
    this.#drain(events);
  }

  #onReasoningDelta(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    optionalIndexField(event, "summary_index");
    const itemId = stringField(event, "item_id");
    const delta = stringField(event, "delta");
    this.#endOrdinarySegment(events);
    if (this.#matchedStop) return;
    const block = this.#block("thinking", itemId, outputIndex, null);
    block.forceStart = true;
    block.seenUnits = this.#addUnits(block.seenUnits, delta.length);
    this.#appendReasoningScalar(block, delta);
    this.#drain(events);
  }

  #onTextDelta(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    const contentIndex = indexField(event, "content_index");
    const itemId = stringField(event, "item_id");
    const delta = stringField(event, "delta");
    const block = this.#block("text", itemId, outputIndex, contentIndex);
    block.seenUnits = this.#addUnits(block.seenUnits, delta.length);
    if (!this.#matchedStop) this.#appendOrdinaryText(block, delta);
    this.#drain(events);
  }

  #onTextDone(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    const contentIndex = indexField(event, "content_index");
    const itemId = stringField(event, "item_id");
    const text = stringField(event, "text");
    const block = this.#block("text", itemId, outputIndex, contentIndex);
    this.#reconcileStreamedText(block, text);
    block.done = true;
    this.#drain(events);
  }

  #onArgumentsDelta(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    const itemId = stringField(event, "item_id");
    const delta = stringField(event, "delta");
    this.#endOrdinarySegment(events);
    if (this.#matchedStop) return;
    const block = this.#block("tool_use", itemId, outputIndex, null);
    this.#setToolMetadata(block, event);
    block.argumentDeltaSeen = true;
    block.argumentUnits = this.#addUnits(block.argumentUnits, delta.length);
    block.argumentBytes = this.#addArgumentBytes(block.argumentBytes, delta);
    this.#appendPending(block, delta);
    this.#drain(events);
  }

  #onArgumentsDone(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    const itemId = stringField(event, "item_id");
    const args = stringField(event, "arguments");
    this.#endOrdinarySegment(events);
    if (this.#matchedStop) return;
    const block = this.#block("tool_use", itemId, outputIndex, null);
    this.#setToolMetadata(block, event);
    this.#reconcileArguments(block, args, args.trim().length === 0);
    // Validate completed JSON now, but wait for authoritative refusal provenance
    // before exposing the tool block.
    if (block.toolId !== null && block.toolName !== null) {
      this.#validateToolArguments(block, itemId, args);
      block.done = true;
    }
    this.#drain(events);
  }

  #onReasoningSummaryDone(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    optionalIndexField(event, "summary_index");
    const itemId = stringField(event, "item_id");
    const text = stringField(event, "text");
    this.#endOrdinarySegment(events);
    if (this.#matchedStop) return;
    const block = this.#block("thinking", itemId, outputIndex, null);
    if (text.length < block.seenUnits) malformedEvent();
    if (text.length > block.seenUnits) this.#appendReasoningScalar(block, text.slice(block.seenUnits));
    this.#endReasoningScalar(block);
    block.seenUnits = text.length;
    this.#drain(events);
  }

  #onOutputItemDone(
    event: Record<string, unknown>,
    events: AnthropicSseEvent[],
  ): void {
    const outputIndex = indexField(event, "output_index");
    const item = record(event.item);
    if (!item || typeof item.type !== "string") malformedEvent();
    const itemId = outputItemId(item);

    if (item.type === "reasoning") {
      this.#endOrdinarySegment(events);
      if (this.#matchedStop) return;
      const block = this.#block("thinking", itemId, outputIndex, null);
      const id = stringField(item, "id");
      const summaryValue = item.summary;
      if (!Array.isArray(summaryValue)) malformedEvent();
      const summaryParts: string[] = [];
      for (const value of summaryValue) {
        const part = record(value);
        if (!part || part.type !== "summary_text" || typeof part.text !== "string") {
          malformedEvent();
        }
        summaryParts.push(part.text);
      }
      const summary = summaryParts.join("");
      if (summary.length < block.seenUnits) malformedEvent();
      this.#assertStreamedPrefix(
        block,
        this.#context.thinkingDisplay === "omitted" ? "" : summary,
      );
      if (summary.length > block.seenUnits) {
        this.#budget.countPayload("reasoning", summary.slice(block.seenUnits));
        this.#appendReasoningScalar(block, summary.slice(block.seenUnits));
      }
      this.#endReasoningScalar(block);
      block.seenUnits = summary.length;
      const encryptedContent = item.encrypted_content;
      if (typeof encryptedContent !== "string" || encryptedContent.length === 0) return;
      const converted = responseToAnthropic(terminalForStreamItem(item), this.#context);
      const thinking = converted.content[0];
      if (!thinking || thinking.type !== "thinking" || thinking.thinking !==
        (this.#context.thinkingDisplay === "omitted" ? "" : summary)) {
        malformedEvent();
      }
      block.itemId = id;
      block.signature = thinking.signature;
      block.forceStart = true;
      block.done = true;
      this.#drain(events);
      return;
    }

    if (item.type === "function_call") {
      this.#endOrdinarySegment(events);
      if (this.#matchedStop) return;
      const block = this.#block("tool_use", itemId, outputIndex, null);
      this.#setToolMetadata(block, item);
      const status = item.status;
      if (status === "incomplete") {
        if (!block.started) {
          this.#clearPending(block);
          block.suppressed = true;
          block.done = true;
        }
        this.#drain(events);
        return;
      }
      const args = stringField(item, "arguments");
      this.#validateToolArguments(block, itemId, args);
      this.#reconcileArguments(block, args);
      block.done = true;
      this.#drain(events);
      return;
    }

    if (item.type === "message") {
      const id = stringField(item, "id");
      if (!Array.isArray(item.content)) malformedEvent();
      for (let contentIndex = 0; contentIndex < item.content.length; contentIndex += 1) {
        const part = record(item.content[contentIndex]);
        if (!part || typeof part.type !== "string") malformedEvent();
        if (part.type !== "output_text") continue;
        const block = this.#block("text", id, outputIndex, contentIndex);
        const text = stringField(part, "text");
        if (text.length > block.seenUnits) {
          this.#budget.countPayload("text", text.slice(block.seenUnits));
        }
        this.#reconcileStreamedText(block, text);
        block.done = true;
      }
      this.#drain(events);
    }
  }

  #complete(finalValue: unknown): AnthropicSseEvent[] {
    this.#protocol.validateFinal(finalValue);
    const converted = responseToAnthropic(finalValue, this.#context);
    const descriptors = finalStreamDescriptors(
      finalValue,
      converted,
      this.#context.maxTokensZero,
    );
    this.#finishing = true;
    this.#textFilter.clear();

    const represented = new Set<StreamBlock>();
    const ranks = new Map<StreamBlock, number>();
    for (let rank = 0; rank < descriptors.length; rank += 1) {
      const descriptor = descriptors[rank];
      const block = this.#block(
        descriptor.kind,
        descriptor.itemId.startsWith("@") ? null : descriptor.itemId,
        descriptor.outputIndex,
        descriptor.contentIndex,
      );
      represented.add(block);
      ranks.set(block, rank);
      block.suppressed = false;
      block.forceStart = true;
      block.done = true;

      if (descriptor.kind === "text") {
        const text = String(descriptor.block.text);
        assertUnicodeWellFormed(text);
        if (text.length > block.seenUnits) {
          this.#budget.countPayload("text", text.slice(block.seenUnits));
          block.seenUnits = text.length;
        }
        this.#assertStreamedPrefix(block, text);
        if (!block.stopped) {
          this.#clearPending(block);
          if (text.length > block.emittedUnits) {
            this.#appendPending(block, text.slice(block.emittedUnits));
          }
        }
      } else if (descriptor.kind === "thinking") {
        const thinking = descriptor.block as AnthropicThinkingBlock;
        assertUnicodeWellFormed(thinking.thinking);
        if (thinking.thinking.length > block.seenUnits) {
          this.#budget.countPayload("reasoning", thinking.thinking.slice(block.seenUnits));
          block.seenUnits = thinking.thinking.length;
        }
        this.#assertStreamedPrefix(block, thinking.thinking);
        if (!block.stopped) {
          this.#clearPending(block);
          if (thinking.thinking.length > block.emittedUnits) {
            this.#appendPending(block, thinking.thinking.slice(block.emittedUnits));
          }
          block.signature = thinking.signature;
        }
      } else {
        const toolBlock = descriptor.block as AnthropicToolUseBlock;
        block.toolId = toolBlock.id;
        block.toolName = toolBlock.name;
        if (!block.stopped) {
          const args = descriptor.arguments ?? "{}";
          assertUnicodeWellFormed(args);
          this.#reconcileArguments(block, args);
          this.#assertStreamedPrefix(block, args);
          const remaining = args.slice(block.emittedUnits);
          const pendingPrefix = block.pending.join("");
          if (!remaining.startsWith(pendingPrefix)) {
            throw invalidUpstream(
              "Responses stream could not be reconciled with its final response.",
            );
          }
          if (remaining.length > pendingPrefix.length) {
            this.#appendPending(block, remaining.slice(pendingPrefix.length));
          }
        }
      }
    }

    for (const block of this.#blocks) {
      if (!represented.has(block)) {
        if (block.started || block.emittedUnits > 0) {
          throw invalidUpstream(
            "Responses stream could not be reconciled with its final response.",
          );
        }
        if (!block.stopped) {
          this.#clearPending(block);
          block.suppressed = true;
          block.done = true;
        }
      }
    }

    const prefix = this.#blocks.slice(0, this.#drainCursor);
    const remaining = this.#blocks.slice(this.#drainCursor);
    remaining.sort((left, right) => {
      const leftRank = ranks.get(left) ?? Number.MAX_SAFE_INTEGER;
      const rightRank = ranks.get(right) ?? Number.MAX_SAFE_INTEGER;
      return leftRank - rightRank;
    });
    this.#blocks = [...prefix, ...remaining];

    const events: AnthropicSseEvent[] = [];
    this.#drain(events);
    if (this.#drainCursor !== this.#blocks.length) {
      throw invalidUpstream("Responses stream could not be reconciled with its final response.");
    }
    events.push({
      type: "message_delta",
      delta: {
        stop_reason: converted.stop_reason,
        stop_sequence: converted.stop_sequence,
      },
      usage: converted.usage,
    });
    events.push({ type: "message_stop" });
    this.#protocol.markTerminal();
    this.#terminal = true;
    this.#clearRetainedState();
    return events;
  }

  #clearRetainedState(): void {
    this.#blocks = [];
    this.#blocksByIdentity.clear();
    this.#blocksByPosition.clear();
    this.#textFilter.clear();
    this.#reasoningFilters.clear();
    this.#budget.clearRetained();
    this.#matchedStop = null;
    this.#drainCursor = 0;
    this.#protocol.clearRetained();
  }

  #block(
    kind: StreamBlockKind,
    itemId: string | null,
    outputIndex: number,
    contentIndex: number | null,
  ): StreamBlock {
    const identity = streamBlockIdentity(kind, itemId, outputIndex, contentIndex);
    const position = `${kind}:${outputIndex}:${contentIndex ?? "-"}`;
    const exact = this.#blocksByIdentity.get(identity);
    if (exact) {
      if (exact.kind !== kind || exact.outputIndex !== outputIndex ||
        exact.contentIndex !== contentIndex ||
        (itemId !== null && exact.itemId !== null && exact.itemId !== itemId)) {
        malformedEvent();
      }
      return exact;
    }

    const byPosition = this.#blocksByPosition.get(position);
    if (byPosition) {
      if (itemId !== null) {
        if (byPosition.itemId !== null && byPosition.itemId !== itemId) malformedEvent();
        byPosition.itemId = itemId;
      }
      this.#blocksByIdentity.set(identity, byPosition);
      return byPosition;
    }

    this.#budget.reserveOutputBlock();
    const block = createStreamBlock(kind, itemId, outputIndex, contentIndex);
    const last = this.#blocks.at(-1);
    if (last === undefined || this.#compareBlocks(last, block) <= 0) {
      this.#blocks.push(block);
    } else {
      let low = this.#drainCursor;
      let high = this.#blocks.length;
      while (low < high) {
        const middle = Math.floor((low + high) / 2);
        if (this.#compareBlocks(this.#blocks[middle], block) <= 0) low = middle + 1;
        else high = middle;
      }
      this.#blocks.splice(low, 0, block);
    }
    this.#blocksByIdentity.set(identity, block);
    this.#blocksByPosition.set(position, block);
    return block;
  }

  #setToolMetadata(block: StreamBlock, item: Record<string, unknown>): void {
    if (item.call_id !== undefined) {
      const toolId = stringField(item, "call_id");
      if (block.toolId !== null && block.toolId !== toolId) malformedEvent();
      block.toolId = toolId;
    }
    if (item.name !== undefined) {
      const toolName = stringField(item, "name");
      if (block.toolName !== null && block.toolName !== toolName) malformedEvent();
      block.toolName = toolName;
    }
  }

  #compareBlocks(left: StreamBlock, right: StreamBlock): number {
    if (left.outputIndex !== right.outputIndex) return left.outputIndex - right.outputIndex;
    return (left.contentIndex ?? 0) - (right.contentIndex ?? 0);
  }

  #validateToolArguments(block: StreamBlock, itemId: string | null, args: string): void {
    if (block.toolId === null || block.toolName === null) return;
    responseToAnthropic(terminalForStreamItem({
      type: "function_call",
      ...(itemId === null ? {} : { id: itemId }),
      status: "completed",
      call_id: block.toolId,
      name: block.toolName,
      arguments: args,
    }), this.#context);
  }

  #reconcileArguments(block: StreamBlock, value: string, countSuffix = true): void {
    const args = value.trim().length > 0 ? value : "{}";
    assertUnicodeWellFormed(args);
    if (args.length < block.argumentUnits) malformedEvent();
    this.#assertArgumentBytes(args);
    if (args.length > block.argumentUnits) {
      const suffix = args.slice(block.argumentUnits);
      if (countSuffix) this.#budget.countPayload("function", suffix);
      block.argumentBytes = this.#addArgumentBytes(block.argumentBytes, suffix);
      this.#appendPending(block, suffix);
      block.argumentUnits = args.length;
    }
  }

  #assertStreamedPrefix(block: StreamBlock, value: string): void {
    if (value.length < block.emittedUnits) {
      throw invalidUpstream(
        "Responses stream could not be reconciled with its final response.",
      );
    }
    const expected = createHash("sha256")
      .update(value.slice(0, block.emittedUnits), "utf8")
      .digest("hex");
    if (block.fingerprint.copy().digest("hex") !== expected) {
      throw invalidUpstream(
        "Responses stream could not be reconciled with its final response.",
      );
    }
  }

  #appendPending(block: StreamBlock, value: string): void {
    if (!value || block.suppressed || block.stopped) return;
    const bytes = Buffer.byteLength(value, "utf8");
    const blockBytes = this.#addUnits(block.pendingBytes, bytes);
    if (blockBytes > this.#limits.outputBytes) {
      throw invalidUpstream("Responses stream exceeded its bounded pending output.");
    }
    this.#budget.reservePendingChunk(
      bytes,
      this.#limits.relayBytes,
      this.#limits.maxTotalJsonItems,
    );
    block.pending.push(value);
    block.pendingBytes = blockBytes;
  }

  #clearPending(block: StreamBlock): void {
    this.#budget.releasePending(block.pendingBytes, block.pending.length);
    block.pending = [];
    block.pendingBytes = 0;
  }

  #shiftPending(block: StreamBlock): string {
    const value = block.pending.shift();
    if (value === undefined) malformedEvent();
    const bytes = Buffer.byteLength(value, "utf8");
    block.pendingBytes -= bytes;
    this.#budget.releasePending(bytes, 1);
    return value;
  }

  #appendOrdinaryText(block: StreamBlock, value: string): void {
    const result = this.#textFilter.append(block, value);
    for (const chunk of result.chunks) this.#appendPending(chunk.owner, chunk.text);
    if (result.matchedSequence !== null) this.#matchedStop = result.matchedSequence;
  }

  #appendReasoningScalar(block: StreamBlock, value: string): void {
    let filter = this.#reasoningFilters.get(block);
    if (!filter) {
      filter = new UnicodeStopFilter<StreamBlock>([]);
      this.#reasoningFilters.set(block, filter);
    }
    const result = filter.append(block, value);
    if (this.#context.thinkingDisplay !== "omitted") {
      for (const chunk of result.chunks) this.#appendPending(chunk.owner, chunk.text);
    }
  }

  #endReasoningScalar(block: StreamBlock): void {
    const filter = this.#reasoningFilters.get(block);
    if (!filter) return;
    const chunks = filter.endSegment();
    if (this.#context.thinkingDisplay !== "omitted") {
      for (const chunk of chunks) this.#appendPending(chunk.owner, chunk.text);
    }
    this.#reasoningFilters.delete(block);
  }

  #endOrdinarySegment(events: AnthropicSseEvent[]): void {
    for (const chunk of this.#textFilter.endSegment()) {
      this.#appendPending(chunk.owner, chunk.text);
    }
    this.#drain(events);
  }

  #reconcileStreamedText(block: StreamBlock, text: string): void {
    assertUnicodeWellFormed(text);
    if (text.length < block.seenUnits) malformedEvent();
    this.#assertStreamedPrefix(block, text);
    if (!this.#matchedStop && text.length > block.seenUnits) {
      this.#appendOrdinaryText(block, text.slice(block.seenUnits));
    }
    block.seenUnits = text.length;
  }

  #drain(events: AnthropicSseEvent[]): void {
    while (this.#drainCursor < this.#blocks.length) {
      const block = this.#blocks[this.#drainCursor];
      if (block.suppressed) {
        if (block.started && !block.stopped && block.index !== null) {
          events.push({ type: "content_block_stop", index: block.index });
        }
        block.stopped = true;
        this.#drainCursor += 1;
        continue;
      }

      if (!block.started) {
        if (!this.#finishing && !this.#protocol.canExpose(block.outputIndex)) return;
        const ready = block.kind === "tool_use"
          ? block.forceStart
          : block.forceStart || block.pending.length > 0;
        if (!ready || (block.kind === "tool_use" &&
          (block.toolId === null || block.toolName === null))) {
          return;
        }
        block.index = this.#nextBlockIndex;
        this.#nextBlockIndex += 1;
        block.started = true;
        events.push({
          type: "content_block_start",
          index: block.index,
          content_block: this.#startBlock(block),
        });
      }

      while (block.pending.length > 0) {
        const pending = this.#shiftPending(block);
        events.push({
          type: "content_block_delta",
          index: block.index!,
          delta: this.#delta(block, pending),
        });
        block.fingerprint.update(pending, "utf8");
        block.emittedUnits = this.#addUnits(block.emittedUnits, pending.length);
      }

      if (block.kind === "thinking" && block.done && block.signature !== null &&
        !block.signatureEmitted) {
        events.push({
          type: "content_block_delta",
          index: block.index!,
          delta: { type: "signature_delta", signature: block.signature },
        });
        block.signatureEmitted = true;
      }

      const hasBufferedText = block.kind === "text" && this.#textFilter.hasPending(block);
      const canStop = block.done && block.pending.length === 0 && !hasBufferedText &&
        (block.kind !== "thinking" || block.signature !== null) &&
        (this.#finishing || this.#matchedStop === null);
      if (!canStop) return;

      events.push({ type: "content_block_stop", index: block.index! });
      block.stopped = true;
      this.#drainCursor += 1;
    }
  }

  #startBlock(block: StreamBlock): AnthropicResponseContentBlock {
    if (block.kind === "thinking") {
      return { type: "thinking", thinking: "", signature: "" };
    }
    if (block.kind === "text") {
      return { type: "text", text: "", citations: null };
    }
    return {
      type: "tool_use",
      id: block.toolId!,
      name: block.toolName!,
      input: {},
      caller: { type: "direct" },
    };
  }

  #delta(block: StreamBlock, value: string): AnthropicContentBlockDelta {
    if (block.kind === "thinking") return { type: "thinking_delta", thinking: value };
    if (block.kind === "text") return { type: "text_delta", text: value };
    return { type: "input_json_delta", partial_json: value };
  }

  #addUnits(current: number, amount: number): number {
    const result = current + amount;
    if (!Number.isSafeInteger(result)) {
      throw invalidUpstream("Responses stream exceeded its bounded counters.");
    }
    return result;
  }

  #addArgumentBytes(current: number, value: string): number {
    const result = this.#addUnits(current, Buffer.byteLength(value, "utf8"));
    if (result > this.#limits.outputBytes) {
      throw invalidUpstream("Responses relay returned invalid function call arguments.");
    }
    return result;
  }

  #assertArgumentBytes(value: string): void {
    if (Buffer.byteLength(value, "utf8") > this.#limits.outputBytes) {
      throw invalidUpstream("Responses relay returned invalid function call arguments.");
    }
  }

  #emitError(error: unknown): AnthropicSseEvent[] {
    if (this.#terminal) return [];
    this.#protocol.markTerminal();
    this.#terminal = true;
    const normalized = error instanceof AnthropicRequestError
      ? { status: error.status }
      : normalizeApiError(error);
    const status = normalized.status === 503 ? 529 : normalized.status;
    const safeError = new AnthropicRequestError(
      status,
      FAILED_EVENT_MESSAGE,
      anthropicErrorType(status),
    );
    const event = anthropicErrorEvent(safeError, this.#context.requestId);
    this.#clearRetainedState();
    return [event];
  }
}
