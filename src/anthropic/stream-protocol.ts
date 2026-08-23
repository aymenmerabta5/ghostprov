import { invalidUpstream, record } from "./response-validation";

type ItemKind = "message" | "reasoning" | "function_call" | string;

type PartState = {
  type: string;
  added: boolean;
  textDone: boolean;
  partDone: boolean;
};

type ItemState = {
  outputIndex: number;
  itemId: string;
  type: ItemKind;
  added: boolean;
  done: boolean;
  argumentsDone: boolean;
  nextContentIndex: number;
  nextSummaryIndex: number;
  nextRawReasoningIndex: number;
  content: Map<number, PartState>;
  summary: Map<number, PartState>;
  rawReasoning: Map<number, PartState>;
};

const PROTOCOL_ERROR = "Responses stream violated its event protocol.";

function fail(): never {
  throw invalidUpstream(PROTOCOL_ERROR);
}

function requiredString(value: Record<string, unknown>, key: string): string {
  const candidate = value[key];
  if (typeof candidate !== "string" || candidate.length === 0) fail();
  return candidate;
}

function requiredText(value: Record<string, unknown>, key: string): string {
  const candidate = value[key];
  if (typeof candidate !== "string") fail();
  return candidate;
}

function requiredIndex(value: Record<string, unknown>, key: string): number {
  const candidate = value[key];
  if (!Number.isSafeInteger(candidate) || Number(candidate) < 0) fail();
  return Number(candidate);
}

function itemRecord(event: Record<string, unknown>): Record<string, unknown> {
  const item = record(event.item);
  if (!item || typeof item.type !== "string" || !item.type) fail();
  return item;
}

function partRecord(event: Record<string, unknown>): Record<string, unknown> {
  const part = record(event.part);
  if (!part || typeof part.type !== "string" || !part.type) fail();
  return part;
}

export type ResponsesStreamProtocolSnapshot = {
  events: number;
  identities: number;
  retainedItems: number;
  retainedIdentities: number;
};

/** Validates official Responses event sequencing, aliases, and item/part lifecycles. */
export class ResponsesStreamProtocol {
  readonly #itemsByIndex = new Map<number, ItemState>();
  readonly #itemsById = new Map<string, ItemState>();
  #lastSequence = 0;
  #nextOutputIndex = 0;
  #eventCount = 0;
  #identityCount = 0;
  #retainedIdentityCount = 0;
  #terminal = false;

  constructor(private readonly limits: { maxEvents: number; maxIdentities: number }) {}

  accept(event: Record<string, unknown>): void {
    if (this.#terminal) fail();
    const sequence = event.sequence_number;
    if (!Number.isSafeInteger(sequence) || Number(sequence) !== this.#lastSequence + 1) fail();
    if (this.#eventCount >= this.limits.maxEvents) fail();
    this.#lastSequence = Number(sequence);
    this.#eventCount += 1;

    const type = event.type;
    if (typeof type !== "string") fail();
    switch (type) {
      case "response.output_item.added": this.#outputItemAdded(event); break;
      case "response.output_item.done": this.#outputItemDone(event); break;
      case "response.content_part.added": this.#contentPartAdded(event); break;
      case "response.content_part.done": this.#contentPartDone(event); break;
      case "response.output_text.delta": this.#outputTextDelta(event); break;
      case "response.output_text.done": this.#outputTextDone(event); break;
      case "response.reasoning_summary_part.added": this.#summaryPartAdded(event); break;
      case "response.reasoning_summary_part.done": this.#summaryPartDone(event); break;
      case "response.reasoning_summary_text.delta": this.#summaryTextDelta(event); break;
      case "response.reasoning_summary_text.done": this.#summaryTextDone(event); break;
      case "response.reasoning_text.delta": this.#rawReasoningDelta(event); break;
      case "response.reasoning_text.done": this.#rawReasoningDone(event); break;
      case "response.function_call_arguments.delta": this.#argumentsDelta(event); break;
      case "response.function_call_arguments.done": this.#argumentsDone(event); break;
    }
  }

  markTerminal(): void {
    this.#terminal = true;
  }

  canExpose(outputIndex: number): boolean {
    for (let index = 0; index < outputIndex; index += 1) {
      if (this.#itemsByIndex.get(index)?.done !== true) return false;
    }
    return true;
  }

  validateFinal(value: unknown): void {
    const response = record(value);
    if (!response || !Array.isArray(response.output)) fail();
    for (let outputIndex = 0; outputIndex < response.output.length; outputIndex += 1) {
      const item = record(response.output[outputIndex]);
      if (!item || typeof item.type !== "string" || !item.type) fail();
      const existing = this.#itemsByIndex.get(outputIndex);
      const itemId = typeof item.id === "string" && item.id
        ? item.id
        : item.type === "function_call"
          ? existing?.itemId ?? `@${outputIndex}`
          : requiredString(item, "id");
      const state = this.#item(outputIndex, itemId, item.type);
      state.done = true;

      if (item.type === "message") {
        if (!Array.isArray(item.content)) fail();
        for (let contentIndex = 0; contentIndex < item.content.length; contentIndex += 1) {
          const part = record(item.content[contentIndex]);
          if (!part || typeof part.type !== "string" || !part.type) fail();
          this.#part(state, "content", contentIndex, part.type);
        }
      } else if (item.type === "reasoning") {
        if (!Array.isArray(item.summary)) fail();
        for (let summaryIndex = 0; summaryIndex < item.summary.length; summaryIndex += 1) {
          const part = record(item.summary[summaryIndex]);
          if (!part || part.type !== "summary_text") fail();
          this.#part(state, "summary", summaryIndex, "summary_text");
        }
      }
    }
  }

  snapshot(): ResponsesStreamProtocolSnapshot {
    return {
      events: this.#eventCount,
      identities: this.#identityCount,
      retainedItems: this.#itemsByIndex.size,
      retainedIdentities: this.#retainedIdentityCount,
    };
  }

  clearRetained(): void {
    this.#itemsByIndex.clear();
    this.#itemsById.clear();
    this.#retainedIdentityCount = 0;
  }

  #outputItemAdded(event: Record<string, unknown>): void {
    const outputIndex = requiredIndex(event, "output_index");
    const item = itemRecord(event);
    const state = this.#item(outputIndex, requiredString(item, "id"), item.type as string);
    if (state.added || state.done) fail();
    state.added = true;
  }

  #outputItemDone(event: Record<string, unknown>): void {
    const outputIndex = requiredIndex(event, "output_index");
    const item = itemRecord(event);
    const state = this.#item(outputIndex, requiredString(item, "id"), item.type as string);
    if (state.done) fail();
    state.done = true;
  }

  #contentPartAdded(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "message");
    const state = this.#part(item, "content", requiredIndex(event, "content_index"),
      partRecord(event).type as string);
    if (state.added || state.partDone || item.done) fail();
    state.added = true;
  }

  #contentPartDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "message");
    const state = this.#part(item, "content", requiredIndex(event, "content_index"),
      partRecord(event).type as string);
    if (state.partDone || item.done) fail();
    state.partDone = true;
  }

  #outputTextDelta(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "message");
    const state = this.#part(item, "content", requiredIndex(event, "content_index"), "output_text");
    if (state.textDone || state.partDone || item.done) fail();
  }

  #outputTextDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "message");
    const state = this.#part(item, "content", requiredIndex(event, "content_index"), "output_text");
    if (state.textDone || state.partDone || item.done) fail();
    state.textDone = true;
  }

  #summaryPartAdded(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    if (partRecord(event).type !== "summary_text") fail();
    const state = this.#part(item, "summary", requiredIndex(event, "summary_index"), "summary_text");
    if (state.added || state.partDone || item.done) fail();
    state.added = true;
  }

  #summaryPartDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    if (partRecord(event).type !== "summary_text") fail();
    const state = this.#part(item, "summary", requiredIndex(event, "summary_index"), "summary_text");
    if (state.partDone || item.done) fail();
    state.partDone = true;
  }

  #summaryTextDelta(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const state = this.#part(item, "summary", requiredIndex(event, "summary_index"), "summary_text");
    if (state.textDone || state.partDone || item.done) fail();
  }

  #summaryTextDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const state = this.#part(item, "summary", requiredIndex(event, "summary_index"), "summary_text");
    if (state.textDone || state.partDone || item.done) fail();
    requiredText(event, "text");
    state.textDone = true;
  }

  #rawReasoningDelta(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const state = this.#part(item, "rawReasoning", requiredIndex(event, "content_index"), "reasoning_text");
    if (state.textDone || state.partDone || item.done) fail();
  }

  #rawReasoningDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const state = this.#part(item, "rawReasoning", requiredIndex(event, "content_index"), "reasoning_text");
    if (state.textDone || state.partDone || item.done) fail();
    requiredText(event, "text");
    state.textDone = true;
  }

  #argumentsDelta(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "function_call");
    if (item.argumentsDone || item.done) fail();
  }

  #argumentsDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "function_call");
    if (item.argumentsDone || item.done) fail();
    item.argumentsDone = true;
  }

  #eventItem(event: Record<string, unknown>, type: ItemKind): ItemState {
    return this.#item(requiredIndex(event, "output_index"), requiredString(event, "item_id"), type);
  }

  #item(outputIndex: number, itemId: string, type: ItemKind): ItemState {
    const byIndex = this.#itemsByIndex.get(outputIndex);
    const byId = this.#itemsById.get(itemId);
    if (byIndex || byId) {
      if (!byIndex || !byId || byIndex !== byId || byIndex.itemId !== itemId || byIndex.type !== type) fail();
      return byIndex;
    }
    if (outputIndex !== this.#nextOutputIndex) fail();
    const state: ItemState = {
      outputIndex, itemId, type,
      added: false, done: false, argumentsDone: false,
      nextContentIndex: 0, nextSummaryIndex: 0, nextRawReasoningIndex: 0,
      content: new Map(), summary: new Map(), rawReasoning: new Map(),
    };
    this.#nextOutputIndex += 1;
    this.#itemsByIndex.set(outputIndex, state);
    this.#itemsById.set(itemId, state);
    this.#recordIdentity();
    return state;
  }

  #part(
    item: ItemState,
    collection: "content" | "summary" | "rawReasoning",
    index: number,
    type: string,
  ): PartState {
    const parts = item[collection];
    const existing = parts.get(index);
    if (existing) {
      if (existing.type !== type) fail();
      return existing;
    }
    const nextKey = collection === "content" ? "nextContentIndex"
      : collection === "summary" ? "nextSummaryIndex" : "nextRawReasoningIndex";
    if (index !== item[nextKey]) fail();
    item[nextKey] += 1;
    const state: PartState = { type, added: false, textDone: false, partDone: false };
    parts.set(index, state);
    this.#recordIdentity();
    return state;
  }

  #recordIdentity(): void {
    if (this.#identityCount >= this.limits.maxIdentities) fail();
    this.#identityCount += 1;
    this.#retainedIdentityCount += 1;
  }
}
