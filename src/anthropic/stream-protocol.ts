import { invalidUpstream, record } from "./response-validation";

type ItemKind = "message" | "reasoning" | "function_call" | string;

type PartState = {
  type: string;
  added: boolean;
  textDone: boolean;
  partDone: boolean;
  seenUnits: number;
};

type ItemState = {
  outputIndex: number;
  itemId: string;
  type: ItemKind;
  added: boolean;
  done: boolean;
  argumentsDone: boolean;
  argumentUnits: number;
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

function validateItem(item: Record<string, unknown>): void {
  if (item.type === "message") {
    if (item.role !== "assistant" || !Array.isArray(item.content)) fail();
    for (const value of item.content) {
      const part = record(value);
      if (!part) fail();
      validateContentPart(part);
    }
  } else if (item.type === "reasoning") {
    if (!Array.isArray(item.summary)) fail();
    for (const value of item.summary) {
      const part = record(value);
      if (!part || part.type !== "summary_text") fail();
      requiredText(part, "text");
    }
  } else if (item.type === "function_call") {
    requiredString(item, "call_id");
    requiredString(item, "name");
    requiredText(item, "arguments");
  }
}

function validateContentPart(part: Record<string, unknown>): void {
  if (part.type === "output_text") {
    requiredText(part, "text");
    if (!Array.isArray(part.annotations) || !Array.isArray(part.logprobs)) fail();
  } else if (part.type === "refusal") {
    requiredText(part, "refusal");
  } else {
    fail();
  }
}

function validateItemStatus(
  item: Record<string, unknown>,
  phase: "added" | "done",
): void {
  if (item.type === "reasoning" && item.status === undefined) return;
  if (item.type !== "message" && item.type !== "reasoning" && item.type !== "function_call") {
    return;
  }
  if (phase === "added") {
    if (item.status !== "in_progress") fail();
  } else if (item.status !== "completed" && item.status !== "incomplete") {
    fail();
  }
}

function responseEnvelope(event: Record<string, unknown>): Record<string, unknown> {
  const response = record(event.response);
  if (!response) fail();
  requiredString(response, "id");
  return response;
}

function validateLifecycleResponse(
  event: Record<string, unknown>,
  status: "in_progress" | "completed" | "incomplete",
): void {
  if (responseEnvelope(event).status !== status) fail();
}

function validateFailedResponse(event: Record<string, unknown>): void {
  const response = responseEnvelope(event);
  if (response.status !== "failed") fail();
  const error = record(response.error);
  if (!error) fail();
  requiredString(error, "code");
  requiredString(error, "message");
}

function validateErrorEvent(event: Record<string, unknown>): void {
  if (event.code !== null) requiredString(event, "code");
  requiredString(event, "message");
  if (event.param !== null && typeof event.param !== "string") fail();
}

export type ResponsesStreamProtocolSnapshot = {
  events: number;
  identities: number;
  retainedItems: number;
  retainedIdentities: number;
};

export type ResponsesStreamAcceptedSuffix = {
  category: "text" | "reasoning" | "function";
  value: string;
};

/** Validates official Responses event sequencing, aliases, and item/part lifecycles. */
export class ResponsesStreamProtocol {
  readonly #itemsByIndex = new Map<number, ItemState>();
  readonly #itemsById = new Map<string, ItemState>();
  #lastSequence: number | null = null;
  #nextOutputIndex = 0;
  #completedOutputPrefix = -1;
  #eventCount = 0;
  #identityCount = 0;
  #retainedIdentityCount = 0;
  #terminal = false;
  #acceptedSuffix: ResponsesStreamAcceptedSuffix | null = null;

  constructor(private readonly limits: { maxEvents: number; maxIdentities: number }) {}

  accept(event: Record<string, unknown>): ResponsesStreamAcceptedSuffix | null {
    if (this.#terminal) fail();
    this.#acceptedSuffix = null;
    const sequence = event.sequence_number;
    if (!Number.isSafeInteger(sequence) || Number(sequence) < 0 ||
      (this.#lastSequence !== null && Number(sequence) !== this.#lastSequence + 1)) fail();
    if (this.#eventCount >= this.limits.maxEvents) fail();
    this.#lastSequence = Number(sequence);
    this.#eventCount += 1;

    const type = event.type;
    if (typeof type !== "string") fail();
    switch (type) {
      case "response.created": validateLifecycleResponse(event, "in_progress"); break;
      case "response.in_progress": validateLifecycleResponse(event, "in_progress"); break;
      case "response.completed": validateLifecycleResponse(event, "completed"); break;
      case "response.incomplete": validateLifecycleResponse(event, "incomplete"); break;
      case "response.failed": validateFailedResponse(event); break;
      case "error": validateErrorEvent(event); break;
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
    return this.#acceptedSuffix;
  }

  markTerminal(): void {
    this.#terminal = true;
  }

  canExpose(outputIndex: number): boolean {
    return outputIndex <= this.#completedOutputPrefix + 1;
  }

  validateFinal(value: unknown): void {
    const response = record(value);
    if (!response || !Array.isArray(response.output)) fail();
    for (const outputIndex of this.#itemsByIndex.keys()) {
      if (outputIndex >= response.output.length) fail();
    }
    for (let outputIndex = 0; outputIndex < response.output.length; outputIndex += 1) {
      const item = record(response.output[outputIndex]);
      if (!item || typeof item.type !== "string" || !item.type) fail();
      validateItem(item);
      const existing = this.#itemsByIndex.get(outputIndex);
      const itemId = typeof item.id === "string" && item.id
        ? item.id
        : item.type === "function_call"
          ? existing?.itemId ?? `@${outputIndex}`
          : requiredString(item, "id");
      const state = this.#item(outputIndex, itemId, item.type);
      state.done = true;
      this.#advanceCompletedPrefix();
      this.#observeItemParts(state, item);
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
    validateItem(item);
    validateItemStatus(item, "added");
    const state = this.#item(outputIndex, requiredString(item, "id"), item.type as string);
    if (state.added || state.done) fail();
    this.#observeItemParts(state, item);
    state.added = true;
  }

  #outputItemDone(event: Record<string, unknown>): void {
    const outputIndex = requiredIndex(event, "output_index");
    const item = itemRecord(event);
    validateItem(item);
    validateItemStatus(item, "done");
    const state = this.#item(outputIndex, requiredString(item, "id"), item.type as string);
    if (state.done) fail();
    this.#observeItemParts(state, item);
    state.done = true;
    this.#advanceCompletedPrefix();
  }

  #contentPartAdded(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "message");
    const part = partRecord(event);
    validateContentPart(part);
    const state = this.#part(item, "content", requiredIndex(event, "content_index"),
      part.type as string);
    if (state.added || state.partDone || item.done) fail();
    state.added = true;
  }

  #contentPartDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "message");
    const part = partRecord(event);
    validateContentPart(part);
    const state = this.#part(item, "content", requiredIndex(event, "content_index"),
      part.type as string);
    if (state.partDone || item.done) fail();
    state.partDone = true;
  }

  #outputTextDelta(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "message");
    const state = this.#part(item, "content", requiredIndex(event, "content_index"), "output_text");
    if (state.textDone || state.partDone || item.done) fail();
    state.seenUnits += requiredText(event, "delta").length;
  }

  #outputTextDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "message");
    const state = this.#part(item, "content", requiredIndex(event, "content_index"), "output_text");
    if (state.textDone || state.partDone || item.done) fail();
    this.#recordDoneSuffix("text", state, requiredText(event, "text"));
    state.textDone = true;
  }

  #summaryPartAdded(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const part = partRecord(event);
    if (part.type !== "summary_text") fail();
    requiredText(part, "text");
    const state = this.#part(item, "summary", requiredIndex(event, "summary_index"), "summary_text");
    if (state.added || state.partDone || item.done) fail();
    state.added = true;
  }

  #summaryPartDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const part = partRecord(event);
    if (part.type !== "summary_text") fail();
    requiredText(part, "text");
    const state = this.#part(item, "summary", requiredIndex(event, "summary_index"), "summary_text");
    if (state.partDone || item.done) fail();
    state.partDone = true;
  }

  #summaryTextDelta(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const state = this.#part(item, "summary", requiredIndex(event, "summary_index"), "summary_text");
    if (state.textDone || state.partDone || item.done) fail();
    state.seenUnits += requiredText(event, "delta").length;
  }

  #summaryTextDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const state = this.#part(item, "summary", requiredIndex(event, "summary_index"), "summary_text");
    if (state.textDone || state.partDone || item.done) fail();
    this.#recordDoneSuffix("reasoning", state, requiredText(event, "text"));
    state.textDone = true;
  }

  #rawReasoningDelta(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const state = this.#part(item, "rawReasoning", requiredIndex(event, "content_index"), "reasoning_text");
    if (state.textDone || state.partDone || item.done) fail();
    state.seenUnits += requiredText(event, "delta").length;
  }

  #rawReasoningDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "reasoning");
    const state = this.#part(item, "rawReasoning", requiredIndex(event, "content_index"), "reasoning_text");
    if (state.textDone || state.partDone || item.done) fail();
    this.#recordDoneSuffix("reasoning", state, requiredText(event, "text"));
    state.textDone = true;
  }

  #argumentsDelta(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "function_call");
    if (item.argumentsDone || item.done) fail();
    item.argumentUnits += requiredText(event, "delta").length;
  }

  #argumentsDone(event: Record<string, unknown>): void {
    const item = this.#eventItem(event, "function_call");
    if (item.argumentsDone || item.done) fail();
    requiredString(event, "name");
    const args = requiredText(event, "arguments");
    if (args.length < item.argumentUnits) fail();
    this.#acceptedSuffix = { category: "function", value: args.slice(item.argumentUnits) };
    item.argumentUnits = args.length;
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
      added: false, done: false, argumentsDone: false, argumentUnits: 0,
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
    const state: PartState = {
      type,
      added: false,
      textDone: false,
      partDone: false,
      seenUnits: 0,
    };
    parts.set(index, state);
    this.#recordIdentity();
    return state;
  }

  #observeItemParts(item: ItemState, value: Record<string, unknown>): void {
    if (value.type === "message") {
      const content = value.content as unknown[];
      if (item.content.size > content.length) fail();
      for (let contentIndex = 0; contentIndex < content.length; contentIndex += 1) {
        const part = record(content[contentIndex]);
        if (!part || typeof part.type !== "string" || !part.type) fail();
        this.#part(item, "content", contentIndex, part.type);
      }
    } else if (value.type === "reasoning") {
      const summary = value.summary as unknown[];
      if (item.summary.size > summary.length) fail();
      for (let summaryIndex = 0; summaryIndex < summary.length; summaryIndex += 1) {
        this.#part(item, "summary", summaryIndex, "summary_text");
      }
    }
  }

  #recordIdentity(): void {
    if (this.#identityCount >= this.limits.maxIdentities) fail();
    this.#identityCount += 1;
    this.#retainedIdentityCount += 1;
  }

  #advanceCompletedPrefix(): void {
    while (this.#itemsByIndex.get(this.#completedOutputPrefix + 1)?.done === true) {
      this.#completedOutputPrefix += 1;
    }
  }

  #recordDoneSuffix(
    category: "text" | "reasoning",
    state: PartState,
    value: string,
  ): void {
    if (value.length < state.seenUnits) fail();
    this.#acceptedSuffix = { category, value: value.slice(state.seenUnits) };
    state.seenUnits = value.length;
  }
}
