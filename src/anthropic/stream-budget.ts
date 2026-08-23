import { invalidUpstream } from "./response-validation";
import type { AnthropicStreamLimits, AnthropicStreamSnapshot } from "./stream-limits";

type StreamByteCategory = "text" | "reasoning" | "function";

type BudgetSnapshot = Pick<
  AnthropicStreamSnapshot,
  | "outputBlocks"
  | "streamedBytes"
  | "functionBytes"
  | "reasoningBytes"
  | "textBytes"
  | "pendingChunks"
  | "retainedPendingChunks"
  | "retainedPendingBytes"
>;

export class AnthropicStreamBudget {
  #outputBlocks = 0;
  #pendingChunks = 0;
  #retainedPendingChunks = 0;
  #retainedPendingBytes = 0;
  #streamedBytes = 0;
  #functionBytes = 0;
  #reasoningBytes = 0;
  #textBytes = 0;

  constructor(private readonly limits: AnthropicStreamLimits) {}

  countPayload(category: StreamByteCategory, value: string): void {
    const bytes = Buffer.byteLength(value, "utf8");
    const total = this.#safeAdd(this.#streamedBytes, bytes);
    const current = category === "text" ? this.#textBytes
      : category === "reasoning" ? this.#reasoningBytes : this.#functionBytes;
    const categoryTotal = this.#safeAdd(current, bytes);
    const categoryLimit = category === "text" ? this.limits.maxTextBytes
      : category === "reasoning" ? this.limits.maxReasoningBytes
        : this.limits.maxFunctionBytes;
    if (total > this.limits.maxStreamedBytes || categoryTotal > categoryLimit) {
      throw invalidUpstream("Responses stream exceeded its cumulative byte limits.");
    }
    this.#streamedBytes = total;
    if (category === "text") this.#textBytes = categoryTotal;
    else if (category === "reasoning") this.#reasoningBytes = categoryTotal;
    else this.#functionBytes = categoryTotal;
  }

  reserveOutputBlock(): void {
    if (this.#outputBlocks >= this.limits.maxOutputBlocks) {
      throw invalidUpstream("Responses stream exceeded its output block limit.");
    }
    this.#outputBlocks += 1;
  }

  reservePendingChunk(bytes: number, maxRetainedBytes: number, maxRetainedChunks: number): void {
    const cumulative = this.#safeAdd(this.#pendingChunks, 1);
    const retainedChunks = this.#safeAdd(this.#retainedPendingChunks, 1);
    const retainedBytes = this.#safeAdd(this.#retainedPendingBytes, bytes);
    if (cumulative > this.limits.maxPendingChunks || retainedBytes > maxRetainedBytes ||
      retainedChunks > maxRetainedChunks) {
      throw invalidUpstream("Responses stream exceeded its bounded pending output.");
    }
    this.#pendingChunks = cumulative;
    this.#retainedPendingChunks = retainedChunks;
    this.#retainedPendingBytes = retainedBytes;
  }

  releasePending(bytes: number, chunks: number): void {
    this.#retainedPendingBytes -= bytes;
    this.#retainedPendingChunks -= chunks;
  }

  clearRetained(): void {
    this.#retainedPendingBytes = 0;
    this.#retainedPendingChunks = 0;
  }

  snapshot(): BudgetSnapshot {
    return {
      outputBlocks: this.#outputBlocks,
      streamedBytes: this.#streamedBytes,
      functionBytes: this.#functionBytes,
      reasoningBytes: this.#reasoningBytes,
      textBytes: this.#textBytes,
      pendingChunks: this.#pendingChunks,
      retainedPendingChunks: this.#retainedPendingChunks,
      retainedPendingBytes: this.#retainedPendingBytes,
    };
  }

  #safeAdd(current: number, amount: number): number {
    const result = current + amount;
    if (!Number.isSafeInteger(result)) {
      throw invalidUpstream("Responses stream exceeded its bounded counters.");
    }
    return result;
  }
}
