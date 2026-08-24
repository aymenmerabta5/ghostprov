export type AnthropicStreamLimits = {
  maxEvents: number;
  maxWireBytes: number;
  maxOutputBlocks: number;
  maxIdentities: number;
  maxStreamedBytes: number;
  maxFunctionBytes: number;
  maxReasoningBytes: number;
  maxTextBytes: number;
  maxPendingChunks: number;
};

export const DEFAULT_ANTHROPIC_STREAM_LIMITS: Readonly<AnthropicStreamLimits> = Object.freeze({
  maxEvents: 1_000_000,
  maxWireBytes: 128 * 1024 * 1024,
  maxOutputBlocks: 4_096,
  maxIdentities: 200_000,
  maxStreamedBytes: 64 * 1024 * 1024,
  maxFunctionBytes: 32 * 1024 * 1024,
  maxReasoningBytes: 32 * 1024 * 1024,
  maxTextBytes: 64 * 1024 * 1024,
  maxPendingChunks: 100_000,
});

export function resolveAnthropicStreamLimits(
  overrides: Partial<AnthropicStreamLimits> | undefined,
): AnthropicStreamLimits {
  const limits = { ...DEFAULT_ANTHROPIC_STREAM_LIMITS, ...overrides };
  for (const value of Object.values(limits)) {
    if (!Number.isSafeInteger(value) || value < 1) {
      throw new RangeError("Anthropic stream limits must be positive safe integers");
    }
  }
  return limits;
}

export type AnthropicStreamSnapshot = {
  events: number;
  wireBytes: number;
  outputBlocks: number;
  identities: number;
  streamedBytes: number;
  functionBytes: number;
  reasoningBytes: number;
  textBytes: number;
  pendingChunks: number;
  retainedBlocks: number;
  retainedIdentities: number;
  retainedPendingChunks: number;
  retainedPendingBytes: number;
  retainedTextCodePoints: number;
  retainedReasoningCodePoints: number;
};
