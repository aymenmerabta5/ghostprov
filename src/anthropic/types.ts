export type AnthropicContentBlock = {
  type: string;
  [key: string]: unknown;
};

export type AnthropicTextBlock = {
  type: "text";
  text: string;
  citations: null;
  [key: string]: unknown;
};

export type AnthropicThinkingBlock = {
  type: "thinking";
  thinking: string;
  signature: string;
  [key: string]: unknown;
};

export type AnthropicToolUseBlock = {
  type: "tool_use";
  id: string;
  name: string;
  input: Record<string, unknown>;
  caller: { type: "direct" };
  [key: string]: unknown;
};

export type AnthropicResponseContentBlock =
  | AnthropicTextBlock
  | AnthropicThinkingBlock
  | AnthropicToolUseBlock;

export type AnthropicStopReason =
  | "end_turn"
  | "max_tokens"
  | "stop_sequence"
  | "tool_use"
  | "pause_turn"
  | "compaction"
  | "refusal"
  | "model_context_window_exceeded";

export type AnthropicAdapterStopReason = Extract<
  AnthropicStopReason,
  "end_turn" | "max_tokens" | "stop_sequence" | "tool_use" | "refusal"
>;

export type AnthropicMessageParam = {
  role: "user" | "assistant";
  content: string | AnthropicContentBlock[];
};

export type AnthropicMessageRequest = {
  model: string;
  max_tokens: number;
  messages: AnthropicMessageParam[];
  stream?: boolean;
  system?: string | Array<{ type: "text"; text: string; [key: string]: unknown }>;
  tools?: Array<Record<string, unknown>>;
  tool_choice?: Record<string, unknown>;
  thinking?: Record<string, unknown>;
  output_config?: Record<string, unknown>;
  [key: string]: unknown;
};

export type AnthropicCacheCreation = {
  ephemeral_1h_input_tokens: number;
  ephemeral_5m_input_tokens: number;
  [key: string]: unknown;
};

export type AnthropicOutputTokensDetails = {
  thinking_tokens: number;
  [key: string]: unknown;
};

export type AnthropicServerToolUse = {
  web_fetch_requests: number;
  web_search_requests: number;
  [key: string]: unknown;
};

export type AnthropicUsageIteration = {
  type: "message" | "fallback_message" | "advisor_message" | "compaction" | (string & {});
  model?: string;
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  cache_creation: AnthropicCacheCreation | null;
  [key: string]: unknown;
};

export type AnthropicUsage = {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number | null;
  cache_read_input_tokens: number | null;
  cache_creation: AnthropicCacheCreation | null;
  inference_geo: string | null;
  output_tokens_details: AnthropicOutputTokensDetails | null;
  server_tool_use: AnthropicServerToolUse | null;
  service_tier: "standard" | "priority" | "batch" | (string & {}) | null;
  speed: "standard" | "fast" | (string & {}) | null;
  iterations: AnthropicUsageIteration[] | null;
  [key: string]: unknown;
};

export type AnthropicContextManagementResponse = {
  applied_edits: unknown[];
};

export type AnthropicMessage = {
  id: string;
  type: "message";
  role: "assistant";
  model: string;
  content: AnthropicResponseContentBlock[];
  container: null;
  stop_details: null;
  stop_reason: AnthropicStopReason;
  stop_sequence: string | null;
  usage: AnthropicUsage;
  context_management?: AnthropicContextManagementResponse | null;
  [key: string]: unknown;
};

export type AnthropicAdapterMessage = AnthropicMessage & {
  stop_reason: AnthropicAdapterStopReason;
};
