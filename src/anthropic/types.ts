export type AnthropicContentBlock = {
  type: string;
  [key: string]: unknown;
};

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
  cache_creation?: AnthropicCacheCreation | null;
  inference_geo?: string | null;
  output_tokens_details?: AnthropicOutputTokensDetails | null;
  server_tool_use?: AnthropicServerToolUse | null;
  service_tier?: "standard" | "priority" | "batch" | (string & {}) | null;
  speed?: "standard" | "fast" | (string & {}) | null;
  iterations?: AnthropicUsageIteration[] | null;
  [key: string]: unknown;
};

export type AnthropicMessage = {
  id: string;
  type: "message";
  role: "assistant";
  model: string;
  content: AnthropicContentBlock[];
  stop_reason: string | null;
  stop_sequence: string | null;
  usage: AnthropicUsage;
  [key: string]: unknown;
};
