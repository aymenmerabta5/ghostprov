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

export type AnthropicUsage = {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number | null;
  cache_read_input_tokens: number | null;
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
