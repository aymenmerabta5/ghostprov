export type ResponsesFinalItemStatus = "in_progress" | "completed" | "incomplete";

export type ResponsesLogprob = {
  token: string;
  bytes: number[];
  logprob: number;
  top_logprobs: Array<{
    token: string;
    bytes: number[];
    logprob: number;
  }>;
};

export type ResponsesOutputText = {
  type: "output_text";
  text: string;
  annotations: readonly unknown[];
  logprobs: ResponsesLogprob[];
  [key: string]: unknown;
};

export type ResponsesOutputRefusal = {
  type: "refusal";
  refusal: string;
  [key: string]: unknown;
};

export type ResponsesFinalMessageItem = {
  type: "message";
  id: string;
  status: ResponsesFinalItemStatus;
  role: "assistant";
  content: Array<ResponsesOutputText | ResponsesOutputRefusal>;
  phase?: "commentary" | "final_answer" | null;
  [key: string]: unknown;
};

export type ResponsesReasoningSummary = {
  type: "summary_text";
  text: string;
  [key: string]: unknown;
};

export type ResponsesFinalReasoningItem = {
  type: "reasoning";
  id: string;
  status?: ResponsesFinalItemStatus;
  encrypted_content?: string | null;
  summary: ResponsesReasoningSummary[];
  content?: Array<{ type: "reasoning_text"; text: string }>;
  [key: string]: unknown;
};

export type ResponsesFinalFunctionCallItem = {
  type: "function_call";
  id?: string;
  status?: ResponsesFinalItemStatus;
  call_id: string;
  name: string;
  arguments: string;
  caller?: { type: "direct" } | { type: "program"; caller_id: string } | null;
  namespace?: string;
  [key: string]: unknown;
};

export type ResponsesFinalOutputItem =
  | ResponsesFinalMessageItem
  | ResponsesFinalReasoningItem
  | ResponsesFinalFunctionCallItem;

export type ResponsesIncompleteReason = "max_output_tokens" | "content_filter";

export type ResponsesUsage = {
  input_tokens: number;
  output_tokens: number;
  input_tokens_details: {
    cached_tokens: number;
    cache_write_tokens: number;
    [key: string]: unknown;
  };
  output_tokens_details: {
    reasoning_tokens: number;
    [key: string]: unknown;
  };
  total_tokens: number;
  [key: string]: unknown;
};

type TerminalResponseProjectionBase = {
  id: string;
  object: "response";
  created_at: number;
  model: string;
  error: null;
  output: ResponsesFinalOutputItem[];
  usage: ResponsesUsage | null;
  service_tier?: unknown;
  [key: string]: unknown;
};

export type TerminalResponseProjection = TerminalResponseProjectionBase & (
  | {
    status: "completed";
    incomplete_details: null;
  }
  | {
    status: "incomplete";
    incomplete_details: { reason?: ResponsesIncompleteReason } | null;
  }
);
