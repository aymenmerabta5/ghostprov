export type ResponsesBody = { model: string; input: unknown; tools?: unknown[]; tool_choice?: unknown; previous_response_id?: string; store?: boolean; stream?: boolean; prompt_cache_key?: string; prompt_cache_options?: unknown; reasoning?: unknown; instructions?: string; text?: unknown; };

// Extended types that cover full passthrough surface required by the plan:
// prompt_cache_breakpoint, input_image data URLs, custom tools, etc.
// These are intentionally permissive (unknown) to ensure verbatim forwarding
// without stripping fields — see Global Constraints: forward verbatim.
export type PromptCacheBreakpoint = { mode: "explicit" } | unknown;
export type PromptCacheOptions = { mode: "explicit"; ttl: "30m" } | unknown;

export type ResponsesBodyFull = ResponsesBody & {
  prompt_cache_breakpoint?: PromptCacheBreakpoint;
  prompt_cache_key?: string;
  prompt_cache_options?: PromptCacheOptions;
  // allow any extra keys (input_image, custom_tool_call, etc.) verbatim
  [key: string]: unknown;
};

export type ToolDefinition = {
  type: "function" | "custom";
  name: string;
  description?: string;
  parameters?: unknown;
  [key: string]: unknown;
};

export type OutputItem =
  | { type: "message"; role?: string; content: unknown }
  | { type: "function_call"; name: string; arguments: string; call_id: string }
  | { type: "custom_tool_call"; name: string; input: unknown; call_id: string }
  | { type: "function_call_output"; call_id: string; output: unknown }
  | { type: string; [key: string]: unknown };

export type ResponsesOutput = {
  id: string;
  output: OutputItem[];
  output_text?: string;
  usage?: unknown;
  [key: string]: unknown;
};
