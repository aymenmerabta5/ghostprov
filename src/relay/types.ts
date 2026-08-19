// Thinking + Fast/Pro support — verbatim passthrough for all fields below.
// See Global Constraints: forward verbatim; prompt_cache handling already exists, keep it.

export type ReasoningEffort = "low" | "medium" | "high" | "xhigh" | "max" | "minimal" | "none";
export type ReasoningMode = "standard" | "pro";
export type ReasoningContext = "current_turn" | "all_turns" | "auto";
export type ReasoningSummary = "auto" | "detailed" | "concise";

export type Reasoning = {
  effort?: ReasoningEffort;
  mode?: ReasoningMode;
  context?: ReasoningContext;
  summary?: ReasoningSummary;
  // allow extra keys verbatim (e.g., future reasoning fields)
  [key: string]: unknown;
};

export type ServiceTier = "fast" | "priority" | "default" | "auto";

export type Phase = "commentary" | "final_answer";

export type ResponsesBody = {
  model: string;
  input: unknown;
  tools?: unknown[];
  tool_choice?: unknown;
  previous_response_id?: string;
  store?: boolean;
  stream?: boolean;
  prompt_cache_key?: string;
  prompt_cache_options?: unknown;
  reasoning?: Reasoning;
  service_tier?: ServiceTier;
  instructions?: string;
  text?: unknown;
  // allow any extra keys verbatim (input_image, custom_tool_call, reasoning, service_tier, etc.)
  [key: string]: unknown;
};

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
  reasoning?: Reasoning;
  service_tier?: ServiceTier;
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
  | { type: "message"; role?: string; content: unknown; phase?: Phase; [key: string]: unknown }
  | { type: "reasoning"; id?: string; content?: unknown; summary?: unknown; effort?: ReasoningEffort; mode?: ReasoningMode; [key: string]: unknown }
  | { type: "function_call"; name: string; arguments: string; call_id: string }
  | { type: "custom_tool_call"; name: string; input: unknown; call_id: string }
  | { type: "function_call_output"; call_id: string; output: unknown }
  | { type: string; [key: string]: unknown };

export type ResponsesOutput = {
  id: string;
  output: OutputItem[];
  output_text?: string;
  usage?: unknown;
  reasoning?: Reasoning;
  service_tier?: ServiceTier;
  [key: string]: unknown;
};

// Helpers for validation (used by client/server but still forward verbatim even if invalid)
export const VALID_REASONING_EFFORTS: readonly ReasoningEffort[] = ["low", "medium", "high", "xhigh", "max", "minimal", "none"] as const;
export const VALID_REASONING_MODES: readonly ReasoningMode[] = ["standard", "pro"] as const;
export const VALID_REASONING_CONTEXTS: readonly ReasoningContext[] = ["current_turn", "all_turns", "auto"] as const;
export const VALID_REASONING_SUMMARIES: readonly ReasoningSummary[] = ["auto", "detailed", "concise"] as const;
export const VALID_SERVICE_TIERS: readonly ServiceTier[] = ["fast", "priority", "default", "auto"] as const;
export const VALID_PHASES: readonly Phase[] = ["commentary", "final_answer"] as const;
