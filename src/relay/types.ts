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

// ---- Image / Vision types (Task Image) ----
export type ImageDetail = "low" | "high" | "auto" | "original";

export type InputImage = {
  type: "input_image";
  image_url?: string;
  file_id?: string;
  detail?: ImageDetail;
  [key: string]: unknown;
};

export type InputText = {
  type: "input_text";
  text: string;
  [key: string]: unknown;
};

export type InputFile = {
  type: "input_file";
  file_id?: string;
  filename?: string;
  file_data?: string;
  [key: string]: unknown;
};

// ---- Audio types (Task Image: Audio and speech, transcription, voice) ----
export type InputAudio = {
  type: "input_audio";
  // Responses API audio input: { data: base64, format: "wav"|"mp3"|... } or file_id
  input_audio?: {
    data: string;
    format: string;
    [key: string]: unknown;
  };
  file_id?: string;
  // transcription subfields (if audio transcription model)
  transcript?: string;
  [key: string]: unknown;
};

export type AudioOutput = {
  type: "output_audio";
  data?: string;
  format?: string;
  transcript?: string;
  [key: string]: unknown;
};

// Generic content part union for input array entries
export type InputContentPart = InputText | InputImage | InputAudio | InputFile | { type: string; [key: string]: unknown };

export type UserMessageInput = {
  role: "user" | "system" | "assistant" | "developer";
  content: (string | InputContentPart)[] | string;
  [key: string]: unknown;
};

// ---- Tool definitions ----
export type FunctionTool = {
  type: "function";
  name: string;
  description?: string;
  parameters?: unknown;
  [key: string]: unknown;
};

export type CustomTool = {
  type: "custom";
  name: string;
  description?: string;
  parameters?: unknown;
  [key: string]: unknown;
};

export type ImageGenerationQuality = "low" | "medium" | "high" | "auto";
export type ImageGenerationSize = "1024x1024" | "1024x1536" | "1536x1024" | "auto" | string;
export type ImageGenerationFormat = "png" | "jpeg" | "webp" | string;
export type ImageGenerationBackground = "transparent" | "opaque" | "auto" | string;

export type ImageGenerationTool = {
  type: "image_generation";
  model?: string; // e.g. "gpt-image-1" | "gpt-image-1.5" | "gpt-image-2"
  action?: "generate" | "edit" | string;
  quality?: ImageGenerationQuality;
  size?: ImageGenerationSize;
  output_format?: ImageGenerationFormat;
  output_compression?: number;
  background?: ImageGenerationBackground;
  moderation?: string;
  partial_images?: number;
  input_fidelity?: "low" | "high" | string;
  input_image_mask?: { file_id?: string; image_url?: string; [key: string]: unknown };
  [key: string]: unknown;
};

export type ToolDefinition =
  | FunctionTool
  | CustomTool
  | ImageGenerationTool
  | { type: string; [key: string]: unknown };

// ---- Output items ----
export type ImageGenerationCall = {
  type: "image_generation_call";
  id: string;
  status?: string; // "completed" | "in_progress" | "generating" | etc.
  result?: string; // base64 b64_json
  revised_prompt?: string;
  [key: string]: unknown;
};

export type ToolDefinitionOld = {
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
  | ImageGenerationCall
  | { type: "output_text"; text: string; [key: string]: unknown }
  | { type: "output_audio"; data?: string; transcript?: string; [key: string]: unknown }
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
export const VALID_IMAGE_DETAILS: readonly ImageDetail[] = ["low", "high", "auto", "original"] as const;
export const VALID_IMAGE_GENERATION_QUALITIES: readonly ImageGenerationQuality[] = ["low", "medium", "high", "auto"] as const;
export const VALID_IMAGE_GENERATION_FORMATS: readonly string[] = ["png", "jpeg", "webp"] as const;
