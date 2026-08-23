import type { ResponsesReasoningItem } from "./reasoning";

export type ResponsesInputText = {
  type: "input_text";
  text: string;
};

export type ResponsesInputImage =
  | { type: "input_image"; image_url: string; file_id?: never }
  | { type: "input_image"; file_id: string; image_url?: never };

export type ResponsesInputFile =
  | { type: "input_file"; file_data: string; filename: string; file_id?: never; file_url?: never }
  | { type: "input_file"; file_id: string; file_data?: never; filename?: never; file_url?: never }
  | { type: "input_file"; file_url: string; file_data?: never; filename?: never; file_id?: never };

export type ResponsesUserContent = ResponsesInputText | ResponsesInputImage | ResponsesInputFile;

export type ResponsesMessageInput =
  | {
    type: "message";
    role: "user";
    content: ResponsesUserContent[];
  }
  | {
    type: "message";
    role: "assistant";
    content: ResponsesInputText[];
  };

export type ResponsesFunctionCallInput = {
  type: "function_call";
  call_id: string;
  name: string;
  arguments: string;
};

export type ResponsesFunctionCallOutput = {
  type: "function_call_output";
  call_id: string;
  output: string | ResponsesUserContent[];
};

export type ResponsesInputItem =
  | ResponsesMessageInput
  | ResponsesReasoningItem
  | ResponsesFunctionCallInput
  | ResponsesFunctionCallOutput;

export type ResponsesFunctionTool = {
  type: "function";
  name: string;
  parameters: Record<string, unknown>;
  description?: string;
  strict?: boolean;
};

export type ResponsesToolChoice = "auto" | "required" | "none" | {
  type: "function";
  name: string;
};

export type ResponsesReasoningEffort = "low" | "medium" | "high" | "xhigh" | "max";

export type ResponsesReasoning = {
  effort: ResponsesReasoningEffort | "none";
  summary?: "auto";
};

export type ResponsesTextConfig = {
  format: {
    type: "json_schema";
    name: "anthropic_output";
    schema: Record<string, unknown>;
    strict: true;
  };
};

export type ResponsesRequestBody = Record<string, unknown> & {
  model: string;
  store: false;
  stream: boolean;
  input: ResponsesInputItem[];
  include: ["reasoning.encrypted_content"];
  max_output_tokens: number;
  instructions?: string;
  tools?: ResponsesFunctionTool[];
  tool_choice?: ResponsesToolChoice;
  parallel_tool_calls?: false;
  reasoning?: ResponsesReasoning;
  text?: ResponsesTextConfig;
  service_tier?: "auto" | "default" | "fast";
  temperature?: number;
  top_p?: number;
  safety_identifier?: string;
  metadata?: { anthropic_user_id_sha256: string };
};
