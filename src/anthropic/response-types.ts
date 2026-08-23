export type ResponsesFinalItemStatus = "completed" | "incomplete";

export type ResponsesOutputText = {
  type: "output_text";
  text: string;
  annotations: readonly unknown[];
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
  status: ResponsesFinalItemStatus;
  encrypted_content: string;
  summary: ResponsesReasoningSummary[];
  [key: string]: unknown;
};

export type ResponsesFinalFunctionCallItem = {
  type: "function_call";
  id: string;
  status: ResponsesFinalItemStatus;
  call_id: string;
  name: string;
  arguments: string;
  [key: string]: unknown;
};

export type ResponsesFinalOutputItem =
  | ResponsesFinalMessageItem
  | ResponsesFinalReasoningItem
  | ResponsesFinalFunctionCallItem;

export type ResponsesIncompleteReason = "max_output_tokens" | "content_filter";

type ResponsesFinalBase = {
  id: string;
  output: ResponsesFinalOutputItem[];
  usage?: unknown;
  service_tier?: unknown;
  [key: string]: unknown;
};

export type ResponsesFinalResponse = ResponsesFinalBase & (
  | {
    status: "completed";
    incomplete_details?: null;
  }
  | {
    status: "incomplete";
    incomplete_details: { reason: ResponsesIncompleteReason };
  }
);
