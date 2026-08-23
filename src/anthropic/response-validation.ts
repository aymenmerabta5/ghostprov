import { AnthropicRequestError } from "./errors";
import type {
  ResponsesFinalFunctionCallItem,
  ResponsesFinalMessageItem,
  ResponsesFinalOutputItem,
  ResponsesFinalReasoningItem,
  ResponsesIncompleteReason,
} from "./response-types";

export type ValidatedTerminalResponseProjection = {
  record: Record<string, unknown>;
  output: unknown[];
  status: "completed" | "incomplete";
  incompleteReason: ResponsesIncompleteReason | null;
};

export function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

export function invalidUpstream(message: string): AnthropicRequestError {
  return new AnthropicRequestError(500, message, "api_error");
}

export function malformedFinal(): never {
  throw invalidUpstream("Responses relay returned a malformed final response.");
}

export function validateTerminalResponseProjection(
  value: unknown,
): ValidatedTerminalResponseProjection {
  const final = record(value);
  if (!final) malformedFinal();
  if (typeof final.id !== "string" || final.id.length === 0) malformedFinal();
  if (final.object !== "response" || final.error !== null) malformedFinal();
  if (typeof final.created_at !== "number" || !Number.isFinite(final.created_at) ||
    final.created_at < 0 || typeof final.model !== "string" || !final.model.trim()) {
    malformedFinal();
  }
  if (final.status !== "completed" && final.status !== "incomplete") {
    throw invalidUpstream("Responses relay did not return a successful terminal response.");
  }
  if (!Array.isArray(final.output)) malformedFinal();
  if (!Object.prototype.hasOwnProperty.call(final, "incomplete_details") ||
    !record(final.usage)) {
    malformedFinal();
  }

  let incompleteReason: ResponsesIncompleteReason | null = null;
  if (final.status === "incomplete") {
    if (final.incomplete_details !== null) {
      const details = record(final.incomplete_details);
      if (!details) malformedFinal();
      if (details.reason !== undefined) {
        if (details.reason !== "max_output_tokens" && details.reason !== "content_filter") {
          malformedFinal();
        }
        incompleteReason = details.reason;
      }
    }
  } else if (final.incomplete_details !== null) {
    malformedFinal();
  }

  return {
    record: final,
    output: final.output,
    status: final.status,
    incompleteReason,
  };
}

function validTerminalItemStatus(
  status: unknown,
  terminalStatus: ValidatedTerminalResponseProjection["status"],
  required: boolean,
): boolean {
  if (status === undefined) return !required;
  if (status !== "completed" && status !== "incomplete") return false;
  return status !== "incomplete" || terminalStatus === "incomplete";
}

function validByteArray(value: unknown): value is number[] {
  return Array.isArray(value) && value.every(
    byte => Number.isSafeInteger(byte) && byte >= 0 && byte <= 255,
  );
}

function validLogprob(value: unknown, includeTop: boolean): boolean {
  const entry = record(value);
  if (!entry || typeof entry.token !== "string" || !validByteArray(entry.bytes) ||
    typeof entry.logprob !== "number" || !Number.isFinite(entry.logprob)) {
    return false;
  }
  if (!includeTop) return true;
  return Array.isArray(entry.top_logprobs) &&
    entry.top_logprobs.every(candidate => validLogprob(candidate, false));
}

function nonnegativeIndex(value: unknown): boolean {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function validAnnotation(value: unknown): boolean {
  const annotation = record(value);
  if (!annotation || typeof annotation.type !== "string" || !annotation.type) return false;
  if (annotation.type === "file_citation") {
    return typeof annotation.file_id === "string" && typeof annotation.filename === "string" &&
      nonnegativeIndex(annotation.index);
  }
  if (annotation.type === "url_citation") {
    return typeof annotation.title === "string" && typeof annotation.url === "string" &&
      nonnegativeIndex(annotation.start_index) && nonnegativeIndex(annotation.end_index) &&
      Number(annotation.end_index) >= Number(annotation.start_index);
  }
  if (annotation.type === "container_file_citation") {
    return typeof annotation.container_id === "string" && typeof annotation.file_id === "string" &&
      typeof annotation.filename === "string" && nonnegativeIndex(annotation.start_index) &&
      nonnegativeIndex(annotation.end_index) &&
      Number(annotation.end_index) >= Number(annotation.start_index);
  }
  if (annotation.type === "file_path") {
    return typeof annotation.file_id === "string" && nonnegativeIndex(annotation.index);
  }
  return true;
}

function validOutputTextKnownFields(part: Record<string, unknown>): boolean {
  if (typeof part.text !== "string" || !Array.isArray(part.annotations) ||
    !part.annotations.every(validAnnotation)) {
    return false;
  }
  return Array.isArray(part.logprobs) &&
    part.logprobs.every(value => validLogprob(value, true));
}

export function validateTerminalOutputItemProjection(
  value: unknown,
  terminalStatus: ValidatedTerminalResponseProjection["status"],
): ResponsesFinalOutputItem {
  const item = record(value);
  if (!item) malformedFinal();

  if (item.type === "message") {
    if (typeof item.id !== "string" || !item.id ||
      !validTerminalItemStatus(item.status, terminalStatus, true) ||
      item.role !== "assistant" || !Array.isArray(item.content)) {
      malformedFinal();
    }
    if (item.phase !== undefined && item.phase !== null &&
      item.phase !== "commentary" && item.phase !== "final_answer") {
      malformedFinal();
    }
    for (const partValue of item.content) {
      const part = record(partValue);
      if (!part) malformedFinal();
      if (part.type === "output_text") {
        if (!validOutputTextKnownFields(part)) malformedFinal();
      } else if (part.type === "refusal") {
        if (typeof part.refusal !== "string") malformedFinal();
      } else {
        malformedFinal();
      }
    }
    return item as ResponsesFinalMessageItem;
  }

  if (item.type === "reasoning") {
    if (typeof item.id !== "string" || !item.id ||
      !validTerminalItemStatus(item.status, terminalStatus, false) ||
      !Array.isArray(item.summary)) {
      malformedFinal();
    }
    if (item.encrypted_content !== undefined && item.encrypted_content !== null &&
      (typeof item.encrypted_content !== "string" || !item.encrypted_content)) {
      malformedFinal();
    }
    for (const summaryValue of item.summary) {
      const summary = record(summaryValue);
      if (!summary || summary.type !== "summary_text" || typeof summary.text !== "string") {
        malformedFinal();
      }
    }
    if (item.content !== undefined) {
      if (!Array.isArray(item.content) || item.content.some(value => {
        const content = record(value);
        return !content || content.type !== "reasoning_text" || typeof content.text !== "string";
      })) {
        malformedFinal();
      }
    }
    return item as ResponsesFinalReasoningItem;
  }

  if (item.type === "function_call") {
    if (!validTerminalItemStatus(item.status, terminalStatus, true) ||
      (item.id !== undefined && (typeof item.id !== "string" || !item.id)) ||
      typeof item.call_id !== "string" || !item.call_id ||
      typeof item.name !== "string" || !item.name || typeof item.arguments !== "string") {
      malformedFinal();
    }
    if (item.namespace !== undefined && typeof item.namespace !== "string") malformedFinal();
    if (item.caller !== undefined && item.caller !== null) {
      const caller = record(item.caller);
      if (!caller || (caller.type !== "direct" &&
        (caller.type !== "program" || typeof caller.caller_id !== "string" || !caller.caller_id))) {
        malformedFinal();
      }
    }
    return item as ResponsesFinalFunctionCallItem;
  }

  malformedFinal();
}

export function finalContainsRefusal(
  output: readonly unknown[],
  terminalStatus: ValidatedTerminalResponseProjection["status"],
): boolean {
  for (const itemValue of output) {
    const item = validateTerminalOutputItemProjection(itemValue, terminalStatus);
    if (item.type === "message" && item.content.some(part => part.type === "refusal")) {
      return true;
    }
  }
  return false;
}
