import { AnthropicRequestError } from "./errors";
import { resolveAnthropicModel, type AnthropicModelOptions } from "./model";
import { decodeReasoningItem } from "./reasoning";
import type { AnthropicMessageRequest } from "./types";

type ResponsesInputItem = Record<string, unknown>;
type ResponsesContentPart = Record<string, unknown>;
type ReasoningEffort = "low" | "medium" | "high" | "xhigh" | "max";

export type AnthropicThinkingDisplay = "summarized" | "omitted";

export type TranslateAnthropicRequestOptions = {
  models: AnthropicModelOptions;
};

export type TranslatedAnthropicRequest = {
  requestedModel: string;
  relayBody: Record<string, unknown> & { input: ResponsesInputItem[] };
  stream: boolean;
  thinkingDisplay: AnthropicThinkingDisplay;
  stopSequences: string[];
  estimatedInputTokens: number;
};

function requireRecord(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new AnthropicRequestError(400, `${name} must be an object`);
  }
  return value as Record<string, unknown>;
}

function requireRequest(value: unknown): AnthropicMessageRequest {
  const body = requireRecord(value, "request");
  if (typeof body.model !== "string" || !body.model) {
    throw new AnthropicRequestError(400, "model is required");
  }
  if (!Number.isInteger(body.max_tokens) || Number(body.max_tokens) < 1) {
    throw new AnthropicRequestError(400, "max_tokens must be a positive integer");
  }
  if (!Array.isArray(body.messages)) {
    throw new AnthropicRequestError(400, "messages is required");
  }
  return body as AnthropicMessageRequest;
}

function requireString(
  record: Record<string, unknown>,
  key: string,
  name: string,
  allowEmpty = false,
): string {
  const value = record[key];
  if (typeof value !== "string" || (!allowEmpty && !value)) {
    throw new AnthropicRequestError(400, `${name} is required`);
  }
  return value;
}

function contentBlocks(value: unknown, name: string): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) {
    throw new AnthropicRequestError(400, `${name} must be a string or an array`);
  }
  return value.map((block, index) => requireRecord(block, `${name}[${index}]`));
}

function blockType(block: Record<string, unknown>, name: string): string {
  return requireString(block, "type", `${name}.type`);
}

function translateImage(
  block: Record<string, unknown>,
  name: string,
): ResponsesContentPart {
  const source = requireRecord(block.source, `${name} image source`);
  const type = requireString(source, "type", `${name} image source type`);

  if (type === "base64") {
    const mediaType = requireString(source, "media_type", `${name} image media_type`);
    const data = requireString(source, "data", `${name} image data`, true);
    return { type: "input_image", image_url: `data:${mediaType};base64,${data}` };
  }
  if (type === "url") {
    return {
      type: "input_image",
      image_url: requireString(source, "url", `${name} image URL`),
    };
  }
  if (type === "file") {
    return {
      type: "input_image",
      file_id: requireString(source, "file_id", `${name} image file_id`),
    };
  }

  throw new AnthropicRequestError(400, `unsupported image source type: ${type}`);
}

function documentFilename(block: Record<string, unknown>, mediaType: string): string {
  const title = typeof block.title === "string" && block.title.trim()
    ? block.title.trim()
    : "document";
  if (mediaType === "application/pdf" && !title.toLowerCase().endsWith(".pdf")) {
    return `${title}.pdf`;
  }
  return title;
}

function translateComposedDocument(
  content: unknown,
  name: string,
): ResponsesContentPart[] {
  if (typeof content === "string") {
    return [{ type: "input_text", text: content }];
  }
  if (!Array.isArray(content)) {
    throw new AnthropicRequestError(400, `${name} document content must be a string or an array`);
  }

  const translated: ResponsesContentPart[] = [];
  for (let index = 0; index < content.length; index += 1) {
    const nestedName = `${name} document content[${index}]`;
    const block = requireRecord(content[index], nestedName);
    const type = blockType(block, nestedName);
    if (type === "text") {
      translated.push({
        type: "input_text",
        text: requireString(block, "text", `${nestedName} text`, true),
      });
    } else if (type === "image") {
      translated.push(translateImage(block, nestedName));
    } else {
      throw new AnthropicRequestError(
        400,
        `unsupported document content block type: ${type}`,
      );
    }
  }
  return translated;
}

function translateDocument(
  block: Record<string, unknown>,
  name: string,
): ResponsesContentPart[] {
  const source = requireRecord(block.source, `${name} document source`);
  const type = requireString(source, "type", `${name} document source type`);

  if (type === "base64") {
    const mediaType = requireString(source, "media_type", `${name} document media_type`);
    const data = requireString(source, "data", `${name} document data`, true);
    return [{
      type: "input_file",
      file_data: `data:${mediaType};base64,${data}`,
      filename: documentFilename(block, mediaType),
    }];
  }
  if (type === "url") {
    return [{
      type: "input_file",
      file_url: requireString(source, "url", `${name} document URL`),
    }];
  }
  if (type === "file") {
    return [{
      type: "input_file",
      file_id: requireString(source, "file_id", `${name} document file_id`),
    }];
  }
  if (type === "text") {
    requireString(source, "media_type", `${name} document media_type`);
    return [{
      type: "input_text",
      text: requireString(source, "data", `${name} document data`, true),
    }];
  }
  if (type === "content") {
    return translateComposedDocument(source.content, name);
  }

  throw new AnthropicRequestError(400, `unsupported document source type: ${type}`);
}

function translateOrdinaryUserBlock(
  block: Record<string, unknown>,
  name: string,
): ResponsesContentPart[] {
  const type = blockType(block, name);
  if (type === "text") {
    return [{
      type: "input_text",
      text: requireString(block, "text", `${name} text`, true),
    }];
  }
  if (type === "image") return [translateImage(block, name)];
  if (type === "document") return translateDocument(block, name);
  throw new AnthropicRequestError(400, `unsupported user content block type: ${type}`);
}

function translateToolResultContent(
  content: unknown,
  name: string,
): string | ResponsesContentPart[] {
  if (content === undefined) return "";
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) {
    throw new AnthropicRequestError(400, `${name} content must be a string or an array`);
  }

  const translated: ResponsesContentPart[] = [];
  for (let index = 0; index < content.length; index += 1) {
    const nestedName = `${name} content[${index}]`;
    const block = requireRecord(content[index], nestedName);
    const type = blockType(block, nestedName);
    if (type === "text") {
      translated.push({
        type: "input_text",
        text: requireString(block, "text", `${nestedName} text`, true),
      });
    } else if (type === "image") {
      translated.push(translateImage(block, nestedName));
    } else if (type === "document") {
      translated.push(...translateDocument(block, nestedName));
    } else {
      throw new AnthropicRequestError(
        400,
        `unsupported tool_result content block type: ${type}`,
      );
    }
  }
  return translated;
}

function translateToolResult(
  block: Record<string, unknown>,
  name: string,
): ResponsesInputItem {
  const callId = requireString(block, "tool_use_id", `${name} tool_result tool_use_id`);
  let output = translateToolResultContent(block.content, name);
  if (block.is_error === true) {
    const retained = typeof output === "string"
      ? [{ type: "input_text", text: output }]
      : output;
    output = [
      { type: "input_text", text: "Tool execution failed:\n" },
      ...retained,
    ];
  }
  return { type: "function_call_output", call_id: callId, output };
}

function appendUserMessage(
  input: ResponsesInputItem[],
  content: unknown,
  messageName: string,
): void {
  const pending: ResponsesContentPart[] = [];
  const flushUserContent = (): void => {
    if (!pending.length) return;
    input.push({ type: "message", role: "user", content: pending.splice(0) });
  };

  if (typeof content === "string") {
    pending.push({ type: "input_text", text: content });
    flushUserContent();
    return;
  }

  const blocks = contentBlocks(content, `${messageName}.content`);
  let ordinaryContentSeen = false;
  for (let index = 0; index < blocks.length; index += 1) {
    const block = blocks[index];
    const name = `${messageName}.content[${index}]`;
    const type = blockType(block, name);
    if (type === "tool_result") {
      if (ordinaryContentSeen) {
        throw new AnthropicRequestError(
          400,
          "tool_result blocks must precede ordinary user content",
        );
      }
      flushUserContent();
      input.push(translateToolResult(block, name));
      continue;
    }

    ordinaryContentSeen = true;
    pending.push(...translateOrdinaryUserBlock(block, name));
  }
  flushUserContent();
}

function translateToolUse(
  block: Record<string, unknown>,
  name: string,
): ResponsesInputItem {
  const callId = requireString(block, "id", `${name} id`);
  const toolName = requireString(block, "name", `${name} name`);
  if (!Object.prototype.hasOwnProperty.call(block, "input") || block.input === undefined) {
    throw new AnthropicRequestError(400, `${name} tool_use input is required`);
  }

  let args: string | undefined;
  try {
    args = JSON.stringify(block.input);
  } catch {
    throw new AnthropicRequestError(400, `${name} tool_use input must be JSON serializable`);
  }
  if (args === undefined) {
    throw new AnthropicRequestError(400, `${name} tool_use input must be JSON serializable`);
  }
  return {
    type: "function_call",
    call_id: callId,
    name: toolName,
    arguments: args,
  };
}

function appendAssistantMessage(
  input: ResponsesInputItem[],
  content: unknown,
  messageName: string,
): void {
  const pending: ResponsesContentPart[] = [];
  const flushAssistantText = (): void => {
    if (!pending.length) return;
    input.push({ type: "message", role: "assistant", content: pending.splice(0) });
  };

  if (typeof content === "string") {
    pending.push({ type: "output_text", text: content });
    flushAssistantText();
    return;
  }

  const blocks = contentBlocks(content, `${messageName}.content`);
  for (let index = 0; index < blocks.length; index += 1) {
    const block = blocks[index];
    const name = `${messageName}.content[${index}]`;
    const type = blockType(block, name);
    if (type === "text") {
      pending.push({
        type: "output_text",
        text: requireString(block, "text", `${name} text`, true),
      });
      continue;
    }
    if (type === "thinking") {
      flushAssistantText();
      requireString(block, "thinking", `${name} thinking`, true);
      const signature = requireString(block, "signature", `${name} signature`, true);
      const restored = decodeReasoningItem(signature);
      if (restored) input.push(restored as unknown as ResponsesInputItem);
      continue;
    }
    if (type === "redacted_thinking") {
      flushAssistantText();
      requireString(block, "data", `${name} data`, true);
      continue;
    }
    if (type === "tool_use") {
      flushAssistantText();
      input.push(translateToolUse(block, name));
      continue;
    }
    throw new AnthropicRequestError(400, `unsupported assistant content block type: ${type}`);
  }
  flushAssistantText();
}

function translateMessages(messages: unknown[]): ResponsesInputItem[] {
  const input: ResponsesInputItem[] = [];
  for (let index = 0; index < messages.length; index += 1) {
    const name = `messages[${index}]`;
    const message = requireRecord(messages[index], name);
    const role = message.role;
    if (role !== "user" && role !== "assistant") {
      throw new AnthropicRequestError(400, `${name}.role must be user or assistant`);
    }
    if (typeof message.content !== "string" && !Array.isArray(message.content)) {
      throw new AnthropicRequestError(400, `${name}.content must be a string or an array`);
    }
    if (role === "user") appendUserMessage(input, message.content, name);
    else appendAssistantMessage(input, message.content, name);
  }
  return input;
}

function translateSystem(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) {
    throw new AnthropicRequestError(400, "system must be a string or an array");
  }

  return value.map((item, index) => {
    const name = `system[${index}]`;
    const block = requireRecord(item, name);
    const type = blockType(block, name);
    if (type !== "text") {
      throw new AnthropicRequestError(400, `unsupported system content block type: ${type}`);
    }
    return requireString(block, "text", `${name} text`, true);
  }).join("\n\n");
}

function translateTools(value: unknown): ResponsesInputItem[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    throw new AnthropicRequestError(400, "tools must be an array");
  }

  return value.map((item, index) => {
    const name = `tools[${index}]`;
    const tool = requireRecord(item, name);
    if (tool.type !== undefined && tool.type !== null && tool.type !== "custom") {
      throw new AnthropicRequestError(400, `unsupported tool type: ${String(tool.type)}`);
    }
    const toolName = requireString(tool, "name", `${name} tool name`);
    if (!Object.prototype.hasOwnProperty.call(tool, "input_schema") || tool.input_schema === undefined) {
      throw new AnthropicRequestError(400, `${name} tool input_schema is required`);
    }
    const inputSchema = requireRecord(tool.input_schema, `${name} tool input_schema`);
    const translated: ResponsesInputItem = {
      type: "function",
      name: toolName,
      parameters: inputSchema,
    };
    if (typeof tool.description === "string") translated.description = tool.description;
    if (typeof tool.strict === "boolean") translated.strict = tool.strict;
    return translated;
  });
}

function translateToolChoice(value: unknown): {
  toolChoice?: unknown;
  parallelToolCalls?: false;
} {
  if (value === undefined || value === null) return {};
  const choice = requireRecord(value, "tool_choice");
  const type = requireString(choice, "type", "tool_choice type");
  let toolChoice: unknown;
  if (type === "auto") toolChoice = "auto";
  else if (type === "any") toolChoice = "required";
  else if (type === "none") toolChoice = "none";
  else if (type === "tool") {
    toolChoice = {
      type: "function",
      name: requireString(choice, "name", "tool_choice tool name"),
    };
  } else {
    throw new AnthropicRequestError(400, `unsupported tool_choice type: ${type}`);
  }

  return {
    toolChoice,
    ...(choice.disable_parallel_tool_use === true ? { parallelToolCalls: false as const } : {}),
  };
}

function translateOutputConfig(value: unknown): {
  effort?: ReasoningEffort;
  text?: Record<string, unknown>;
} {
  if (value === undefined || value === null) return {};
  const config = requireRecord(value, "output_config");
  const allowedEfforts = new Set<unknown>(["low", "medium", "high", "xhigh", "max"]);
  const effort = allowedEfforts.has(config.effort)
    ? config.effort as ReasoningEffort
    : undefined;

  let text: Record<string, unknown> | undefined;
  if (config.format !== undefined && config.format !== null) {
    const format = requireRecord(config.format, "output_config.format");
    if (format.type === "json_schema") {
      const schema = requireRecord(format.schema, "output_config.format schema");
      text = {
        format: {
          type: "json_schema",
          name: "anthropic_output",
          schema,
          strict: true,
        },
      };
    }
  }
  return { effort, text };
}

function effortFromBudget(value: unknown): ReasoningEffort {
  if (!Number.isInteger(value) || Number(value) < 1) {
    throw new AnthropicRequestError(400, "thinking budget_tokens must be a positive integer");
  }
  const budget = Number(value);
  if (budget < 4096) return "low";
  if (budget < 16000) return "medium";
  if (budget < 32000) return "high";
  return "max";
}

function translateThinking(
  value: unknown,
  explicitEffort: ReasoningEffort | undefined,
): {
  reasoning?: Record<string, unknown>;
  display: AnthropicThinkingDisplay;
} {
  if (value === undefined || value === null) {
    return {
      ...(explicitEffort
        ? { reasoning: { effort: explicitEffort, summary: "auto" } }
        : {}),
      display: "summarized",
    };
  }

  const thinking = requireRecord(value, "thinking");
  const display: AnthropicThinkingDisplay = thinking.display === "omitted"
    ? "omitted"
    : "summarized";
  if (thinking.type === "disabled") {
    return { reasoning: { effort: "none" }, display };
  }
  if (thinking.type === "adaptive") {
    return {
      reasoning: { effort: explicitEffort ?? "high", summary: "auto" },
      display,
    };
  }
  if (thinking.type === "enabled") {
    return {
      reasoning: {
        effort: explicitEffort ?? effortFromBudget(thinking.budget_tokens),
        summary: "auto",
      },
      display,
    };
  }

  return {
    ...(explicitEffort
      ? { reasoning: { effort: explicitEffort, summary: "auto" } }
      : {}),
    display,
  };
}

function stopSequences(value: unknown): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || value.some(item => typeof item !== "string")) {
    throw new AnthropicRequestError(400, "stop_sequences must contain only strings");
  }
  return [...value] as string[];
}

function estimateInputTokens(value: unknown): number {
  try {
    const serialized = JSON.stringify(value);
    return Math.max(1, Math.ceil(Buffer.byteLength(serialized ?? "", "utf8") / 4));
  } catch {
    return 1;
  }
}

export function translateAnthropicRequest(
  value: unknown,
  options: TranslateAnthropicRequestOptions,
): TranslatedAnthropicRequest {
  const body = requireRequest(value);
  const input = translateMessages(body.messages);
  const instructions = translateSystem(body.system);
  const tools = translateTools(body.tools);
  const { toolChoice, parallelToolCalls } = translateToolChoice(body.tool_choice);
  const output = translateOutputConfig(body.output_config);
  const thinking = translateThinking(body.thinking, output.effort);
  const stream = body.stream === true;
  const requestedModel = body.model;
  const requestedStopSequences = stopSequences(body.stop_sequences);
  const serviceTier = body.speed === "fast" ? "fast" : undefined;

  const relayBody: Record<string, unknown> & { input: ResponsesInputItem[] } = {
    model: resolveAnthropicModel(body.model, options.models),
    store: false,
    stream,
    input,
    include: ["reasoning.encrypted_content"],
    max_output_tokens: body.max_tokens,
    ...(instructions ? { instructions } : {}),
    ...(tools.length ? { tools } : {}),
    ...(toolChoice !== undefined ? { tool_choice: toolChoice } : {}),
    ...(parallelToolCalls !== undefined ? { parallel_tool_calls: parallelToolCalls } : {}),
    ...(thinking.reasoning ? { reasoning: thinking.reasoning } : {}),
    ...(output.text ? { text: output.text } : {}),
    ...(serviceTier ? { service_tier: serviceTier } : {}),
    ...(typeof body.temperature === "number" ? { temperature: body.temperature } : {}),
    ...(typeof body.top_p === "number" ? { top_p: body.top_p } : {}),
  };

  return {
    requestedModel,
    relayBody,
    stream,
    thinkingDisplay: thinking.display,
    stopSequences: requestedStopSequences,
    estimatedInputTokens: estimateInputTokens({ instructions, input, tools }),
  };
}
