import { AnthropicRequestError } from "./errors";
import { decodeReasoningItem } from "./reasoning";
import type {
  ResponsesFunctionCallInput,
  ResponsesFunctionCallOutput,
  ResponsesInputItem,
  ResponsesInputText,
  ResponsesUserContent,
} from "./request-types";
import {
  type AnthropicRequestLimits,
  hasOwn,
  RequestMediaBudget,
  requireArray,
  requireHttpUrl,
  requireOptionalBoolean,
  requireRecord,
  requireString,
  serializeBoundedJson,
  validateBase64,
} from "./request-validation";

const IMAGE_MEDIA_TYPES = new Set([
  "image/jpeg",
  "image/png",
  "image/gif",
  "image/webp",
]);

function contentBlocks(
  value: unknown,
  name: string,
  limits: AnthropicRequestLimits,
): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) {
    throw new AnthropicRequestError(400, `${name} must be a string or an array`);
  }
  return requireArray(value, name, limits)
    .map((block, index) => requireRecord(block, `${name}[${index}]`));
}

function blockType(block: Record<string, unknown>, name: string): string {
  return requireString(block, "type", `${name}.type`);
}

function translateImage(
  block: Record<string, unknown>,
  name: string,
  budget: RequestMediaBudget,
): ResponsesUserContent {
  const source = requireRecord(block.source, `${name} image source`);
  const type = requireString(source, "type", `${name} image source type`);

  if (type === "base64") {
    const mediaType = requireString(source, "media_type", `${name} image media_type`);
    if (!IMAGE_MEDIA_TYPES.has(mediaType)) {
      throw new AnthropicRequestError(400, `${name} image media_type is unsupported`);
    }
    const data = requireString(source, "data", `${name} image data`);
    validateBase64(data, `${name} image data`, budget);
    return { type: "input_image", image_url: `data:${mediaType};base64,${data}` };
  }
  if (type === "url") {
    const url = requireString(source, "url", `${name} image URL`);
    return { type: "input_image", image_url: requireHttpUrl(url, `${name} image URL`) };
  }
  if (type === "file") {
    return {
      type: "input_image",
      file_id: requireString(source, "file_id", `${name} image file_id`),
    };
  }

  throw new AnthropicRequestError(400, `unsupported image source type: ${type}`);
}

function documentFilename(block: Record<string, unknown>): string {
  const title = typeof block.title === "string" && block.title.trim()
    ? block.title.trim()
    : "document";
  return title.toLowerCase().endsWith(".pdf") ? title : `${title}.pdf`;
}

function translateComposedDocument(
  content: unknown,
  name: string,
  budget: RequestMediaBudget,
  limits: AnthropicRequestLimits,
): ResponsesUserContent[] {
  if (typeof content === "string") return [{ type: "input_text", text: content }];
  const blocks = requireArray(content, `${name} document content`, limits);
  const translated: ResponsesUserContent[] = [];
  for (let index = 0; index < blocks.length; index += 1) {
    const nestedName = `${name} document content[${index}]`;
    const block = requireRecord(blocks[index], nestedName);
    const type = blockType(block, nestedName);
    if (type === "text") {
      translated.push({
        type: "input_text",
        text: requireString(block, "text", `${nestedName} text`, true),
      });
    } else if (type === "image") {
      translated.push(translateImage(block, nestedName, budget));
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
  budget: RequestMediaBudget,
  limits: AnthropicRequestLimits,
): ResponsesUserContent[] {
  const source = requireRecord(block.source, `${name} document source`);
  const type = requireString(source, "type", `${name} document source type`);

  if (type === "base64") {
    const mediaType = requireString(source, "media_type", `${name} document media_type`);
    if (mediaType !== "application/pdf") {
      throw new AnthropicRequestError(400, `${name} base64 document must use application/pdf`);
    }
    const data = requireString(source, "data", `${name} document data`);
    validateBase64(data, `${name} document data`, budget);
    return [{
      type: "input_file",
      file_data: `data:application/pdf;base64,${data}`,
      filename: documentFilename(block),
    }];
  }
  if (type === "url") {
    const url = requireString(source, "url", `${name} document URL`);
    return [{
      type: "input_file",
      file_url: requireHttpUrl(url, `${name} document URL`),
    }];
  }
  if (type === "file") {
    return [{
      type: "input_file",
      file_id: requireString(source, "file_id", `${name} document file_id`),
    }];
  }
  if (type === "text") {
    const mediaType = requireString(source, "media_type", `${name} document media_type`);
    if (mediaType !== "text/plain") {
      throw new AnthropicRequestError(400, `${name} text document must use text/plain`);
    }
    return [{
      type: "input_text",
      text: requireString(source, "data", `${name} document data`, true),
    }];
  }
  if (type === "content") return translateComposedDocument(source.content, name, budget, limits);
  throw new AnthropicRequestError(400, `unsupported document source type: ${type}`);
}

function translateSearchResult(
  block: Record<string, unknown>,
  name: string,
  limits: AnthropicRequestLimits,
): ResponsesInputText {
  const title = requireString(block, "title", `${name} search_result title`);
  const source = requireString(block, "source", `${name} search_result source`);
  const content = requireArray(block.content, `${name} search_result content`, limits);
  const excerpts: string[] = [];
  for (let index = 0; index < content.length; index += 1) {
    const nestedName = `${name} search_result content[${index}]`;
    const nested = requireRecord(content[index], nestedName);
    const type = blockType(nested, nestedName);
    if (type !== "text") {
      throw new AnthropicRequestError(400, `unsupported search_result content block type: ${type}`);
    }
    excerpts.push(requireString(nested, "text", `${nestedName} text`, true));
  }
  return {
    type: "input_text",
    text: ["Search result", `Title: ${title}`, `Source: ${source}`, "", ...excerpts].join("\n"),
  };
}

function translateOrdinaryUserBlock(
  block: Record<string, unknown>,
  name: string,
  budget: RequestMediaBudget,
  limits: AnthropicRequestLimits,
): ResponsesUserContent[] {
  const type = blockType(block, name);
  if (type === "text") {
    return [{
      type: "input_text",
      text: requireString(block, "text", `${name} text`, true),
    }];
  }
  if (type === "image") return [translateImage(block, name, budget)];
  if (type === "document") return translateDocument(block, name, budget, limits);
  if (type === "search_result") return [translateSearchResult(block, name, limits)];
  throw new AnthropicRequestError(400, `unsupported user content block type: ${type}`);
}

function translateToolResultContent(
  content: unknown,
  name: string,
  budget: RequestMediaBudget,
  limits: AnthropicRequestLimits,
): string | ResponsesUserContent[] {
  if (content === undefined) return "";
  if (typeof content === "string") return content;
  const blocks = requireArray(content, `${name} content`, limits);
  const translated: ResponsesUserContent[] = [];
  for (let index = 0; index < blocks.length; index += 1) {
    const nestedName = `${name} content[${index}]`;
    const block = requireRecord(blocks[index], nestedName);
    const type = blockType(block, nestedName);
    if (type === "text") {
      translated.push({
        type: "input_text",
        text: requireString(block, "text", `${nestedName} text`, true),
      });
    } else if (type === "image") {
      translated.push(translateImage(block, nestedName, budget));
    } else if (type === "document") {
      translated.push(...translateDocument(block, nestedName, budget, limits));
    } else if (type === "search_result") {
      translated.push(translateSearchResult(block, nestedName, limits));
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
  callId: string,
  budget: RequestMediaBudget,
  limits: AnthropicRequestLimits,
): ResponsesFunctionCallOutput {
  const isError = requireOptionalBoolean(block, "is_error", `${name} is_error`);
  let output = translateToolResultContent(block.content, name, budget, limits);
  if (isError === true) {
    const retained: ResponsesUserContent[] = typeof output === "string"
      ? [{ type: "input_text", text: output }]
      : output;
    output = [
      { type: "input_text", text: "Tool execution failed:\n" },
      ...retained,
    ];
  }
  return { type: "function_call_output", call_id: callId, output };
}

type ToolHistory = {
  seen: Set<string>;
  completed: Set<string>;
  pending: Set<string>;
};

function appendUserMessage(
  input: ResponsesInputItem[],
  content: unknown,
  messageName: string,
  history: ToolHistory,
  budget: RequestMediaBudget,
  limits: AnthropicRequestLimits,
): void {
  const pendingContent: ResponsesUserContent[] = [];
  const flushUserContent = (): void => {
    if (!pendingContent.length) return;
    input.push({ type: "message", role: "user", content: pendingContent.splice(0) });
  };

  if (typeof content === "string") {
    if (history.pending.size) {
      throw new AnthropicRequestError(400, "missing tool_result blocks for pending tool_use IDs");
    }
    pendingContent.push({ type: "input_text", text: content });
    flushUserContent();
    return;
  }

  const blocks = contentBlocks(content, `${messageName}.content`, limits);
  const remaining = new Set(history.pending);
  const resultsInTurn = new Set<string>();
  let ordinaryContentSeen = false;

  for (let index = 0; index < blocks.length; index += 1) {
    const block = blocks[index];
    const name = `${messageName}.content[${index}]`;
    const type = blockType(block, name);
    if (type === "tool_result") {
      const callId = requireString(block, "tool_use_id", `${name} tool_result tool_use_id`);
      if (ordinaryContentSeen) {
        throw new AnthropicRequestError(400, "tool_result blocks must precede ordinary user content");
      }
      if (resultsInTurn.has(callId)) {
        throw new AnthropicRequestError(400, `duplicate tool_result for ${callId}`);
      }
      if (!history.pending.size) {
        const kind = history.completed.has(callId) ? "repeated" : "orphan";
        throw new AnthropicRequestError(400, `${kind} tool_result for ${callId}`);
      }
      if (!remaining.has(callId)) {
        const kind = history.completed.has(callId) ? "repeated" : "mismatched";
        throw new AnthropicRequestError(400, `${kind} tool_result for ${callId}`);
      }

      resultsInTurn.add(callId);
      remaining.delete(callId);
      flushUserContent();
      input.push(translateToolResult(block, name, callId, budget, limits));
      continue;
    }

    if (remaining.size) {
      throw new AnthropicRequestError(
        400,
        "tool_result blocks must precede ordinary user content and cover every pending tool_use",
      );
    }
    ordinaryContentSeen = true;
    pendingContent.push(...translateOrdinaryUserBlock(block, name, budget, limits));
  }

  if (remaining.size) {
    throw new AnthropicRequestError(400, "missing tool_result blocks for pending tool_use IDs");
  }
  if (history.pending.size) {
    for (const callId of history.pending) history.completed.add(callId);
    history.pending.clear();
  }
  flushUserContent();
}

function translateToolUse(
  block: Record<string, unknown>,
  name: string,
  history: ToolHistory,
  callsInTurn: Set<string>,
  limits: AnthropicRequestLimits,
): ResponsesFunctionCallInput {
  const callId = requireString(block, "id", `${name} id`);
  if (callsInTurn.has(callId)) {
    throw new AnthropicRequestError(400, `duplicate tool_use id: ${callId}`);
  }
  if (history.seen.has(callId)) {
    throw new AnthropicRequestError(400, `repeated tool_use id: ${callId}`);
  }
  const toolName = requireString(block, "name", `${name} name`);
  if (!hasOwn(block, "input") || block.input === undefined) {
    throw new AnthropicRequestError(400, `${name} tool_use input is required`);
  }
  const toolInput = requireRecord(block.input, `${name} tool_use input`);
  const args = serializeBoundedJson(
    toolInput,
    `${name} tool_use input`,
    limits.relayBytes,
    limits,
  );
  callsInTurn.add(callId);
  history.seen.add(callId);
  return { type: "function_call", call_id: callId, name: toolName, arguments: args };
}

function appendAssistantMessage(
  input: ResponsesInputItem[],
  content: unknown,
  messageName: string,
  history: ToolHistory,
  limits: AnthropicRequestLimits,
): string[] {
  const pendingText: ResponsesInputText[] = [];
  const callsInTurn = new Set<string>();
  const flushAssistantText = (): void => {
    if (!pendingText.length) return;
    input.push({ type: "message", role: "assistant", content: pendingText.splice(0) });
  };

  if (typeof content === "string") {
    pendingText.push({ type: "input_text", text: content });
    flushAssistantText();
    return [];
  }

  const blocks = contentBlocks(content, `${messageName}.content`, limits);
  for (let index = 0; index < blocks.length; index += 1) {
    const block = blocks[index];
    const name = `${messageName}.content[${index}]`;
    const type = blockType(block, name);
    if (type === "text") {
      pendingText.push({
        type: "input_text",
        text: requireString(block, "text", `${name} text`, true),
      });
    } else if (type === "thinking") {
      flushAssistantText();
      requireString(block, "thinking", `${name} thinking`, true);
      const signature = requireString(block, "signature", `${name} signature`, true);
      const restored = decodeReasoningItem(signature);
      if (restored) input.push(restored);
    } else if (type === "redacted_thinking") {
      flushAssistantText();
      requireString(block, "data", `${name} data`, true);
    } else if (type === "tool_use") {
      flushAssistantText();
      input.push(translateToolUse(block, name, history, callsInTurn, limits));
    } else {
      throw new AnthropicRequestError(400, `unsupported assistant content block type: ${type}`);
    }
  }
  flushAssistantText();
  return [...callsInTurn];
}

type LogicalMessage = {
  role: "user" | "assistant" | "system" | "developer";
  content: Record<string, unknown>[];
  name: string;
};

function coalesceMessages(
  messages: unknown[],
  limits: AnthropicRequestLimits,
  allowControlRoles: boolean,
): LogicalMessage[] {
  const logical: LogicalMessage[] = [];
  for (let index = 0; index < messages.length; index += 1) {
    const name = `messages[${index}]`;
    const message = requireRecord(messages[index], name);
    const role = message.role;
    const controlRole = role === "system" || role === "developer";
    if (role !== "user" && role !== "assistant" && !controlRole) {
      throw new AnthropicRequestError(
        400,
        `${name}.role must be user, assistant, system, or developer`,
      );
    }
    if (controlRole && !allowControlRoles) {
      throw new AnthropicRequestError(400, `${name}.role must be user or assistant`);
    }
    if (typeof message.content !== "string" && !Array.isArray(message.content)) {
      throw new AnthropicRequestError(400, `${name}.content must be a string or an array`);
    }
    const blocks = typeof message.content === "string"
      ? [{ type: "text", text: message.content }]
      : contentBlocks(message.content, `${name}.content`, limits);
    const previous = logical.at(-1);
    if (previous?.role === role) {
      previous.content.push(...blocks);
      if (previous.content.length > limits.maxArrayItems) {
        throw new AnthropicRequestError(
          400,
          `${previous.name}.content contains more than ${limits.maxArrayItems} items`,
        );
      }
    } else {
      logical.push({ role, content: [...blocks], name });
    }
  }
  return logical;
}

export function translateMessages(
  messages: unknown[],
  budget: RequestMediaBudget,
  limits: AnthropicRequestLimits,
  allowControlRoles = false,
): ResponsesInputItem[] {
  const input: ResponsesInputItem[] = [];
  const history: ToolHistory = {
    seen: new Set(),
    completed: new Set(),
    pending: new Set(),
  };

  for (const message of coalesceMessages(messages, limits, allowControlRoles)) {
    if (message.role === "user") {
      appendUserMessage(input, message.content, message.name, history, budget, limits);
    } else if (message.role === "assistant") {
      if (history.pending.size) {
        throw new AnthropicRequestError(400, "assistant turn intervened before pending tool_result blocks");
      }
      history.pending = new Set(
        appendAssistantMessage(input, message.content, message.name, history, limits),
      );
    } else {
      if (history.pending.size) {
        throw new AnthropicRequestError(
          400,
          `${message.role} turn intervened before pending tool_result blocks`,
        );
      }
      const content = message.content.map((block, index) => {
        const name = `${message.name}.content[${index}]`;
        const type = blockType(block, name);
        if (type !== "text") {
          throw new AnthropicRequestError(
            400,
            `unsupported ${message.role} content block type: ${type}`,
          );
        }
        return {
          type: "input_text" as const,
          text: requireString(block, "text", `${name} text`, true),
        };
      });
      input.push({ type: "message", role: message.role, content });
    }
  }

  if (history.pending.size) {
    throw new AnthropicRequestError(400, "missing tool_result blocks for final pending tool_use IDs");
  }
  return input;
}

export function translateSystem(
  value: unknown,
  limits: AnthropicRequestLimits,
): string | undefined {
  if (value === undefined) return undefined;
  if (value === null) throw new AnthropicRequestError(400, "system must not be null");
  if (typeof value === "string") return value;
  const blocks = requireArray(value, "system", limits);
  return blocks.map((item, index) => {
    const name = `system[${index}]`;
    const block = requireRecord(item, name);
    const type = blockType(block, name);
    if (type !== "text") {
      throw new AnthropicRequestError(400, `unsupported system content block type: ${type}`);
    }
    return requireString(block, "text", `${name} text`, true);
  }).join("\n\n");
}
