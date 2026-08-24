import { AnthropicRequestError } from "./errors";
import {
  hasOwn,
  requireArray,
  requireRecord,
  requireString,
  type AnthropicRequestLimits,
} from "./request-validation";

function optionalNullable(
  record: Record<string, unknown>,
  key: string,
  validate: (value: unknown) => void,
): void {
  if (!hasOwn(record, key) || record[key] === null) return;
  validate(record[key]);
}

function nonnegativeInteger(value: unknown, name: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new AnthropicRequestError(400, `${name} must be a nonnegative integer`);
  }
  return Number(value);
}

function typedCount(value: unknown, name: string, types: readonly string[]): void {
  const record = requireRecord(value, name);
  const type = requireString(record, "type", `${name}.type`);
  if (!types.includes(type)) {
    throw new AnthropicRequestError(400, `${name}.type is unsupported`);
  }
  nonnegativeInteger(record.value, `${name}.value`);
}

function stringArray(value: unknown, name: string, limits: AnthropicRequestLimits): void {
  const values = requireArray(value, name, limits);
  if (values.some(item => typeof item !== "string")) {
    throw new AnthropicRequestError(400, `${name} must contain only strings`);
  }
}

function validateClearToolUses(
  edit: Record<string, unknown>,
  name: string,
  limits: AnthropicRequestLimits,
): void {
  optionalNullable(edit, "clear_at_least", value => {
    typedCount(value, `${name}.clear_at_least`, ["input_tokens"]);
  });
  optionalNullable(edit, "clear_tool_inputs", value => {
    if (typeof value !== "boolean") stringArray(value, `${name}.clear_tool_inputs`, limits);
  });
  optionalNullable(edit, "exclude_tools", value => {
    stringArray(value, `${name}.exclude_tools`, limits);
  });
  if (hasOwn(edit, "keep")) {
    typedCount(edit.keep, `${name}.keep`, ["tool_uses"]);
  }
  if (hasOwn(edit, "trigger")) {
    typedCount(edit.trigger, `${name}.trigger`, ["input_tokens", "tool_uses"]);
  }
}

function validateClearThinking(edit: Record<string, unknown>, name: string): void {
  if (!hasOwn(edit, "keep")) return;
  if (edit.keep === "all") return;
  const keep = requireRecord(edit.keep, `${name}.keep`);
  const type = requireString(keep, "type", `${name}.keep.type`);
  if (type === "all") return;
  if (type !== "thinking_turns") {
    throw new AnthropicRequestError(400, `${name}.keep.type is unsupported`);
  }
  nonnegativeInteger(keep.value, `${name}.keep.value`);
}

function validateCompact(edit: Record<string, unknown>, name: string): void {
  optionalNullable(edit, "instructions", value => {
    if (typeof value !== "string") {
      throw new AnthropicRequestError(400, `${name}.instructions must be a string or null`);
    }
  });
  if (hasOwn(edit, "pause_after_compaction") &&
    typeof edit.pause_after_compaction !== "boolean") {
    throw new AnthropicRequestError(400, `${name}.pause_after_compaction must be a boolean`);
  }
  optionalNullable(edit, "trigger", value => {
    typedCount(value, `${name}.trigger`, ["input_tokens"]);
  });
}

export function validateContextManagement(
  value: unknown,
  limits: AnthropicRequestLimits,
): void {
  if (value === undefined || value === null) return;
  const config = requireRecord(value, "context_management");
  if (!hasOwn(config, "edits")) return;
  const edits = requireArray(config.edits, "context_management.edits", limits);
  for (let index = 0; index < edits.length; index += 1) {
    const name = `context_management.edits[${index}]`;
    const edit = requireRecord(edits[index], name);
    const type = requireString(edit, "type", `${name}.type`);
    if (type === "clear_tool_uses_20250919") {
      validateClearToolUses(edit, name, limits);
    } else if (type === "clear_thinking_20251015") {
      validateClearThinking(edit, name);
    } else if (type === "compact_20260112") {
      validateCompact(edit, name);
    } else {
      throw new AnthropicRequestError(400, `${name}.type is unsupported`);
    }
  }
}

function validateCacheControl(value: unknown, name: string): void {
  if (value === undefined || value === null) return;
  const cache = requireRecord(value, name);
  if (cache.type !== "ephemeral") {
    throw new AnthropicRequestError(400, `${name}.type must be ephemeral`);
  }
  if (hasOwn(cache, "ttl") && cache.ttl !== "5m" && cache.ttl !== "1h") {
    throw new AnthropicRequestError(400, `${name}.ttl must be 5m or 1h`);
  }
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function pushBlocks(value: unknown, stack: unknown[]): void {
  if (!Array.isArray(value)) return;
  for (let index = value.length - 1; index >= 0; index -= 1) stack.push(value[index]);
}

export function validatePromptCacheControls(
  body: Record<string, unknown>,
  limits: AnthropicRequestLimits,
): void {
  validateCacheControl(body.cache_control, "cache_control");

  if (Array.isArray(body.tools)) {
    for (let index = 0; index < body.tools.length; index += 1) {
      const tool = record(body.tools[index]);
      if (tool) validateCacheControl(tool.cache_control, `tools[${index}].cache_control`);
    }
  }

  const stack: unknown[] = [];
  if (Array.isArray(body.system)) pushBlocks(body.system, stack);
  if (Array.isArray(body.messages)) {
    for (const messageValue of body.messages) {
      const message = record(messageValue);
      pushBlocks(message?.content, stack);
    }
  }

  let visited = 0;
  while (stack.length) {
    visited += 1;
    if (visited > limits.maxTotalJsonItems) {
      throw new AnthropicRequestError(400, "cache_control traversal contains too many items");
    }
    const block = record(stack.pop());
    if (!block) continue;
    validateCacheControl(block.cache_control, "content block cache_control");
    if (block.type === "tool_result" || block.type === "search_result") {
      pushBlocks(block.content, stack);
    } else if (block.type === "document") {
      const source = record(block.source);
      if (source?.type === "content") pushBlocks(source.content, stack);
    }
  }
}
