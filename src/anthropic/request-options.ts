import { createHash } from "node:crypto";
import { AnthropicRequestError } from "./errors";
import type {
  ResponsesFunctionTool,
  ResponsesReasoning,
  ResponsesReasoningEffort,
  ResponsesTextConfig,
  ResponsesToolChoice,
} from "./request-types";
import {
  type AnthropicRequestLimits,
  hasOwn,
  requireArray,
  requireOptionalBoolean,
  requireRecord,
  requireString,
} from "./request-validation";

const OUTPUT_EFFORTS = new Set<unknown>(["low", "medium", "high", "xhigh", "max"]);

export const MAX_ANTHROPIC_STOP_SEQUENCES = 4;
export const MAX_ANTHROPIC_STOP_SEQUENCE_LENGTH = 8191;

export type AnthropicThinkingDisplay = "summarized" | "omitted";

export function translateTools(
  value: unknown,
  limits: AnthropicRequestLimits,
): ResponsesFunctionTool[] {
  if (value === undefined) return [];
  if (value === null) throw new AnthropicRequestError(400, "tools must not be null");
  const tools = requireArray(value, "tools", limits);
  const names = new Set<string>();
  return tools.map((item, index) => {
    const name = `tools[${index}]`;
    const tool = requireRecord(item, name);
    if (tool.type !== undefined && tool.type !== null && tool.type !== "custom") {
      throw new AnthropicRequestError(400, `unsupported tool type: ${String(tool.type)}`);
    }
    const toolName = requireString(tool, "name", `${name} tool name`);
    if (names.has(toolName)) {
      throw new AnthropicRequestError(400, `duplicate tool name: ${toolName}`);
    }
    names.add(toolName);
    if (!hasOwn(tool, "input_schema") || tool.input_schema === undefined) {
      throw new AnthropicRequestError(400, `${name} tool input_schema is required`);
    }
    const inputSchema = requireRecord(tool.input_schema, `${name} tool input_schema`);
    if (inputSchema.type !== "object") {
      throw new AnthropicRequestError(400, `${name} tool input_schema.type must be object`);
    }
    const strict = requireOptionalBoolean(tool, "strict", `${name} strict`);
    requireOptionalBoolean(tool, "defer_loading", `${name} defer_loading`);
    if (tool.eager_input_streaming !== null) {
      requireOptionalBoolean(tool, "eager_input_streaming", `${name} eager_input_streaming`);
    }

    const translated: ResponsesFunctionTool = {
      type: "function",
      name: toolName,
      parameters: inputSchema,
    };
    if (hasOwn(tool, "description")) {
      if (typeof tool.description !== "string") {
        throw new AnthropicRequestError(400, `${name} description must be a string`);
      }
      translated.description = tool.description;
    }
    if (strict !== undefined) translated.strict = strict;
    return translated;
  });
}

export function translateToolChoice(
  value: unknown,
  declaredToolNames: ReadonlySet<string>,
): {
  toolChoice?: ResponsesToolChoice;
  parallelToolCalls?: false;
} {
  if (value === undefined) return {};
  if (value === null) throw new AnthropicRequestError(400, "tool_choice must not be null");
  const choice = requireRecord(value, "tool_choice");
  const type = requireString(choice, "type", "tool_choice type");
  const disableParallel = requireOptionalBoolean(
    choice,
    "disable_parallel_tool_use",
    "tool_choice disable_parallel_tool_use",
  );

  let toolChoice: ResponsesToolChoice;
  if (type === "auto") toolChoice = "auto";
  else if (type === "any") {
    if (!declaredToolNames.size) {
      throw new AnthropicRequestError(400, "tool_choice any requires at least one declared tool");
    }
    toolChoice = "required";
  }
  else if (type === "none") toolChoice = "none";
  else if (type === "tool") {
    const name = requireString(choice, "name", "tool_choice tool name");
    if (!declaredToolNames.has(name)) {
      throw new AnthropicRequestError(400, `tool_choice tool is not declared: ${name}`);
    }
    toolChoice = {
      type: "function",
      name,
    };
  } else {
    throw new AnthropicRequestError(400, `unsupported tool_choice type: ${type}`);
  }

  return {
    toolChoice,
    ...(disableParallel === true ? { parallelToolCalls: false as const } : {}),
  };
}

export function translateOutputConfig(value: unknown): {
  effort?: ResponsesReasoningEffort;
  text?: ResponsesTextConfig;
} {
  if (value === undefined) return {};
  if (value === null) throw new AnthropicRequestError(400, "output_config must not be null");
  const config = requireRecord(value, "output_config");
  let effort: ResponsesReasoningEffort | undefined;
  if (hasOwn(config, "effort") && config.effort !== null) {
    if (!OUTPUT_EFFORTS.has(config.effort)) {
      throw new AnthropicRequestError(400, "output_config.effort is unsupported");
    }
    effort = config.effort as ResponsesReasoningEffort;
  }

  let text: ResponsesTextConfig | undefined;
  if (hasOwn(config, "format") && config.format !== null) {
    const format = requireRecord(config.format, "output_config.format");
    const type = requireString(format, "type", "output_config.format type");
    if (type !== "json_schema") {
      throw new AnthropicRequestError(400, `unsupported output_config.format type: ${type}`);
    }
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
  return { effort, text };
}

function effortFromBudget(budget: number): ResponsesReasoningEffort {
  if (budget < 4096) return "low";
  if (budget < 16000) return "medium";
  if (budget < 32000) return "high";
  return "max";
}

export function translateThinking(
  value: unknown,
  explicitEffort: ResponsesReasoningEffort | undefined,
  maxTokens: number,
): {
  reasoning?: ResponsesReasoning;
  display: AnthropicThinkingDisplay;
  manual: boolean;
} {
  if (value === undefined) {
    return {
      ...(explicitEffort
        ? { reasoning: { effort: explicitEffort, summary: "auto" as const } }
        : {}),
      display: "summarized",
      manual: false,
    };
  }
  if (value === null) throw new AnthropicRequestError(400, "thinking must not be null");

  const thinking = requireRecord(value, "thinking");
  const type = requireString(thinking, "type", "thinking type");
  let display: AnthropicThinkingDisplay = "summarized";
  if (hasOwn(thinking, "display") && thinking.display !== null) {
    if (thinking.display !== "summarized" && thinking.display !== "omitted") {
      throw new AnthropicRequestError(400, "thinking display must be summarized or omitted");
    }
    display = thinking.display;
  }

  if (type === "disabled") return { reasoning: { effort: "none" }, display, manual: false };
  if (type === "adaptive") {
    return {
      reasoning: { effort: explicitEffort ?? "high", summary: "auto" },
      display,
      manual: false,
    };
  }
  if (type === "enabled") {
    const budget = thinking.budget_tokens;
    if (!Number.isSafeInteger(budget) || Number(budget) < 1024 || Number(budget) >= maxTokens) {
      throw new AnthropicRequestError(
        400,
        "thinking budget_tokens must be an integer at least 1024 and less than max_tokens",
      );
    }
    return {
      reasoning: {
        effort: explicitEffort ?? effortFromBudget(Number(budget)),
        summary: "auto",
      },
      display,
      manual: true,
    };
  }
  throw new AnthropicRequestError(400, `unsupported thinking type: ${type}`);
}

export function translateServiceTier(body: Record<string, unknown>): "auto" | "default" | "fast" {
  let serviceTier: "auto" | "default" = "auto";
  if (hasOwn(body, "service_tier")) {
    if (body.service_tier === "auto") serviceTier = "auto";
    else if (body.service_tier === "standard_only") serviceTier = "default";
    else throw new AnthropicRequestError(400, "service_tier must be auto or standard_only");
  }

  if (hasOwn(body, "speed")) {
    if (body.speed === "fast") return "fast";
    if (body.speed === "standard") return serviceTier;
    throw new AnthropicRequestError(400, "speed must be standard or fast");
  }
  return serviceTier;
}

export function translateMetadata(value: unknown): {
  safetyIdentifier?: string;
  metadata?: { anthropic_user_id_sha256: string };
} {
  if (value === undefined) return {};
  const metadata = requireRecord(value, "metadata");
  if (!hasOwn(metadata, "user_id") || metadata.user_id === null) return {};
  const userId = requireString(metadata, "user_id", "metadata.user_id");
  const digest = createHash("sha256").update(userId, "utf8").digest("hex");
  return {
    safetyIdentifier: digest,
    metadata: { anthropic_user_id_sha256: digest },
  };
}

function optionalUnitNumber(body: Record<string, unknown>, key: string): number | undefined {
  if (!hasOwn(body, key)) return undefined;
  const value = body[key];
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 1) {
    throw new AnthropicRequestError(400, `${key} must be a number between 0 and 1`);
  }
  return value;
}

export function translateSampling(body: Record<string, unknown>): {
  temperature?: number;
  topP?: number;
} {
  const temperature = optionalUnitNumber(body, "temperature");
  const topP = optionalUnitNumber(body, "top_p");
  if (hasOwn(body, "top_k")) {
    if (!Number.isSafeInteger(body.top_k) || Number(body.top_k) < 0) {
      throw new AnthropicRequestError(400, "top_k must be a nonnegative integer");
    }
  }
  return { temperature, topP };
}

export function translateStopSequences(
  value: unknown,
  limits: AnthropicRequestLimits,
): string[] {
  if (value === undefined) return [];
  if (value === null) throw new AnthropicRequestError(400, "stop_sequences must not be null");
  const values = requireArray(value, "stop_sequences", limits);
  if (values.some(item => typeof item !== "string")) {
    throw new AnthropicRequestError(400, "stop_sequences must contain only strings");
  }
  if (values.length > MAX_ANTHROPIC_STOP_SEQUENCES) {
    throw new AnthropicRequestError(
      400,
      `stop_sequences may contain at most ${MAX_ANTHROPIC_STOP_SEQUENCES} values`,
    );
  }
  const strings = values as string[];
  if (strings.some(item => item.length === 0)) {
    throw new AnthropicRequestError(400, "stop_sequences values must not be empty");
  }
  if (strings.some(item => item.length > MAX_ANTHROPIC_STOP_SEQUENCE_LENGTH)) {
    throw new AnthropicRequestError(
      400,
      `stop_sequences values may contain at most ${MAX_ANTHROPIC_STOP_SEQUENCE_LENGTH} characters`,
    );
  }
  return [...strings];
}
