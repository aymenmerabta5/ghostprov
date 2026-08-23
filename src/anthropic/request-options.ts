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
  hasOwn,
  requireArray,
  requireOptionalBoolean,
  requireRecord,
  requireString,
  serializeBoundedJson,
} from "./request-validation";

const OUTPUT_EFFORTS = new Set<unknown>(["low", "medium", "high", "xhigh", "max"]);

export type AnthropicThinkingDisplay = "summarized" | "omitted";

export function translateTools(value: unknown): ResponsesFunctionTool[] {
  if (value === undefined || value === null) return [];
  const tools = requireArray(value, "tools");
  return tools.map((item, index) => {
    const name = `tools[${index}]`;
    const tool = requireRecord(item, name);
    if (tool.type !== undefined && tool.type !== null && tool.type !== "custom") {
      throw new AnthropicRequestError(400, `unsupported tool type: ${String(tool.type)}`);
    }
    const toolName = requireString(tool, "name", `${name} tool name`);
    if (!hasOwn(tool, "input_schema") || tool.input_schema === undefined) {
      throw new AnthropicRequestError(400, `${name} tool input_schema is required`);
    }
    const inputSchema = requireRecord(tool.input_schema, `${name} tool input_schema`);
    if (inputSchema.type !== "object") {
      throw new AnthropicRequestError(400, `${name} tool input_schema.type must be object`);
    }
    serializeBoundedJson(inputSchema, `${name} tool input_schema`);

    const strict = requireOptionalBoolean(tool, "strict", `${name} strict`);
    requireOptionalBoolean(tool, "defer_loading", `${name} defer_loading`);
    requireOptionalBoolean(tool, "eager_input_streaming", `${name} eager_input_streaming`);

    const translated: ResponsesFunctionTool = {
      type: "function",
      name: toolName,
      parameters: inputSchema,
    };
    if (typeof tool.description === "string") translated.description = tool.description;
    if (strict !== undefined) translated.strict = strict;
    return translated;
  });
}

export function translateToolChoice(value: unknown): {
  toolChoice?: ResponsesToolChoice;
  parallelToolCalls?: false;
} {
  if (value === undefined || value === null) return {};
  const choice = requireRecord(value, "tool_choice");
  const type = requireString(choice, "type", "tool_choice type");
  const disableParallel = requireOptionalBoolean(
    choice,
    "disable_parallel_tool_use",
    "tool_choice disable_parallel_tool_use",
  );

  let toolChoice: ResponsesToolChoice;
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
    ...(disableParallel === true ? { parallelToolCalls: false as const } : {}),
  };
}

export function translateOutputConfig(value: unknown): {
  effort?: ResponsesReasoningEffort;
  text?: ResponsesTextConfig;
} {
  if (value === undefined || value === null) return {};
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
    serializeBoundedJson(schema, "output_config.format schema");
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
} {
  if (value === undefined || value === null) {
    return {
      ...(explicitEffort
        ? { reasoning: { effort: explicitEffort, summary: "auto" as const } }
        : {}),
      display: "summarized",
    };
  }

  const thinking = requireRecord(value, "thinking");
  const type = requireString(thinking, "type", "thinking type");
  let display: AnthropicThinkingDisplay = "summarized";
  if (hasOwn(thinking, "display") && thinking.display !== null) {
    if (thinking.display !== "summarized" && thinking.display !== "omitted") {
      throw new AnthropicRequestError(400, "thinking display must be summarized or omitted");
    }
    display = thinking.display;
  }

  if (type === "disabled") return { reasoning: { effort: "none" }, display };
  if (type === "adaptive") {
    return {
      reasoning: { effort: explicitEffort ?? "high", summary: "auto" },
      display,
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
    };
  }
  throw new AnthropicRequestError(400, `unsupported thinking type: ${type}`);
}

export function translateServiceTier(body: Record<string, unknown>): "auto" | "default" | "fast" | undefined {
  let serviceTier: "auto" | "default" | undefined;
  if (hasOwn(body, "service_tier")) {
    if (body.service_tier === "auto") serviceTier = "auto";
    else if (body.service_tier === "standard_only") serviceTier = "default";
    else throw new AnthropicRequestError(400, "service_tier must be auto or standard_only");
  }

  if (hasOwn(body, "speed")) {
    if (body.speed === "fast") return "fast";
    if (body.speed === "standard") return serviceTier ?? "default";
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

export function translateStopSequences(value: unknown): string[] {
  if (value === undefined || value === null) return [];
  const values = requireArray(value, "stop_sequences");
  if (values.some(item => typeof item !== "string")) {
    throw new AnthropicRequestError(400, "stop_sequences must contain only strings");
  }
  return [...values] as string[];
}
