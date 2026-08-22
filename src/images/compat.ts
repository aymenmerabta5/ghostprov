import { ApiError } from "../http/errors";
import type { ImageGenerationTool } from "../relay/types";

export interface ImageCompatOptions {
  responsesModel: string;
  maxEditInputBytes: number;
}

export type SupportedImageModel = "gpt-image-2" | "gpt-image-2-2026-04-21";
export type ImageQuality = "low" | "medium" | "high" | "auto";
export type ImageOutputFormat = "png" | "jpeg" | "webp";
export type ImageBackground = "opaque" | "auto";
export type ImageModeration = "low" | "auto";

export interface ParsedGenerationRequest {
  kind: "generation";
  responsesModel: string;
  prompt: string;
  model: SupportedImageModel;
  n: number;
  quality?: ImageQuality;
  size?: string;
  outputFormat?: ImageOutputFormat;
  outputCompression?: number;
  background?: ImageBackground;
  moderation?: ImageModeration;
  user?: string;
}

const IMAGE_MODELS = new Set<SupportedImageModel>([
  "gpt-image-2",
  "gpt-image-2-2026-04-21",
]);
const QUALITIES = new Set<ImageQuality>(["low", "medium", "high", "auto"]);
const OUTPUT_FORMATS = new Set<ImageOutputFormat>(["png", "jpeg", "webp"]);
const BACKGROUNDS = new Set<ImageBackground>(["opaque", "auto"]);
const MODERATION = new Set<ImageModeration>(["low", "auto"]);
const GENERATION_FIELDS = new Set([
  "prompt",
  "model",
  "n",
  "quality",
  "size",
  "background",
  "output_format",
  "output_compression",
  "moderation",
  "response_format",
  "stream",
  "partial_images",
  "style",
  "user",
]);

function invalid(
  message: string,
  param: string,
  code: string = "invalid_value",
  status = 400,
): ApiError {
  return new ApiError(status, message, {
    type: "invalid_request_error",
    param,
    code,
  });
}

function objectBody(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw invalid("Request body must be a JSON object.", "body", "invalid_type");
  }
  return value as Record<string, unknown>;
}

function rejectUnknownFields(body: Record<string, unknown>, allowed: Set<string>): void {
  const unknown = Object.keys(body).find(key => !allowed.has(key));
  if (unknown) throw invalid(`Unknown parameter: '${unknown}'.`, unknown, "unknown_parameter");
}

function requiredPrompt(value: unknown): string {
  if (value === undefined || value === null) {
    throw invalid("Missing required parameter: 'prompt'.", "prompt", "missing_required_parameter");
  }
  if (typeof value !== "string") throw invalid("'prompt' must be a string.", "prompt", "invalid_type");
  const prompt = value.trim();
  if (!prompt) throw invalid("'prompt' must not be empty.", "prompt");
  if (prompt.length > 32_000) throw invalid("'prompt' must not exceed 32000 characters.", "prompt");
  return prompt;
}

function optionalEnum<T extends string>(
  value: unknown,
  allowed: ReadonlySet<T>,
  param: string,
): T | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string" || !allowed.has(value as T)) {
    throw invalid(`Invalid value for '${param}'.`, param);
  }
  return value as T;
}

function parseModel(value: unknown): SupportedImageModel {
  if (value === undefined || value === null) return "gpt-image-2";
  if (typeof value !== "string") throw invalid("'model' must be a string.", "model", "invalid_type");
  if (!IMAGE_MODELS.has(value as SupportedImageModel)) {
    throw invalid(`Model '${value}' is not supported by this Images compatibility route.`, "model", "model_not_supported");
  }
  return value as SupportedImageModel;
}

function parseCount(value: unknown): number {
  if (value === undefined || value === null) return 1;
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1 || value > 10) {
    throw invalid("'n' must be an integer between 1 and 10.", "n");
  }
  return value;
}

function parseSize(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (value === "auto") return value;
  if (typeof value !== "string") throw invalid("'size' must be a string.", "size", "invalid_type");
  const match = /^(\d+)x(\d+)$/.exec(value);
  if (!match) throw invalid("'size' must be 'auto' or WIDTHxHEIGHT.", "size");
  const width = Number(match[1]);
  const height = Number(match[2]);
  const ratio = width / height;
  const maxPixels = 3840 * 2160;
  if (
    width < 16 || height < 16 ||
    width % 16 !== 0 || height % 16 !== 0 ||
    width > 3840 || height > 3840 ||
    width * height > maxPixels ||
    ratio < 1 / 3 || ratio > 3
  ) {
    throw invalid(
      "'size' dimensions must be divisible by 16, use a 1:3 to 3:1 aspect ratio, and stay within gpt-image-2 limits.",
      "size",
    );
  }
  return value;
}

function parseCompression(value: unknown, outputFormat: ImageOutputFormat | undefined): number | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0 || value > 100) {
    throw invalid("'output_compression' must be an integer between 0 and 100.", "output_compression");
  }
  if (outputFormat !== "jpeg" && outputFormat !== "webp") {
    throw invalid(
      "'output_compression' is supported only when 'output_format' is 'jpeg' or 'webp'.",
      "output_compression",
      "unsupported_parameter",
    );
  }
  return value;
}

function parseUser(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string" || value.length === 0) {
    throw invalid("'user' must be a non-empty string.", "user", "invalid_type");
  }
  return value;
}

function validateNonStreaming(body: Record<string, unknown>): void {
  if (body.stream !== undefined && body.stream !== null && body.stream !== false) {
    throw invalid("Image streaming is not supported by this compatibility route.", "stream", "unsupported_parameter");
  }
  if (body.partial_images !== undefined && body.partial_images !== null) {
    throw invalid("'partial_images' requires image streaming, which is not supported.", "partial_images", "unsupported_parameter");
  }
}

function validateResponseFormat(value: unknown): void {
  if (value === undefined || value === null || value === "b64_json") return;
  if (value === "url") {
    throw invalid("GPT Image models return base64 data; URL responses are not supported.", "response_format", "unsupported_parameter");
  }
  throw invalid("Invalid value for 'response_format'.", "response_format");
}

export function parseGenerationRequest(
  value: unknown,
  options: ImageCompatOptions,
): ParsedGenerationRequest {
  const body = objectBody(value);
  rejectUnknownFields(body, GENERATION_FIELDS);
  validateNonStreaming(body);
  validateResponseFormat(body.response_format);

  if (body.style !== undefined && body.style !== null) {
    throw invalid("'style' is a DALL-E-only parameter and is not supported for gpt-image-2.", "style", "unsupported_parameter");
  }

  if (body.background === "transparent") {
    throw invalid("gpt-image-2 does not support transparent backgrounds.", "background", "unsupported_parameter");
  }
  const background = optionalEnum(body.background, BACKGROUNDS, "background");
  const outputFormat = optionalEnum(body.output_format, OUTPUT_FORMATS, "output_format");

  return {
    kind: "generation",
    responsesModel: options.responsesModel,
    prompt: requiredPrompt(body.prompt),
    model: parseModel(body.model),
    n: parseCount(body.n),
    quality: optionalEnum(body.quality, QUALITIES, "quality"),
    size: parseSize(body.size),
    outputFormat,
    outputCompression: parseCompression(body.output_compression, outputFormat),
    background,
    moderation: optionalEnum(body.moderation, MODERATION, "moderation"),
    user: parseUser(body.user),
  };
}

function generationTool(request: ParsedGenerationRequest): ImageGenerationTool {
  const tool: ImageGenerationTool = {
    type: "image_generation",
    model: request.model,
  };
  if (request.quality !== undefined) tool.quality = request.quality;
  if (request.size !== undefined) tool.size = request.size;
  if (request.outputFormat !== undefined) tool.output_format = request.outputFormat;
  if (request.outputCompression !== undefined) tool.output_compression = request.outputCompression;
  if (request.background !== undefined) tool.background = request.background;
  if (request.moderation !== undefined) tool.moderation = request.moderation;
  return tool;
}

export function buildGenerationBodies(request: ParsedGenerationRequest): Record<string, unknown>[] {
  return Array.from({ length: request.n }, () => {
    const body: Record<string, unknown> = {
      model: request.responsesModel,
      store: false,
      input: request.prompt,
      tools: [generationTool(request)],
      tool_choice: { type: "image_generation" },
    };
    if (request.user !== undefined) body.user = request.user;
    return body;
  });
}
