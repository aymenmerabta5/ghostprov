import { ApiError } from "../http/errors";
import type { ImageGenerationTool, InputImage } from "../relay/types";

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

export interface ParsedEditRequest {
  kind: "edit";
  responsesModel: string;
  prompt: string;
  model: SupportedImageModel;
  n: number;
  images: InputImage[];
  maskDataUrl?: string;
  quality?: ImageQuality;
  size?: string;
  outputFormat?: ImageOutputFormat;
  outputCompression?: number;
  background?: ImageBackground;
  inputFidelity?: "low" | "high";
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
const EDIT_FIELDS = new Set([
  "prompt",
  "image",
  "mask",
  "model",
  "n",
  "quality",
  "size",
  "background",
  "output_format",
  "output_compression",
  "input_fidelity",
  "stream",
  "partial_images",
  "user",
]);
const INPUT_FIDELITIES = new Set(["low", "high"] as const);
const MAX_EDIT_IMAGE_COUNT = 16;
const MAX_EDIT_IMAGE_BYTES = 50 * 1024 * 1024;
const MAX_MASK_BYTES = 4 * 1024 * 1024;

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

function rejectUnknownFormFields(form: FormData): void {
  for (const key of new Set(form.keys())) {
    if (!EDIT_FIELDS.has(key)) throw invalid(`Unknown parameter: '${key}'.`, key, "unknown_parameter");
  }
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

function parseFormCount(value: string | undefined): number {
  if (value === undefined) return 1;
  if (!/^\d+$/.test(value)) throw invalid("'n' must be an integer between 1 and 10.", "n");
  return parseCount(Number(value));
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

function validateFormNonStreaming(form: FormData): void {
  const stream = formString(form, "stream");
  if (stream !== undefined && stream !== "false") {
    throw invalid("Image streaming is not supported by this compatibility route.", "stream", "unsupported_parameter");
  }
  if (form.has("partial_images")) {
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

function formString(form: FormData, name: string): string | undefined {
  const value = form.get(name);
  if (value === null) return undefined;
  if (typeof value !== "string") throw invalid(`'${name}' must be a string.`, name, "invalid_type");
  return value;
}

function formEnum<T extends string>(
  form: FormData,
  name: string,
  allowed: ReadonlySet<T>,
): T | undefined {
  const value = formString(form, name);
  return optionalEnum(value, allowed, name);
}

function parseFormCompression(
  value: string | undefined,
  outputFormat: ImageOutputFormat | undefined,
): number | undefined {
  if (value === undefined) return undefined;
  if (!/^\d+$/.test(value)) {
    throw invalid("'output_compression' must be an integer between 0 and 100.", "output_compression");
  }
  return parseCompression(Number(value), outputFormat);
}

type UploadedBlob = File;
type DetectedImageMime = "image/png" | "image/jpeg" | "image/webp";

function declaredMime(file: UploadedBlob): DetectedImageMime | null | "unsupported" {
  const value = file.type.toLowerCase();
  if (!value || value === "application/octet-stream") return null;
  if (value === "image/png" || value === "image/jpeg" || value === "image/webp") return value;
  return "unsupported";
}

function filenameMime(filename: string | undefined): DetectedImageMime | null | "unsupported" {
  if (!filename || !filename.includes(".")) return null;
  const extension = filename.slice(filename.lastIndexOf(".") + 1).toLowerCase();
  if (extension === "png") return "image/png";
  if (extension === "jpg" || extension === "jpeg") return "image/jpeg";
  if (extension === "webp") return "image/webp";
  return "unsupported";
}

function detectedMime(bytes: Uint8Array): DetectedImageMime | null {
  if (
    bytes.length >= 8 &&
    bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47 &&
    bytes[4] === 0x0d && bytes[5] === 0x0a && bytes[6] === 0x1a && bytes[7] === 0x0a
  ) return "image/png";
  if (bytes.length >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) {
    return "image/jpeg";
  }
  if (
    bytes.length >= 12 &&
    bytes[0] === 0x52 && bytes[1] === 0x49 && bytes[2] === 0x46 && bytes[3] === 0x46 &&
    bytes[8] === 0x57 && bytes[9] === 0x45 && bytes[10] === 0x42 && bytes[11] === 0x50
  ) return "image/webp";
  return null;
}

async function encodeUploadedImage(
  file: UploadedBlob,
  param: "image" | "mask",
  mask = false,
): Promise<{ mime: DetectedImageMime; dataUrl: string }> {
  const declared = declaredMime(file);
  const fromName = filenameMime(file.name);
  if (declared === "unsupported" || fromName === "unsupported") {
    throw invalid(`Unsupported ${param} file type.`, param, "unsupported_file_type");
  }

  const bytes = new Uint8Array(await file.arrayBuffer());
  const detected = detectedMime(bytes);
  if (!detected) throw invalid(`The '${param}' file is not a valid PNG, JPEG, or WebP image.`, param, "invalid_image");
  if (mask && detected !== "image/png") {
    throw invalid("'mask' must be a PNG image.", "mask", "unsupported_file_type");
  }
  if ((declared && declared !== detected) || (fromName && fromName !== detected)) {
    throw invalid(`The '${param}' file type does not match its contents.`, param, "invalid_image");
  }
  return {
    mime: detected,
    dataUrl: `data:${detected};base64,${Buffer.from(bytes).toString("base64")}`,
  };
}

function formFiles(form: FormData, name: string): UploadedBlob[] {
  const values = form.getAll(name);
  if (values.some(value => typeof value === "string")) {
    throw invalid(`'${name}' must contain uploaded files.`, name, "invalid_type");
  }
  return values as UploadedBlob[];
}

function ensureEditBudget(
  images: UploadedBlob[],
  masks: UploadedBlob[],
  maxEditInputBytes: number,
): void {
  if (images.length === 0) {
    throw invalid("Missing required parameter: 'image'.", "image", "missing_required_parameter");
  }
  if (images.length > MAX_EDIT_IMAGE_COUNT) {
    throw invalid(`No more than ${MAX_EDIT_IMAGE_COUNT} image files are supported.`, "image", "too_many_files");
  }
  if (masks.length > 1) throw invalid("Only one 'mask' file is supported.", "mask", "too_many_files");
  for (const image of images) {
    if (image.size > MAX_EDIT_IMAGE_BYTES) {
      throw invalid("Each 'image' file must be 50 MiB or smaller.", "image", "file_too_large");
    }
  }
  if (masks[0] && masks[0].size > MAX_MASK_BYTES) {
    throw invalid("'mask' must be 4 MiB or smaller.", "mask", "file_too_large");
  }
  const total = [...images, ...masks].reduce((bytes, file) => bytes + file.size, 0);
  if (total > maxEditInputBytes) {
    throw invalid(
      `Total image edit input exceeds the configured ${maxEditInputBytes}-byte limit.`,
      "image",
      "request_too_large",
      413,
    );
  }
}

export async function parseEditRequest(
  form: FormData,
  options: ImageCompatOptions,
): Promise<ParsedEditRequest> {
  rejectUnknownFormFields(form);
  validateFormNonStreaming(form);

  const images = formFiles(form, "image");
  const masks = formFiles(form, "mask");
  ensureEditBudget(images, masks, options.maxEditInputBytes);

  const backgroundText = formString(form, "background");
  if (backgroundText === "transparent") {
    throw invalid("gpt-image-2 does not support transparent backgrounds.", "background", "unsupported_parameter");
  }
  const background = optionalEnum(backgroundText, BACKGROUNDS, "background");
  const outputFormat = formEnum(form, "output_format", OUTPUT_FORMATS);

  const encodedImages = await Promise.all(images.map(async file => {
    const encoded = await encodeUploadedImage(file, "image");
    return {
      type: "input_image" as const,
      image_url: encoded.dataUrl,
      detail: "auto" as const,
    };
  }));
  const encodedMask = masks[0]
    ? (await encodeUploadedImage(masks[0], "mask", true)).dataUrl
    : undefined;

  return {
    kind: "edit",
    responsesModel: options.responsesModel,
    prompt: requiredPrompt(formString(form, "prompt")),
    model: parseModel(formString(form, "model")),
    n: parseFormCount(formString(form, "n")),
    images: encodedImages,
    maskDataUrl: encodedMask,
    quality: formEnum(form, "quality", QUALITIES),
    size: parseSize(formString(form, "size")),
    outputFormat,
    outputCompression: parseFormCompression(formString(form, "output_compression"), outputFormat),
    background,
    inputFidelity: formEnum(form, "input_fidelity", INPUT_FIDELITIES),
    user: parseUser(formString(form, "user")),
  };
}

function editTool(request: ParsedEditRequest): ImageGenerationTool {
  const tool: ImageGenerationTool = {
    type: "image_generation",
    model: request.model,
    action: "edit",
  };
  if (request.quality !== undefined) tool.quality = request.quality;
  if (request.size !== undefined) tool.size = request.size;
  if (request.outputFormat !== undefined) tool.output_format = request.outputFormat;
  if (request.outputCompression !== undefined) tool.output_compression = request.outputCompression;
  if (request.background !== undefined) tool.background = request.background;
  if (request.inputFidelity !== undefined) tool.input_fidelity = request.inputFidelity;
  if (request.maskDataUrl !== undefined) tool.input_image_mask = { image_url: request.maskDataUrl };
  return tool;
}

export function buildEditBodies(request: ParsedEditRequest): Record<string, unknown>[] {
  const input = [{
    role: "user",
    content: [
      { type: "input_text", text: request.prompt },
      ...request.images,
    ],
  }];
  return Array.from({ length: request.n }, () => {
    const body: Record<string, unknown> = {
      model: request.responsesModel,
      store: false,
      input,
      tools: [editTool(request)],
      tool_choice: { type: "image_generation" },
    };
    if (request.user !== undefined) body.user = request.user;
    return body;
  });
}
