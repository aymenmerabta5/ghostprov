/**
 * Image helpers for vision + image generation (Task Image)
 *
 * - input_image via image_url (http URL or data URL base64) OR file_id, with detail low/high/auto/original
 * - encode bytes -> data URL
 * - upload via Files API -> file_id input_image
 * - extract image_generation_call results (b64_json) and save to file
 *
 * All helpers preserve verbatim forwarding guarantees: relayCreate simply JSON.stringify's the body,
 * so input_image objects, image_generation tool definitions, and image_generation_call outputs all survive
 * through the configured Responses transport unchanged.
 */

import type { ImageDetail, InputImage, OutputItem, ImageGenerationCall } from "../relay/types";
import { VALID_IMAGE_DETAILS } from "../relay/types";

// ---- Validation ----
export function isValidImageDetail(v: unknown): v is ImageDetail {
  return typeof v === "string" && (VALID_IMAGE_DETAILS as readonly string[]).includes(v);
}

export function isDataUrl(url: string): boolean {
  return typeof url === "string" && url.startsWith("data:");
}

export function isHttpUrl(url: string): boolean {
  return typeof url === "string" && (url.startsWith("http://") || url.startsWith("https://"));
}

export function isBase64ImageUrl(url: string): boolean {
  return isDataUrl(url) && /^data:image\/[a-zA-Z0-9.+-]+;base64,/.test(url);
}

// ---- Base64 <-> Bytes ----
export function bytesToBase64(bytes: Uint8Array): string {
  // Node/Bun Buffer is fastest and handles binary correctly
  // eslint-disable-next-line no-undef
  if (typeof Buffer !== "undefined") {
    return Buffer.from(bytes).toString("base64");
  }
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary);
}

export function base64ToBytes(b64: string): Uint8Array {
  if (typeof Buffer !== "undefined") {
    return new Uint8Array(Buffer.from(b64, "base64"));
  }
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

export function parseDataUrl(dataUrl: string): { mime: string; b64: string } | null {
  const m = /^data:([^;]+);base64,(.*)$/.exec(dataUrl);
  if (!m) return null;
  return { mime: m[1], b64: m[2] };
}

// ---- Data URL creation ----
export function encodeImageBytesToDataUrl(bytes: Uint8Array, mimeType: string = "image/png"): string {
  const b64 = bytesToBase64(bytes);
  return `data:${mimeType};base64,${b64}`;
}

export function encodeBase64ToDataUrl(b64: string, mimeType: string = "image/png"): string {
  // b64 is already base64, no re-encode
  return `data:${mimeType};base64,${b64}`;
}

export function inferMimeFromFilename(filename: string): string {
  const lower = filename.toLowerCase();
  if (lower.endsWith(".png")) return "image/png";
  if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
  if (lower.endsWith(".webp")) return "image/webp";
  if (lower.endsWith(".gif")) return "image/gif";
  if (lower.endsWith(".bmp")) return "image/bmp";
  return "image/png";
}

// ---- InputImage factories (verbatim structures forwarded via relay) ----
export function createInputImage(params: { image_url?: string; file_id?: string; detail?: ImageDetail }): InputImage {
  const out: InputImage = { type: "input_image" };
  if (params.image_url !== undefined) out.image_url = params.image_url;
  if (params.file_id !== undefined) out.file_id = params.file_id;
  if (params.detail !== undefined) out.detail = params.detail;
  return out;
}

export function createInputImageFromUrl(url: string, detail: ImageDetail = "auto"): InputImage {
  if (!url || typeof url !== "string") throw new Error("createInputImageFromUrl: url required");
  return createInputImage({ image_url: url, detail });
}

export function createInputImageFromDataUrl(dataUrl: string, detail: ImageDetail = "auto"): InputImage {
  if (!isDataUrl(dataUrl)) throw new Error("createInputImageFromDataUrl: not a data URL");
  return createInputImage({ image_url: dataUrl, detail });
}

export function createInputImageFromFileId(fileId: string, detail: ImageDetail = "auto"): InputImage {
  if (!fileId || typeof fileId !== "string") throw new Error("createInputImageFromFileId: fileId required");
  return createInputImage({ file_id: fileId, detail });
}

export function createInputImageFromBytes(bytes: Uint8Array, mimeType: string = "image/png", detail: ImageDetail = "auto"): InputImage {
  const dataUrl = encodeImageBytesToDataUrl(bytes, mimeType);
  return createInputImage({ image_url: dataUrl, detail });
}

export function createInputImageFromBase64(b64: string, mimeType: string = "image/png", detail: ImageDetail = "auto"): InputImage {
  const dataUrl = encodeBase64ToDataUrl(b64, mimeType);
  return createInputImage({ image_url: dataUrl, detail });
}

// ---- Image generation output helpers ----
export function isImageGenerationCall(item: unknown): item is ImageGenerationCall {
  return !!item && typeof item === "object" && (item as any).type === "image_generation_call";
}

export function extractImageGenerationCalls(output: unknown): ImageGenerationCall[] {
  if (!Array.isArray(output)) return [];
  return (output as unknown[]).filter(isImageGenerationCall) as ImageGenerationCall[];
}

export function extractImageGenerationResults(output: unknown): string[] {
  const calls = extractImageGenerationCalls(output);
  const results: string[] = [];
  for (const c of calls) {
    if (typeof c.result === "string" && c.result.length > 0) results.push(c.result);
    // some models may use b64_json alias
    else if (typeof (c as any).b64_json === "string") results.push((c as any).b64_json);
  }
  return results;
}

export function extractRevisedPrompts(output: unknown): string[] {
  const calls = extractImageGenerationCalls(output);
  return calls.map((c) => c.revised_prompt).filter((v): v is string => typeof v === "string" && v.length > 0);
}

// ---- Saving b64 to file (Node/Bun) ----
export async function saveBase64ImageToFile(b64: string, filePath: string): Promise<void> {
  const bytes = base64ToBytes(b64);
  // Use node:fs/promises if available (Bun supports node:fs)
  const fs = await import("node:fs/promises");
  await fs.writeFile(filePath, bytes);
}

export async function saveImageGenerationResultsToFiles(results: string[], dir: string, prefix: string = "image"): Promise<string[]> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  // ensure dir exists
  await fs.mkdir(dir, { recursive: true });
  const outPaths: string[] = [];
  for (let i = 0; i < results.length; i++) {
    const filePath = path.join(dir, `${prefix}_${String(i).padStart(3, "0")}.png`);
    await saveBase64ImageToFile(results[i], filePath);
    outPaths.push(filePath);
  }
  return outPaths;
}

// ---- Convenience: build vision input array ----
export function buildVisionInput(
  prompt: string,
  images: InputImage[],
): { role: "user"; content: Array<{ type: string; [key: string]: unknown }> } {
  return {
    role: "user",
    content: [{ type: "input_text", text: prompt }, ...images],
  };
}

export function buildVisionInputWithTextAndImages(text: string, imageInputs: InputImage[]): unknown[] {
  return [{ role: "user", content: [{ type: "input_text", text }, ...imageInputs] }];
}
