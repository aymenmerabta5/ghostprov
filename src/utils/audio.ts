/**
 * Audio helpers for transcription, speech, and voice (Task Image)
 *
 * Covers:
 * - input_audio passthrough (base64 + format, or file_id)
 * - output audio / transcription handling
 * - helpers to encode audio bytes to base64 and to upload via Files API
 *
 * Like image helpers, all structures are forwarded verbatim via relayCreate's Yjs+render+zip loop.
 * No fields are stripped; unknown keys survive.
 */

import type { InputAudio, OutputItem } from "../relay/types";

// ---- Validation ----
export const VALID_AUDIO_FORMATS = ["wav", "mp3", "mp4", "mpeg", "mpga", "m4a", "ogg", "flac", "webm"] as const;
export type AudioFormat = (typeof VALID_AUDIO_FORMATS)[number] | string;

export function isValidAudioFormat(v: unknown): boolean {
  return typeof v === "string" && v.length > 0;
}

// ---- Base64 helpers (mirrored from image.ts for standalone usage) ----
export function bytesToBase64(bytes: Uint8Array): string {
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

// ---- InputAudio factories ----
export function createInputAudioFromBytes(
  bytes: Uint8Array,
  format: AudioFormat = "wav",
): InputAudio {
  const b64 = bytesToBase64(bytes);
  return {
    type: "input_audio",
    input_audio: { data: b64, format },
  };
}

export function createInputAudioFromBase64(b64: string, format: AudioFormat = "wav"): InputAudio {
  return {
    type: "input_audio",
    input_audio: { data: b64, format },
  };
}

export function createInputAudioFromFileId(fileId: string): InputAudio {
  if (!fileId || typeof fileId !== "string") throw new Error("createInputAudioFromFileId: fileId required");
  return {
    type: "input_audio",
    file_id: fileId,
  };
}

// Legacy transcription alias: some APIs use { type:"input_audio", input_audio:{data, format} } verbatim;
// this helper builds the same but ensures correct shape.
export function createTranscriptionInputFromBytes(
  bytes: Uint8Array,
  format: AudioFormat = "wav",
  model: string = "gpt-4o-transcribe",
): { input_audio: InputAudio; model: string } {
  return { input_audio: createInputAudioFromBytes(bytes, format), model };
}

// Upload + file_id path (mirrors image via-upload helper)
export async function createInputAudioFromBytesViaUpload(
  bytes: Uint8Array,
  filename: string,
  purpose: string = "audio",
): Promise<InputAudio> {
  const ext = filename.split(".").pop()?.toLowerCase() ?? "wav";
  const mimeMap: Record<string, string> = {
    wav: "audio/wav",
    mp3: "audio/mpeg",
    m4a: "audio/m4a",
    mp4: "audio/mp4",
    ogg: "audio/ogg",
    flac: "audio/flac",
    webm: "audio/webm",
  };
  const mime = mimeMap[ext] ?? "audio/wav";
  const blob = new Blob([bytes as unknown as BlobPart], { type: mime });
  const { uploadFile } = await import("../proxy/files");
  const file = await uploadFile(blob as unknown as Blob, filename, purpose);
  const fileId: string = (file as any)?.id ?? (file as any)?.file_id;
  if (!fileId) throw new Error("upload failed: no file id returned");
  return createInputAudioFromFileId(fileId);
}

// ---- Output audio helpers ----
export function isOutputAudio(item: unknown): boolean {
  return !!item && typeof item === "object" && (item as any).type === "output_audio";
}

export function extractOutputAudio(output: unknown): Array<{ data?: string; transcript?: string; [key: string]: unknown }> {
  if (!Array.isArray(output)) return [];
  const out: Array<{ data?: string; transcript?: string; [key: string]: unknown }> = [];
  for (const item of output as OutputItem[]) {
    if ((item as any).type === "output_audio") {
      out.push(item as any);
    }
    // also handle message content with output_audio
    if ((item as any).type === "message" && (item as any).content) {
      const content = (item as any).content;
      if (Array.isArray(content)) {
        for (const part of content) {
          if ((part as any)?.type === "output_audio") out.push(part as any);
        }
      }
    }
  }
  return out;
}

export function extractTranscripts(output: unknown): string[] {
  const audios = extractOutputAudio(output);
  return audios.map((a) => a.transcript).filter((v): v is string => typeof v === "string" && v.length > 0);
}

export async function saveBase64AudioToFile(b64: string, filePath: string): Promise<void> {
  const bytes = base64ToBytes(b64);
  const fs = await import("node:fs/promises");
  await fs.writeFile(filePath, bytes);
}

// ---- Speech generation helper (for completeness) ----
export type SpeechVoice = "alloy" | "ash" | "ballad" | "coral" | "echo" | "sage" | "shimmer" | "verse" | "marin" | "cedar" | string;

export function createSpeechInput(text: string, voice: SpeechVoice = "alloy", model: string = "gpt-4o-mini-tts"): { input: string; voice: SpeechVoice; model: string } {
  return { input: text, voice, model };
}

// ---- Convenience: build audio+text input ----
export function buildAudioInput(
  prompt: string,
  audios: InputAudio[],
): { role: "user"; content: Array<{ type: string; [key: string]: unknown }> } {
  return {
    role: "user",
    content: [{ type: "input_text", text: prompt }, ...audios],
  };
}
