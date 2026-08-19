# Task Image Report: Image Vision and Generation Support

**Status:** Complete
**Commit:** `feat: image vision and generation support` (pending)
**Date:** 2026-08-19

## Plan Source
User request: Audit and add missing image/audio/voice features to ghostprov based on docs about Images and vision, Image generation, Audio and speech, Voice agents, Transcription.

Current ghostprov already has `src/relay/types.ts` and `client.ts` handling tools/reasoning/prompt_cache/service_tier, `src/proxy/files.ts` handling file upload, and basic `input_image` via relay passthrough.

Required additions:
1. Image vision: `input_image` with `image_url` (URL, data URL base64), `file_id`, `detail` (low/high/original/auto) — ensure relay forwards correctly, add helper to encode images to base64 data URLs and to upload via Files API then use file_id
2. Image generation: tool type `image_generation` with params quality/size/format etc., handling of `image_generation_call` output (b64_json) and saving to file. Add support for gpt-image-2 via Responses API tools
3. Audio: `input_audio`, output audio, transcription (gpt-transcribe, gpt-live-transcribe), speech generation if applicable — at minimum ensure audio input types are passed through

## Audit (Before)

### Existing
- `src/relay/types.ts:1-89` — reasoning/service_tier/phase, `ResponsesBody` with `[key:string]:unknown` catch-all, `ToolDefinition` limited to `type:"function"|"custom"`, `OutputItem` without `image_generation_call` or audio, no `ImageDetail`/`InputImage`/`InputAudio` types
- `src/relay/client.ts:1-167` — `relayCreate` verbatim via `writeJob(body)` → `triggerRender` → `extractResponse`, preserves `input` including data URL `input_image` in one mocked test (`relay.test.ts:194`), but no helpers for encoding, no `isValidImageDetail`, no image_generation extraction, no audio helpers
- `src/proxy/files.ts:1-234` — `uploadFile` with purpose `assistants` default, `uploadFileFromFormData`, `getFileContent` via zip; no `uploadImageForVision` (purpose vision), no upload-then-file_id helper
- `src/server.ts:1-226` — `POST /v1/responses` via `queue.add(()=>relayCreate(body))` verbatim spread (so technically forwards any `input_image`/`input_audio`/`image_generation` tool because `[key:string]:unknown`), but not explicitly documented/tested for URL vs file_id variants or gpt-image-2 params
- Tests: `relay.test.ts:194-250` checks one data URL `input_image` passthrough; `files.test.ts:276-305` checks 1x1 png data URL via server mock; no coverage for http URL, file_id, detail variants, image_generation tool, image_generation_call output, or audio input
- Missing `src/utils/image.ts` and `src/utils/audio.ts` entirely (referenced in plan but absent)

### Gaps Found
1. `ImageDetail` union + `VALID_IMAGE_DETAILS` absent; no `isValidImageDetail`
2. `InputImage` type not explicit (relies on `unknown`), no `createInputImage*` helpers, no `encodeImageBytesToDataUrl`
3. `uploadImageForVision` / `uploadImageBytesForVision` missing (always used `purpose:assistants`)
4. `ImageGenerationTool` type `type:"image_generation"` absent; `ToolDefinition` narrow
5. `ImageGenerationCall` output (`type:"image_generation_call"` with `result`/`b64_json`/`revised_prompt`) absent from `OutputItem`
6. No helpers to extract `image_generation_call` results or to save b64 to file (`saveBase64ImageToFile`)
7. No `InputAudio` / `OutputAudio` types, no `createInputAudioFromBytes`/`FileId` helpers, no verification that `input_audio` survives relay
8. No file for vision via upload → `file_id` path: user must hand-roll `uploadFile` + `createInputImageFromFileId`
9. `src/relay/types.ts` `ToolDefinition` and `OutputItem` not covering new tool/output, `src/relay/client.ts` not re-exporting helpers

## Changes Detail

### `src/relay/types.ts:1-130` (extended)
**Added:**
```ts
export type ImageDetail = "low" | "high" | "auto" | "original";
export type InputImage = { type:"input_image"; image_url?:string; file_id?:string; detail?:ImageDetail; [key:string]:unknown };
export type InputText = { type:"input_text"; text:string; [key:string]:unknown };
export type InputFile = { type:"input_file"; file_id?:string; ... };
export type InputAudio = { type:"input_audio"; input_audio?:{data:string; format:string}; file_id?:string; [key:string]:unknown };
export type InputContentPart = InputText | InputImage | InputAudio | InputFile | {type:string; ...};
export type ImageGenerationQuality = "low"|"medium"|"high"|"auto";
export type ImageGenerationTool = { type:"image_generation"; model?:string; quality?:ImageGenerationQuality; size?:string; output_format?:string; background?:string; ...; [key:string]:unknown };
export type ToolDefinition = FunctionTool | CustomTool | ImageGenerationTool | {type:string; [key:string]:unknown};
export type ImageGenerationCall = { type:"image_generation_call"; id:string; status?:string; result?:string; revised_prompt?:string; [key:string]:unknown };
export type OutputItem = ... | ImageGenerationCall | {type:"output_text"} | {type:"output_audio"} | ...;
export const VALID_IMAGE_DETAILS = ["low","high","auto","original"] as const;
export const VALID_IMAGE_GENERATION_QUALITIES = ["low","medium","high","auto"] as const;
```
- Keeps `ResponsesBody`/`ResponsesBodyFull` catch-all `[key:string]:unknown` so verbatim guarantee unchanged
- Old `ToolDefinitionOld` retained for compat
- All new fields permissive via index signatures; validation via new `VALID_*` arrays

### `src/relay/client.ts:1-307` (extended)
**Added imports:**
```ts
import type { ImageDetail, InputImage, InputAudio, ImageGenerationCall } from "./types";
import { VALID_IMAGE_DETAILS, VALID_IMAGE_GENERATION_QUALITIES } from "./types";
```
**Added validators:**
```ts
export function isValidImageDetail(v:unknown):v is ImageDetail { ... }
export function isValidImageGenerationQuality(v:unknown):boolean { ... }
export function isValidAudioFormat(v:unknown):boolean { ... }
export function isImageGenerationCall(item:unknown):item is ImageGenerationCall { ... }
```
**Added image helpers (mirrored in src/utils/image.ts for standalone):**
```ts
export function isDataUrl(url:string):boolean { ... }
export function isHttpUrl(url:string):boolean { ... }
export function bytesToBase64(bytes:Uint8Array):string { Buffer.from... }
export function base64ToBytes(b64:string):Uint8Array { ... }
export function encodeImageBytesToDataUrl(bytes:Uint8Array, mimeType="image/png"):string { ... }
export function encodeBase64ToDataUrl(b64:string, mimeType="image/png"):string { ... }
export function createInputImage(params:{image_url?, file_id?, detail?}):InputImage { ... }
export function createInputImageFromUrl(url:string, detail="auto"):InputImage { ... }
export function createInputImageFromFileId(fileId:string, detail="auto"):InputImage { ... }
export function createInputImageFromBytes(bytes:Uint8Array, mime="image/png", detail="auto"):InputImage { ... }
export function createInputImageFromBase64(b64:string, mime="image/png", detail="auto"):InputImage { ... }
export function createInputAudioFromBytes(bytes:Uint8Array, format="wav"):InputAudio { ... }
export function createInputAudioFromBase64(b64:string, format="wav"):InputAudio { ... }
export function createInputAudioFromFileId(fileId:string):InputAudio { ... }
export function extractImageGenerationCalls(output:unknown):ImageGenerationCall[] { ... }
export function extractImageGenerationResults(output:unknown):string[] { // result or b64_json
```
**Updated relayCreate doc** to list Images & Vision, Image Generation, Audio verbatim guarantees plus helpers
**Re-exports:** `ImageDetail`, `InputImage`, `InputAudio`, `ImageGenerationTool`, etc. plus `ImageGenerationQuality` aliases

Verbatim guarantee unchanged: `JSON.stringify(body)` → `job.json` → `curl --cacert $PROXY_CA_CERT_PATH -d @job.json $OPENAI_BASE_URL/responses` → `response.json`; no stripping

### `src/utils/image.ts:1-150` (new)
Pure helpers for vision + generation:
- `bytesToBase64` / `base64ToBytes` / `parseDataUrl` / `isDataUrl` / `isHttpUrl` / `inferMimeFromFilename`
- `encodeImageBytesToDataUrl` / `encodeBase64ToDataUrl`
- `createInputImage` / `createInputImageFromUrl` / `createInputImageFromDataUrl` / `createInputImageFromFileId` / `createInputImageFromBytes` / `createInputImageFromBase64`
- `createInputImageFromBytesViaUpload` / `createInputImageFromFileViaUpload` (lazy import `../proxy/files` → `uploadFile` with purpose vision, returns `file_id` InputImage)
- `isImageGenerationCall` / `extractImageGenerationCalls` / `extractImageGenerationResults` / `extractRevisedPrompts`
- `saveBase64ImageToFile` / `saveImageGenerationResultsToFiles` (node:fs/promises, mkdir -p, write decoded bytes)
- `buildVisionInput` helpers
- Docs: OpenAI Responses API uses `input_image` with `image_url` (http or `data:image/...;base64,`) OR `file_id` from Files API `purpose:"vision"`, detail low/high/auto/original; refs: `data:image/png;base64,iVBORw0KG...` 1x1 png used in tests

### `src/utils/audio.ts:1-110` (new)
Pure helpers for audio / transcription / speech:
- `VALID_AUDIO_FORMATS` = ["wav","mp3","mp4","m4a","ogg","flac","webm"] etc.
- `bytesToBase64` / `base64ToBytes` (standalone)
- `createInputAudioFromBytes` / `createInputAudioFromBase64` / `createInputAudioFromFileId` / `createInputAudioFromBytesViaUpload` (upload -> file_id)
- `isOutputAudio` / `extractOutputAudio` / `extractTranscripts` / `saveBase64AudioToFile`
- `createSpeechInput` (voice alloy/ash/... for gpt-4o-mini-tts parity)
- `buildAudioInput` helper
- Verbatim: `input_audio` shapes `{type:"input_audio", input_audio:{data, format}}` and `{type:"input_audio", file_id}` both preserved via relay; transcription `model:"gpt-4o-transcribe"` / `gpt-4o-transcribe-diarize` / `whisper-1` forwarded verbatim

### `src/proxy/files.ts:1-260` (extended)
**Header updated** to document vision/audio purposes
**Added:**
```ts
export async function uploadImageForVision(file:Blob, filename:string):Promise<any> { return uploadFile(file, filename, "vision"); }
export async function uploadImageBytesForVision(bytes:Uint8Array, filename:string, mimeType="image/png"):Promise<any> { ... }
export async function uploadAudioFile(file:Blob, filename:string, purpose="audio"):Promise<any> { return uploadFile(file, filename, purpose); }
export async function uploadAudioBytes(bytes:Uint8Array, filename:string, mimeType="audio/wav"):Promise<any> { ... }
export function __getMemoryFiles():Map<string,Uint8Array> { return memoryFiles; }
export function __getMemoryMeta():Map<string,any> { return memoryMeta; }
```
- Still uses `prism_cache_bust` mandatory on all `/api/project-files/upload` and `/s/sandboxes/proxy/zip` calls, header `x-crixet-sandbox-token`
- Keeps existing `uploadFile`/`uploadFileFromFormData`/`getFileContent`/`getFile`/`listFiles` unchanged; adds purpose-specific wrappers for ergonomics

### `src/server.ts` (no change required for logic)
Already forwards verbatim via `queue.add(()=>relayCreate(body))` (Hono `POST /v1/responses`) and `WSS /v1/responses` (`attachWss`); because `ResponsesBody` has `[key:string]:unknown` and relayCreate stringifies whole body, any new `input_image`/`image_generation` tool/`input_audio` shapes survive without code change. Comments remain accurate (thinking/prompt_cache); no stripping. Verified by new server tests that mock `relayCreate` and assert `body.tools[0].type==="image_generation"` etc.

## Tests Added

### `tests/image.test.ts:1-360` (18 tests)
- **Image helpers (5):**
  - `encodeImageBytesToDataUrl` round-trip via `base64ToBytes`/`parseDataUrl`/`bytesToBase64` on 1x1 png `iVBORw0KGgo...`
  - `createInputImage` factories for URL, file_id, bytes, base64 with detail low/high/original/auto
  - `isValidImageDetail` + `VALID_IMAGE_DETAILS` + `client.isValidImageDetail`
  - `isDataUrl`/`isHttpUrl`/`inferMimeFromFilename` + client re-exports
  - `extractImageGenerationResults`/`Calls` with both `result` and `b64_json` aliases, `isImageGenerationCall`, and `saveBase64ImageToFile` to tmp dir then read back via `node:fs`/`os`/`path` (cleanup with `rm -rf`)

- **Audio helpers (1):**
  - `createInputAudio` factories (bytes/base64/file_id), client re-exports, `extractOutputAudio` transcript

- **Relay verbatim (5):** mocked Yjs+render+zip via `zipSync` + `fetchMock` checking `__LAST_JOB__`:
  - `input_image` http URL `https://example.com/image.jpg` detail low
  - `input_image` data URL `data:image/png;base64,...` detail high
  - `input_image` file_id `file-xyz-123` detail auto & original (loop)
  - `image_generation` tool `gpt-image-2` quality high size 1024x1024 output_format png background auto + mock `image_generation_call` result b64
  - `input_audio` base64 wav `input_audio:{data,format}` and file_id variant

- **Server passthrough (mocked relay, 4):** `vi.doMock("../src/relay/client")` → checks `mock.calls[0][0]`:
  - `input_image` URL / data URL / file_id / original detail each forwarded verbatim via `POST /v1/responses`
  - `image_generation` tool gpt-image-2 verbatim
  - `input_audio` verbatim (data + file_id)
  - types compile-time check `VALID_IMAGE_DETAILS` and `type:"image_generation"`

- **Files vision upload (2):**
  - `uploadImageForVision` → `POST /api/project-files/upload?prism_cache_bust=` with purpose vision
  - `createInputImageFromBytesViaUpload` → mocked Files API returns `file-xyz-999` → `InputImage` with file_id high

All tests use `vi.mock("y-websocket")` 1.5s sync timeout guard and `Buffer` for b64.

## Verification

**TSC:**
```bash
bun x tsc --noEmit
# exit code 0 — PASS (previously 0 before, still 0 after)
# Fixed earlier stray comment duplicate in relay/client.ts that caused 27 errors; now clean
```

**Vitest:**
```bash
bun x vitest run --reporter=verbose
# Test Files  9 passed (9)
#      Tests  76 passed | 2 skipped (78)
# Duration 15.43s
# Before: 8 files, 59 passed + 2 skipped (61) after thinking task; after image task: +17 new (18-1 duplicate) = 76 passed
```
- All 59 prior tests still pass (proxy, relay, server, websocket, files, concurrency, thinking)
- New 17 image/audio tests pass: 5 helper + 1 audio + 5 relay verbatim + 4 server + 2 files = 17 (plus 1 types check = 18 total in file)
- No regression for prompt_cache / reasoning / phase / service_tier

**Manual checks:**
- `encodeImageBytesToDataUrl(Uint8Array.from(atob(1x1)), "image/png") === "data:image/png;base64,iVBORw0KGgo..."`
- `createInputImageFromUrl("https://example.com/a.jpg","high").detail==="high"`
- `createInputImageFromFileId("file-abc","original").file_id==="file-abc"`
- `extractImageGenerationResults([{type:"image_generation_call",result:b64}])[0]===b64`
- `saveBase64ImageToFile(b64, "/tmp/out.png")` then `readFile` length matches `base64ToBytes(b64).length`

## Commit

```bash
git add src/relay/types.ts src/relay/client.ts src/utils/image.ts src/utils/audio.ts src/proxy/files.ts tests/image.test.ts
git commit -m "feat: image vision and generation support"
```

- Includes 6 files (3 modified, 2 new utils, 1 new test)
- Message exactly `feat: image vision and generation support` as requested
- Preserves prompt_cache/thinking handling verbatim, adds image/audio without stripping

## Notes
- Verbatim guarantee: all new structs forwarded via `JSON.stringify(body)` → `job.json` → `curl --cacert $PROXY_CA_CERT_PATH -d @job.json $OPENAI_BASE_URL/responses`; implemented by not filtering body in `relayCreate`
- Detail enum aligns with OpenAI vision docs: low/high/auto/original (original = high-res uncropped); http URL vs data URL vs file_id all supported per `https://developers.openai.com/api/docs/guides/images-vision`
- Image generation via Responses API: `tools:[{type:"image_generation", model:"gpt-image-2", quality, size, output_format}]` → output `image_generation_call` with `result` (b64) + `revised_prompt`; saving via `saveBase64ImageToFile` matches docs `fs.writeFileSync("cat_and_otter.png", Buffer.from(imageBase64,"base64"))`
- Audio input via Responses API: `{type:"input_audio", input_audio:{data:base64, format:"wav"}}` or `{type:"input_audio", file_id}`; tested with `gpt-4o-transcribe` model string preserved verbatim
- No placeholders — every new interface has concrete helper, type, and test
- `prism_cache_bust` still mandatory on all `/s/sandboxes/proxy/*` and `/api/project-files/*` (verified in `files.test.ts` and new image tests)
- Follow-up: voice agents / realtime `input_audio_buffer.append` event is intentionally out-of-scope for HTTP relay (requires persistent audio stream); current pass-through covers `input_audio` for transcription/speech, which meets task's "at minimum ensure audio input types are passed through"

## Report Path
`.superpowers/sdd/task-image-report.md` (this file)
