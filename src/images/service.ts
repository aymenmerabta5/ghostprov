import { ApiError } from "../http/errors";
import type { RunHandle } from "../relay/run-handle";
import { ImageBatchResultMemory } from "./result-memory";
import {
  releaseAtomicBytePermit,
  transferAtomicBytePermit,
} from "../v2/memory/permit-carrier";

export type ImageRelayStarter = (
  body: Record<string, unknown>,
  signal?: AbortSignal,
) => Promise<RunHandle>;

export interface ImagesResponseImage {
  b64_json: string;
  revised_prompt?: string;
}

export interface ImagesUsage {
  input_tokens: number;
  input_tokens_details: {
    image_tokens: number;
    text_tokens: number;
  };
  output_tokens: number;
  output_tokens_details: {
    image_tokens: number;
    text_tokens: number;
  };
  total_tokens: number;
}

export interface ImagesResponse {
  created: number;
  data: ImagesResponseImage[];
  background?: "transparent" | "opaque" | "auto";
  output_format?: "png" | "jpeg" | "webp";
  quality?: "low" | "medium" | "high" | "auto";
  size?: string;
  usage?: ImagesUsage;
}

interface ExtractedImage {
  createdAt: number;
  image: ImagesResponseImage;
  background?: ImagesResponse["background"];
  outputFormat?: ImagesResponse["output_format"];
  quality?: ImagesResponse["quality"];
  size?: string;
  usage?: ImagesUsage;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function numeric(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function usageFromFinal(final: Record<string, unknown>): ImagesUsage | undefined {
  const toolUsage = record(final.tool_usage);
  const imageGen = record(toolUsage?.image_gen);
  if (!imageGen) return undefined;
  const inputDetails = record(imageGen.input_tokens_details);
  const outputDetails = record(imageGen.output_tokens_details);
  return {
    input_tokens: numeric(imageGen.input_tokens),
    input_tokens_details: {
      image_tokens: numeric(inputDetails?.image_tokens),
      text_tokens: numeric(inputDetails?.text_tokens),
    },
    output_tokens: numeric(imageGen.output_tokens),
    output_tokens_details: {
      image_tokens: numeric(outputDetails?.image_tokens),
      text_tokens: numeric(outputDetails?.text_tokens),
    },
    total_tokens: numeric(imageGen.total_tokens),
  };
}

function enumValue<T extends string>(value: unknown, allowed: readonly T[]): T | undefined {
  return typeof value === "string" && (allowed as readonly string[]).includes(value)
    ? value as T
    : undefined;
}

function invalidUpstream(message: string): ApiError {
  return new ApiError(502, message, {
    type: "upstream_error",
    code: "invalid_upstream_response",
  });
}

function extractImage(finalValue: unknown): ExtractedImage {
  const final = record(finalValue);
  if (!final) throw invalidUpstream("Image relay returned a non-object final response.");
  if (final.status !== "completed") {
    throw invalidUpstream(`Image relay ended with status '${String(final.status)}'.`);
  }
  const output = Array.isArray(final.output) ? final.output : [];
  const calls = output
    .map(record)
    .filter((item): item is Record<string, unknown> => item?.type === "image_generation_call");
  if (calls.length !== 1) {
    throw invalidUpstream(`Image relay returned ${calls.length} image generation results; expected exactly one.`);
  }
  const call = calls[0];
  const hasResult = typeof call.result === "string" && call.result.length > 0;
  const completedWithProviderGeneratingStatus = final.status === "completed" &&
    call.status === "generating" && hasResult;
  if (
    call.status !== undefined &&
    call.status !== "completed" &&
    !completedWithProviderGeneratingStatus
  ) {
    throw invalidUpstream(`Image generation call ended with status '${String(call.status)}'.`);
  }
  if (!hasResult) {
    throw invalidUpstream("Image generation call completed without base64 result data.");
  }

  const image: ImagesResponseImage = { b64_json: call.result as string };
  if (typeof call.revised_prompt === "string" && call.revised_prompt.length > 0) {
    image.revised_prompt = call.revised_prompt;
  }
  const extracted: ExtractedImage = {
    createdAt: typeof final.created_at === "number" && Number.isFinite(final.created_at)
      ? final.created_at
      : Math.floor(Date.now() / 1000),
    image,
    background: enumValue(call.background, ["transparent", "opaque", "auto"] as const),
    outputFormat: enumValue(call.output_format, ["png", "jpeg", "webp"] as const),
    quality: enumValue(call.quality, ["low", "medium", "high", "auto"] as const),
    size: typeof call.size === "string" ? call.size : undefined,
    usage: usageFromFinal(final),
  };
  transferAtomicBytePermit(final, image, "image-compat-output");
  return extracted;
}

function addUsage(total: ImagesUsage, value: ImagesUsage): void {
  total.input_tokens += value.input_tokens;
  total.input_tokens_details.image_tokens += value.input_tokens_details.image_tokens;
  total.input_tokens_details.text_tokens += value.input_tokens_details.text_tokens;
  total.output_tokens += value.output_tokens;
  total.output_tokens_details.image_tokens += value.output_tokens_details.image_tokens;
  total.output_tokens_details.text_tokens += value.output_tokens_details.text_tokens;
  total.total_tokens += value.total_tokens;
}

function aggregate(extracted: ExtractedImage[]): ImagesResponse {
  const response: ImagesResponse = {
    created: Math.min(...extracted.map(item => item.createdAt)),
    data: extracted.map(item => item.image),
  };
  const first = extracted[0];
  if (first.background !== undefined) response.background = first.background;
  if (first.outputFormat !== undefined) response.output_format = first.outputFormat;
  if (first.quality !== undefined) response.quality = first.quality;
  if (first.size !== undefined) response.size = first.size;

  const usage: ImagesUsage = {
    input_tokens: 0,
    input_tokens_details: { image_tokens: 0, text_tokens: 0 },
    output_tokens: 0,
    output_tokens_details: { image_tokens: 0, text_tokens: 0 },
    total_tokens: 0,
  };
  let hasUsage = false;
  for (const item of extracted) {
    if (!item.usage) continue;
    hasUsage = true;
    addUsage(usage, item.usage);
  }
  if (hasUsage) response.usage = usage;
  return response;
}

async function finalWithDrainedEvents(
  handle: RunHandle,
  signal?: AbortSignal,
): Promise<unknown> {
  const iterator = handle.events();
  let settledFinal: unknown = null;
  const final = Promise.resolve(handle.final).then(value => {
    settledFinal = value;
    return value;
  });
  const releaseFinal = (): void => {
    if (settledFinal) releaseAtomicBytePermit(settledFinal);
    else void final.then(releaseAtomicBytePermit, () => undefined);
  };
  const returnIterator = (): void => {
    try {
      void Promise.resolve(iterator.return(undefined)).catch(() => undefined);
    } catch {
      // Cleanup is best-effort and must never replace the authoritative error.
    }
  };
  const drain = (async () => {
    for await (const _event of iterator) {
      // Image compatibility returns only the authoritative final object, but
      // V2's bounded event buffer must still be consumed concurrently.
    }
  })();
  const work = Promise.all([final, drain])
    .then(([final]) => final);
  if (!signal) {
    try {
      return await work;
    } catch (error) {
      releaseFinal();
      returnIterator();
      throw error;
    }
  }

  let onAbort: (() => void) | undefined;
  const cancelled = new Promise<never>((_resolve, reject) => {
    const abort = () => reject(new DOMException("Image relay was aborted", "AbortError"));
    if (signal.aborted) abort();
    else {
      onAbort = abort;
      signal.addEventListener("abort", abort, { once: true });
    }
  });
  try {
    return await Promise.race([work, cancelled]);
  } catch (error) {
    releaseFinal();
    returnIterator();
    // A non-cooperative handle must not create an unhandled rejection after
    // downstream cancellation wins the race.
    void work.catch(() => undefined);
    throw error;
  } finally {
    if (onAbort) signal.removeEventListener("abort", onAbort);
  }
}

export async function executeImageBatch(
  bodies: Record<string, unknown>[],
  startRelay: ImageRelayStarter,
  signal?: AbortSignal,
  resultMemory?: ImageBatchResultMemory,
): Promise<ImagesResponse> {
  if (bodies.length === 0) throw invalidUpstream("Image batch contained no relay requests.");
  const retainedResults = bodies.length > 1
    ? resultMemory ?? new ImageBatchResultMemory(bodies.length)
    : undefined;

  const settled = await Promise.allSettled(bodies.map(async body => {
    const handle = await startRelay(body, signal);
    const final = await finalWithDrainedEvents(handle, signal);
    try {
      // A completed sibling cannot keep the shared jumbo event lane while
      // another image still needs it. Preserve its charge outside that lane
      // before waiting for the rest of this batch.
      retainedResults?.retain(final);
      return final;
    } catch (error) {
      releaseAtomicBytePermit(final);
      throw error;
    }
  }));
  const rejected = settled.find(
    (result): result is PromiseRejectedResult => result.status === "rejected",
  );
  const fulfilled = settled.filter(
    (result): result is PromiseFulfilledResult<unknown> => result.status === "fulfilled",
  );
  if (rejected) {
    for (const result of fulfilled) releaseAtomicBytePermit(result.value);
    retainedResults?.release();
    throw rejected.reason;
  }

  const extracted: ExtractedImage[] = [];
  try {
    for (const result of fulfilled) extracted.push(extractImage(result.value));
    return aggregate(extracted);
  } catch (error) {
    // Some finals may already have transferred ownership into image outputs;
    // release both carrier locations idempotently to cover a mixed batch.
    for (const result of fulfilled) releaseAtomicBytePermit(result.value);
    for (const item of extracted) releaseAtomicBytePermit(item.image);
    retainedResults?.release();
    throw error;
  }
}
