import type { Hono } from "hono";
import { ApiError, apiErrorResponse } from "../http/errors";
import {
  buildEditBodies,
  buildGenerationBodies,
  parseEditRequest,
  parseGenerationRequest,
  type ImageCompatOptions,
} from "./compat";
import { executeImageBatch, type ImageRelayStarter } from "./service";
import type { IncomingResourceLease } from "../router/router";
import { resolveHttpClientId, takeIncomingBodyLease } from "../http/request-lease";
import { permittedJsonResponse } from "../http/permitted-json";
import { jsonEncodedByteLengthAsync } from "../router/workload";

export interface ImageRouteDependencies {
  options: ImageCompatOptions;
  startRelay: (
    body: Record<string, unknown>,
    signal: AbortSignal | undefined,
    clientId: string,
    incomingLease?: IncomingResourceLease,
    retainMemoryUntil?: Promise<unknown>,
    structuralBodyBytes?: number,
  ) => ReturnType<ImageRelayStarter>;
}

function relayStarterForRequest(
  dependencies: ImageRouteDependencies,
  env: unknown,
  explicitClientId?: string,
  retainMemoryUntil?: Promise<unknown>,
  initialIncomingLease?: IncomingResourceLease,
): ImageRelayStarter {
  let incomingLease = initialIncomingLease ?? takeIncomingBodyLease(env);
  let structuralBodyBytes: Promise<number> | null = null;
  const clientId = resolveHttpClientId(env, explicitClientId);
  return async (body, signal) => {
    // Promise.all invokes each mapper synchronously up to this call. Clearing
    // before dispatch makes exactly one fan-out job the handoff owner even
    // when all relay promises are pending concurrently.
    const handoff = incomingLease;
    incomingLease = undefined;
    structuralBodyBytes ??= jsonEncodedByteLengthAsync(body, { signal });
    let measuredBytes: number;
    try {
      measuredBytes = await structuralBodyBytes;
    } catch (error) {
      handoff?.release();
      throw error;
    }
    return dependencies.startRelay(
      body,
      signal,
      clientId,
      handoff,
      handoff ? retainMemoryUntil : undefined,
      measuredBytes,
    );
  };
}

function invalidBody(message: string, code: string): ApiError {
  return new ApiError(400, message, {
    type: "invalid_request_error",
    param: "body",
    code,
  });
}

function unsupported(message: string): Response {
  return apiErrorResponse(new ApiError(501, message, {
    type: "unsupported_feature",
    code: "unsupported_feature",
  }));
}

export function registerImageRoutes(app: Hono, dependencies: ImageRouteDependencies): void {
  app.post("/v1/images/generations", async c => {
    try {
      let value: unknown;
      try {
        value = await c.req.json();
      } catch {
        throw invalidBody("Request body is not valid JSON.", "invalid_json");
      }
      const parsed = parseGenerationRequest(value, dependencies.options);
      const bodies = buildGenerationBodies(parsed);
      let finishBatch!: () => void;
      const batchLifetime = new Promise<void>(resolve => { finishBatch = resolve; });
      try {
        const response = await executeImageBatch(
          bodies,
          relayStarterForRequest(
            dependencies,
            c.env,
            c.req.header("x-ghostprov-client-id"),
            batchLifetime,
          ),
          c.req.raw.signal,
        );
        return permittedJsonResponse(response, { owners: response.data });
      } finally {
        finishBatch();
      }
    } catch (error) {
      return apiErrorResponse(error);
    }
  });

  app.post("/v1/images/edits", async c => {
    const incomingLease = takeIncomingBodyLease(c.env);
    try {
      if (c.req.raw.signal.aborted) throw c.req.raw.signal.reason;
      let form: FormData;
      try {
        form = await c.req.formData();
      } catch {
        throw invalidBody("Request body is not valid multipart form data.", "invalid_multipart_form");
      }
      const parsed = await parseEditRequest(form, dependencies.options);
      if (c.req.raw.signal.aborted) throw c.req.raw.signal.reason;
      const bodies = buildEditBodies(parsed);
      let finishBatch!: () => void;
      const batchLifetime = new Promise<void>(resolve => { finishBatch = resolve; });
      try {
        const response = await executeImageBatch(
          bodies,
          relayStarterForRequest(
            dependencies,
            c.env,
            c.req.header("x-ghostprov-client-id"),
            batchLifetime,
            incomingLease,
          ),
          c.req.raw.signal,
        );
        return permittedJsonResponse(response, { owners: response.data });
      } finally {
        finishBatch();
      }
    } catch (error) {
      return apiErrorResponse(error);
    } finally {
      incomingLease?.release();
    }
  });

  app.post("/v1/images/variations", () => unsupported(
    "Image variations are unavailable through the configured Responses-only upstream; use /v1/images/edits with gpt-image-2.",
  ));
  app.post("/v1/audio/speech", () => unsupported(
    "Audio output is unavailable through the configured Responses-only upstream.",
  ));
  app.post("/v1/audio/transcriptions", () => unsupported(
    "Audio input is unavailable through the configured Responses-only upstream.",
  ));
  app.post("/v1/audio/translations", () => unsupported(
    "Audio input is unavailable through the configured Responses-only upstream.",
  ));
}
