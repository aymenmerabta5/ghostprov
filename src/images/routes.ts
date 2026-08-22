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

export interface ImageRouteDependencies {
  options: ImageCompatOptions;
  startRelay: ImageRelayStarter;
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
      const response = await executeImageBatch(
        buildGenerationBodies(parsed),
        dependencies.startRelay,
        c.req.raw.signal,
      );
      return c.json(response);
    } catch (error) {
      return apiErrorResponse(error);
    }
  });

  app.post("/v1/images/edits", async c => {
    try {
      let form: FormData;
      try {
        form = await c.req.formData();
      } catch {
        throw invalidBody("Request body is not valid multipart form data.", "invalid_multipart_form");
      }
      const parsed = await parseEditRequest(form, dependencies.options);
      const response = await executeImageBatch(
        buildEditBodies(parsed),
        dependencies.startRelay,
        c.req.raw.signal,
      );
      return c.json(response);
    } catch (error) {
      return apiErrorResponse(error);
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
