import { ApiError } from "../../src/http/errors";

export class ForwardingProtocolError extends Error {
  readonly name = "ForwardingProtocolError";
}

export function positiveSafeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
  return value;
}

export function abortError(): DOMException {
  return new DOMException("Verifier request was cancelled", "AbortError");
}

export function verifierClosingError(): ApiError {
  return new ApiError(503, "Claude Code verifier is shutting down", {
    type: "service_unavailable_error",
    code: "verifier_shutting_down",
  });
}

export function verifierOverloadedError(): ApiError {
  return new ApiError(503, "Claude Code verifier is overloaded", {
    type: "overloaded_error",
    code: "verifier_overloaded",
  });
}

export function verifierForwardTimeoutError(): ApiError {
  return new ApiError(504, "Claude Code verifier upstream request timed out", {
    type: "timeout_error",
    code: "verifier_forward_timeout",
  });
}
