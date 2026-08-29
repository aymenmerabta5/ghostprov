import { describe, expect, it } from "bun:test";
import {
  MAX_RESPONSES_JSON_DEPTH,
  MAX_RESPONSES_JSON_ITEMS,
  ResponsesInputJsonError,
  ResponsesInputStructureError,
  validateResponsesInputJson,
} from "../src/relay/responses-validation";
import { normalizeApiError } from "../src/http/errors";

describe("Responses nested JSON validation", () => {
  it("rejects an oversized flat input before visiting its elements", () => {
    const input = new Array<unknown>(MAX_RESPONSES_JSON_ITEMS + 1);
    Object.defineProperty(input, "0", {
      get() { throw new Error("oversized input element was read"); },
    });
    expect(() => validateResponsesInputJson({ input })).toThrow(
      ResponsesInputStructureError,
    );
  });

  it("rejects excessive depth before recursive canonicalization", () => {
    let nested: unknown = "leaf";
    for (let depth = 0; depth <= MAX_RESPONSES_JSON_DEPTH; depth++) {
      nested = { child: nested };
    }

    expect(() => validateResponsesInputJson({
      input: [{ type: "message", role: "user", content: nested }],
    })).toThrow(ResponsesInputStructureError);
  });

  it("accepts ordinary input without recursively parsing message text", () => {
    expect(() => validateResponsesInputJson({
      input: [{
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "not JSON and not required to be" }],
      }],
    })).not.toThrow();
  });

  it("accepts valid function-call argument JSON", () => {
    expect(() => validateResponsesInputJson({
      input: [{ type: "function_call", call_id: "call_1", name: "read", arguments: "{\"path\":\"a\"}" }],
    })).not.toThrow();
  });

  it("rejects malformed arguments with a safe exact input path", () => {
    let thrown: unknown;
    try {
      validateResponsesInputJson({
        input: [
          { type: "message", role: "user", content: "continue" },
          { type: "function_call", call_id: "call_1", name: "read", arguments: "{\"path\":" },
        ],
      });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(ResponsesInputJsonError);
    expect(normalizeApiError(thrown)).toEqual({
      status: 400,
      error: {
        message: "Function call arguments at input[1].arguments must contain valid JSON",
        type: "invalid_request_error",
        param: "input[1].arguments",
        code: "invalid_function_call_arguments",
      },
    });
  });

  it("rejects a non-string function-call argument field", () => {
    expect(() => validateResponsesInputJson({
      input: [{ type: "function_call", call_id: "call_1", name: "read", arguments: {} }],
    })).toThrow("must be a JSON string");
  });

  it("rejects lone Unicode surrogates before canonical request serialization", () => {
    let thrown: unknown;
    try {
      validateResponsesInputJson({
        input: [{
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "broken-\ud800-unicode" }],
        }],
      });
    } catch (error) {
      thrown = error;
    }
    expect(normalizeApiError(thrown)).toEqual({
      status: 400,
      error: {
        message: "Responses value at input[0] must contain valid Unicode scalar values",
        type: "invalid_request_error",
        param: "input[0]",
        code: "invalid_unicode",
      },
    });
  });

  it("rejects lone Unicode surrogates decoded from function arguments", () => {
    expect(() => validateResponsesInputJson({
      input: [{
        type: "function_call",
        call_id: "call_unicode",
        name: "read",
        arguments: "{\"path\":\"\\ud800\"}",
      }],
    })).toThrow("valid Unicode");
  });
});
