/**
 * OpenAI Responses `function_call.arguments` is itself a JSON string. The
 * outer request may therefore be valid JSON while a nested arguments payload
 * is not. Validate only the top-level input items which can carry that field;
 * do not recursively walk large message content or tool output.
 */
export class ResponsesInputJsonError extends Error {
  readonly name = "ResponsesInputJsonError";
  readonly status = 400;
  readonly code = "invalid_function_call_arguments";
  readonly param: string;

  constructor(index: number, reason: "type" | "syntax") {
    const param = `input[${index}].arguments`;
    super(reason === "type"
      ? `Function call arguments at ${param} must be a JSON string`
      : `Function call arguments at ${param} must contain valid JSON`);
    this.param = param;
  }
}

export function validateResponsesInputJson(body: Record<string, unknown>): void {
  const input = body.input;
  if (!Array.isArray(input)) return;

  for (let index = 0; index < input.length; index++) {
    const item = input[index];
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    const value = item as Record<string, unknown>;
    if (value.type !== "function_call") continue;
    if (typeof value.arguments !== "string") {
      throw new ResponsesInputJsonError(index, "type");
    }
    try {
      JSON.parse(value.arguments);
    } catch {
      throw new ResponsesInputJsonError(index, "syntax");
    }
  }
}
