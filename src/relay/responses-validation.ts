/**
 * OpenAI Responses `function_call.arguments` is itself a JSON string. The
 * outer request may therefore be valid JSON while a nested arguments payload
 * is not. The provider also rejects lone UTF-16 surrogates even though
 * JavaScript JSON.stringify can escape them, so the same preflight enforces
 * well-formed Unicode without copying request strings.
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

export class ResponsesInputUnicodeError extends Error {
  readonly name = "ResponsesInputUnicodeError";
  readonly status = 400;
  readonly code = "invalid_unicode";

  constructor(readonly param: string) {
    super(`Responses value at ${param} must contain valid Unicode scalar values`);
  }
}

export class ResponsesInputStructureError extends Error {
  readonly name = "ResponsesInputStructureError";
  readonly status = 400;
  readonly code = "invalid_json_structure";

  constructor(readonly param: string, readonly reason: "depth" | "items" = "depth") {
    super(reason === "depth"
      ? `Responses value at ${param} exceeds the maximum JSON nesting depth`
      : `Responses value at ${param} contains too many JSON values`);
  }
}

/** Keeps later canonicalization/stringification paths below call-stack limits. */
export const MAX_RESPONSES_JSON_DEPTH = 256;
export const MAX_RESPONSES_JSON_ITEMS = 65_536;

const SURROGATE = /[\ud800-\udfff]/;

function hasLoneSurrogate(value: string): boolean {
  const native = (value as string & { isWellFormed?: () => boolean }).isWellFormed;
  if (typeof native === "function") return !native.call(value);
  if (!SURROGATE.test(value)) return false;
  for (let index = 0; index < value.length; index++) {
    const unit = value.charCodeAt(index);
    if (unit < 0xd800 || unit > 0xdfff) continue;
    if (unit <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (low >= 0xdc00 && low <= 0xdfff) {
        index++;
        continue;
      }
    }
    return true;
  }
  return false;
}

function validateUnicodeTree(
  root: unknown,
  param: string,
  budget: { remaining: number },
): void {
  const pending: Array<{ value: unknown; depth: number }> = [{ value: root, depth: 0 }];
  const seen = new WeakSet<object>();
  while (pending.length > 0) {
    const { value, depth } = pending.pop()!;
    if (--budget.remaining < 0) throw new ResponsesInputStructureError(param, "items");
    if (typeof value === "string") {
      if (hasLoneSurrogate(value)) throw new ResponsesInputUnicodeError(param);
      continue;
    }
    if (!value || typeof value !== "object") continue;
    if (depth > MAX_RESPONSES_JSON_DEPTH) throw new ResponsesInputStructureError(param);
    if (seen.has(value)) continue;
    seen.add(value);
    if (Array.isArray(value)) {
      if (value.length > budget.remaining) {
        throw new ResponsesInputStructureError(param, "items");
      }
      for (let index = 0; index < value.length; index++) {
        pending.push({ value: value[index], depth: depth + 1 });
      }
      continue;
    }
    for (const [key, child] of Object.entries(value)) {
      if (hasLoneSurrogate(key)) throw new ResponsesInputUnicodeError(param);
      pending.push({ value: child, depth: depth + 1 });
    }
  }
}

export function validateResponsesInputJson(body: Record<string, unknown>): void {
  const input = body.input;
  const bodyBudget = { remaining: MAX_RESPONSES_JSON_ITEMS };
  for (const [key, value] of Object.entries(body)) {
    if (key === "input" && Array.isArray(value)) {
      if (value.length > bodyBudget.remaining) {
        throw new ResponsesInputStructureError("input", "items");
      }
      for (let index = 0; index < value.length; index++) {
        validateUnicodeTree(value[index], `input[${index}]`, bodyBudget);
      }
    } else {
      validateUnicodeTree(value, key, bodyBudget);
    }
  }
  if (!Array.isArray(input)) return;

  for (let index = 0; index < input.length; index++) {
    const item = input[index];
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    const value = item as Record<string, unknown>;
    if (value.type !== "function_call") continue;
    if (typeof value.arguments !== "string") {
      throw new ResponsesInputJsonError(index, "type");
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(value.arguments);
    } catch {
      throw new ResponsesInputJsonError(index, "syntax");
    }
    validateUnicodeTree(
      parsed,
      `input[${index}].arguments`,
      { remaining: MAX_RESPONSES_JSON_ITEMS },
    );
  }
}
