import type { Hono } from "hono";
import { ApiError, apiErrorResponse } from "./errors";
import { permittedJsonResponse } from "./permitted-json";
import { releaseAtomicBytePermit } from "../v2/memory/permit-carrier";
import {
  MAX_RESPONSES_JSON_DEPTH,
  MAX_RESPONSES_JSON_ITEMS,
  validateResponsesInputJson,
} from "../relay/responses-validation";
import { jsonEncodedByteLengthAsync } from "../router/workload";
import type { IncomingResourceLease } from "../router/router";
import type { AuthenticatedTenantScope } from "./api-key";
import {
  linkIncomingLeaseAbort,
  resolveHttpClientId,
  takeIncomingBodyLease,
} from "./request-lease";
import {
  STANDALONE_SEARCH_MAX_REQUEST_BYTES,
  STANDALONE_SEARCH_MAX_RESPONSE_BYTES,
} from "../search/limits";

export const STANDALONE_SEARCH_PATH = "/v1/alpha/search";
export {
  STANDALONE_SEARCH_MAX_REQUEST_BYTES,
  STANDALONE_SEARCH_MAX_RESPONSE_BYTES,
};

const MAX_COMMAND_ITEMS = 64;
const MAX_INPUT_ITEMS = 4_096;
const MAX_DOMAINS = 64;
const MAX_OUTPUT_TOKENS = 100_000;
const MAX_RETRY_AFTER_SECONDS = 2_147_483_647;
const UTC_OFFSET = /^[+-](?:0\d|1[0-4]):[0-5]\d$/;
const IMF_FIXDATE = /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} GMT$/;

export type StandaloneSearchJsonPrimitive = string | number | boolean | null;
export type StandaloneSearchJsonValue =
  | StandaloneSearchJsonPrimitive
  | StandaloneSearchJsonObject
  | readonly StandaloneSearchJsonValue[];
export type StandaloneSearchJsonObject = Readonly<{
  [key: string]: StandaloneSearchJsonValue;
}>;

export interface StandaloneSearchQuery {
  readonly q: string;
  readonly recency?: number;
  readonly domains?: readonly string[];
}

export interface StandaloneSearchOpenCommand {
  readonly ref_id: string;
  readonly lineno?: number;
}

export interface StandaloneSearchClickCommand {
  readonly ref_id: string;
  readonly id: number;
}

export interface StandaloneSearchFindCommand {
  readonly ref_id: string;
  readonly pattern: string;
}

export interface StandaloneSearchScreenshotCommand {
  readonly ref_id: string;
  readonly pageno: number;
}

export interface StandaloneSearchFinanceCommand {
  readonly ticker: string;
  readonly type: "equity" | "fund" | "crypto" | "index";
  readonly market?: string;
}

export interface StandaloneSearchWeatherCommand {
  readonly location: string;
  readonly start?: string;
  readonly duration?: number;
}

export interface StandaloneSearchSportsCommand {
  readonly fn: "schedule" | "standings";
  readonly league:
    | "nba" | "wnba" | "nfl" | "nhl" | "mlb"
    | "epl" | "ncaamb" | "ncaawb" | "ipl";
  readonly tool?: "sports";
  readonly team?: string;
  readonly opponent?: string;
  readonly date_from?: string;
  readonly date_to?: string;
  readonly num_games?: number;
  readonly locale?: string;
}

export interface StandaloneSearchTimeCommand {
  readonly utc_offset: string;
}

export interface StandaloneSearchCommands {
  readonly search_query?: readonly StandaloneSearchQuery[];
  readonly image_query?: readonly StandaloneSearchQuery[];
  readonly open?: readonly StandaloneSearchOpenCommand[];
  readonly click?: readonly StandaloneSearchClickCommand[];
  readonly find?: readonly StandaloneSearchFindCommand[];
  readonly screenshot?: readonly StandaloneSearchScreenshotCommand[];
  readonly finance?: readonly StandaloneSearchFinanceCommand[];
  readonly weather?: readonly StandaloneSearchWeatherCommand[];
  readonly sports?: readonly StandaloneSearchSportsCommand[];
  readonly time?: readonly StandaloneSearchTimeCommand[];
  readonly response_length?: "short" | "medium" | "long";
}

export interface StandaloneSearchUserLocation {
  readonly type: "approximate";
  readonly country?: string;
  readonly region?: string;
  readonly city?: string;
  readonly timezone?: string;
}

export interface StandaloneSearchFilters {
  readonly allowed_domains?: readonly string[];
  readonly blocked_domains?: readonly string[];
}

export interface StandaloneSearchImageSettings {
  readonly max_results?: number;
  readonly caption?: boolean;
}

export interface StandaloneSearchSettings {
  readonly utc_offset?: string;
  readonly user_location?: StandaloneSearchUserLocation;
  readonly search_context_size?: "low" | "medium" | "high";
  readonly filters?: StandaloneSearchFilters;
  readonly image_settings?: StandaloneSearchImageSettings;
  readonly allowed_callers?: readonly ("direct" | "shell" | "code_interpreter")[];
  readonly external_web_access?: boolean | "cached" | "indexed" | "live";
}

export interface StandaloneSearchRequest {
  readonly id: string;
  readonly model: string;
  readonly input?: string | readonly StandaloneSearchJsonObject[] | null;
  readonly commands?: StandaloneSearchCommands | null;
  readonly settings?: StandaloneSearchSettings | null;
  readonly max_output_tokens?: number | null;
  readonly reasoning?: StandaloneSearchJsonObject | null;
}

export type StandaloneSearchResponse = StandaloneSearchJsonObject & Readonly<{ output: string }>;

export type StandaloneSearchHandlerResult =
  | Readonly<{
      kind: "success";
      /** Upstream 2xx status. A JSON-bearing 204/205 is intentionally rejected. */
      status: number;
      body: StandaloneSearchResponse;
    }>
  | Readonly<{
      kind: "failure";
      /** Credential-scrubbed upstream 4xx/5xx status and JSON error body. */
      status: number;
      body: StandaloneSearchJsonObject;
      /** The only upstream response header this surface accepts. */
      retryAfter?: string;
    }>;

export interface StandaloneSearchHandlerContext {
  readonly signal: AbortSignal;
  /** Lease remains route-owned until `transferIncomingLease` is called. */
  readonly incomingLease?: IncomingResourceLease;
  /** One-shot router handoff. The new owner must release on every failure path. */
  readonly transferIncomingLease: () => IncomingResourceLease | undefined;
  readonly clientId: string;
  readonly authorizationScope: AuthenticatedTenantScope | null;
  readonly encodedBodyBytes: number;
  readonly structuralBodyBytes: number;
}

export type StandaloneSearchHandler = (
  body: StandaloneSearchRequest,
  context: Readonly<StandaloneSearchHandlerContext>,
) => StandaloneSearchHandlerResult | Promise<StandaloneSearchHandlerResult>;

export interface StandaloneSearchRouteOptions {
  readonly handler?: StandaloneSearchHandler;
  readonly authorizationScope: AuthenticatedTenantScope | null;
}

type ContractFailure = (message: string, param: string, code: string) => never;

function requestFailure(message: string, param: string, code: string): never {
  throw new ApiError(400, message, {
    type: "invalid_request_error",
    param,
    code,
  });
}

function upstreamContractFailure(): never {
  throw new ApiError(502, "Standalone search returned an invalid response.", {
    type: "upstream_error",
    code: "invalid_search_response",
  });
}

function unavailable(): Response {
  return apiErrorResponse(new ApiError(503, "Standalone search is unavailable.", {
    type: "service_unavailable_error",
    code: "standalone_search_unavailable",
  }));
}

function utf8Bytes(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

function wellFormed(value: string): boolean {
  const native = (value as string & { isWellFormed?: () => boolean }).isWellFormed;
  if (typeof native === "function") return native.call(value);
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
    return false;
  }
  return true;
}

/** Validates a genuine finite JSON tree and freezes it before it crosses a seam. */
function validateAndFreezeJsonTree(
  root: unknown,
  fail: ContractFailure,
  param: string,
): void {
  const pending: Array<{ value: unknown; depth: number }> = [{ value: root, depth: 0 }];
  const seen = new WeakSet<object>();
  let items = 0;
  while (pending.length > 0) {
    const { value, depth } = pending.pop()!;
    if (++items > MAX_RESPONSES_JSON_ITEMS) {
      fail("JSON value contains too many items.", param, "invalid_json_structure");
    }
    if (value === null || typeof value === "boolean") continue;
    if (typeof value === "number") {
      if (!Number.isFinite(value)) fail("JSON numbers must be finite.", param, "invalid_type");
      continue;
    }
    if (typeof value === "string") {
      if (!wellFormed(value)) {
        fail("JSON strings must contain valid Unicode scalar values.", param, "invalid_unicode");
      }
      continue;
    }
    if (!value || typeof value !== "object") {
      fail("Value must contain only JSON data.", param, "invalid_type");
    }
    if (depth > MAX_RESPONSES_JSON_DEPTH) {
      fail("JSON value exceeds the maximum nesting depth.", param, "invalid_json_structure");
    }
    if (seen.has(value)) {
      fail("Value must be a JSON tree without cycles or aliases.", param, "invalid_json_structure");
    }
    seen.add(value);

    const array = Array.isArray(value);
    const prototype = Object.getPrototypeOf(value);
    if (array ? prototype !== Array.prototype : prototype !== Object.prototype && prototype !== null) {
      fail("Value must contain only plain JSON arrays and objects.", param, "invalid_type");
    }
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Reflect.ownKeys(descriptors);
    if (array) {
      const values = value as unknown[];
      if (values.length > MAX_RESPONSES_JSON_ITEMS - items) {
        fail("JSON value contains too many items.", param, "invalid_json_structure");
      }
      for (const key of keys) {
        if (key === "length") continue;
        if (typeof key !== "string" || !/^(?:0|[1-9]\d*)$/.test(key)) {
          fail("JSON arrays cannot contain named properties.", param, "invalid_type");
        }
        const index = Number(key);
        const descriptor = descriptors[key];
        if (!Number.isSafeInteger(index) || index >= values.length || !descriptor?.enumerable || !("value" in descriptor)) {
          fail("JSON arrays must contain ordinary indexed values.", param, "invalid_type");
        }
      }
      for (let index = 0; index < values.length; index++) {
        if (!Object.prototype.hasOwnProperty.call(descriptors, String(index))) {
          fail("JSON arrays cannot contain holes.", param, "invalid_type");
        }
        pending.push({ value: descriptors[String(index)]!.value, depth: depth + 1 });
      }
    } else {
      for (const key of keys) {
        if (typeof key !== "string") {
          fail("JSON objects cannot contain symbol properties.", param, "invalid_type");
        }
        if (!wellFormed(key)) {
          fail("JSON object keys must contain valid Unicode scalar values.", param, "invalid_unicode");
        }
        const descriptor = descriptors[key];
        if (!descriptor?.enumerable || !("value" in descriptor)) {
          fail("JSON objects must contain ordinary data properties.", param, "invalid_type");
        }
        pending.push({ value: descriptor.value, depth: depth + 1 });
      }
    }
    try {
      Object.freeze(value);
    } catch {
      fail("JSON value could not be made immutable.", param, "invalid_type");
    }
  }
}

function record(value: unknown, param: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    requestFailure(`${param} must be a JSON object.`, param, "invalid_type");
  }
  return value as Record<string, unknown>;
}

function exactKeys(value: Record<string, unknown>, allowed: ReadonlySet<string>, param: string): void {
  if (Object.keys(value).some(key => !allowed.has(key))) {
    requestFailure(`${param} contains an unsupported field.`, param, "unknown_parameter");
  }
}

function text(
  value: unknown,
  param: string,
  maxBytes: number,
  options: { allowEmpty?: boolean; pattern?: RegExp } = {},
): string {
  if (
    typeof value !== "string" ||
    (!options.allowEmpty && value.length === 0) ||
    utf8Bytes(value) > maxBytes ||
    (options.pattern && !options.pattern.test(value))
  ) {
    requestFailure(`${param} is invalid.`, param, "invalid_value");
  }
  return value;
}

function integer(value: unknown, param: string, min: number, max: number): number {
  if (!Number.isSafeInteger(value) || (value as number) < min || (value as number) > max) {
    requestFailure(`${param} must be an integer from ${min} through ${max}.`, param, "invalid_value");
  }
  return value as number;
}

function optionalText(
  value: Record<string, unknown>,
  key: string,
  param: string,
  maxBytes: number,
  options?: { allowEmpty?: boolean; pattern?: RegExp },
): void {
  if (Object.prototype.hasOwnProperty.call(value, key)) {
    text(value[key], `${param}.${key}`, maxBytes, options);
  }
}

function optionalInteger(
  value: Record<string, unknown>,
  key: string,
  param: string,
  min: number,
  max: number,
): void {
  if (Object.prototype.hasOwnProperty.call(value, key)) {
    integer(value[key], `${param}.${key}`, min, max);
  }
}

function enumValue<T extends string>(
  value: unknown,
  param: string,
  allowed: ReadonlySet<T>,
): T {
  if (typeof value !== "string" || !allowed.has(value as T)) {
    requestFailure(`${param} is invalid.`, param, "invalid_value");
  }
  return value as T;
}

function commandArray(
  value: unknown,
  param: string,
  validate: (item: Record<string, unknown>, param: string) => void,
): number {
  if (!Array.isArray(value) || value.length > MAX_COMMAND_ITEMS) {
    requestFailure(`${param} must be a bounded array.`, param, "invalid_value");
  }
  value.forEach((item, index) => validate(record(item, `${param}[${index}]`), `${param}[${index}]`));
  return value.length;
}

function domains(value: unknown, param: string): void {
  if (!Array.isArray(value) || value.length > MAX_DOMAINS) {
    requestFailure(`${param} must be a bounded domain array.`, param, "invalid_value");
  }
  for (let index = 0; index < value.length; index++) {
    text(value[index], `${param}[${index}]`, 253, { allowEmpty: true });
  }
}

function query(value: Record<string, unknown>, param: string): void {
  exactKeys(value, new Set(["q", "recency", "domains"]), param);
  text(value.q, `${param}.q`, 4_096, { allowEmpty: true });
  optionalInteger(value, "recency", param, 0, 36_500);
  if (Object.prototype.hasOwnProperty.call(value, "domains")) domains(value.domains, `${param}.domains`);
}

function ref(value: Record<string, unknown>, param: string): void {
  text(value.ref_id, `${param}.ref_id`, 512);
}

function validateCommands(value: Record<string, unknown>): void {
  const allowed = new Set([
    "search_query", "image_query", "open", "click", "find", "screenshot",
    "finance", "weather", "sports", "time", "response_length",
  ]);
  exactKeys(value, allowed, "commands");
  let items = 0;
  if ("search_query" in value) items += commandArray(value.search_query, "commands.search_query", query);
  if ("image_query" in value) items += commandArray(value.image_query, "commands.image_query", query);
  if ("open" in value) items += commandArray(value.open, "commands.open", (item, param) => {
    exactKeys(item, new Set(["ref_id", "lineno"]), param);
    ref(item, param);
    optionalInteger(item, "lineno", param, 0, Number.MAX_SAFE_INTEGER);
  });
  if ("click" in value) items += commandArray(value.click, "commands.click", (item, param) => {
    exactKeys(item, new Set(["ref_id", "id"]), param);
    ref(item, param);
    integer(item.id, `${param}.id`, 0, Number.MAX_SAFE_INTEGER);
  });
  if ("find" in value) items += commandArray(value.find, "commands.find", (item, param) => {
    exactKeys(item, new Set(["ref_id", "pattern"]), param);
    ref(item, param);
    text(item.pattern, `${param}.pattern`, 4_096);
  });
  if ("screenshot" in value) items += commandArray(value.screenshot, "commands.screenshot", (item, param) => {
    exactKeys(item, new Set(["ref_id", "pageno"]), param);
    ref(item, param);
    integer(item.pageno, `${param}.pageno`, 0, Number.MAX_SAFE_INTEGER);
  });
  if ("finance" in value) items += commandArray(value.finance, "commands.finance", (item, param) => {
    exactKeys(item, new Set(["ticker", "type", "market"]), param);
    text(item.ticker, `${param}.ticker`, 32, { pattern: /^[A-Za-z0-9][A-Za-z0-9._:-]{0,31}$/ });
    enumValue(item.type, `${param}.type`, new Set(["equity", "fund", "crypto", "index"]));
    optionalText(item, "market", param, 32, { allowEmpty: true });
  });
  if ("weather" in value) items += commandArray(value.weather, "commands.weather", (item, param) => {
    exactKeys(item, new Set(["location", "start", "duration"]), param);
    text(item.location, `${param}.location`, 512);
    optionalText(item, "start", param, 32);
    optionalInteger(item, "duration", param, 0, 366);
  });
  if ("sports" in value) items += commandArray(value.sports, "commands.sports", (item, param) => {
    exactKeys(item, new Set([
      "fn", "league", "tool", "team", "opponent", "date_from", "date_to",
      "num_games", "locale",
    ]), param);
    enumValue(item.fn, `${param}.fn`, new Set(["schedule", "standings"]));
    enumValue(item.league, `${param}.league`, new Set([
      "nba", "wnba", "nfl", "nhl", "mlb", "epl", "ncaamb", "ncaawb", "ipl",
    ]));
    if ("tool" in item) enumValue(item.tool, `${param}.tool`, new Set(["sports"]));
    for (const key of ["team", "opponent", "date_from", "date_to", "locale"]) {
      optionalText(item, key, param, 64);
    }
    optionalInteger(item, "num_games", param, 1, 100);
  });
  if ("time" in value) items += commandArray(value.time, "commands.time", (item, param) => {
    exactKeys(item, new Set(["utc_offset"]), param);
    text(item.utc_offset, `${param}.utc_offset`, 6, { pattern: UTC_OFFSET });
  });
  if ("response_length" in value) {
    enumValue(value.response_length, "commands.response_length", new Set(["short", "medium", "long"]));
  }
  if (items > MAX_COMMAND_ITEMS) {
    requestFailure("commands must contain at most 64 command items.", "commands", "invalid_value");
  }
}

function validateSettings(value: Record<string, unknown>): void {
  exactKeys(value, new Set([
    "utc_offset", "user_location", "search_context_size", "filters",
    "image_settings", "allowed_callers", "external_web_access",
  ]), "settings");
  optionalText(value, "utc_offset", "settings", 6, { pattern: UTC_OFFSET });
  if ("search_context_size" in value) {
    enumValue(value.search_context_size, "settings.search_context_size", new Set(["low", "medium", "high"]));
  }
  if (
    "external_web_access" in value &&
    typeof value.external_web_access !== "boolean" &&
    !new Set(["cached", "indexed", "live"]).has(value.external_web_access as string)
  ) {
    requestFailure(
      "settings.external_web_access must be a boolean or supported search mode.",
      "settings.external_web_access",
      "invalid_type",
    );
  }
  if ("allowed_callers" in value) {
    const callers = value.allowed_callers;
    const allowed = new Set(["direct", "shell", "code_interpreter"]);
    if (!Array.isArray(callers) || callers.length > 3 ||
      callers.some(caller => typeof caller !== "string" || !allowed.has(caller)) ||
      new Set(callers).size !== callers.length) {
      requestFailure(
        "settings.allowed_callers contains an unsupported or duplicate caller.",
        "settings.allowed_callers",
        "invalid_value",
      );
    }
  }
  if ("filters" in value) {
    const filters = record(value.filters, "settings.filters");
    exactKeys(filters, new Set(["allowed_domains", "blocked_domains"]), "settings.filters");
    if ("allowed_domains" in filters) domains(filters.allowed_domains, "settings.filters.allowed_domains");
    if ("blocked_domains" in filters) domains(filters.blocked_domains, "settings.filters.blocked_domains");
  }
  if ("image_settings" in value) {
    const image = record(value.image_settings, "settings.image_settings");
    exactKeys(image, new Set(["max_results", "caption"]), "settings.image_settings");
    optionalInteger(image, "max_results", "settings.image_settings", 1, 100);
    if ("caption" in image && typeof image.caption !== "boolean") {
      requestFailure("settings.image_settings.caption must be a boolean.", "settings.image_settings.caption", "invalid_type");
    }
  }
  if ("user_location" in value) {
    const location = record(value.user_location, "settings.user_location");
    exactKeys(location, new Set(["type", "country", "region", "city", "timezone"]), "settings.user_location");
    if (location.type !== "approximate") {
      requestFailure("settings.user_location.type must be approximate.", "settings.user_location.type", "invalid_value");
    }
    optionalText(location, "country", "settings.user_location", 2, { pattern: /^[A-Z]{2}$/ });
    optionalText(location, "region", "settings.user_location", 128);
    optionalText(location, "city", "settings.user_location", 128);
    optionalText(location, "timezone", "settings.user_location", 128);
  }
}

interface PreparedStandaloneSearchRequest {
  readonly body: StandaloneSearchRequest;
  readonly structuralBodyBytes: number;
}

async function prepareStandaloneSearchRequest(
  value: unknown,
  signal?: AbortSignal,
): Promise<PreparedStandaloneSearchRequest> {
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException("Standalone search request aborted", "AbortError");
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    requestFailure("Request body must be a JSON object.", "body", "invalid_type");
  }
  validateAndFreezeJsonTree(value, requestFailure, "body");
  const body = value as Record<string, unknown>;
  exactKeys(
    body,
    new Set(["id", "model", "input", "commands", "settings", "max_output_tokens", "reasoning"]),
    "body",
  );
  text(body.id, "id", 128);
  text(body.model, "model", 256);
  if (typeof body.input === "string") {
    text(body.input, "input", STANDALONE_SEARCH_MAX_REQUEST_BYTES, { allowEmpty: true });
  } else if (body.input !== undefined && body.input !== null &&
    (!Array.isArray(body.input) || body.input.length > MAX_INPUT_ITEMS)) {
    requestFailure(
      `input must be text or an array of at most ${MAX_INPUT_ITEMS} JSON objects.`,
      "input",
      "invalid_type",
    );
  }
  if (Array.isArray(body.input)) {
    body.input.forEach((item, index) => record(item, `input[${index}]`));
  }
  if (body.commands !== undefined && body.commands !== null) {
    validateCommands(record(body.commands, "commands"));
  }
  if (body.settings !== undefined && body.settings !== null) {
    validateSettings(record(body.settings, "settings"));
  }
  if (body.max_output_tokens !== undefined && body.max_output_tokens !== null) {
    integer(body.max_output_tokens, "max_output_tokens", 1, MAX_OUTPUT_TOKENS);
  }
  if (body.reasoning !== undefined && body.reasoning !== null) record(body.reasoning, "reasoning");
  // Reuse the Responses input contract for embedded function-call arguments.
  validateResponsesInputJson(body);
  const bytes = await jsonEncodedByteLengthAsync(body, { signal });
  if (bytes > STANDALONE_SEARCH_MAX_REQUEST_BYTES) {
    throw new ApiError(413, "Standalone search request exceeds the configured byte limit.", {
      type: "invalid_request_error",
      param: "body",
      code: "request_too_large",
    });
  }
  return Object.freeze({
    body: body as unknown as StandaloneSearchRequest,
    structuralBodyBytes: bytes,
  });
}

export async function parseStandaloneSearchRequest(
  value: unknown,
  signal?: AbortSignal,
): Promise<StandaloneSearchRequest> {
  return (await prepareStandaloneSearchRequest(value, signal)).body;
}

function resultRecord(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) upstreamContractFailure();
  const descriptors = Object.getOwnPropertyDescriptors(value);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (typeof key !== "string" || !descriptors[key]?.enumerable || !("value" in descriptors[key]!)) {
      upstreamContractFailure();
    }
  }
  return value as Record<string, unknown>;
}

function exactResultKeys(value: Record<string, unknown>, allowed: ReadonlySet<string>): void {
  if (Object.keys(value).some(key => !allowed.has(key))) upstreamContractFailure();
}

function safeSuccessStatus(value: unknown): number {
  if (
    !Number.isSafeInteger(value) || (value as number) < 200 || (value as number) > 299 ||
    value === 204 || value === 205
  ) upstreamContractFailure();
  return value as number;
}

function safeFailureStatus(value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 400 || (value as number) > 599) {
    upstreamContractFailure();
  }
  return value as number;
}

function safeRetryAfter(value: unknown): string {
  if (typeof value !== "string" || utf8Bytes(value) > 64 || !wellFormed(value)) {
    upstreamContractFailure();
  }
  if (/^(?:0|[1-9]\d{0,9})$/.test(value)) {
    const seconds = Number(value);
    if (Number.isSafeInteger(seconds) && seconds <= MAX_RETRY_AFTER_SECONDS) return value;
  }
  if (IMF_FIXDATE.test(value) && Number.isFinite(Date.parse(value))) return value;
  upstreamContractFailure();
}

async function handlerResultResponse(
  value: unknown,
  signal: AbortSignal,
): Promise<Response> {
  const result = resultRecord(value);
  let bodyOwner: unknown = null;
  try {
    if (result.kind === "success") {
      exactResultKeys(result, new Set(["kind", "status", "body"]));
      const status = safeSuccessStatus(result.status);
      const body = result.body;
      bodyOwner = body;
      if (body === null || typeof body !== "object" || Array.isArray(body)) upstreamContractFailure();
      try {
        validateAndFreezeJsonTree(body, upstreamContractFailure, "body");
        const response = body as Record<string, unknown>;
        if (typeof response.output !== "string") upstreamContractFailure();
        if (await jsonEncodedByteLengthAsync(body, { signal }) > STANDALONE_SEARCH_MAX_RESPONSE_BYTES) {
          upstreamContractFailure();
        }
      } catch (error) {
        if (signal.aborted) throw error;
        upstreamContractFailure();
      }
      const response = permittedJsonResponse(body, { status });
      bodyOwner = null;
      return response;
    }
    if (result.kind === "failure") {
      exactResultKeys(result, new Set(["kind", "status", "body", "retryAfter"]));
      const status = safeFailureStatus(result.status);
      const body = result.body;
      bodyOwner = body;
      if (body === null || typeof body !== "object" || Array.isArray(body)) upstreamContractFailure();
      try {
        validateAndFreezeJsonTree(body, upstreamContractFailure, "body");
        if (await jsonEncodedByteLengthAsync(body, { signal }) > STANDALONE_SEARCH_MAX_RESPONSE_BYTES) {
          upstreamContractFailure();
        }
      } catch (error) {
        if (signal.aborted) throw error;
        upstreamContractFailure();
      }
      const headers = new Headers();
      if (Object.prototype.hasOwnProperty.call(result, "retryAfter")) {
        headers.set("retry-after", safeRetryAfter(result.retryAfter));
      }
      const response = permittedJsonResponse(body, { status, headers });
      bodyOwner = null;
      return response;
    }
    upstreamContractFailure();
  } finally {
    releaseAtomicBytePermit(bodyOwner);
  }
}

export function registerStandaloneSearchRoute(
  app: Hono,
  options: StandaloneSearchRouteOptions,
): void {
  app.post(STANDALONE_SEARCH_PATH, async c => {
    if (!options.handler) return unavailable();
    const incomingLease = takeIncomingBodyLease(c.env);
    let incomingLeaseOwned = true;
    const releaseIncomingLease = () => {
      if (!incomingLeaseOwned) return;
      incomingLeaseOwned = false;
      incomingLease?.release();
    };
    const requestSignal = c.req.raw.signal;
    const searchAbort = new AbortController();
    const abortSearch = () => searchAbort.abort(requestSignal.reason);
    if (requestSignal.aborted) abortSearch();
    else requestSignal.addEventListener("abort", abortSearch, { once: true });
    const unlinkIncomingSignal = linkIncomingLeaseAbort(searchAbort, incomingLease);
    try {
      let value: unknown;
      try {
        value = await c.req.json();
      } catch {
        requestFailure("Request body is not valid JSON.", "body", "invalid_json");
      }
      const prepared = await prepareStandaloneSearchRequest(value, searchAbort.signal);
      const encodedBodyBytes = Number.isSafeInteger(incomingLease?.bodyBytes) &&
          Number(incomingLease?.bodyBytes) >= 0
        ? Number(incomingLease!.bodyBytes)
        : prepared.structuralBodyBytes;
      const context = Object.freeze<StandaloneSearchHandlerContext>({
        signal: searchAbort.signal,
        incomingLease,
        transferIncomingLease: () => {
          if (!incomingLeaseOwned) {
            throw new Error("Standalone search incoming lease was already transferred");
          }
          incomingLeaseOwned = false;
          return incomingLease;
        },
        clientId: resolveHttpClientId(c.env, c.req.header("x-ghostprov-client-id")),
        authorizationScope: options.authorizationScope,
        encodedBodyBytes,
        structuralBodyBytes: prepared.structuralBodyBytes,
      });
      const result = await options.handler(prepared.body, context);
      return await handlerResultResponse(result, searchAbort.signal);
    } catch (error) {
      return apiErrorResponse(error);
    } finally {
      unlinkIncomingSignal();
      requestSignal.removeEventListener("abort", abortSearch);
      releaseIncomingLease();
    }
  });
}
