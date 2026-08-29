import { Hono } from "hono";
import { anthropicModelsFromEnv } from "../src/anthropic/model";
import { registerAnthropicRoutes } from "../src/anthropic/routes";
import { translateAnthropicRequest } from "../src/anthropic/request";
import { responseToAnthropic } from "../src/anthropic/response";
import { projectAnthropicReplay } from "../src/anthropic/continuation";
import { sanitizeDiagnosticMessage } from "../src/observability/redaction";
import {
  createAuthenticatedTenantScope,
} from "../src/http/api-key";
import { V2_R8_RELEASE } from "../src/v2/sidecar/assets";
import { releaseAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import WebSocket from "ws";
import type { RunHandle } from "../src/relay/run-handle";
import { createBrowserlessSystem, type BrowserlessSystem } from "../src/server/system";

const CANDIDATE_PORT = 18_888;
const PROTECTED_ROOT = "http://127.0.0.1:18787";
const PROJECT_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const LIVE_GUARD = "dedicated-test-projects";
const REQUEST_TIMEOUT_MS = 120_000;
const FLEET_READY_TIMEOUT_MS = 120_000;
const TARGET_CONCURRENCY = 16;
const MAX_OBSERVATIONS = 512;

type Observation = Readonly<{
  source: string;
  requestBytes: number;
  inputItems: number;
  hasPreviousResponse: boolean;
}>;

type LatencySummary = Readonly<{
  count: number;
  p50Ms: number;
  p95Ms: number;
  maxMs: number;
}>;

type ReadinessSummary = Readonly<{
  requested: number;
  required: number;
  ready: number;
  unavailable: number;
  capacity: number;
  available: number;
}>;

type TransportRuntimeCounters = Readonly<{
  incarnation: number;
  dataPublishedBytes: number;
  flightCompletions: number;
  retransmittedBytes: number;
}>;

export type CandidateTransportSnapshot = Readonly<{
  ready: number;
  unavailable: number;
  capacity: number;
  available: number;
  quarantined: number;
  ambiguousQuarantined: number;
  runtimes: ReadonlyMap<string, TransportRuntimeCounters>;
}>;

export type TransportDelta = Readonly<{
  dataPublishedBytes: number;
  flightCompletions: number;
  retransmittedBytes: number;
  runtimesUsed: number;
}>;

type MeasuredResponse = Readonly<{
  value: Record<string, unknown>;
  clientBytes: number;
  latencyMs: number;
  transport: TransportDelta;
}>;

export type PersistedContinuationLiveOptions = Readonly<{
  projectIds: readonly string[];
  model: string;
}>;

export function resolvePersistedContinuationLiveOptions(
  env: Record<string, string | undefined>,
): PersistedContinuationLiveOptions {
  if (env.GP_PERSISTED_CONTINUATION_LIVE !== "1") {
    throw new Error("Set GP_PERSISTED_CONTINUATION_LIVE=1 for the paid live verifier");
  }
  if (env.GP_CONTINUATION_TEST_CONFIRM !== LIVE_GUARD) {
    throw new Error(`Set GP_CONTINUATION_TEST_CONFIRM=${LIVE_GUARD}`);
  }
  if (env.GP_AUTO_CREATE_PROJECTS === "1") {
    throw new Error("The persisted-continuation verifier never creates projects");
  }
  const projectIds = (env.GP_CONTINUATION_TEST_PROJECT_IDS ?? "")
    .split(",")
    .map(value => value.trim().toLowerCase())
    .filter(Boolean);
  if (projectIds.length < 2 || projectIds.length > 8 ||
    new Set(projectIds).size !== projectIds.length ||
    projectIds.some(value => !PROJECT_ID.test(value))) {
    throw new Error("GP_CONTINUATION_TEST_PROJECT_IDS must contain 2-8 unique dedicated UUIDs");
  }
  if (!env.GP_CONTINUATION_TEST_COOKIE_FILE?.trim()) {
    throw new Error("GP_CONTINUATION_TEST_COOKIE_FILE is required for the dedicated live verifier");
  }
  return Object.freeze({
    projectIds: Object.freeze([...projectIds]),
    model: env.GP_LIVE_MODEL?.trim() || "gpt-5.6-sol",
  });
}

export function requiredLiveRuntimes(projectCount: number): number {
  if (!Number.isInteger(projectCount) || projectCount < 2 || projectCount > 8) {
    throw new RangeError("live verifier project count must be between 2 and 8");
  }
  return Math.max(2, projectCount - 1);
}

function progress(
  phase: string,
  fields: Readonly<Record<string, string | number | boolean>> = {},
): void {
  console.log(JSON.stringify({
    liveVerifier: "persisted-continuation",
    phase,
    ...fields,
  }));
}

function percentile(values: readonly number[], fraction: number): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)] ?? 0;
}

function summarize(values: readonly number[]): LatencySummary {
  return {
    count: values.length,
    p50Ms: rounded(percentile(values, 0.5)),
    p95Ms: rounded(percentile(values, 0.95)),
    maxMs: rounded(Math.max(0, ...values)),
  };
}

function rounded(value: number): number {
  return Math.round(value * 100) / 100;
}

function byteReductionPercent(fullBytes: number, suffixBytes: number): number {
  if (fullBytes < 1) return 0;
  return rounded(Math.max(0, 1 - suffixBytes / fullBytes) * 100);
}

function outputText(final: Record<string, unknown>): string {
  const output = Array.isArray(final.output) ? final.output : [];
  return output.flatMap(item => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return [];
    const content = (item as Record<string, unknown>).content;
    if (!Array.isArray(content)) return [];
    return content.flatMap(part => {
      if (!part || typeof part !== "object" || Array.isArray(part)) return [];
      const record = part as Record<string, unknown>;
      return record.type === "output_text" && typeof record.text === "string"
        ? [record.text]
        : [];
    });
  }).join("");
}

function responseId(final: Record<string, unknown>): string {
  if (final.status !== "completed" || typeof final.id !== "string" || !final.id) {
    throw new Error("Live provider response did not complete with an ID");
  }
  return final.id;
}

async function protectedHealth(): Promise<void> {
  const response = await fetch(`${PROTECTED_ROOT}/health`, {
    signal: AbortSignal.timeout(10_000),
  });
  const body = await response.json().catch(() => null) as { ok?: unknown } | null;
  if (!response.ok || body?.ok !== true) {
    throw new Error("Protected listener 18787 failed its read-only health check");
  }
}

async function drain(handle: RunHandle): Promise<Record<string, unknown>> {
  const [final] = await Promise.all([
    handle.final as Promise<Record<string, unknown>>,
    Array.fromAsync(handle.events()),
  ]);
  responseId(final);
  return final;
}

async function launch(
  env: Record<string, string | undefined>,
  options: PersistedContinuationLiveOptions,
): Promise<{
  system: BrowserlessSystem;
  observations: Observation[];
  readiness: ReadinessSummary;
  restore(): void;
}> {
  const system = await createBrowserlessSystem({
    ...env,
    GP_CANDIDATE: "1",
    GP_AUTO_CREATE_PROJECTS: "0",
    PORT: String(CANDIDATE_PORT),
    PRISM_COOKIE_FILE: env.GP_CONTINUATION_TEST_COOKIE_FILE!.trim(),
    GP_FLEET_MANIFEST_FILE: env.GP_CONTINUATION_TEST_MANIFEST_FILE?.trim() ||
      ".ghostprov/continuation-live-fleet.json",
    PRISM_PROJECT_IDS: options.projectIds.join(","),
  });
  const observations: Observation[] = [];
  const original = system.router.startRelay.bind(system.router);
  system.router.startRelay = async (body, relayOptions) => {
    if (observations.length >= MAX_OBSERVATIONS) {
      throw new Error("Live verifier observation capacity is exhausted");
    }
    observations.push(Object.freeze({
      source: relayOptions?.source ?? "internal",
      requestBytes: Buffer.byteLength(JSON.stringify(body), "utf8"),
      inputItems: Array.isArray(body.input) ? body.input.length : body.input == null ? 0 : 1,
      hasPreviousResponse: typeof body.previous_response_id === "string",
    }));
    return original(body, relayOptions);
  };
  const requiredReady = requiredLiveRuntimes(options.projectIds.length);
  let readiness: ReadinessSummary;
  try {
    await system.start();
    progress("fleet.awaiting-readiness", {
      requested: options.projectIds.length,
      required: requiredReady,
      timeoutMs: FLEET_READY_TIMEOUT_MS,
    });
    let fleet;
    try {
      fleet = await system.fleet.waitForReady(
        requiredReady,
        AbortSignal.timeout(FLEET_READY_TIMEOUT_MS),
      );
    } catch (error) {
      const snapshot = system.fleet.snapshot();
      throw new Error(
        `Live fleet readiness failed: requested=${options.projectIds.length} ` +
          `required=${requiredReady} ready=${snapshot.ready} unavailable=${snapshot.unavailable}`,
        { cause: error },
      );
    }
    const router = system.router.snapshot();
    if (router.capacity < TARGET_CONCURRENCY) {
      throw new Error(
        `Live fleet capacity ${router.capacity} is below target ${TARGET_CONCURRENCY}`,
      );
    }
    readiness = Object.freeze({
      requested: options.projectIds.length,
      required: requiredReady,
      ready: fleet.ready,
      unavailable: fleet.unavailable,
      capacity: router.capacity,
      available: router.available,
    });
    progress("fleet.ready", readiness);
  } catch (error) {
    system.router.startRelay = original;
    await system.close().catch(() => undefined);
    throw error;
  }
  return {
    system,
    observations,
    readiness,
    restore: () => { system.router.startRelay = original; },
  };
}

async function postJson(
  path: string,
  apiKey: string,
  body: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const response = await fetch(`http://127.0.0.1:${CANDIDATE_PORT}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`,
      ...(path.startsWith("/v1/messages")
        ? { "anthropic-version": "2023-06-01", "x-api-key": apiKey }
        : {}),
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  const value = await response.json().catch(() => null) as Record<string, unknown> | null;
  if (!response.ok || !value) throw new Error(`Live request failed with HTTP ${response.status}`);
  return value;
}


function objectValue(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Candidate ${label} is not an object`);
  }
  return value as Record<string, unknown>;
}

function counterValue(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`Candidate ${label} is not a non-negative integer`);
  }
  return value as number;
}

export async function candidateTransportSnapshot(): Promise<CandidateTransportSnapshot> {
  const response = await fetch(`http://127.0.0.1:${CANDIDATE_PORT}/__sandboxes`, {
    signal: AbortSignal.timeout(10_000),
  });
  const text = await response.text();
  if (!response.ok || Buffer.byteLength(text, "utf8") > 2 * 1024 * 1024) {
    throw new Error("Candidate sandbox snapshot is unavailable or oversized");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error("Candidate sandbox snapshot is invalid JSON");
  }
  const root = objectValue(parsed, "sandbox snapshot");
  const runtimes = Array.isArray(root.runtimes) ? root.runtimes : null;
  if (!runtimes || runtimes.length < 2 || runtimes.length > 8) {
    throw new Error("Candidate sandbox snapshot has an invalid runtime count");
  }

  const counters = new Map<string, TransportRuntimeCounters>();
  let quarantined = 0;
  let ambiguousQuarantined = 0;
  for (const value of runtimes) {
    const runtime = objectValue(value, "runtime");
    quarantined += counterValue(runtime.quarantined, "runtime quarantine count");
    ambiguousQuarantined += counterValue(
      runtime.ambiguousQuarantined,
      "runtime ambiguous quarantine count",
    );
    if (runtime.state !== "ready") continue;
    if (
      runtime.releaseRevision !== V2_R8_RELEASE.revision ||
      runtime.protocolRevision !== V2_R8_RELEASE.protocolRevision ||
      runtime.transportProfile !== V2_R8_RELEASE.transportProfile ||
      runtime.buildHash !== V2_R8_RELEASE.buildHash ||
      runtime.schemaHash !== V2_R8_RELEASE.schemaHash
    ) {
      throw new Error("Candidate ready runtime does not have the frozen R8 identity");
    }
    if (typeof runtime.projectId !== "string" || !PROJECT_ID.test(runtime.projectId)) {
      throw new Error("Candidate ready runtime has an invalid project ID");
    }
    if (counters.has(runtime.projectId)) {
      throw new Error("Candidate sandbox snapshot has a duplicate project ID");
    }
    const diagnostics = objectValue(runtime.diagnostics, "runtime diagnostics");
    const host = objectValue(diagnostics.host, "host diagnostics");
    counters.set(runtime.projectId, Object.freeze({
      incarnation: counterValue(runtime.incarnation, "runtime incarnation"),
      dataPublishedBytes: counterValue(
        host.dataPublishedBytesTotal,
        "data publication bytes",
      ),
      flightCompletions: counterValue(
        host.flightCompletionsTotal,
        "flight completion count",
      ),
      retransmittedBytes: counterValue(
        host.retransmittedBytesTotal,
        "retransmitted bytes",
      ),
    }));
  }

  const snapshot = Object.freeze({
    ready: counterValue(root.ready, "ready runtime count"),
    unavailable: counterValue(root.unavailable, "unavailable runtime count"),
    capacity: counterValue(root.capacity, "capacity"),
    available: counterValue(root.available, "available capacity"),
    quarantined,
    ambiguousQuarantined,
    runtimes: counters as ReadonlyMap<string, TransportRuntimeCounters>,
  });
  if (snapshot.ready !== counters.size || snapshot.ready < 2) {
    throw new Error("Candidate does not expose two healthy runtimes");
  }
  if (snapshot.capacity < TARGET_CONCURRENCY) {
    throw new Error("Candidate capacity is below the 16-agent target");
  }
  return snapshot;
}

export function transportDelta(
  before: CandidateTransportSnapshot,
  after: CandidateTransportSnapshot,
): TransportDelta {
  if (before.runtimes.size !== after.runtimes.size) {
    throw new Error("Candidate runtime set changed during a measurement");
  }
  let dataPublishedBytes = 0;
  let flightCompletions = 0;
  let retransmittedBytes = 0;
  let runtimesUsed = 0;
  for (const [projectId, initial] of before.runtimes) {
    const final = after.runtimes.get(projectId);
    if (!final || final.incarnation !== initial.incarnation) {
      throw new Error("Candidate runtime incarnation changed during a measurement");
    }
    const data = final.dataPublishedBytes - initial.dataPublishedBytes;
    const flights = final.flightCompletions - initial.flightCompletions;
    const retransmitted = final.retransmittedBytes - initial.retransmittedBytes;
    if (data < 0 || flights < 0 || retransmitted < 0) {
      throw new Error("Candidate transport counter moved backwards");
    }
    dataPublishedBytes += data;
    flightCompletions += flights;
    retransmittedBytes += retransmitted;
    if (data > 0 || flights > 0) runtimesUsed++;
  }
  return Object.freeze({
    dataPublishedBytes,
    flightCompletions,
    retransmittedBytes,
    runtimesUsed,
  });
}

async function measureAction<T>(action: () => Promise<T>): Promise<{
  value: T;
  latencyMs: number;
  transport: TransportDelta;
}> {
  const before = await candidateTransportSnapshot();
  const started = performance.now();
  const value = await action();
  const latencyMs = performance.now() - started;
  const after = await candidateTransportSnapshot();
  return {
    value,
    latencyMs,
    transport: transportDelta(before, after),
  };
}

async function measuredPostJson(
  path: string,
  apiKey: string,
  body: Record<string, unknown>,
): Promise<MeasuredResponse> {
  const clientBytes = Buffer.byteLength(JSON.stringify(body), "utf8");
  const measured = await measureAction(() => postJson(path, apiKey, body));
  if (measured.transport.flightCompletions < 1) {
    throw new Error("Candidate transport did not record a completed request flight");
  }
  return Object.freeze({
    value: measured.value,
    clientBytes,
    latencyMs: measured.latencyMs,
    transport: measured.transport,
  });
}

function userItem(text: string): Record<string, unknown> {
  return {
    type: "message",
    role: "user",
    content: [{ type: "input_text", text }],
  };
}

async function verifyHttpAndInference(
  apiKey: string,
  model: string,
  observations: Observation[],
): Promise<{
  parentId: string;
  rootRelayBytes: number;
  explicitSuffixBytes: number;
  inferredClientBytes: number;
  inferredSuffixBytes: number;
  inferredReductionPercent: number;
}> {
  const rootInput = [userItem("You must call live_echo once. After its result, repeat the value exactly.")];
  const tools = [{
    type: "function",
    name: "live_echo",
    description: "Returns the supplied verification value",
    parameters: {
      type: "object",
      properties: { value: { type: "string" } },
      required: ["value"],
      additionalProperties: false,
    },
    strict: true,
  }];
  const root = await postJson("/v1/responses", apiKey, {
    model,
    stream: false,
    store: false,
    input: rootInput,
    max_output_tokens: 128,
    reasoning: { effort: "low" },
    tools,
    tool_choice: "auto",
  });
  const parentId = responseId(root);
  const output = Array.isArray(root.output) ? root.output : [];
  const call = output.find(item => item && typeof item === "object" &&
    !Array.isArray(item) && (item as Record<string, unknown>).type === "function_call") as
      | Record<string, unknown>
      | undefined;
  if (!call || typeof call.call_id !== "string") throw new Error("Live root did not call live_echo");

  const explicitMarker = "HTTP_EXPLICIT_MEMORY_OK";
  const explicit = await postJson("/v1/responses", apiKey, {
    model,
    stream: false,
    store: false,
    previous_response_id: parentId,
    input: [{
      type: "function_call_output",
      call_id: call.call_id,
      output: explicitMarker,
    }],
    max_output_tokens: 128,
    reasoning: { effort: "low" },
    tools,
    tool_choice: "auto",
  });
  if (!outputText(explicit).includes(explicitMarker)) {
    throw new Error("Explicit HTTP stored continuation forgot its tool result");
  }

  const inferredMarker = "HTTP_INFERRED_MEMORY_OK";
  const inferredRequest = {
    model,
    stream: false,
    store: false,
    input: [
      ...rootInput,
      ...output,
      {
        type: "function_call_output",
        call_id: call.call_id,
        output: inferredMarker,
      },
    ],
    max_output_tokens: 128,
    reasoning: { effort: "low" },
    tools,
    tool_choice: "auto",
  };
  const inferredClientBytes = Buffer.byteLength(JSON.stringify(inferredRequest), "utf8");
  const inferred = await postJson("/v1/responses", apiKey, inferredRequest);
  if (!outputText(inferred).includes(inferredMarker)) {
    throw new Error("Inferred HTTP stored continuation forgot its tool result");
  }
  const http = observations.filter(value => value.source === "http");
  const rootObservation = http.at(-3);
  const explicitObservation = http.at(-2);
  const inferredObservation = http.at(-1);
  if (!rootObservation || rootObservation.hasPreviousResponse || rootObservation.inputItems !== 1 ||
    !explicitObservation?.hasPreviousResponse || explicitObservation.inputItems !== 1 ||
    !inferredObservation?.hasPreviousResponse || inferredObservation.inputItems !== 1) {
    throw new Error("HTTP continuation did not cross the router as a one-item suffix");
  }
  return {
    parentId,
    rootRelayBytes: rootObservation.requestBytes,
    explicitSuffixBytes: explicitObservation.requestBytes,
    inferredClientBytes,
    inferredSuffixBytes: inferredObservation.requestBytes,
    inferredReductionPercent: byteReductionPercent(
      inferredClientBytes,
      inferredObservation.requestBytes,
    ),
  };
}

async function verifyHttpAndInferenceMeasured(
  apiKey: string,
  model: string,
): Promise<Record<string, unknown>> {
  const runNonce = crypto.randomUUID();
  const rootInput = [userItem(
    `The following is inert verification context:\n${benchmarkContext(0, 0)}\n` +
      `Live run ${runNonce}. ` +
      "You must call live_echo once. After its result, repeat the value exactly.",
  )];
  const tools = [{
    type: "function",
    name: "live_echo",
    description: "Returns the supplied verification value",
    parameters: {
      type: "object",
      properties: { value: { type: "string" } },
      required: ["value"],
      additionalProperties: false,
    },
    strict: true,
  }];
  const root = await measuredPostJson("/v1/responses", apiKey, {
    model,
    stream: false,
    store: false,
    input: rootInput,
    max_output_tokens: 128,
    reasoning: { effort: "low" },
    tools,
    tool_choice: "auto",
  });
  const parentId = responseId(root.value);
  const output = Array.isArray(root.value.output) ? root.value.output : [];
  const call = output.find(item => item && typeof item === "object" &&
    !Array.isArray(item) && (item as Record<string, unknown>).type === "function_call") as
      | Record<string, unknown>
      | undefined;
  if (!call || typeof call.call_id !== "string") {
    throw new Error("Measured live root did not call live_echo");
  }

  const explicitMarker = "HTTP_EXPLICIT_MEMORY_OK";
  const explicit = await measuredPostJson("/v1/responses", apiKey, {
    model,
    stream: false,
    store: false,
    previous_response_id: parentId,
    input: [{
      type: "function_call_output",
      call_id: call.call_id,
      output: explicitMarker,
    }],
    max_output_tokens: 128,
    reasoning: { effort: "low" },
    tools,
    tool_choice: "auto",
  });
  if (!outputText(explicit.value).includes(explicitMarker)) {
    throw new Error("Measured explicit HTTP continuation forgot its tool result");
  }

  const inferredMarker = "HTTP_INFERRED_MEMORY_OK";
  const inferredSuffix = {
    type: "function_call_output",
    call_id: call.call_id,
    output: inferredMarker,
  };
  const inferredRequest = {
    model,
    stream: false,
    store: false,
    input: [
      ...rootInput,
      ...output,
      inferredSuffix,
    ],
    max_output_tokens: 128,
    reasoning: { effort: "low" },
    tools,
    tool_choice: "auto",
  };
  const inferred = await measuredPostJson("/v1/responses", apiKey, inferredRequest);
  if (!outputText(inferred.value).includes(inferredMarker)) {
    throw new Error("Measured inferred HTTP continuation forgot its tool result");
  }
  const inferredReduction = byteReductionPercent(
    inferred.clientBytes,
    inferred.transport.dataPublishedBytes,
  );
  const contractRelayJsonBytes = Buffer.byteLength(JSON.stringify({
    ...inferredRequest,
    stream: true,
    store: true,
    previous_response_id: parentId,
    input: [inferredSuffix],
  }), "utf8");
  const contractRelayReduction = byteReductionPercent(
    inferred.clientBytes,
    contractRelayJsonBytes,
  );
  progress("http-continuation.measured", {
    rootDataChannelBytes: root.transport.dataPublishedBytes,
    explicitDataChannelBytes: explicit.transport.dataPublishedBytes,
    inferredClientBytes: inferred.clientBytes,
    inferredContractRelayJsonBytes: contractRelayJsonBytes,
    inferredDataChannelBytes: inferred.transport.dataPublishedBytes,
    inferredContractReductionPercent: contractRelayReduction,
    inferredWireReductionPercent: inferredReduction,
  });
  const parityTolerance = Math.max(
    1_024,
    Math.ceil(explicit.transport.dataPublishedBytes * 0.25),
  );
  if (
    Math.abs(
      inferred.transport.dataPublishedBytes - explicit.transport.dataPublishedBytes,
    ) > parityTolerance ||
    inferred.transport.dataPublishedBytes >= root.transport.dataPublishedBytes ||
    contractRelayReduction < 90
  ) {
    throw new Error("Measured inferred HTTP traffic did not match the explicit delta control");
  }

  return {
    contextBytes: CONCURRENCY_CONTEXT_BYTES,
    root: measuredResponseSummary(root),
    explicitContinuation: measuredResponseSummary(explicit),
    inferredFullHistory: {
      ...measuredResponseSummary(inferred),
      contractRelayJsonBytes,
      clientToContractRelayReductionPercent: contractRelayReduction,
      clientToDataChannelReductionPercent: inferredReduction,
      explicitDeltaWireParityToleranceBytes: parityTolerance,
    },
    memoryRecall: "passed",
  };
}

function measuredResponseSummary(measured: MeasuredResponse): Record<string, unknown> {
  return {
    clientBytes: measured.clientBytes,
    latencyMs: rounded(measured.latencyMs),
    dataChannelBytes: measured.transport.dataPublishedBytes,
    flightCompletions: measured.transport.flightCompletions,
    retransmittedBytes: measured.transport.retransmittedBytes,
    runtimesUsed: measured.transport.runtimesUsed,
  };
}

async function verifyClaude(
  apiKey: string,
  observations: Observation[],
): Promise<{
  fullHistoryClientBytes: number;
  suffixBytes: number;
  reductionPercent: number;
}> {
  const nonce = "CLAUDE_FULL_HISTORY_MEMORY_7K9Q";
  const firstRequest = {
    model: "claude-sonnet-4-6",
    max_tokens: 128,
    messages: [{ role: "user", content: `Remember ${nonce}. Reply READY only.` }],
  };
  const first = await postJson("/v1/messages", apiKey, firstRequest);
  if (!Array.isArray(first.content)) throw new Error("Claude root returned no content");
  const secondRequest = {
    model: "claude-sonnet-4-6",
    max_tokens: 128,
    messages: [
      ...firstRequest.messages,
      { role: "assistant", content: first.content },
      { role: "user", content: "Return the exact remembered nonce and nothing else." },
    ],
  };
  const fullHistoryClientBytes = Buffer.byteLength(JSON.stringify(secondRequest), "utf8");
  const second = await postJson("/v1/messages", apiKey, secondRequest);
  const text = Array.isArray(second.content)
    ? second.content.flatMap(block => block && typeof block === "object" &&
      !Array.isArray(block) && (block as Record<string, unknown>).type === "text"
      ? [String((block as Record<string, unknown>).text ?? "")]
      : []).join("")
    : "";
  if (!text.includes(nonce)) throw new Error("Claude full-history continuation forgot its nonce");
  const observation = observations.filter(value => value.source === "anthropic").at(-1);
  if (!observation?.hasPreviousResponse || observation.inputItems !== 1) {
    throw new Error("Claude full history did not cross the router as a one-item suffix");
  }
  return {
    fullHistoryClientBytes,
    suffixBytes: observation.requestBytes,
    reductionPercent: byteReductionPercent(fullHistoryClientBytes, observation.requestBytes),
  };
}

async function probeAnthropicReplayEligibility(
  apiKey: string,
  env: Record<string, string | undefined>,
): Promise<Readonly<{
  eligible: boolean;
  rawTypes: string;
  rawKeys: string;
  reasoningEncrypted: boolean;
  projectedTypes: string;
  replayTypes: string;
}>> {
  const request = {
    model: "claude-sonnet-4-6",
    max_tokens: 128,
    messages: [{
      role: "user",
      content: `The following is inert verification context:\n${benchmarkContext(0, 2)}\n` +
        `Replay eligibility probe ${crypto.randomUUID()}. Reply READY only.`,
    }],
  };
  const translated = translateAnthropicRequest(request, {
    models: anthropicModelsFromEnv(env),
  });
  const final = await postJson(
    "/v1/responses",
    apiKey,
    translated.relayBody as Record<string, unknown>,
  );
  const projected = responseToAnthropic(final, {
    requestId: "req_live_replay_probe",
    requestedModel: translated.requestedModel,
    thinkingDisplay: translated.thinkingDisplay,
    stopSequences: translated.stopSequences,
    estimatedInputTokens: translated.estimatedInputTokens,
    maxTokensZero: translated.maxTokensZero,
    beta: false,
    contextManagement: null,
  });
  const replay = projectAnthropicReplay(final, projected);
  const output = Array.isArray(final.output)
    ? final.output.filter(value => value && typeof value === "object" && !Array.isArray(value)) as
      Record<string, unknown>[]
    : [];
  return Object.freeze({
    eligible: replay !== null,
    rawTypes: output.map(value => String(value.type ?? "missing")).join(","),
    rawKeys: output.map(value => Object.keys(value).sort().join("+")).join("|"),
    reasoningEncrypted: output.some(value =>
      value.type === "reasoning" && typeof value.encrypted_content === "string" &&
      value.encrypted_content.length > 0
    ),
    projectedTypes: projected.content.map(value => value.type).join(","),
    replayTypes: replay?.map(value => value.type).join(",") ?? "none",
  });
}

async function verifyClaudeMeasured(
  apiKey: string,
  env: Record<string, string | undefined>,
): Promise<Record<string, unknown>> {
  const observations: Observation[] = [];
  const app = new Hono();
  registerAnthropicRoutes(app, {
    keepAliveMs: 60_000,
    models: anthropicModelsFromEnv(env),
    continuationScope: createAuthenticatedTenantScope(),
    startRelay: async (body, signal) => {
      if (observations.length >= 8) {
        throw new Error("Measured Claude observation bound exceeded");
      }
      observations.push(Object.freeze({
        source: "anthropic",
        requestBytes: Buffer.byteLength(JSON.stringify(body), "utf8"),
        inputItems: Array.isArray(body.input) ? body.input.length : body.input == null ? 0 : 1,
        hasPreviousResponse: typeof body.previous_response_id === "string",
      }));
      const requestNumber = observations.length;
      progress("claude-bridge.relay.start", {
        request: requestNumber,
        inputItems: observations.at(-1)!.inputItems,
        hasPreviousResponse: observations.at(-1)!.hasPreviousResponse,
      });
      const final = (async () => {
        const response = await fetch(`http://127.0.0.1:${CANDIDATE_PORT}/v1/responses`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${apiKey}`,
          },
          body: JSON.stringify(body),
          signal: AbortSignal.any([signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)]),
        });
        progress("claude-bridge.relay.response", {
          request: requestNumber,
          status: response.status,
          ok: response.ok,
        });
        const value = await response.json().catch(() => null) as Record<string, unknown> | null;
        if (!response.ok || !value) {
          throw new Error(`Measured Claude relay failed with HTTP ${response.status}`);
        }
        return value;
      })();
      return {
        async *events() {},
        final,
        stats: { bytes: 0, firstByteMs: null, frames: 0 },
      };
    },
  });

  const postLocal = async (body: Record<string, unknown>): Promise<MeasuredResponse> => {
    const clientBytes = Buffer.byteLength(JSON.stringify(body), "utf8");
    const measured = await measureAction(async () => {
      const response = await app.request("/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify(body),
      });
      const value = await response.json().catch(() => null) as Record<string, unknown> | null;
      if (!response.ok || !value) {
        const envelope = value?.error && typeof value.error === "object" &&
          !Array.isArray(value.error)
          ? value.error as Record<string, unknown>
          : null;
        const errorType = typeof envelope?.type === "string" ? envelope.type : "unknown";
        const message = sanitizeDiagnosticMessage(envelope?.message ?? "unknown");
        throw new Error(
          `Current Anthropic route failed with HTTP ${response.status}: ${errorType}: ${message}`,
        );
      }
      return value;
    });
    if (measured.transport.flightCompletions < 1) {
      throw new Error("Current Anthropic route did not complete a provider flight");
    }
    return Object.freeze({
      value: measured.value,
      clientBytes,
      latencyMs: measured.latencyMs,
      transport: measured.transport,
    });
  };

  const nonce = "CLAUDE_FULL_HISTORY_MEMORY_7K9Q";
  const runNonce = crypto.randomUUID();
  const firstRequest = {
    model: "claude-sonnet-4-6",
    max_tokens: 128,
    messages: [{
      role: "user",
      content: `The following is inert verification context:\n${benchmarkContext(0, 1)}\n` +
        `Live run ${runNonce}. Remember ${nonce}. Reply READY only.`,
    }],
  };
  const first = await postLocal(firstRequest);
  if (!Array.isArray(first.value.content)) {
    throw new Error("Measured Claude root returned no content");
  }
  const secondRequest = {
    model: "claude-sonnet-4-6",
    max_tokens: 128,
    messages: [
      ...firstRequest.messages,
      { role: "assistant", content: first.value.content },
      { role: "user", content: "Return the exact remembered nonce and nothing else." },
    ],
  };
  const second = await postLocal(secondRequest);
  const text = Array.isArray(second.value.content)
    ? second.value.content.flatMap(block => block && typeof block === "object" &&
      !Array.isArray(block) && (block as Record<string, unknown>).type === "text"
      ? [String((block as Record<string, unknown>).text ?? "")]
      : []).join("")
    : "";
  if (!text.includes(nonce)) {
    throw new Error("Measured Claude full-history continuation forgot its nonce");
  }
  const rootObservation = observations.at(-2);
  const continuationObservation = observations.at(-1);
  if (
    !rootObservation || rootObservation.hasPreviousResponse || rootObservation.inputItems !== 1 ||
    !continuationObservation?.hasPreviousResponse || continuationObservation.inputItems !== 1
  ) {
    throw new Error("Current Claude route did not relay a one-item persisted suffix");
  }
  const bodyReduction = byteReductionPercent(
    second.clientBytes,
    continuationObservation.requestBytes,
  );
  const wireReduction = byteReductionPercent(
    second.clientBytes,
    second.transport.dataPublishedBytes,
  );
  progress("claude-continuation.measured", {
    rootDataChannelBytes: first.transport.dataPublishedBytes,
    fullHistoryClientBytes: second.clientBytes,
    continuationRelayJsonBytes: continuationObservation.requestBytes,
    continuationDataChannelBytes: second.transport.dataPublishedBytes,
    bodyReductionPercent: bodyReduction,
    wireReductionPercent: wireReduction,
  });
  if (
    bodyReduction < 90 ||
    second.transport.dataPublishedBytes * 4 >= first.transport.dataPublishedBytes
  ) {
    throw new Error("Current Claude continuation did not reduce relay traffic materially");
  }
  return {
    mode: "current-in-memory-route-through-live-r8",
    contextBytes: CONCURRENCY_CONTEXT_BYTES,
    root: {
      ...measuredResponseSummary(first),
      relayJsonBytes: rootObservation.requestBytes,
    },
    fullHistoryContinuation: {
      ...measuredResponseSummary(second),
      relayJsonBytes: continuationObservation.requestBytes,
      clientToRelayJsonReductionPercent: bodyReduction,
      clientToDataChannelReductionPercent: wireReduction,
      inputItems: continuationObservation.inputItems,
      hasPreviousResponse: continuationObservation.hasPreviousResponse,
    },
    memoryRecall: "passed",
  };
}

async function verifyWebSocket(apiKey: string, model: string): Promise<void> {
  const socket = new WebSocket(`ws://127.0.0.1:${CANDIDATE_PORT}/v1/responses`, {
    headers: { authorization: `Bearer ${apiKey}` },
  });
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  const waitFor = (streamId: string) => new Promise<Record<string, unknown>>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`WebSocket ${streamId} timed out`)), REQUEST_TIMEOUT_MS);
    const onMessage = (raw: WebSocket.RawData) => {
      const event = JSON.parse(raw.toString()) as Record<string, unknown>;
      if (event.stream_id !== streamId) return;
      if (event.type === "error") {
        clearTimeout(timer);
        socket.off("message", onMessage);
        reject(new Error(`WebSocket ${streamId} returned an error`));
      } else if (event.type === "response.completed") {
        clearTimeout(timer);
        socket.off("message", onMessage);
        resolve(event.response as Record<string, unknown>);
      }
    };
    socket.on("message", onMessage);
  });
  try {
    const rootPending = waitFor("ws-root");
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "ws-root",
      model,
      store: false,
      input: "Remember WS_MEMORY_4P2M and reply READY.",
      reasoning: { effort: "low" },
    }));
    const root = await rootPending;
    const parentId = responseId(root);
    const childPending = waitFor("ws-child");
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "ws-child",
      model,
      store: false,
      previous_response_id: parentId,
      input: "Return the exact remembered token.",
      reasoning: { effort: "low" },
    }));
    const child = await childPending;
    if (!outputText(child).includes("WS_MEMORY_4P2M")) {
      throw new Error("WebSocket stored continuation forgot its token");
    }
  } finally {
    socket.close();
  }
}

async function verifyWebSocketMeasured(
  apiKey: string,
  model: string,
): Promise<Record<string, unknown>> {
  const measured = await measureAction(() => verifyWebSocket(apiKey, model));
  if (measured.transport.flightCompletions < 2) {
    throw new Error("Candidate transport did not record both WebSocket flights");
  }
  return {
    latencyMs: rounded(measured.latencyMs),
    dataChannelBytes: measured.transport.dataPublishedBytes,
    flightCompletions: measured.transport.flightCompletions,
    retransmittedBytes: measured.transport.retransmittedBytes,
    runtimesUsed: measured.transport.runtimesUsed,
    memoryRecall: "passed",
  };
}

async function verifyDifferentProject(
  system: BrowserlessSystem,
  parentId: string,
  model: string,
): Promise<void> {
  const scope = system.router.responseProvenanceAuthorizationScope();
  const lookup = scope ? system.router.lookupResponseProvenance(parentId, scope) : { kind: "unknown" as const };
  const origin = lookup.kind === "known" ? lookup.provenance.projectId : null;
  const target = system.registry.runtimes().find(runtime => runtime.projectId !== origin);
  if (!target) throw new Error("A second dedicated test project is required");
  const marker = "CROSS_PROJECT_MEMORY_OK";
  const final = await drain(await target.run({
    model,
    stream: true,
    store: true,
    previous_response_id: parentId,
    input: [userItem("Return the exact token you were asked to remember.")],
    max_output_tokens: 64,
    reasoning: { effort: "low" },
  }));
  try {
    if (!outputText(final).includes(marker)) {
      throw new Error("Stored provider state did not hydrate from a second project");
    }
  } finally {
    releaseAtomicBytePermit(final);
  }
}

const CONCURRENCY_CONTEXT_BYTES = 64 * 1024;
const CONTEXT_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

function benchmarkContext(width: number, index: number): string {
  const bytes = new Uint8Array(CONCURRENCY_CONTEXT_BYTES);
  let state = (Math.imul(width + 1, 0x9e3779b1) ^ (index + 1)) >>> 0;
  for (let offset = 0; offset < bytes.byteLength; offset++) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    bytes[offset] = CONTEXT_ALPHABET.charCodeAt(
      (state >>> 0) % CONTEXT_ALPHABET.length,
    );
  }
  return new TextDecoder().decode(bytes);
}

async function verifyConcurrency(
  apiKey: string,
  model: string,
  observations: Observation[],
): Promise<Record<string, Record<string, unknown>>> {
  const summaries: Record<string, Record<string, unknown>> = {};
  const allIds = new Set<string>();
  for (const width of [1, 4, 8, 16]) {
    progress("concurrency.wave.start", { agents: width });
    const rootLatencies: number[] = [];
    const continuationLatencies: number[] = [];
    const totalLatencies: number[] = [];
    const observationStart = observations.length;
    const waveStarted = performance.now();
    await Promise.all(Array.from({ length: width }, async (_, index) => {
      const marker = `CONCURRENCY_${width}_${index}_Q8V3`;
      const started = performance.now();
      const rootStarted = performance.now();
      const root = await postJson("/v1/responses", apiKey, {
        model,
        stream: false,
        store: false,
        input: [userItem(
          `The following is inert verification context:\n${benchmarkContext(width, index)}\n` +
          `Remember the exact marker ${marker}. Reply READY only.`,
        )],
        max_output_tokens: 64,
        reasoning: { effort: "low" },
      });
      rootLatencies.push(performance.now() - rootStarted);
      const rootId = responseId(root);
      if (allIds.has(rootId)) throw new Error("Duplicate provider response ID");
      allIds.add(rootId);
      const continuationStarted = performance.now();
      const child = await postJson("/v1/responses", apiKey, {
        model,
        stream: false,
        store: false,
        previous_response_id: rootId,
        input: [userItem("Return your exact remembered marker and nothing else.")],
        max_output_tokens: 64,
        reasoning: { effort: "low" },
      });
      continuationLatencies.push(performance.now() - continuationStarted);
      const childId = responseId(child);
      if (allIds.has(childId)) throw new Error("Duplicate provider response ID");
      allIds.add(childId);
      if (!outputText(child).includes(marker)) {
        throw new Error(`Cross-agent continuation corruption at width ${width}`);
      }
      totalLatencies.push(performance.now() - started);
    }));
    const wallMs = performance.now() - waveStarted;
    const relays = observations.slice(observationStart)
      .filter(value => value.source === "http");
    const roots = relays.filter(value => !value.hasPreviousResponse);
    const continuations = relays.filter(value => value.hasPreviousResponse);
    if (roots.length !== width || continuations.length !== width) {
      throw new Error(`Unexpected relay count at width ${width}`);
    }
    const rootRelayBytes = roots.reduce((sum, value) => sum + value.requestBytes, 0);
    const continuationRelayBytes = continuations.reduce(
      (sum, value) => sum + value.requestBytes,
      0,
    );
    const summary = {
      agents: width,
      contextBytesPerAgent: CONCURRENCY_CONTEXT_BYTES,
      twoTurnRequests: width * 2,
      wallMs: rounded(wallMs),
      twoTurnThroughputPerSecond: rounded(width * 2 / (wallMs / 1_000)),
      rootLatency: summarize(rootLatencies),
      continuationLatency: summarize(continuationLatencies),
      twoTurnLatency: summarize(totalLatencies),
      rootRelayBytes,
      continuationRelayBytes,
      continuationVsRootRelayByteReductionPercent: byteReductionPercent(
        rootRelayBytes,
        continuationRelayBytes,
      ),
    };
    summaries[String(width)] = summary;
    progress("concurrency.wave.passed", {
      agents: width,
      wallMs: summary.wallMs,
      requestsPerSecond: summary.twoTurnThroughputPerSecond,
      relayByteReductionPercent: summary.continuationVsRootRelayByteReductionPercent,
    });
  }
  return summaries;
}

async function verifyConcurrencyMeasured(
  apiKey: string,
  model: string,
): Promise<Record<string, Record<string, unknown>>> {
  const summaries: Record<string, Record<string, unknown>> = {};
  const allIds = new Set<string>();
  const runNonce = crypto.randomUUID();
  for (const width of [1, 4, 8, 16]) {
    progress("concurrency.wave.start", { agents: width });
    const markers = Array.from(
      { length: width },
      (_, index) => `CONCURRENCY_${width}_${index}_${runNonce}_Q8V3`,
    );
    const rootBodies = markers.map((marker, index) => ({
      model,
      stream: false,
      store: false,
      input: [userItem(
        `The following is inert verification context:\n${benchmarkContext(width, index)}\n` +
          `Remember the exact marker ${marker}. Reply READY only.`,
      )],
      max_output_tokens: 64,
      reasoning: { effort: "low" },
    }));
    const waveStarted = performance.now();
    const before = await candidateTransportSnapshot();
    const rootWaveStarted = performance.now();
    const roots = await Promise.all(rootBodies.map(async (body, index) => {
      const started = performance.now();
      const value = await postJson("/v1/responses", apiKey, body);
      const id = responseId(value);
      if (allIds.has(id)) throw new Error("Duplicate provider response ID");
      allIds.add(id);
      return {
        value,
        id,
        latencyMs: performance.now() - started,
        index,
      };
    }));
    const rootWallMs = performance.now() - rootWaveStarted;
    const afterRoots = await candidateTransportSnapshot();
    const rootTransport = transportDelta(before, afterRoots);
    if (rootTransport.flightCompletions !== width) {
      throw new Error(`Unexpected root flight count at width ${width}`);
    }

    const continuationBodies = roots.map(root => ({
      model,
      stream: false,
      store: false,
      previous_response_id: root.id,
      input: [userItem("Return your exact remembered marker and nothing else.")],
      max_output_tokens: 64,
      reasoning: { effort: "low" },
    }));
    const continuationWaveStarted = performance.now();
    const children = await Promise.all(continuationBodies.map(async (body, index) => {
      const started = performance.now();
      const value = await postJson("/v1/responses", apiKey, body);
      const id = responseId(value);
      if (allIds.has(id)) throw new Error("Duplicate provider response ID");
      allIds.add(id);
      if (!outputText(value).includes(markers[index]!)) {
        throw new Error(`Cross-agent continuation corruption at width ${width}`);
      }
      return {
        value,
        latencyMs: performance.now() - started,
        totalLatencyMs: performance.now() - waveStarted,
      };
    }));
    const continuationWallMs = performance.now() - continuationWaveStarted;
    const afterChildren = await candidateTransportSnapshot();
    const continuationTransport = transportDelta(afterRoots, afterChildren);
    if (continuationTransport.flightCompletions !== width) {
      throw new Error(`Unexpected continuation flight count at width ${width}`);
    }
    if (
      afterChildren.quarantined !== before.quarantined ||
      afterChildren.ambiguousQuarantined !== before.ambiguousQuarantined
    ) {
      throw new Error(`Concurrency wave ${width} changed quarantine counters`);
    }

    const wallMs = performance.now() - waveStarted;
    const rootClientBytes = rootBodies.reduce(
      (sum, body) => sum + Buffer.byteLength(JSON.stringify(body), "utf8"),
      0,
    );
    const continuationClientBytes = continuationBodies.reduce(
      (sum, body) => sum + Buffer.byteLength(JSON.stringify(body), "utf8"),
      0,
    );
    const fullHistoryBaselineClientBytes = roots.reduce((sum, root, index) => {
      const rootInput = rootBodies[index]!.input;
      const output = Array.isArray(root.value.output) ? root.value.output : [];
      const baseline = {
        ...rootBodies[index],
        input: [
          ...rootInput,
          ...output,
          ...continuationBodies[index]!.input,
        ],
      };
      return sum + Buffer.byteLength(JSON.stringify(baseline), "utf8");
    }, 0);
    const reduction = byteReductionPercent(
      fullHistoryBaselineClientBytes,
      continuationTransport.dataPublishedBytes,
    );
    if (reduction < 90) {
      throw new Error(`Continuation relay reduction fell below 90% at width ${width}`);
    }

    const summary = {
      agents: width,
      contextBytesPerAgent: CONCURRENCY_CONTEXT_BYTES,
      twoTurnRequests: width * 2,
      wallMs: rounded(wallMs),
      rootWallMs: rounded(rootWallMs),
      continuationWallMs: rounded(continuationWallMs),
      twoTurnThroughputPerSecond: rounded(width * 2 / (wallMs / 1_000)),
      rootLatency: summarize(roots.map(value => value.latencyMs)),
      continuationLatency: summarize(children.map(value => value.latencyMs)),
      twoTurnLatency: summarize(children.map(value => value.totalLatencyMs)),
      rootClientBytes,
      continuationClientBytes,
      fullHistoryBaselineClientBytes,
      rootDataChannelBytes: rootTransport.dataPublishedBytes,
      continuationDataChannelBytes: continuationTransport.dataPublishedBytes,
      continuationVsFullHistoryDataReductionPercent: reduction,
      rootRuntimesUsed: rootTransport.runtimesUsed,
      continuationRuntimesUsed: continuationTransport.runtimesUsed,
      rootRetransmittedBytes: rootTransport.retransmittedBytes,
      continuationRetransmittedBytes: continuationTransport.retransmittedBytes,
      quarantineDelta: 0,
      ambiguousQuarantineDelta: 0,
    };
    summaries[String(width)] = summary;
    progress("concurrency.wave.passed", {
      agents: width,
      wallMs: summary.wallMs,
      requestsPerSecond: summary.twoTurnThroughputPerSecond,
      relayByteReductionPercent: summary.continuationVsFullHistoryDataReductionPercent,
    });
  }
  return summaries;
}

async function verifyExistingCandidate(
  options: PersistedContinuationLiveOptions,
  env: Record<string, string | undefined>,
): Promise<void> {
  const apiKey = env.GHOSTPROV_API_KEY?.trim();
  if (!apiKey) throw new Error("GHOSTPROV_API_KEY is required for the existing candidate verifier");
  progress("existing-candidate.snapshot.start");
  const initial = await candidateTransportSnapshot();
  progress("existing-candidate.snapshot.passed", {
    ready: initial.ready,
    unavailable: initial.unavailable,
    capacity: initial.capacity,
    available: initial.available,
  });
  const results: Record<string, unknown> = {
    fleet: {
      requested: options.projectIds.length,
      ready: initial.ready,
      unavailable: initial.unavailable,
      capacity: initial.capacity,
      available: initial.available,
      releaseRevision: V2_R8_RELEASE.revision,
      buildHash: V2_R8_RELEASE.buildHash,
      schemaHash: V2_R8_RELEASE.schemaHash,
    },
  };

  progress("claude-continuation.start");
  const replayProbe = await probeAnthropicReplayEligibility(apiKey, env);
  results.claudeReplayEligibility = replayProbe;
  progress("claude-replay-eligibility.measured", replayProbe);
  if (!replayProbe.eligible) {
    throw new Error("Live provider response is ineligible for Anthropic replay");
  }
  progress("claude-continuation.resume");
  results.claude = await verifyClaudeMeasured(apiKey, env);
  progress("claude-continuation.passed");

  progress("http-continuation.start");
  results.http = await verifyHttpAndInferenceMeasured(apiKey, options.model);
  progress("http-continuation.passed");

  progress("websocket-continuation.start");
  results.websocket = await verifyWebSocketMeasured(apiKey, options.model);
  progress("websocket-continuation.passed");

  progress("concurrency.matrix.start");
  results.concurrency = await verifyConcurrencyMeasured(apiKey, options.model);
  progress("concurrency.matrix.passed");

  const final = await candidateTransportSnapshot();
  if (final.quarantined !== initial.quarantined ||
    final.ambiguousQuarantined !== initial.ambiguousQuarantined) {
    throw new Error("Live matrix changed candidate quarantine counters");
  }
  await protectedHealth();
  console.log(JSON.stringify({
    live: true,
    mode: "existing-owned-candidate",
    candidatePort: CANDIDATE_PORT,
    projectCount: options.projectIds.length,
    projectsCreated: 0,
    projectsDeleted: 0,
    protected18787: "healthy-before-and-after",
    directHydrationChecks: "deferred-until-candidate-process-ownership-is-restored",
    results,
  }, null, 2));
}

export async function main(
  env: Record<string, string | undefined> = process.env,
): Promise<void> {
  const options = resolvePersistedContinuationLiveOptions(env);
  progress("protected-health.before.start");
  await protectedHealth();
  progress("protected-health.before.passed");
  if (env.GP_CONTINUATION_USE_EXISTING_CANDIDATE === "1") {
    await verifyExistingCandidate(options, env);
    return;
  }
  let active: Awaited<ReturnType<typeof launch>> | null = null;
  let restartParentId = "";
  let restartProjectId = "";
  const results: Record<string, unknown> = {};
  try {
    progress("fleet.initial.start");
    active = await launch(env, options);
    results.initialFleet = active.readiness;
    const apiKey = active.system.config.apiKey;

    progress("http-continuation.start");
    const http = await verifyHttpAndInference(apiKey, options.model, active.observations);
    results.http = {
      rootRelayBytes: http.rootRelayBytes,
      explicitSuffixBytes: http.explicitSuffixBytes,
      inferredFullHistoryClientBytes: http.inferredClientBytes,
      inferredSuffixBytes: http.inferredSuffixBytes,
      inferredReductionPercent: http.inferredReductionPercent,
    };
    progress("http-continuation.passed", {
      inferredReductionPercent: http.inferredReductionPercent,
    });

    progress("websocket-continuation.start");
    await verifyWebSocket(apiKey, options.model);
    results.websocket = "passed";
    progress("websocket-continuation.passed");

    progress("claude-continuation.start");
    results.claude = await verifyClaude(apiKey, active.observations);
    progress("claude-continuation.passed");

    progress("cross-project-hydration.start");
    const crossProjectRoot = await postJson("/v1/responses", apiKey, {
      model: options.model,
      stream: false,
      store: false,
      input: [userItem("Remember CROSS_PROJECT_MEMORY_OK and reply READY only.")],
      max_output_tokens: 64,
      reasoning: { effort: "low" },
    });
    await verifyDifferentProject(
      active.system,
      responseId(crossProjectRoot),
      options.model,
    );
    results.differentProjectHydration = "passed";
    progress("cross-project-hydration.passed");

    progress("same-project-restart.root.start");
    const restartRoot = await postJson("/v1/responses", apiKey, {
      model: options.model,
      stream: false,
      store: false,
      input: [userItem("Remember RESTART_MEMORY_2D6N and reply READY only.")],
      max_output_tokens: 64,
      reasoning: { effort: "low" },
    });
    restartParentId = responseId(restartRoot);
    const restartScope = active.system.router.responseProvenanceAuthorizationScope();
    const restartLookup = restartScope
      ? active.system.router.lookupResponseProvenance(restartParentId, restartScope)
      : { kind: "unknown" as const };
    if (restartLookup.kind !== "known") {
      throw new Error("Restart root lacked authoritative project provenance");
    }
    restartProjectId = restartLookup.provenance.projectId;
    progress("same-project-restart.relaunch.start");
    active.restore();
    await active.system.close();
    active = null;

    active = await launch(env, options);
    results.restartedFleet = active.readiness;
    const restartedRuntime = active.system.registry.runtimes().find(
      runtime => runtime.projectId === restartProjectId,
    );
    if (!restartedRuntime) throw new Error("Fresh runtime for the original project is missing");
    const restarted = await drain(await restartedRuntime.run({
      model: options.model,
      stream: true,
      store: true,
      previous_response_id: restartParentId,
      input: [userItem("Return the exact remembered token.")],
      max_output_tokens: 64,
      reasoning: { effort: "low" },
    }));
    try {
      if (!outputText(restarted).includes("RESTART_MEMORY_2D6N")) {
        throw new Error("Fresh same-project runtime did not hydrate stored provider state");
      }
    } finally {
      releaseAtomicBytePermit(restarted);
    }
    results.sameProjectFreshRuntime = "passed";
    progress("same-project-restart.passed");

    progress("concurrency.matrix.start");
    results.concurrency = await verifyConcurrency(
      active.system.config.apiKey,
      options.model,
      active.observations,
    );
    progress("concurrency.matrix.passed");
  } finally {
    if (active) {
      active.restore();
      await active.system.close();
    }
  }
  progress("protected-health.after.start");
  await protectedHealth();
  progress("protected-health.after.passed");
  console.log(JSON.stringify({
    live: true,
    candidatePort: CANDIDATE_PORT,
    projectCount: options.projectIds.length,
    projectsCreated: 0,
    projectsDeleted: 0,
    protected18787: "healthy-before-and-after",
    results,
  }, null, 2));
}

if (import.meta.main) {
  try {
    await main();
  } catch (error) {
    progress("failed", {
      errorName: error instanceof Error ? error.name : "UnknownError",
    });
    throw error;
  }
}
