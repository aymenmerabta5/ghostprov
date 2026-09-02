import { createHash, randomUUID } from "node:crypto";
import {
  mkdir,
  rename,
  stat,
  unlink,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { PrismAccountSession } from "../src/prism/account-session";
import {
  PrismProjectError,
  PrismProjectsClient,
} from "../src/prism/projects-client";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { SandboxGateway } from "../src/sandbox/gateway";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { SandboxYDoc } from "../src/sandbox/ydoc";
import type { StandaloneSearchRequest } from "../src/http/standalone-search";
import { buildStandaloneSearchResponsesRequest } from "../src/search/responses-adapter";

const SEARCH_MODEL = "gpt-5.6-sol";
const SEARCH_TIMEOUT_SECONDS = 30;
const SEARCH_RESPONSE_LIMIT_BYTES = 64 * 1024;
const LOG_RESPONSE_LIMIT_BYTES = 512 * 1024;
const MARKER_PREFIX = "GPSEARCHPART";
const MARKER_DONE = "GPSEARCHDONE";
const MAX_MARKER_PARTS = 32;
const MAX_MARKER_JSON_BYTES = 4 * 1024;

const MAIN_SOURCE = [
  "\\documentclass{article}",
  "\\begin{document}",
  "\\input{probe-output.tex}",
  "\\end{document}",
  "",
].join("\n");

const OUTPUT_PLACEHOLDER = "\\typeout{GPSEARCHWAITING}\n";

export interface StandaloneSearchProbeOptions {
  readonly cookieFile: string;
  readonly prismOrigin: string;
  readonly timeoutMs: number;
  readonly stateDirectory: string;
}

export interface StandaloneSearchProbeMetadata {
  readonly version: 1;
  readonly ok: boolean;
  readonly outcome:
    | "ok"
    | "http_error"
    | "body_too_large"
    | "invalid_json"
    | "invalid_schema"
    | "missing_environment"
    | "transport_error";
  readonly status: number | null;
  readonly responseBytes: number;
  readonly jsonObject: boolean;
  readonly outputChars: number | null;
  readonly resultsCount: number | null;
  readonly encryptedOutputPresent: boolean | null;
  readonly unexpectedFieldCount: number | null;
  readonly errorType: string | null;
  readonly errorCode: string | null;
  readonly errorParam: string | null;
  readonly errorClassification: "model_unsupported" | "invalid_parameter" | "unknown" | null;
  readonly webSearchSeen: boolean;
  readonly terminal: "response.completed" | "response.failed" | "response.incomplete" | null;
}

export type ExactProjectCleanupStatus =
  | "not_needed"
  | "deleted"
  | "absent"
  | "failed";

export interface StandaloneSearchProbeReport {
  readonly search: StandaloneSearchProbeMetadata;
  readonly projectCreated: true;
  readonly sandboxProvisioned: true;
  readonly queryAttempts: 1;
  readonly cleanup: "deleted" | "absent";
  readonly cleanupStateRetained: boolean;
  readonly accountFlushSucceeded: boolean;
  readonly configuredProjectsTouched: false;
  readonly protectedServiceTouched: false;
}

interface ProbeState {
  readonly version: 1;
  readonly runId: string;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly phase: string;
  readonly updatedAt: string;
}

interface ExactProjectDeleter {
  deleteManaged(id: ProjectId, signal?: AbortSignal): Promise<void>;
}

export class StandaloneSearchProbeFailure extends Error {
  readonly name = "StandaloneSearchProbeFailure";

  constructor(
    readonly stage: string,
    readonly cleanup: ExactProjectCleanupStatus,
    readonly search: StandaloneSearchProbeMetadata | null,
    readonly cleanupStateRetained: boolean,
  ) {
    super(`Standalone search probe failed during ${stage}`);
  }
}

function requiredGuard(
  env: Record<string, string | undefined>,
  name: string,
): void {
  if (env[name] !== "1") throw new Error(`Set ${name}=1 to run the standalone search probe`);
}

export function resolveStandaloneSearchProbeOptions(
  env: Record<string, string | undefined>,
): StandaloneSearchProbeOptions {
  requiredGuard(env, "GP_LIVE_STANDALONE_SEARCH");
  requiredGuard(env, "GP_LIVE_CREATE_TEST_PROJECTS");
  requiredGuard(env, "GP_LIVE_SANDBOX_PROBE");
  requiredGuard(env, "GP_ALLOW_PROJECT_DELETE");

  if (env.GP_LIVE_PROJECT_COUNT !== undefined && env.GP_LIVE_PROJECT_COUNT !== "1") {
    throw new Error("GP_LIVE_PROJECT_COUNT must be exactly 1 for the standalone search probe");
  }
  const rawCookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!rawCookieFile) throw new Error("PRISM_COOKIE_FILE is required");

  const rawOrigin = (env.PRISM_ORIGIN ?? "https://prism.openai.com").trim();
  let origin: URL;
  try {
    origin = new URL(rawOrigin);
  } catch {
    throw new Error("PRISM_ORIGIN is invalid");
  }
  if (
    (origin.protocol !== "https:" && origin.protocol !== "http:") ||
    origin.username ||
    origin.password ||
    origin.search ||
    origin.hash ||
    (origin.pathname !== "/" && origin.pathname !== "")
  ) {
    throw new Error("PRISM_ORIGIN must be a credential-free HTTP(S) origin");
  }

  const timeoutMs = Number(env.GP_LIVE_STANDALONE_SEARCH_TIMEOUT_MS ?? 120_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 60_000 || timeoutMs > 300_000) {
    throw new Error("GP_LIVE_STANDALONE_SEARCH_TIMEOUT_MS must be 60000..300000");
  }

  const stateRoot = path.resolve(env.GP_TEST_STATE_DIR?.trim() || ".ghostprov");
  return Object.freeze({
    cookieFile: path.resolve(rawCookieFile),
    prismOrigin: origin.origin,
    timeoutMs,
    stateDirectory: path.join(stateRoot, "standalone-search-probes"),
  });
}

export function createStandaloneSearchRequest(id: string): Readonly<Record<string, unknown>> {
  if (!/^[A-Za-z0-9-]{1,128}$/.test(id)) {
    throw new TypeError("Standalone search request ID is invalid");
  }
  return Object.freeze({
    id,
    model: SEARCH_MODEL,
    input: Object.freeze([
      Object.freeze({
        type: "message",
        role: "user",
        content: Object.freeze([
          Object.freeze({
            type: "input_text",
            text: "Find the official OpenAI API documentation page.",
          }),
        ]),
      }),
    ]),
    commands: Object.freeze({
      search_query: Object.freeze([
        Object.freeze({
          q: "OpenAI API documentation",
          domains: Object.freeze(["developers.openai.com"]),
        }),
      ]),
      response_length: "short",
    }),
    settings: Object.freeze({
      search_context_size: "low",
      allowed_callers: Object.freeze(["direct"]),
      external_web_access: true,
    }),
    max_output_tokens: 10_000,
  });
}

export function nativeSandboxSearchUpstreamRequest(
  request: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> {
  const expected = ["id", "model", "input", "commands", "settings", "max_output_tokens"];
  const keys = Object.keys(request).sort();
  if (keys.length !== expected.length || !expected.sort().every((key, index) => keys[index] === key)) {
    throw new TypeError("Standalone search probe request shape is invalid");
  }
  if (typeof request.id !== "string" || !/^[A-Za-z0-9-]{1,128}$/.test(request.id)) {
    throw new TypeError("Standalone search request ID is invalid");
  }
  return Object.freeze(buildStandaloneSearchResponsesRequest(
    request as unknown as StandaloneSearchRequest,
  ));
}

export function buildSandboxSearchPython(request: Readonly<Record<string, unknown>>): string {
  const upstreamRequest = nativeSandboxSearchUpstreamRequest(request);
  const encodedRequest = Buffer.from(JSON.stringify(upstreamRequest), "utf8").toString("base64");
  return String.raw`
import base64, json, os, ssl, urllib.error, urllib.request

LIMIT = ${SEARCH_RESPONSE_LIMIT_BYTES}
TIMEOUT = ${SEARCH_TIMEOUT_SECONDS}
REQUEST = base64.b64decode("${encodedRequest}")
def emit(metadata):
    raw = json.dumps(metadata, separators=(",", ":"), sort_keys=True).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii")
    chunks = [encoded[index:index + 40] for index in range(0, len(encoded), 40)]
    with open("/workspace/probe-output.tex", "w", encoding="ascii") as output:
        total = len(chunks)
        for index, chunk in enumerate(chunks, 1):
            output.write("\\typeout{${MARKER_PREFIX}:%d:%d:%s}\n" % (index, total, chunk))
        output.write("\\typeout{${MARKER_DONE}:%d}\n" % total)

def base_metadata(outcome, status=None, response_bytes=0):
    return {
        "version": 1,
        "ok": False,
        "outcome": outcome,
        "status": status,
        "responseBytes": response_bytes,
        "jsonObject": False,
        "outputChars": None,
        "resultsCount": None,
        "encryptedOutputPresent": None,
        "unexpectedFieldCount": None,
        "errorType": None,
        "errorCode": None,
        "errorParam": None,
        "errorClassification": None,
        "webSearchSeen": False,
        "terminal": None,
    }

def safe_error_token(value):
    if not isinstance(value, str) or len(value) < 1 or len(value) > 64:
        return None
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-[]"
    return value if all(character in allowed for character in value) else None

def error_metadata(body):
    result = {
        "errorType": None,
        "errorCode": None,
        "errorParam": None,
        "errorClassification": "unknown",
    }
    try:
        root = json.loads(body.decode("utf-8"))
    except Exception:
        return result
    if not isinstance(root, dict):
        return result
    error = root.get("error")
    if not isinstance(error, dict):
        return result
    result["errorType"] = safe_error_token(error.get("type"))
    result["errorCode"] = safe_error_token(error.get("code"))
    result["errorParam"] = safe_error_token(error.get("param"))
    message = error.get("message")
    normalized = message.lower() if isinstance(message, str) else ""
    discriminator = " ".join(filter(None, [result["errorType"], result["errorCode"]])).lower()
    if (
        "model_not_found" in discriminator
        or "unsupported_model" in discriminator
        or ("model" in normalized and any(term in normalized for term in [
            "not found", "not supported", "unsupported", "does not exist", "unavailable"
        ]))
    ):
        result["errorClassification"] = "model_unsupported"
    elif (
        result["errorParam"] is not None
        or "invalid" in discriminator
        or "invalid" in normalized
        or "required" in normalized
    ):
        result["errorClassification"] = "invalid_parameter"
    return result

def read_bounded(response):
    raw_length = response.headers.get("content-length")
    try:
        if raw_length is not None and int(raw_length) > LIMIT:
            return None, True
    except Exception:
        pass
    body = response.read(LIMIT + 1)
    if len(body) > LIMIT:
        return None, True
    return body, False

def run():
    base = os.environ.get("OPENAI_BASE_URL", "").strip()
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    ca = os.environ.get("PROXY_CA_CERT_PATH", "").strip()
    if not base or not key:
        return base_metadata("missing_environment")
    context = ssl.create_default_context(cafile=ca) if ca and os.path.isfile(ca) else ssl.create_default_context()
    request = urllib.request.Request(
        base.rstrip("/") + "/responses",
        data=REQUEST,
        method="POST",
        headers={
            "authorization": "Bearer " + key,
            "content-type": "application/json",
            "accept": "text/event-stream",
            "originator": "codex_exec",
        },
    )
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=context),
        NoRedirect(),
    )
    try:
        with opener.open(request, timeout=TIMEOUT) as response:
            status = int(response.status)
            body, too_large = read_bounded(response)
    except urllib.error.HTTPError as error:
        status = int(error.code)
        try:
            body, too_large = read_bounded(error)
            size = 0 if body is None else len(body)
        except Exception:
            body = None
            too_large = False
            size = 0
        metadata = base_metadata("body_too_large" if too_large else "http_error", status, size)
        if body is not None:
            metadata.update(error_metadata(body))
        return metadata
    except Exception:
        return base_metadata("transport_error")

    if too_large or body is None:
        return base_metadata("body_too_large", status)
    metadata = base_metadata("invalid_json", status, len(body))
    try:
        text = body.decode("utf-8")
    except Exception:
        return metadata

    terminal_response = None
    source_count = 0
    output_chars = 0
    encrypted = False
    web_search_seen = False
    try:
        for raw_frame in text.replace("\r\n", "\n").replace("\r", "\n").split("\n\n"):
            event_name = None
            data_lines = []
            for line in raw_frame.split("\n"):
                if line.startswith("event:"):
                    event_name = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
            if not data_lines:
                continue
            value = json.loads("\n".join(data_lines))
            if not isinstance(value, dict):
                continue
            event_type = value.get("type") if isinstance(value.get("type"), str) else event_name
            if isinstance(event_type, str) and "web_search" in event_type:
                web_search_seen = True
            if event_type in {"response.completed", "response.failed", "response.incomplete"}:
                metadata["terminal"] = event_type
                terminal_response = value.get("response")
        if not isinstance(terminal_response, dict):
            metadata["outcome"] = "invalid_schema"
            return metadata
        metadata["jsonObject"] = True
        for item in terminal_response.get("output", []):
            if not isinstance(item, dict):
                continue
            if item.get("type") == "web_search_call":
                web_search_seen = True
                action = item.get("action")
                if isinstance(action, dict) and isinstance(action.get("sources"), list):
                    source_count += len(action["sources"])
            if item.get("type") == "reasoning" and isinstance(item.get("encrypted_content"), str):
                encrypted = encrypted or bool(item["encrypted_content"])
            if item.get("type") == "message" and isinstance(item.get("content"), list):
                for part in item["content"]:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "output_text" and isinstance(part.get("text"), str):
                        output_chars += len(part["text"])
                    if isinstance(part.get("annotations"), list):
                        source_count += len(part["annotations"])
    except Exception:
        metadata["outcome"] = "invalid_json"
        return metadata

    metadata["webSearchSeen"] = web_search_seen
    metadata["outputChars"] = output_chars
    metadata["resultsCount"] = source_count
    metadata["encryptedOutputPresent"] = encrypted
    metadata["unexpectedFieldCount"] = 0
    metadata["ok"] = (
        200 <= status < 300
        and metadata["terminal"] == "response.completed"
        and web_search_seen
        and output_chars > 0
    )
    metadata["outcome"] = "ok" if metadata["ok"] else "invalid_schema"
    return metadata

try:
    emit(run())
except BaseException:
    emit(base_metadata("transport_error"))
`;
}

export function buildSandboxSearchLauncher(python: string): string {
  if (python.includes("\nPYEOF\n")) throw new Error("Sandbox search source contains its heredoc terminator");
  return [
    "# GhostProv standalone-search launcher for one disposable project",
    "if (!-e '/tmp/gp-standalone-search.done') {",
    "  open my $gp_fh, '>', '/tmp/gp_standalone_search.py';",
    "  print $gp_fh <<'PYEOF';",
    python,
    "PYEOF",
    "  close $gp_fh;",
    "  system('python3 /tmp/gp_standalone_search.py >/tmp/gp-standalone-search.stdout 2>&1');",
    "  open my $gp_done, '>', '/tmp/gp-standalone-search.done';",
    "  print $gp_done \"done\\n\";",
    "  close $gp_done;",
    "}",
    "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
    "1;",
    "",
  ].join("\n");
}

function object(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function nullableInteger(value: unknown, min: number, max: number): number | null | undefined {
  if (value === null) return null;
  return Number.isSafeInteger(value) && (value as number) >= min && (value as number) <= max
    ? value as number
    : undefined;
}

function parseMetadata(value: unknown): StandaloneSearchProbeMetadata | null {
  const raw = object(value);
  if (!raw || raw.version !== 1 || typeof raw.ok !== "boolean") return null;
  const outcomes = new Set<StandaloneSearchProbeMetadata["outcome"]>([
    "ok",
    "http_error",
    "body_too_large",
    "invalid_json",
    "invalid_schema",
    "missing_environment",
    "transport_error",
  ]);
  if (typeof raw.outcome !== "string" || !outcomes.has(raw.outcome as StandaloneSearchProbeMetadata["outcome"])) {
    return null;
  }
  const status = nullableInteger(raw.status, 100, 599);
  const responseBytes = nullableInteger(raw.responseBytes, 0, SEARCH_RESPONSE_LIMIT_BYTES);
  const outputChars = nullableInteger(raw.outputChars, 0, SEARCH_RESPONSE_LIMIT_BYTES);
  const resultsCount = nullableInteger(raw.resultsCount, 0, 10_000);
  const unexpectedFieldCount = nullableInteger(raw.unexpectedFieldCount, 0, 1_000);
  const safeErrorToken = (candidate: unknown): string | null | undefined => {
    if (candidate === null) return null;
    return typeof candidate === "string" && /^[A-Za-z0-9_.\[\]-]{1,64}$/.test(candidate)
      ? candidate
      : undefined;
  };
  const errorType = safeErrorToken(raw.errorType);
  const errorCode = safeErrorToken(raw.errorCode);
  const errorParam = safeErrorToken(raw.errorParam);
  const classifications = new Set(["model_unsupported", "invalid_parameter", "unknown"]);
  const errorClassification = raw.errorClassification === null
    ? null
    : typeof raw.errorClassification === "string" && classifications.has(raw.errorClassification)
      ? raw.errorClassification as StandaloneSearchProbeMetadata["errorClassification"]
      : undefined;
  const terminals = new Set(["response.completed", "response.failed", "response.incomplete"]);
  const terminal = raw.terminal === null
    ? null
    : typeof raw.terminal === "string" && terminals.has(raw.terminal)
      ? raw.terminal as StandaloneSearchProbeMetadata["terminal"]
      : undefined;
  if (
    status === undefined ||
    responseBytes === undefined || responseBytes === null ||
    outputChars === undefined ||
    resultsCount === undefined ||
    unexpectedFieldCount === undefined ||
    errorType === undefined ||
    errorCode === undefined ||
    errorParam === undefined ||
    errorClassification === undefined ||
    terminal === undefined ||
    typeof raw.webSearchSeen !== "boolean" ||
    typeof raw.jsonObject !== "boolean" ||
    (raw.encryptedOutputPresent !== null && typeof raw.encryptedOutputPresent !== "boolean")
  ) return null;
  return Object.freeze({
    version: 1,
    ok: raw.ok,
    outcome: raw.outcome as StandaloneSearchProbeMetadata["outcome"],
    status,
    responseBytes,
    jsonObject: raw.jsonObject,
    outputChars,
    resultsCount,
    encryptedOutputPresent: raw.encryptedOutputPresent as boolean | null,
    unexpectedFieldCount,
    errorType,
    errorCode,
    errorParam,
    errorClassification,
    webSearchSeen: raw.webSearchSeen,
    terminal,
  });
}

export function parseStandaloneSearchProbeMarkers(
  logs: string,
): StandaloneSearchProbeMetadata | null {
  if (logs.length > LOG_RESPONSE_LIMIT_BYTES) return null;
  const partPattern = new RegExp(
    `${MARKER_PREFIX}:(\\d{1,2}):(\\d{1,2}):([A-Za-z0-9+/=]{1,48})`,
    "g",
  );
  const donePattern = new RegExp(`${MARKER_DONE}:(\\d{1,2})`, "g");
  const parts = new Map<number, string>();
  let expectedTotal: number | null = null;
  for (const match of logs.matchAll(partPattern)) {
    const index = Number(match[1]);
    const total = Number(match[2]);
    const chunk = match[3];
    if (
      !Number.isSafeInteger(index) ||
      !Number.isSafeInteger(total) ||
      total < 1 ||
      total > MAX_MARKER_PARTS ||
      index < 1 ||
      index > total
    ) return null;
    if (expectedTotal !== null && expectedTotal !== total) return null;
    expectedTotal = total;
    const previous = parts.get(index);
    if (previous !== undefined && previous !== chunk) return null;
    parts.set(index, chunk);
  }
  let doneTotal: number | null = null;
  for (const match of logs.matchAll(donePattern)) {
    const value = Number(match[1]);
    if (doneTotal !== null && doneTotal !== value) return null;
    doneTotal = value;
  }
  if (
    expectedTotal === null ||
    doneTotal !== expectedTotal ||
    parts.size !== expectedTotal
  ) return null;
  const encoded = Array.from({ length: expectedTotal }, (_, index) => parts.get(index + 1)).join("");
  let bytes: Buffer;
  try {
    bytes = Buffer.from(encoded, "base64");
  } catch {
    return null;
  }
  if (bytes.byteLength < 2 || bytes.byteLength > MAX_MARKER_JSON_BYTES) return null;
  try {
    return parseMetadata(JSON.parse(bytes.toString("utf8")));
  } catch {
    return null;
  }
}

async function sleep(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) throw signal.reason;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(done, ms);
    const abort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      reject(signal.reason);
    };
    function done() {
      signal.removeEventListener("abort", abort);
      resolve();
    }
    signal.addEventListener("abort", abort, { once: true });
  });
}

async function waitForSearchMetadata(
  gateway: SandboxGateway,
  signal: AbortSignal,
): Promise<StandaloneSearchProbeMetadata> {
  while (!signal.aborted) {
    const logs = await gateway.fetchText("logs", { method: "GET", signal });
    if (logs.status === 200) {
      const metadata = parseStandaloneSearchProbeMarkers(logs.text);
      if (metadata) return metadata;
    }
    await sleep(250, signal);
  }
  throw signal.reason;
}

async function writeState(file: string, state: ProbeState): Promise<void> {
  await mkdir(path.dirname(file), { recursive: true });
  const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(temporary, JSON.stringify(state), { encoding: "utf8", mode: 0o600 });
  await rename(temporary, file);
}

export async function deleteExactCreatedProject(
  projects: ExactProjectDeleter,
  exactProjectId: ProjectId,
  creationAttempted: boolean,
  signal?: AbortSignal,
): Promise<ExactProjectCleanupStatus> {
  if (!creationAttempted) return "not_needed";
  try {
    await projects.deleteManaged(exactProjectId, signal);
    return "deleted";
  } catch (error) {
    if (error instanceof PrismProjectError && error.status === 404) return "absent";
    throw error;
  }
}

async function assertCredentialFile(file: string): Promise<void> {
  const metadata = await stat(file);
  if (!metadata.isFile() || metadata.size < 1) {
    throw new Error("PRISM_COOKIE_FILE must point to a nonempty file");
  }
}

function state(
  runId: string,
  id: ProjectId,
  title: string,
  phase: string,
): ProbeState {
  return {
    version: 1,
    runId,
    projectId: id,
    title,
    phase,
    updatedAt: new Date().toISOString(),
  };
}

export async function runStandaloneSearchProbe(
  env: Record<string, string | undefined> = process.env,
): Promise<StandaloneSearchProbeReport> {
  const options = resolveStandaloneSearchProbeOptions(env);
  await assertCredentialFile(options.cookieFile);

  const account = await PrismAccountSession.create({
    origin: options.prismOrigin,
    cookieFile: options.cookieFile,
  });

  const runId = randomUUID();
  const exactProjectId = projectId(randomUUID());
  const title = `GhostProv Standalone Search Probe ${new Date().toISOString()}`;
  const stateFile = path.join(options.stateDirectory, `${runId}.json`);
  await writeState(stateFile, state(runId, exactProjectId, title, "planned"));

  const projects = new PrismProjectsClient(account);
  const provisioning = new PrismProvisioningClient({ account });
  const operationSignal = AbortSignal.timeout(options.timeoutMs);
  let ydoc: SandboxYDoc | null = null;
  let stage = "project_creation";
  let creationAttempted = false;
  let projectCreated = false;
  let sandboxProvisioned = false;
  let queryAttempts = 0;
  let metadata: StandaloneSearchProbeMetadata | null = null;
  let failure: unknown = null;
  let cleanup: ExactProjectCleanupStatus = "not_needed";
  let cleanupStateRetained = true;
  let accountFlushSucceeded = true;

  try {
    creationAttempted = true;
    await writeState(stateFile, state(runId, exactProjectId, title, "creating"));
    await projects.create({ projectId: exactProjectId, title }, operationSignal);
    projectCreated = true;
    await writeState(stateFile, state(runId, exactProjectId, title, "created"));

    stage = "sandbox_provisioning";
    const sandbox = await provisioning.acquireSandbox(operationSignal, { maxAttempts: 1 });
    const credentials = await provisioning.provisionAcquired(
      exactProjectId,
      sandbox,
      operationSignal,
    );
    sandboxProvisioned = true;
    await writeState(stateFile, state(runId, exactProjectId, title, "provisioned"));

    const gateway = new SandboxGateway({
      projectId: exactProjectId,
      account,
      sandbox: credentials.sandbox,
      responseBodyLimitBytes: LOG_RESPONSE_LIMIT_BYTES,
    });
    let initialYSweet = true;
    ydoc = new SandboxYDoc({
      projectId: exactProjectId,
      mintYSweet: async () => {
        if (initialYSweet) {
          initialYSweet = false;
          return credentials.ySweet;
        }
        return provisioning.mintYSweet(exactProjectId, {
          source: "standalone-search-probe-reconnect",
          bootstrapAttempt: 1,
          previouslyConnected: true,
          sandboxUrl: credentials.sandbox.url,
          sandboxId: credentials.sandbox.sandboxId ?? null,
          sandboxSessionId: credentials.sandbox.sessionId ?? null,
        }, operationSignal);
      },
    });
    await ydoc.connect();
    const request = createStandaloneSearchRequest(randomUUID());
    const python = buildSandboxSearchPython(request);
    await ydoc.writePath("main.tex", MAIN_SOURCE);
    await ydoc.writePath("probe-output.tex", OUTPUT_PLACEHOLDER);
    await ydoc.writePath(".latexmkrc", buildSandboxSearchLauncher(python));

    stage = "search";
    queryAttempts = 1;
    await writeState(stateFile, state(runId, exactProjectId, title, "query_attempted"));
    const syncState = await ydoc.renderSyncState();
    await gateway.triggerRender({
      mainDocument: "main.tex",
      clientStateVector: syncState.clientStateVector,
      clientDeleteSetUpdate: syncState.clientDeleteSetUpdate,
    }, operationSignal);
    metadata = await waitForSearchMetadata(gateway, operationSignal);
  } catch (error) {
    failure = error;
  } finally {
    stage = failure === null ? "cleanup" : stage;
    await ydoc?.destroy().catch(() => undefined);
    const cleanupSignal = AbortSignal.timeout(30_000);
    if (creationAttempted) {
      try {
        await writeState(stateFile, state(runId, exactProjectId, title, "cleanup_pending"));
      } catch (error) {
        // The in-memory exact UUID remains authoritative for deletion. Retain
        // the write failure for the final result, but never let journaling skip
        // the actual project cleanup attempt.
        if (failure === null) failure = error;
      }
    }
    try {
      cleanup = await deleteExactCreatedProject(
        projects,
        exactProjectId,
        creationAttempted,
        cleanupSignal,
      );
    } catch (error) {
      cleanup = "failed";
      if (failure === null) failure = error;
    }
    try {
      await account.flush();
    } catch {
      // Project deletion is the authoritative cleanup. A later cookie-store
      // persistence failure is reported without rewriting a proven search and
      // confirmed deletion into a false live-test failure.
      accountFlushSucceeded = false;
    }
    if (cleanup === "deleted" || cleanup === "absent" || cleanup === "not_needed") {
      try {
        await unlink(stateFile);
        cleanupStateRetained = false;
      } catch (error) {
        const code = (error as { code?: unknown }).code;
        if (code === "ENOENT") cleanupStateRetained = false;
        else if (failure === null) failure = error;
      }
    }
  }

  if (
    failure !== null ||
    !projectCreated ||
    !sandboxProvisioned ||
    queryAttempts !== 1 ||
    metadata === null ||
    cleanup === "failed" ||
    cleanup === "not_needed"
  ) {
    throw new StandaloneSearchProbeFailure(
      stage,
      cleanup,
      metadata,
      cleanupStateRetained,
    );
  }

  return Object.freeze({
    search: metadata,
    projectCreated: true,
    sandboxProvisioned: true,
    queryAttempts: 1,
    cleanup,
    cleanupStateRetained,
    accountFlushSucceeded,
    configuredProjectsTouched: false,
    protectedServiceTouched: false,
  });
}

function safeFailureReport(error: unknown): Record<string, unknown> {
  if (error instanceof StandaloneSearchProbeFailure) {
    return {
      event: "standalone_search_probe.failed",
      stage: error.stage,
      search: error.search,
      cleanup: error.cleanup,
      cleanupStateRetained: error.cleanupStateRetained,
      configuredProjectsTouched: false,
      protectedServiceTouched: false,
    };
  }
  return {
    event: "standalone_search_probe.failed",
    stage: "preflight",
    errorName: error instanceof Error ? error.name.slice(0, 64) : "UnknownError",
    configuredProjectsTouched: false,
    protectedServiceTouched: false,
  };
}

export async function main(
  env: Record<string, string | undefined> = process.env,
): Promise<number> {
  try {
    const report = await runStandaloneSearchProbe(env);
    console.log(JSON.stringify({
      event: "standalone_search_probe.completed",
      ...report,
    }, null, 2));
    return report.search.ok ? 0 : 1;
  } catch (error) {
    console.error(JSON.stringify(safeFailureReport(error), null, 2));
    return 1;
  }
}

if (import.meta.main) process.exitCode = await main();
