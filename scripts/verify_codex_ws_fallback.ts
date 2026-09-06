import { existsSync } from "node:fs";
import { appendFile, mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, sep } from "node:path";
import { createHash } from "node:crypto";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import { closeResponsesWebSockets, DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES } from "../src/ws/server";
import { WebSocketProcessBudget } from "../src/ws/session";
import type { ObservabilityEvent } from "../src/observability/events";
import { assertSafeObservabilityEvent, sanitizeDiagnosticMessage } from "../src/observability/redaction";

const TEST_KEY = "offline-codex-ws-fallback-key-0123456789abcdef";
const MAX_OUTPUT_BYTES = 2 * 1024 * 1024;
const PROCESS_TIMEOUT_MS = 60_000;
const HISTORY_ITEMS = 18;
const HISTORY_CHUNK = "offline payload ".repeat(1024 * 1024 / 16);

function argument(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

function childEnvironment(codexHome: string): Record<string, string> {
  const result: Record<string, string> = {
    CODEX_HOME: codexHome, HOME: codexHome, USERPROFILE: codexHome,
    NO_COLOR: "1", GHOSTPROV_OFFLINE_TEST_KEY: TEST_KEY,
  };
  // No credential variables or user configuration are inherited.
  for (const name of [
    "PATH", "Path", "PATHEXT", "SystemRoot", "SYSTEMROOT", "WINDIR", "COMSPEC",
    "TEMP", "TMP", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE",
  ]) {
    const value = process.env[name];
    if (value !== undefined) result[name] = value;
  }
  return result;
}

async function readBounded(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > MAX_OUTPUT_BYTES) throw new Error("Isolated Codex output exceeded its bound");
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  return Buffer.concat(chunks, bytes).toString("utf8");
}

function countPrompt(input: unknown, expected: string): number {
  if (!Array.isArray(input)) return 0;
  return input.filter(item => item && typeof item === "object" &&
    item.role === "user" && Array.isArray(item.content) &&
    item.content.some((part: unknown) => part && typeof part === "object" &&
      "text" in part && part.text === expected)).length;
}

/** Only clones the verifier's own tiny task; never opens a user task or home. */
async function seedLargeHistory(codexHome: string, stdout: string): Promise<string> {
  const started = stdout.split("\n").filter(Boolean).map(line => JSON.parse(line))
    .find(event => event.type === "thread.started");
  const sourceId: unknown = started?.thread_id;
  if (typeof sourceId !== "string" || !/^[a-f0-9-]{36}$/.test(sourceId)) {
    throw new Error("Verifier did not receive its temporary task ID");
  }
  const sessionRoot = resolve(codexHome, "sessions");
  for await (const relative of new Bun.Glob(`**/*-${sourceId}.jsonl`).scan(sessionRoot)) {
    const source = resolve(sessionRoot, relative);
    if (!source.startsWith(`${sessionRoot}${sep}`)) throw new Error("Unexpected fixture path");
    const file = Bun.file(source);
    if (file.size > 2 * 1024 * 1024) throw new Error("Seed task exceeds its bound");
    const id = crypto.randomUUID();
    const target = source.replace(sourceId, id);
    const seed = (await file.text()).replaceAll(sourceId, id);
    const seedRows = seed.trimEnd().split("\n").map(line => JSON.parse(line));
    let ordinal = seedRows.at(-1)?.ordinal;
    if (!Number.isSafeInteger(ordinal) || ordinal < 0) throw new Error("Seed task has no valid record ordinal");
    ordinal++;
    await writeFile(target, seed);
    const timestamp = new Date().toISOString();
    for (let index = 0; index < HISTORY_ITEMS; index++) {
      const messages = [
        { type: "message", id: `msg_fixture_user_${index}`, role: "user", content: [{ type: "input_text", text: HISTORY_CHUNK }] },
        { type: "message", id: `msg_fixture_assistant_${index}`, role: "assistant", content: [{ type: "output_text", text: "OK" }], phase: "final_answer" },
      ];
      await appendFile(target, messages.map(payload => JSON.stringify({ timestamp, ordinal: ordinal++, type: "response_item", payload }) + "\n").join(""));
    }
    return id;
  }
  throw new Error("Could not find the verifier's temporary seed task");
}

export async function main(): Promise<void> {
  if (!process.argv.includes("--run")) {
    throw new Error("Pass --run to execute the isolated, loopback-only installed-Codex verifier");
  }
  const executable = argument("--codex-executable") ?? Bun.which("codex.exe") ?? Bun.which("codex");
  if (!executable || !existsSync(executable) || /\.cmd$/i.test(executable)) {
    throw new Error("Supply an installed native Codex executable with --codex-executable");
  }
  const tempBase = resolve(tmpdir());
  const root = await mkdtemp(resolve(tempBase, "ghostprov-codex-ws-fallback-"));
  const assertOwnedTemporaryRoot = () => {
    if (!resolve(root).startsWith(`${tempBase}${sep}ghostprov-codex-ws-fallback-`)) {
      throw new Error("Refusing cleanup outside the verifier's temporary directory");
    }
  };
  assertOwnedTemporaryRoot();
  const scope = createAuthenticatedTenantScope();
  const budget = new WebSocketProcessBudget({
    maxSessions: 8, maxQueuedBytes: 128 * 1024 ** 2, maxIngressBytes: 256 * 1024 ** 2,
  });
  const events: ObservabilityEvent[] = [];
  const calls: Array<{
    source: string; promptPreserved: boolean; historyItems: number; requestBytes: number;
    stream: unknown; store: unknown;
    relayBytes: number; continuedFromPrevious: boolean;
  }> = [];
  let expectedPrompt = "";
  let activeBodyLeases = 0;
  let incomingBodyBytes = 0;
  const router: RouterLike = {
    responseProvenanceAuthorizationScope: () => scope,
    async acquireIncomingBody(bytes) {
      activeBodyLeases++;
      incomingBodyBytes = Math.max(incomingBodyBytes, bytes);
      let released = false;
      return {
        async ensureBytes(nextBytes) { incomingBodyBytes = Math.max(incomingBodyBytes, nextBytes); },
        release() { if (!released) { released = true; activeBodyLeases--; } },
      };
    },
    async startRelay(body, options) {
      if (options?.authorizationScope !== scope) throw new Error("Trusted scope was lost");
      if (calls.length >= 8) throw new Error("Unexpected repeated relay execution in verifier");
      calls.push({
        source: options.source ?? "unknown", promptPreserved: countPrompt(body.input, expectedPrompt) > 0,
        historyItems: countPrompt(body.input, HISTORY_CHUNK), requestBytes: options.encodedBodyBytes ?? incomingBodyBytes,
        stream: body.stream, store: body.store,
        relayBytes: Buffer.byteLength(JSON.stringify(body)),
        continuedFromPrevious: body.previous_response_id === `resp_offline_${calls.length}`,
      });
      const id = `resp_offline_${calls.length}`;
      const item = {
        type: "message", id: `msg_offline_${calls.length}`, status: "completed", role: "assistant",
        content: [{ type: "output_text", text: "OK", annotations: [] }],
      };
      const response = {
        id, object: "response", status: "completed", model: "gpt-5.6-sol", output: [item],
        usage: {
          input_tokens: 1, input_tokens_details: { cached_tokens: 0 },
          output_tokens: 1, output_tokens_details: { reasoning_tokens: 0 }, total_tokens: 2,
        },
      };
      const frames = [
        { type: "response.created", response: { id, status: "in_progress" } },
        { type: "response.output_item.done", output_index: 0, item },
        { type: "response.completed", response },
      ];
      return {
        async *events() { for (const frame of frames) yield { event: frame.type, data: JSON.stringify(frame) }; },
        final: Promise.resolve(response), stats: { bytes: 0, firstByteMs: null, frames: frames.length },
      };
    },
    snapshot: () => ({ capacity: 8, active: 0, available: 8, queued: 0, runtimes: [], resources: {} }),
  };
  const app = createGhostProvApp({ router, apiKey: TEST_KEY, authorizationScope: scope });
  const server = createGhostProvServer({
    app, router, apiKey: TEST_KEY, authorizationScope: scope, port: 0, candidate: true,
    webSocketProcessBudget: budget,
    logger: { emit(event) {
      assertSafeObservabilityEvent(event);
      if (events.length < 32) events.push(event);
    } },
  });
  let listening = false;
  try {
    const codexHome = resolve(root, "home");
    const workspace = resolve(root, "workspace");
    await Promise.all([mkdir(codexHome), mkdir(workspace)]);
    const env = childEnvironment(codexHome);
    const versionResult = Bun.spawnSync([executable, "--version"], { cwd: workspace, env });
    if (versionResult.exitCode !== 0) throw new Error("Could not read installed Codex version");
    const version = versionResult.stdout.toString().trim();
    await new Promise<void>((resolveListen, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => { server.off("error", reject); resolveListen(); });
    });
    listening = true;
    const address = server.address();
    if (!address || typeof address === "string" || address.port === 18_787) {
      throw new Error("Verifier did not acquire an isolated OS-assigned port");
    }
    const provider = `model_providers.ghostprov_offline={name='Offline verifier',` +
      `base_url='http://127.0.0.1:${address.port}/v1',wire_api='responses',` +
      "env_key='GHOSTPROV_OFFLINE_TEST_KEY',requires_openai_auth=false,supports_websockets=true," +
      "request_max_retries=0,stream_max_retries=100,stream_idle_timeout_ms=5000}";
    const command = [
      executable, "exec", "--ignore-user-config", "--ignore-rules",
      "--skip-git-repo-check", "--strict-config", "--sandbox", "read-only",
      "--model", "gpt-5.6-sol", "--config", provider,
      "--config", 'model_provider="ghostprov_offline"',
      "--config", "model_context_window=64000000",
      "--config", "model_auto_compact_token_limit=60000000", "--json",
    ];
    const results: Array<Record<string, unknown>> = [];
    let largeSessionId = "";
    for (const testCase of [
      { name: "small_before", large: false, source: "websocket" },
      { name: "large_history", large: true, source: "http" },
      { name: "large_history_followup", large: true, source: "http" },
      { name: "small_after", large: false, source: "websocket" },
    ]) {
      expectedPrompt = testCase.large
        ? "Continue the saved test history. Reply with exactly OK."
        : "Reply with exactly OK and do not use tools.";
      const start = performance.now();
      const firstCall = calls.length;
      const firstEvent = events.length;
      incomingBodyBytes = 0;
      const invocation = testCase.large ? [...command, "resume", largeSessionId, "-"] : [...command, "-"];
      const child = Bun.spawn(invocation, {
        cwd: workspace, env, stdin: "pipe", stdout: "pipe", stderr: "pipe", windowsHide: true,
      });
      let timedOut = false;
      const timer = setTimeout(() => { timedOut = true; child.kill(); }, PROCESS_TIMEOUT_MS);
      let result: [number, string, string];
      try {
        const execution = Promise.all([child.exited, readBounded(child.stdout), readBounded(child.stderr)]);
        child.stdin.write(expectedPrompt);
        await child.stdin.end();
        result = await execution;
      } catch (error) {
        child.kill();
        await child.exited.catch(() => undefined);
        throw error;
      } finally { clearTimeout(timer); }
      const [exitCode, stdout, stderr] = result;
      const actual = calls.slice(firstCall);
      const diagnostics = events.slice(firstEvent).map(event => event.event);
      if (
        exitCode !== 0 || !stdout.includes('"turn.completed"') || !stdout.includes("OK") ||
        actual.length !== 1 || actual[0]!.source !== testCase.source ||
        !actual[0]!.promptPreserved || actual[0]!.stream !== true || actual[0]!.store !== true ||
        (testCase.name === "large_history" && (actual[0]!.historyItems !== HISTORY_ITEMS || actual[0]!.requestBytes <= DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES)) ||
        (testCase.name === "large_history_followup" &&
          (actual[0]!.historyItems !== 0 || !actual[0]!.continuedFromPrevious || actual[0]!.relayBytes > 64 * 1024)) ||
        activeBodyLeases !== 0 ||
        (testCase.name === "large_history" && diagnostics.join(",") !== "websocket.payload_rejected,websocket.http_fallback") ||
        (testCase.name === "large_history_followup" && diagnostics.join(",") !== "websocket.http_fallback") ||
        (!testCase.large && diagnostics.length !== 0)
      ) {
        throw new Error(JSON.stringify({
          case: testCase.name, exitCode, timedOut, elapsedMs: Math.round(performance.now() - start),
          calls: actual, diagnostics,
          stderr: stderr.split(/\r?\n/).filter(Boolean).slice(-8)
            .map(line => sanitizeDiagnosticMessage(line.replaceAll(root, "[fixture]").replace(/rollout-[^\s]+\.jsonl/g, "[rollout]"))),
          completed: stdout.includes('"turn.completed"'), stdoutBytes: stdout.length, stderrBytes: stderr.length,
        }));
      }
      results.push({
        case: testCase.name, exitCode, transport: testCase.source,
        promptBytes: Buffer.byteLength(expectedPrompt),
        promptSha256: createHash("sha256").update(expectedPrompt).digest("hex"),
        historyItems: actual[0]!.historyItems, requestBytes: actual[0]!.requestBytes,
        relayBytes: actual[0]!.relayBytes, continuedFromPrevious: actual[0]!.continuedFromPrevious,
        relayExecutions: actual.length, promptPreserved: true,
        diagnostics, elapsedMs: Math.round(performance.now() - start),
      });
      if (testCase.name === "small_before") largeSessionId = await seedLargeHistory(codexHome, stdout);
      expectedPrompt = "";
    }
    await closeResponsesWebSockets(server, { timeoutMs: 1_000 });
    const finalBudget = budget.snapshot();
    if (finalBudget.sessions || finalBudget.queuedBytes || finalBudget.ingressReservedBytes || activeBodyLeases) {
      throw new Error("Verifier retained request or WebSocket reservations after cleanup");
    }
    const report = { codex: version, result: "passed", providerCalls: 0, results, finalBudget };
    const json = `${JSON.stringify(report, null, 2)}\n`;
    const reportPath = argument("--report");
    if (reportPath) await writeFile(resolve(reportPath), json);
    console.log(json);
  } finally {
    await closeResponsesWebSockets(server, { timeoutMs: 1_000 });
    if (listening) {
      server.closeAllConnections();
      await new Promise<void>(resolveClose => server.close(() => resolveClose()));
    }
    assertOwnedTemporaryRoot();
    await rm(root, { recursive: true, force: true });
  }
}

if (import.meta.main) await main();
