import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { dirname, join, resolve, sep } from "node:path";
import { tmpdir } from "node:os";
import { WebSocket, WebSocketServer } from "ws";
import { definitiveProviderRateLimitFailedEvent } from "../src/http/errors";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";
import { V2CommittedError } from "../src/v2/transport/run-handle";

const LIVE_GUARD = "GP_RUN_CODEX_RATE_LIMIT_RETRY";
const MAX_PROCESS_OUTPUT_BYTES = 1024 * 1024;
const PROCESS_TIMEOUT_MS = 30_000;
const EXPECTED_RETRY_DELAY_MS = 250;
const MINIMUM_OBSERVED_RETRY_DELAY_MS = 175;
const PROTECTED_SERVICE_PORT = 18_787;

interface ProcessResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

async function readBounded(
  stream: ReadableStream<Uint8Array>,
  maxBytes: number,
): Promise<string> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > maxBytes) throw new Error("Codex verifier output exceeded its byte limit");
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const joined = new Uint8Array(bytes);
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(joined);
}

function isolatedEnvironment(codexHome: string): Record<string, string> {
  const allowed = [
    "PATH",
    "Path",
    "PATHEXT",
    "SystemRoot",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "TEMP",
    "TMP",
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
    "LOCALAPPDATA",
    "APPDATA",
    "PROGRAMDATA",
  ];
  const environment: Record<string, string> = {};
  for (const name of allowed) {
    const value = process.env[name];
    if (value !== undefined) environment[name] = value;
  }
  environment.CODEX_HOME = codexHome;
  environment.HOME = codexHome;
  environment.USERPROFILE = codexHome;
  environment.NO_COLOR = "1";
  return environment;
}

function installedCodexCommand(): string[] | null {
  const launcher = Bun.which("codex");
  if (process.platform === "win32" && launcher?.toLowerCase().endsWith(".cmd")) {
    const base = dirname(launcher);
    const script = join(base, "node_modules", "@openai", "codex", "bin", "codex.js");
    const adjacentNode = join(base, "node.exe");
    const node = existsSync(adjacentNode) ? adjacentNode : Bun.which("node.exe");
    if (node && existsSync(script)) return [node, script];
  }
  if (launcher && !launcher.toLowerCase().endsWith(".cmd")) return [launcher];
  const native = process.platform === "win32" ? Bun.which("codex.exe") : null;
  return native ? [native] : null;
}

async function runBounded(
  command: string[],
  cwd: string,
  env: Record<string, string>,
): Promise<ProcessResult> {
  const processHandle = Bun.spawn(command, {
    cwd,
    env,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const execution = Promise.all([
    processHandle.exited,
    readBounded(processHandle.stdout, MAX_PROCESS_OUTPUT_BYTES),
    readBounded(processHandle.stderr, MAX_PROCESS_OUTPUT_BYTES),
  ]);
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error("Codex compatibility verifier timed out")),
      PROCESS_TIMEOUT_MS,
    );
  });
  try {
    const [exitCode, stdout, stderr] = await Promise.race([execution, timeout]);
    return { exitCode, stdout, stderr };
  } catch (error) {
    processHandle.kill();
    await processHandle.exited.catch(() => undefined);
    throw error;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

function send(socket: WebSocket, event: Record<string, unknown>): void {
  if (socket.readyState !== WebSocket.OPEN) {
    throw new Error("Codex compatibility socket closed before its terminal event");
  }
  socket.send(JSON.stringify(event));
}

async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolveListen, reject) => {
    const onError = (error: Error) => reject(error);
    server.once("error", onError);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", onError);
      resolveListen();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Verifier listener has no TCP address");
  if (address.port === PROTECTED_SERVICE_PORT) {
    throw new Error(`Verifier refuses protected service port ${PROTECTED_SERVICE_PORT}`);
  }
  return address.port;
}

async function closeServer(
  server: Server,
  webSockets: WebSocketServer,
  peers: Set<WebSocket>,
): Promise<void> {
  for (const peer of peers) peer.terminate();
  await Promise.all([
    new Promise<void>(resolveClose => webSockets.close(() => resolveClose())),
    new Promise<void>(resolveClose => server.close(() => resolveClose())),
  ]);
}

function assertSafeTemporaryDirectory(path: string): void {
  const root = resolve(tmpdir());
  const candidate = resolve(path);
  if (!candidate.startsWith(`${root}${sep}`) || candidate === root) {
    throw new Error("Refusing to clean a verifier directory outside the OS temporary root");
  }
}

export async function main(
  environment: Record<string, string | undefined> = process.env,
): Promise<void> {
  if (environment[LIVE_GUARD] !== "1") {
    throw new Error(`Set ${LIVE_GUARD}=1 to run the isolated installed-Codex verifier`);
  }
  const codexCommand = installedCodexCommand();
  if (!codexCommand) throw new Error("Installed Codex CLI was not found on PATH");

  const temporaryRoot = await mkdtemp(resolve(tmpdir(), "ghostprov-codex-rate-limit-"));
  assertSafeTemporaryDirectory(temporaryRoot);
  try {
    const codexHome = resolve(temporaryRoot, "codex-home");
    const workspace = resolve(temporaryRoot, "workspace");
    await Promise.all([mkdir(codexHome), mkdir(workspace)]);
    const childEnvironment = isolatedEnvironment(codexHome);

    const version = Bun.spawnSync({
      cmd: [...codexCommand, "--version"],
      cwd: workspace,
      env: childEnvironment,
      stdout: "pipe",
      stderr: "pipe",
    });
    if (version.exitCode !== 0) throw new Error("Unable to read the installed Codex version");
    const versionText = version.stdout.toString().trim();

    const message = "Rate limit reached for gpt-5.6-sol on tokens per min (TPM). " +
      `Please try again in ${EXPECTED_RETRY_DELAY_MS}ms.`;
    const providerError = await createProviderErrorEnvelope({
      origin: "sse",
      status: 429,
      headers: new Headers({ "retry-after": "0.250" }),
      value: {
        type: "rate_limit_error",
        message,
        param: null,
        code: "rate_limit_exceeded",
      },
    });
    const failed = definitiveProviderRateLimitFailedEvent(new V2CommittedError(
      message,
      "rate_limit_exceeded",
      false,
      { retryAfterMs: EXPECTED_RETRY_DELAY_MS },
      providerError,
    ));
    if (!failed) throw new Error("GhostProv did not produce a retryable failed-response event");

    const server = createServer((_request, response) => {
      response.writeHead(404, { "content-type": "application/json" });
      response.end('{"error":{"message":"WebSocket compatibility verifier only"}}');
    });
    const webSockets = new WebSocketServer({ noServer: true });
    const peers = new Set<WebSocket>();
    const samplingRequestTimes: number[] = [];
    let warmups = 0;
    webSockets.on("connection", socket => {
      peers.add(socket);
      socket.once("close", () => peers.delete(socket));
      socket.on("message", raw => {
        let request: Record<string, unknown>;
        try {
          const decoded = JSON.parse(raw.toString());
          if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) throw new Error();
          request = decoded as Record<string, unknown>;
        } catch {
          socket.close(1002, "invalid request");
          return;
        }
        if (request.type !== "response.create") {
          socket.close(1002, "unexpected request type");
          return;
        }
        if (request.generate === false) {
          warmups++;
          send(socket, {
            type: "response.completed",
            response: {
              id: `resp_warmup_${warmups}`,
              status: "completed",
              output: [],
              usage: {
                input_tokens: 0,
                input_tokens_details: { cached_tokens: 0 },
                output_tokens: 0,
                output_tokens_details: { reasoning_tokens: 0 },
                total_tokens: 0,
              },
            },
          });
          return;
        }

        samplingRequestTimes.push(performance.now());
        const attempt = samplingRequestTimes.length;
        if (attempt === 1) {
          send(socket, {
            type: "response.created",
            response: { id: "resp_rate_limited", status: "in_progress" },
          });
          send(socket, failed);
          return;
        }
        if (attempt !== 2) {
          socket.close(1011, "unexpected extra sampling retry");
          return;
        }
        send(socket, {
          type: "response.created",
          response: { id: "resp_retry_succeeded", status: "in_progress" },
        });
        send(socket, {
          type: "response.output_item.done",
          item: {
            type: "message",
            id: "msg_retry_succeeded",
            status: "completed",
            role: "assistant",
            content: [{ type: "output_text", text: "OK" }],
          },
        });
        send(socket, {
          type: "response.completed",
          response: {
            id: "resp_retry_succeeded",
            status: "completed",
            output: [],
            usage: {
              input_tokens: 1,
              input_tokens_details: { cached_tokens: 0 },
              output_tokens: 1,
              output_tokens_details: { reasoning_tokens: 0 },
              total_tokens: 2,
            },
          },
        });
      });
    });
    server.on("upgrade", (request, socket, head) => {
      if (new URL(request.url ?? "/", "http://127.0.0.1").pathname !== "/v1/responses") {
        socket.destroy();
        return;
      }
      webSockets.handleUpgrade(request, socket, head, peer => {
        webSockets.emit("connection", peer, request);
      });
    });

    try {
      const port = await listen(server);
      const provider = `model_providers.ghostprov_compat={ name = "GhostProv compatibility verifier", ` +
        `base_url = "http://127.0.0.1:${port}/v1", wire_api = "responses", ` +
        "requires_openai_auth = false, supports_websockets = true, request_max_retries = 0, " +
        "stream_max_retries = 2, stream_idle_timeout_ms = 5000 }";
      const result = await runBounded([
        ...codexCommand,
        "exec",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--strict-config",
        "--sandbox",
        "read-only",
        "--model",
        "gpt-5.6-sol",
        "--config",
        provider,
        "--config",
        'model_provider="ghostprov_compat"',
        "--json",
        "Reply with exactly OK and do not use tools.",
      ], workspace, childEnvironment);

      if (result.exitCode !== 0) {
        throw new Error(`Codex compatibility run failed: ${result.stderr.slice(0, 2_048)}`);
      }
      if (samplingRequestTimes.length !== 2) {
        throw new Error(`Expected exactly two sampling attempts, observed ${samplingRequestTimes.length}`);
      }
      const observedDelayMs = samplingRequestTimes[1]! - samplingRequestTimes[0]!;
      if (observedDelayMs < MINIMUM_OBSERVED_RETRY_DELAY_MS) {
        throw new Error(`Codex retried too early after ${Math.round(observedDelayMs)}ms`);
      }
      if (result.stdout.includes("exceeded retry limit") || result.stderr.includes("exceeded retry limit")) {
        throw new Error("Codex misclassified the temporary provider limit as an exhausted retry limit");
      }
      if (!result.stdout.includes("OK")) {
        throw new Error("Codex did not complete after the retry succeeded");
      }
      console.log(JSON.stringify({
        codex: versionText,
        warmups,
        samplingAttempts: samplingRequestTimes.length,
        observedRetryDelayMs: Math.round(observedDelayMs),
        result: "passed",
      }));
    } finally {
      await closeServer(server, webSockets, peers).catch(() => undefined);
    }
  } finally {
    assertSafeTemporaryDirectory(temporaryRoot);
    await rm(temporaryRoot, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  void main().catch(error => {
    console.error(error instanceof Error ? error.message : "Codex rate-limit verifier failed");
    process.exitCode = 1;
  });
}
