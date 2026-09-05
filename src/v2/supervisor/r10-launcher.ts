import type { BootstrapBinding } from "../bootstrap/crypto";
import { canonicalJson } from "../protocol/shared";

export const SUPERVISOR_STATIC_PATH = ".ghostprov-v2/supervisor/static.json" as const;
export const SUPERVISOR_CREDENTIAL_PATH = ".ghostprov-v2/supervisor/bootstrap.enc.json" as const;
export const SUPERVISOR_INSTALL_PATH = ".ghostprov-v2/supervisor/install.sh" as const;
export const SUPERVISOR_MAX_SCRIPT_BYTES = 8 * 1024;

const SAFE_NONCE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$(?![\s\S])/;
const SAFE_ENTRYPOINT = /^\.ghostprov-v2\/revisions\/r10\/[a-f0-9]{64}\/[a-f0-9]{64}\/sidecar\.bundle\.js$(?![\s\S])/;
const DENO_ENV_ALLOWLIST = "OPENAI_BASE_URL,OPENAI_API_KEY,PROXY_CA_CERT_PATH,NODE_ENV,PRODUCTION,FORCE_COLOR,NO_COLOR,COLORTERM,TERM,GHOSTPROV_WORKSPACE_ROOT,CODEX_THREAD_ID";
const SUPERVISOR_READ_SUFFIX = "/tmp/ghostprov-v2,/proc,/usr/bin/setsid,/bin/sh,/usr/local/bin/deno,/usr/bin/deno,/root/.deno/bin/deno";
const SUPERVISOR_WRITE_SUFFIX = "/tmp/ghostprov-v2";
const WORKSPACE_PATTERN = "^/codex_workspace/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";

export interface SupervisorStaticConfig {
  readonly version: 1;
  readonly binding: BootstrapBinding;
  readonly entrypointPath: string;
  readonly connectTimeoutMs: number;
}

export interface SupervisorExecRequest {
  readonly entrypointPath: string;
  readonly supervisorNonce: string;
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

export function supervisorStaticConfig(input: {
  readonly binding: BootstrapBinding;
  readonly entrypointPath: string;
  readonly connectTimeoutMs?: number;
}): string {
  const timeout = input.connectTimeoutMs ?? 20_000;
  if (!SAFE_NONCE.test(input.binding.nonce)) throw new TypeError("Supervisor nonce is invalid");
  if (!SAFE_ENTRYPOINT.test(input.entrypointPath)) throw new TypeError("Supervisor entrypoint is invalid");
  if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 300_000) {
    throw new RangeError("Supervisor connect timeout is invalid");
  }
  return canonicalJson({
    version: 1,
    binding: input.binding,
    entrypointPath: input.entrypointPath,
    connectTimeoutMs: timeout,
  } satisfies SupervisorStaticConfig);
}

export function supervisorInstallScript(input: {
  readonly entrypointPath: string;
  readonly supervisorNonce: string;
}): string {
  if (!SAFE_ENTRYPOINT.test(input.entrypointPath)) throw new TypeError("Supervisor entrypoint is invalid");
  if (!SAFE_NONCE.test(input.supervisorNonce)) throw new TypeError("Supervisor nonce is invalid");
  const script = [
    "#!/bin/sh",
    "set -eu",
    `ENTRYPOINT_RELATIVE=${shellQuote(input.entrypointPath)}`,
    `NONCE=${shellQuote(input.supervisorNonce)}`,
    "WORKSPACE=$(readlink -f /workspace)",
    `if ! printf '%s\\n' "$WORKSPACE" | grep -Eq ${shellQuote(WORKSPACE_PATTERN)}; then exit 72; fi`,
    'ENTRYPOINT="$WORKSPACE/$ENTRYPOINT_RELATIVE"',
    'export GHOSTPROV_WORKSPACE_ROOT="$WORKSPACE"',
    "DENO=${DENO_BIN:-}",
    'if [ -n "$DENO" ] && [ ! -x "$DENO" ]; then DENO=; fi',
    'if [ -z "$DENO" ]; then',
    "  for candidate in /usr/local/bin/deno /usr/bin/deno /root/.deno/bin/deno; do",
    '    if [ -x "$candidate" ]; then DENO="$candidate"; break; fi',
    "  done",
    "fi",
    'if [ -z "$DENO" ]; then DENO=$(command -v deno 2>/dev/null || true); fi',
    'if [ -z "$DENO" ] || [ ! -x "$DENO" ]; then exit 71; fi',
    'exec "$DENO" run --cached-only "--cert=${PROXY_CA_CERT_PATH}" '.concat("\\"),
    `  --allow-env=${DENO_ENV_ALLOWLIST} `.concat("\\"),
    "  --allow-net --allow-run --allow-sys ".concat("\\"),
    `  "--allow-read=$WORKSPACE/.ghostprov-v2,${SUPERVISOR_READ_SUFFIX}" `.concat("\\"),
    `  "--allow-write=$WORKSPACE/.ghostprov-v2,${SUPERVISOR_WRITE_SUFFIX}" `.concat("\\"),
    '  "$ENTRYPOINT" "--ghostprov-install-supervisor=$NONCE" 2>&1',
    "",
  ].join("\n");
  if (new TextEncoder().encode(script).byteLength > SUPERVISOR_MAX_SCRIPT_BYTES) {
    throw new RangeError("Supervisor install script exceeds its byte bound");
  }
  return script;
}

export function supervisorInstallCommand(input: SupervisorExecRequest): string {
  if (!SAFE_ENTRYPOINT.test(input.entrypointPath)) throw new TypeError("Supervisor entrypoint is invalid");
  if (!SAFE_NONCE.test(input.supervisorNonce)) throw new TypeError("Supervisor nonce is invalid");
  const entrypointRelative = shellQuote(input.entrypointPath);
  const nonce = shellQuote(input.supervisorNonce);
  const command = [
    `ENTRYPOINT_RELATIVE=${entrypointRelative}`,
    `NONCE=${nonce}`,
    "WORKSPACE=$(readlink -f /workspace)",
    `if ! printf '%s\\n' "$WORKSPACE" | grep -Eq ${shellQuote(WORKSPACE_PATTERN)}; then echo GPV2_INSTALL_EXIT 72; exit 0; fi`,
    'ENTRYPOINT="$WORKSPACE/$ENTRYPOINT_RELATIVE"',
    'export GHOSTPROV_WORKSPACE_ROOT="$WORKSPACE"',
    "DENO=${DENO_BIN:-}",
    'if [ -n "$DENO" ] && [ ! -x "$DENO" ]; then DENO=; fi',
    'if [ -z "$DENO" ]; then for candidate in /usr/local/bin/deno /usr/bin/deno /root/.deno/bin/deno; do if [ -x "$candidate" ]; then DENO="$candidate"; break; fi; done; fi',
    'if [ -z "$DENO" ]; then DENO=$(command -v deno 2>/dev/null || true); fi',
    'if [ -z "$DENO" ] || [ ! -x "$DENO" ]; then echo GPV2_INSTALL_EXIT 71; exit 0; fi',
    '"$DENO" run --cached-only "--cert=${PROXY_CA_CERT_PATH}"' +
      ` --allow-env=${DENO_ENV_ALLOWLIST}` +
      " --allow-net --allow-run --allow-sys" +
      ` "--allow-read=$WORKSPACE/.ghostprov-v2,${SUPERVISOR_READ_SUFFIX}"` +
      ` "--allow-write=$WORKSPACE/.ghostprov-v2,${SUPERVISOR_WRITE_SUFFIX}"` +
      ' "$ENTRYPOINT" "--ghostprov-install-supervisor=$NONCE" 2>&1',
    "status=$?",
    "echo GPV2_INSTALL_EXIT $status",
  ].join("; ");
  if (new TextEncoder().encode(command).byteLength > 2_048) {
    throw new RangeError("Supervisor install command exceeds the agent bound");
  }
  return command;
}
