import { readFile } from "node:fs/promises";
import type { SupervisorBootstrapAttempt } from "../../src/v2/supervisor/bootstrap-journal";
import type { V2SidecarAssets } from "../../src/v2/sidecar/assets";
import { canonicalJson, sha256Hex } from "../../src/v2/protocol/shared";
import { supervisorStaticConfig } from "../../src/v2/supervisor/r10-launcher";
import { directSupervisorExecBody } from "../../src/prism/direct-exec";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;
const HASH = /^[a-f0-9]{64}$(?![\s\S])/;
const CHECKS = ["workspace", "static", "artifacts", "retirement", "owners", "processes", "unchanged"] as const;
const CODES = ["retired", "workspace_identity", "authority_file", "retirement_record", "owner_record", "owner_locked", "live_process", "process_scan", "authority_changed", "checker_failure"] as const;
const PROCESS_REASONS = ["related_process", "argument_match", "relationship_changed", "known_pid_after_scan", "pid_file_present"] as const;
const PROCESS_FLAGS = ["oldPidMatch", "oldStartTickMatch", "childPidMatch", "parentMatch", "groupMatch", "sessionMatch", "diagnosticSelf",
  "ghostprovArgument", "oldEntrypointArgument", "oldWorkspaceArgument"] as const;
const FILE_ROLES = ["static", "artifact", "retirement", "supervisor_owner", "child_owner", "supervisor_pid", "child_pid", "process", "diagnostic_script", "unknown"] as const;
const FILE_REASONS = ["absent", "symlink", "not_directory", "nonregular", "uid_mismatch", "mode_writable", "size_bound", "size_mismatch", "hash_mismatch", "changed", "permission", "io", "locked", "format_invalid", "identity_mismatch"] as const;
const FILE_COMPONENTS = ["workspace", "ancestor", "leaf", "opened", "content", "recheck"] as const;
export const PREFLIGHT_RESULT_LIMIT = 4096;
const READBACK_KEYS = ["path", "relative_path", "bytes", "mime_type", "body_b64", "session_id", "detail"] as const;
const READBACK_MIME_TYPES = ["application/json", "application/json; charset=utf-8", "application/json;charset=utf-8",
  "text/plain", "text/plain; charset=utf-8", "application/octet-stream"] as const;

export interface PreflightArtifact {
  readonly path: string;
  readonly bytes: number;
  readonly sha256: string;
}
export interface PreflightAuthority {
  readonly version: 1;
  readonly sessionId: string;
  readonly probeNonce: string;
  readonly oldSessionId: string;
  readonly supervisorNonce: string;
  readonly supervisorInstanceId: string;
  readonly entrypoint: string;
  readonly static: PreflightArtifact;
  readonly artifacts: readonly PreflightArtifact[];
}
export interface PreflightResult {
  readonly version: 1;
  readonly sessionId: string;
  readonly probeNonce: string;
  readonly authoritySha256: string;
  readonly scriptSha256: string;
  readonly outcome: "retired" | "blocked";
  readonly code: typeof CODES[number];
  readonly checks: Readonly<Record<typeof CHECKS[number], boolean>>;
  readonly processesInspected: number;
  readonly retirementSha256: string | null;
  readonly processScansCompleted: number;
  readonly processConflictCount: number;
  readonly processConflictsTruncated: boolean;
  readonly processConflicts: readonly PreflightProcessConflict[];
  readonly fileFailure: { readonly role: typeof FILE_ROLES[number]; readonly reason: typeof FILE_REASONS[number]; readonly component: typeof FILE_COMPONENTS[number] } | null;
}
export type PreflightProcessConflict = Readonly<Record<typeof PROCESS_FLAGS[number], boolean>> & {
  readonly reason: typeof PROCESS_REASONS[number]; readonly scan: number; readonly state: string | null;
};

export async function createPreflightAuthority(input: {
  readonly attempt: SupervisorBootstrapAttempt;
  readonly assets: V2SidecarAssets;
  readonly supervisorInstanceId: string;
  readonly sessionId: string;
  readonly probeNonce: string;
}): Promise<PreflightAuthority> {
  directSupervisorExecBody({ sessionId: input.sessionId });
  directSupervisorExecBody({ sessionId: input.attempt.sessionId });
  if (!UUID.test(input.probeNonce) || !UUID.test(input.supervisorInstanceId) || input.sessionId === input.attempt.sessionId ||
    input.attempt.phase !== "submitted" || input.assets.revision !== "r10" || input.assets.buildHash !== input.attempt.binding.buildHash ||
    input.assets.schemaHash !== input.attempt.binding.schemaHash) throw new Error("preflight_authority_mismatch");
  const config = supervisorStaticConfig({ binding: input.attempt.binding, entrypointPath: input.assets.entrypointPath,
    connectTimeoutMs: input.attempt.connectTimeoutMs });
  const artifact = (path: string, content: string, sha256: string) => ({ path, bytes: Buffer.byteLength(content), sha256 });
  const artifacts = [...input.assets.files, input.assets.manifest].map(file => artifact(file.path, file.content, file.sha256));
  if (artifacts.length > 8 || artifacts.some(file => file.bytes < 1 || file.bytes > 8 * 1024 * 1024)) throw new Error("preflight_artifact_bound");
  return { version: 1, sessionId: input.sessionId, probeNonce: input.probeNonce, oldSessionId: input.attempt.sessionId,
    supervisorNonce: input.attempt.binding.nonce, supervisorInstanceId: input.supervisorInstanceId, entrypoint: input.assets.entrypointPath,
    static: artifact(".ghostprov-v2/supervisor/static.json", config, await sha256Hex(config)), artifacts };
}

export function preflightResultPath(probeNonce: string): string {
  if (!UUID.test(probeNonce)) throw new Error("preflight_nonce_invalid");
  return `.ghostprov-v2/retirement-preflight-${probeNonce}.json`;
}

export async function buildRetirementPreflightScript(authority: PreflightAuthority) {
  const source = (await readFile(new URL("./retirement-preflight.py", import.meta.url), "utf8")).replaceAll("\r\n", "\n");
  const bytes = Buffer.from(canonicalJson(authority));
  if (bytes.length > 16 * 1024 || Buffer.byteLength(source) > 24 * 1024) throw new Error("preflight_script_bound");
  const script = `#!/bin/sh\nset -eu\nexec /usr/bin/python3 -I -S <<'GP_RETIREMENT_PREFLIGHT_PY'\nimport base64\nAUTHORITY_BYTES = base64.b64decode('${bytes.toString("base64")}')\n${source}\nGP_RETIREMENT_PREFLIGHT_PY\n`;
  return { script, scriptSha256: await sha256Hex(script), authoritySha256: await sha256Hex(bytes), authority };
}

function exact(value: Record<string, unknown>, keys: readonly string[]): boolean {
  return Object.keys(value).sort().join(",") === [...keys].sort().join(",");
}
function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

/** Shape only: never include raw identities, paths, encoded content, or unrecognized keys/values. */
export function summarizeRetirementPreflightReadback(status: number, value: unknown, expected: {
  sessionId: string; probeNonce: string;
}) {
  const body = record(value);
  const keys = body ? Object.keys(body) : [];
  const responseKeys = READBACK_KEYS.filter(key => keys.includes(key));
  const relative = preflightResultPath(expected.probeNonce);
  const encoded = body?.body_b64;
  const decoded = typeof encoded === "string" && encoded.length <= 5500 ? Buffer.from(encoded, "base64") : null;
  const base64Valid = decoded !== null && decoded.toString("base64") === encoded;
  const bodyBytes = Number.isSafeInteger(body?.bytes) && (body!.bytes as number) >= 0 && (body!.bytes as number) <= 64 * 1024
    ? body!.bytes as number : null;
  const mimeType = READBACK_MIME_TYPES.find(value => value === body?.mime_type) ?? null;
  return { status: Number.isSafeInteger(status) && status >= 100 && status <= 599 ? status : null,
    parsedObject: body !== null, responseKeys, unknownKeyCount: keys.length - responseKeys.length,
    expectedPathMatch: body?.path === `/codex_workspace/${expected.sessionId}/${relative}`,
    expectedSessionMatch: body?.session_id === expected.sessionId,
    expectedRelativePathMatch: body?.relative_path === relative,
    expectedSessionRelativePathMatch: body?.relative_path === `${expected.sessionId}/${relative}`,
    mimeType, mimeTypeAllowlisted: mimeType !== null, bodyBytes,
    base64Valid, decodedBodyBytes: base64Valid ? decoded!.length : null,
    declaredBodyBytesMatch: base64Valid && bodyBytes !== null && decoded!.length === bodyBytes };
}

export function parseRetirementPreflightResult(value: unknown, expected: {
  sessionId: string; probeNonce: string; authoritySha256: string; scriptSha256: string;
}): PreflightResult {
  const result = record(value), checks = record(result?.checks);
  if (!result || !checks || !exact(result, ["version", "sessionId", "probeNonce", "authoritySha256", "scriptSha256", "outcome", "code", "checks", "processesInspected", "retirementSha256",
    "processScansCompleted", "processConflictCount", "processConflictsTruncated", "processConflicts", "fileFailure"]) ||
    !exact(checks, CHECKS) || CHECKS.some(key => typeof checks[key] !== "boolean") || result.version !== 1 ||
    result.sessionId !== expected.sessionId || result.probeNonce !== expected.probeNonce || result.authoritySha256 !== expected.authoritySha256 ||
    result.scriptSha256 !== expected.scriptSha256 || !CODES.includes(result.code as typeof CODES[number]) ||
    !Number.isSafeInteger(result.processesInspected) || (result.processesInspected as number) < 0 || (result.processesInspected as number) > 16_384 ||
    !Number.isSafeInteger(result.processScansCompleted) || (result.processScansCompleted as number) < 0 || (result.processScansCompleted as number) > 2 ||
    !Number.isSafeInteger(result.processConflictCount) || (result.processConflictCount as number) < 0 || (result.processConflictCount as number) > 32_772 ||
    !Array.isArray(result.processConflicts) || result.processConflicts.length > 8 || result.processConflicts.some(value => {
      const conflict = record(value);
      return !conflict || !exact(conflict, ["reason", "scan", "state", ...PROCESS_FLAGS]) ||
        !PROCESS_REASONS.includes(conflict.reason as typeof PROCESS_REASONS[number]) ||
        !Number.isSafeInteger(conflict.scan) || (conflict.scan as number) < 0 || (conflict.scan as number) > 2 ||
        (conflict.state !== null && (typeof conflict.state !== "string" || !/^[RSDZTtXxKWPI]$(?![\s\S])/.test(conflict.state))) ||
        PROCESS_FLAGS.some(key => typeof conflict[key] !== "boolean");
    }) || (result.processConflictCount as number) < result.processConflicts.length ||
    result.processConflictsTruncated !== ((result.processConflictCount as number) > result.processConflicts.length) ||
    (result.fileFailure !== null && (() => {
      const file = record(result.fileFailure);
      return !file || !exact(file, ["role", "reason", "component"]) || !FILE_ROLES.includes(file.role as typeof FILE_ROLES[number]) ||
        !FILE_REASONS.includes(file.reason as typeof FILE_REASONS[number]) || !FILE_COMPONENTS.includes(file.component as typeof FILE_COMPONENTS[number]);
    })()) ||
    (result.retirementSha256 !== null && (typeof result.retirementSha256 !== "string" || !HASH.test(result.retirementSha256))) ||
    !["retired", "blocked"].includes(result.outcome as string) ||
    (result.outcome === "retired" ? result.code !== "retired" || CHECKS.some(key => checks[key] !== true) || result.retirementSha256 === null ||
      result.processConflictCount !== 0 || result.processScansCompleted !== 2 || result.fileFailure !== null
      : result.code === "retired" || CHECKS.every(key => checks[key] === true))) throw new Error("preflight_result_invalid");
  return result as unknown as PreflightResult;
}

export function decodeRetirementPreflightFile(value: unknown, expected: {
  sessionId: string; probeNonce: string; authoritySha256: string; scriptSha256: string;
}): PreflightResult {
  const body = record(value);
  const relative = preflightResultPath(expected.probeNonce);
  if (!body || !exact(body, ["path", "relative_path", "bytes", "mime_type", "body_b64", "session_id"]) ||
    body.session_id !== expected.sessionId || body.path !== `/codex_workspace/${expected.sessionId}/${relative}` ||
    body.relative_path !== `${expected.sessionId}/${relative}` ||
    !Number.isSafeInteger(body.bytes) || (body.bytes as number) < 1 || (body.bytes as number) > PREFLIGHT_RESULT_LIMIT ||
    body.mime_type !== "application/json" || typeof body.body_b64 !== "string" || body.body_b64.length > 5500) {
    throw new Error("preflight_read_identity_invalid");
  }
  const bytes = Buffer.from(body.body_b64, "base64");
  if (bytes.length !== body.bytes || bytes.toString("base64") !== body.body_b64) throw new Error("preflight_read_encoding_invalid");
  return parseRetirementPreflightResult(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)), expected);
}
