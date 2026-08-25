import type { Clock, RandomSource } from "../../core/ports";
import { SYSTEM_CLOCK, SYSTEM_RANDOM } from "../../core/ports";
import type { YSweetEnvelope } from "../../prism/protocol";
import type { RenderSyncState } from "../../sandbox/ydoc";
import type { ProjectId } from "../../sandbox/types";
import type { AwarenessChannel } from "../awareness/channel";
import {
  createCredentialRotationKey,
  sealBootstrapCredential,
  sealCredentialRotation,
  type BootstrapBinding,
  type SealedBootstrapCredential,
} from "./crypto";
import {
  base64UrlDecode,
  canonicalJson,
  utf8Text,
  GP_V2_R1_SCHEMA_VERSION,
  GP_V2_R2_SCHEMA_VERSION,
  type AwarenessEnvelope,
  type HostFrame,
  type ReliableFrame,
  type SidecarFrame,
  type R2AwarenessEnvelope,
} from "../protocol/shared";
import {
  assertV2SidecarAssets,
  resolveV2SidecarRevision,
  type V2SidecarAssets,
  type V2SidecarRevision,
} from "../sidecar/assets";
import { deriveV2SidecarEventMemorySizing } from "../memory/event-memory-sizing";
import { V2_TERMINAL_SPOOL_CAPACITY } from "../sidecar/capacity";

const STATIC_PATH = ".ghostprov-v2/static.json";
const WORKER_PATH = ".ghostprov-v2/worker.json";
const CIPHERTEXT_PATH = ".ghostprov-v2/bootstrap.enc.json";
const REVISION_PATH = ".ghostprov-v2/revision.json";
const LAUNCHER_PATH = ".latexmkrc";
const MAIN_PATH = "main.tex";
const HELLO_PREFIX = "GPV2_BOOTSTRAP ";
const OWNER_REUSED_PREFIX = "GPV2_OWNER_REUSED ";
const SIDECAR_HEX_PREFIX = "GPV2_SIDECAR_HEX ";
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const BOOTSTRAP_ATTEMPT_CLAIM_LIMIT = 256;

function perlSingleQuoted(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/**
 * Shared Perl helpers for the launcher PID interlock. A PID is never trusted
 * by itself: the process must still have the recorded Linux start tick, be its
 * own process-group/session leader, and expose the exact immutable entrypoint
 * plus launch nonce as distinct argv fields.
 */
function processInterlockHelpers(): string[] {
  return [
    "use Fcntl qw(:flock);",
    "sub gp_read_file {",
    "  my ($gp_path, $gp_limit) = @_;",
    "  return undef unless open my $gp_fh, '<', $gp_path;",
    "  local $/; my $gp_value = <$gp_fh>; close $gp_fh;",
    "  return undef unless defined $gp_value && length($gp_value) <= $gp_limit;",
    "  return $gp_value;",
    "}",
    "sub gp_read_tail {",
    "  my ($gp_path, $gp_limit) = @_;",
    "  return undef unless defined($gp_limit) && $gp_limit =~ /^\\d+$/ && $gp_limit > 0 && $gp_limit <= 4096;",
    "  return undef unless open my $gp_fh, '<', $gp_path;",
    "  my $gp_size = -s $gp_fh;",
    "  my $gp_offset = defined($gp_size) && $gp_size > $gp_limit ? $gp_size - $gp_limit : 0;",
    "  seek $gp_fh, $gp_offset, 0;",
    "  my $gp_value = '';",
    "  my $gp_read = read $gp_fh, $gp_value, $gp_limit;",
    "  close $gp_fh;",
    "  return defined($gp_read) && $gp_read > 0 ? $gp_value : undef;",
    "}",
    "sub gp_record_is {",
    "  my ($gp_path, $gp_expected_record) = @_;",
    "  my $gp_actual_record = gp_read_file($gp_path, 4096);",
    "  return defined($gp_actual_record) && $gp_actual_record eq $gp_expected_record;",
    "}",
    "sub gp_proc_fields {",
    "  my ($gp_pid) = @_;",
    "  my $gp_stat = gp_read_file(\"/proc/$gp_pid/stat\", 4096);",
    "  return () unless defined $gp_stat;",
    "  my ($gp_tail) = $gp_stat =~ /^\\d+\\s+\\(.*\\)\\s+\\S+\\s+(.*)$/s;",
    "  return () unless defined $gp_tail;",
    "  my @gp_fields = split /\\s+/, $gp_tail;",
    "  return () unless @gp_fields > 18;",
    "  return @gp_fields;",
    "}",
    "sub gp_process_start {",
    "  my @gp_fields = gp_proc_fields($_[0]);",
    "  return @gp_fields > 18 && $gp_fields[18] =~ /^\\d+$/ ? $gp_fields[18] : '';",
    "}",
    "sub gp_process_state_start {",
    "  my ($gp_pid) = @_;",
    "  my $gp_stat = gp_read_file(\"/proc/$gp_pid/stat\", 4096);",
    "  return () unless defined $gp_stat;",
    "  my ($gp_state, $gp_tail) = $gp_stat =~ /^\\d+\\s+\\(.*\\)\\s+(\\S+)\\s+(.*)$/s;",
    "  return () unless defined($gp_state) && defined($gp_tail);",
    "  my @gp_fields = split /\\s+/, $gp_tail;",
    "  return () unless @gp_fields > 18 && $gp_fields[18] =~ /^\\d+$/;",
    "  return ($gp_state, $gp_fields[18]);",
    "}",
    "sub gp_process_is_same_live {",
    "  my ($gp_pid, $gp_start) = @_;",
    "  my ($gp_state, $gp_live_start) = gp_process_state_start($gp_pid);",
    "  return defined($gp_state) && $gp_state ne 'Z' && $gp_state ne 'X' && $gp_state ne 'x' && $gp_live_start eq $gp_start;",
    "}",
    "sub gp_process_matches {",
    "  my ($gp_pid, $gp_nonce, $gp_start, $gp_expected_entrypoint) = @_;",
    "  return 0 unless $gp_pid =~ /^\\d+$/ && $gp_pid > 1;",
    "  my $gp_cmd = gp_read_file(\"/proc/$gp_pid/cmdline\", 16384);",
    "  return 0 unless defined $gp_cmd;",
    "  my @gp_args = grep { length $_ } split /\\0/, $gp_cmd;",
    "  return 0 unless @gp_args && $gp_args[0] =~ m{(?:^|/)deno$};",
    "  my @gp_entries = grep { $_ eq $gp_expected_entrypoint } @gp_args;",
    "  my @gp_nonces = grep { $_ eq \"--ghostprov-launch-nonce=$gp_nonce\" } @gp_args;",
    "  my @gp_starts = grep { $_ eq \"--ghostprov-process-start=$gp_start\" } @gp_args;",
    "  return 0 unless @gp_entries == 1 && @gp_nonces == 1 && @gp_starts == 1;",
    "  my @gp_fields = gp_proc_fields($gp_pid);",
    "  return 0 unless @gp_fields > 18;",
    "  return 0 unless $gp_fields[1] eq \"$gp_pid\" && $gp_fields[2] eq \"$gp_pid\";",
    "  return 0 unless $gp_fields[18] eq \"$gp_start\";",
    "  return 1;",
    "}",
    "sub gp_exact_r2_entrypoint {",
    "  my ($gp_pid) = @_;",
    "  my $gp_cmd = gp_read_file(\"/proc/$gp_pid/cmdline\", 16384);",
    "  return undef unless defined $gp_cmd;",
    "  my @gp_args = grep { length $_ } split /\\0/, $gp_cmd;",
    "  my @gp_entries = grep { m{^/workspace/\\.ghostprov-v2/revisions/r2/[a-f0-9]{64}/[a-f0-9]{64}/sidecar\\.bundle\\.js$} } @gp_args;",
    "  return @gp_entries == 1 ? $gp_entries[0] : undef;",
    "}",
    "sub gp_exact_sidecar_entrypoint {",
    "  my ($gp_pid) = @_;",
    "  my $gp_cmd = gp_read_file(\"/proc/$gp_pid/cmdline\", 16384);",
    "  return undef unless defined $gp_cmd;",
    "  my @gp_args = grep { length $_ } split /\\0/, $gp_cmd;",
    "  my @gp_entries = grep { m{^/workspace/\\.ghostprov-v2/revisions/r[12]/[a-f0-9]{64}/[a-f0-9]{64}/sidecar\\.bundle\\.js$} } @gp_args;",
    "  return @gp_entries == 1 ? $gp_entries[0] : undef;",
    "}",
    "sub gp_group_alive {",
    "  my ($gp_group) = @_;",
    "  return 0 unless defined($gp_group) && $gp_group =~ /^\\d+$/ && $gp_group > 1;",
    "  for my $gp_stat_path (glob('/proc/[0-9]*/stat')) {",
    "    my $gp_stat = gp_read_file($gp_stat_path, 4096);",
    "    next unless defined $gp_stat;",
    "    my ($gp_state, $gp_tail) = $gp_stat =~ /^\\d+\\s+\\(.*\\)\\s+(\\S+)\\s+(.*)$/s;",
    "    next unless defined($gp_state) && defined($gp_tail);",
    "    my @gp_fields = split /\\s+/, $gp_tail;",
    "    next unless @gp_fields > 2;",
    "    return 1 if $gp_state ne 'Z' && $gp_state ne 'X' && $gp_state ne 'x' && $gp_fields[1] eq \"$gp_group\" && $gp_fields[2] eq \"$gp_group\";",
    "  }",
    "  return 0;",
    "}",
    "sub gp_identity_is_current {",
    "  my ($gp_identity_path, $gp_identity_record, $gp_require_lock) = @_;",
    "  return $gp_require_lock ? gp_owner_record_is_locked($gp_identity_path, $gp_identity_record) : gp_record_is($gp_identity_path, $gp_identity_record);",
    "}",
    "sub gp_static_authority_is_current {",
    "  my ($gp_static_path, $gp_expected_static) = @_;",
    "  return 1 unless defined($gp_static_path) || defined($gp_expected_static);",
    "  return 0 unless defined($gp_static_path) && defined($gp_expected_static);",
    "  my $gp_current_static = gp_read_file($gp_static_path, 16384);",
    "  return defined($gp_current_static) && $gp_current_static eq $gp_expected_static;",
    "}",
    "sub gp_signal_if_authoritative {",
    "  my ($gp_signal, $gp_target, $gp_static_path, $gp_expected_static) = @_;",
    "  return 0 unless gp_static_authority_is_current($gp_static_path, $gp_expected_static);",
    "  return kill $gp_signal, $gp_target;",
    "}",
    "sub gp_stop_if_current {",
    "  my ($gp_identity_path, $gp_identity_record, $gp_require_lock, $gp_pid, $gp_nonce, $gp_start, $gp_expected_entrypoint, $gp_static_path, $gp_expected_static) = @_;",
    "  return 0 unless gp_static_authority_is_current($gp_static_path, $gp_expected_static);",
    "  return 0 unless gp_identity_is_current($gp_identity_path, $gp_identity_record, $gp_require_lock);",
    "  return 0 unless gp_process_matches($gp_pid, $gp_nonce, $gp_start, $gp_expected_entrypoint);",
    "  return 0 unless gp_signal_if_authoritative('TERM', -$gp_pid, $gp_static_path, $gp_expected_static);",
    "  for (my $gp_try = 0; $gp_try < 20 && gp_group_alive($gp_pid); $gp_try++) { waitpid($gp_pid, 1); select undef, undef, undef, 0.1 if gp_group_alive($gp_pid); }",
    "  if (gp_group_alive($gp_pid)) {",
    "    return 0 unless gp_identity_is_current($gp_identity_path, $gp_identity_record, $gp_require_lock);",
    "    return 0 unless gp_process_matches($gp_pid, $gp_nonce, $gp_start, $gp_expected_entrypoint);",
    "    return 0 unless gp_signal_if_authoritative('KILL', -$gp_pid, $gp_static_path, $gp_expected_static);",
    "    for (my $gp_try = 0; $gp_try < 20 && gp_group_alive($gp_pid); $gp_try++) { waitpid($gp_pid, 1); select undef, undef, undef, 0.1 if gp_group_alive($gp_pid); }",
    "  }",
    "  return gp_group_alive($gp_pid) ? 0 : 1;",
    "}",
    "sub gp_owner_record_if_locked {",
    "  my ($gp_owner_path) = @_;",
    "  return undef unless open my $gp_owner_fh, '+<', $gp_owner_path;",
    "  if (flock($gp_owner_fh, LOCK_EX | LOCK_NB)) { flock($gp_owner_fh, LOCK_UN); close $gp_owner_fh; return undef; }",
    "  seek $gp_owner_fh, 0, 0; local $/; my $gp_owner_record = <$gp_owner_fh>; close $gp_owner_fh;",
    "  return undef unless defined $gp_owner_record && length($gp_owner_record) <= 4096;",
    "  return $gp_owner_record;",
    "}",
    "sub gp_owner_record_is_locked {",
    "  my ($gp_owner_path, $gp_expected_record) = @_;",
    "  my $gp_actual_record = gp_owner_record_if_locked($gp_owner_path);",
    "  return defined($gp_actual_record) && $gp_actual_record eq $gp_expected_record;",
    "}",
    "sub gp_unlink_pid_if_unowned_and_same {",
    "  my ($gp_owner_path, $gp_pid_path, $gp_expected_record) = @_;",
    "  return unless open my $gp_owner_fh, '+<', $gp_owner_path;",
    "  if (flock($gp_owner_fh, LOCK_EX | LOCK_NB)) {",
    "    unlink $gp_pid_path if gp_record_is($gp_pid_path, $gp_expected_record);",
    "    flock($gp_owner_fh, LOCK_UN);",
    "  }",
    "  close $gp_owner_fh;",
    "}",
    "sub gp_hex_diag { return 'GPV2_SIDECAR_HEX ' . unpack('H*', $_[0]); }",
  ];
}

const INERT_LAUNCHER = [
  "# GhostProv V2 launcher inactive",
  "$pdf_mode = 1;",
  "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
  "",
].join("\n");

function terminationLauncher(entrypointPath: string, launchNonce: string): string {
  const expected = perlSingleQuoted(`/workspace/${entrypointPath}`);
  const nonce = perlSingleQuoted(launchNonce);
  return [
    "# GhostProv V2 sidecar termination launcher",
    "$pdf_mode = 1;",
    ...processInterlockHelpers(),
    "my $gp_pid_file = '/tmp/ghostprov-v2/sidecar.pid';",
    "my $gp_owner_file = '/tmp/ghostprov-v2/sidecar.owner.lock';",
    `my $gp_expected = '${expected}';`,
    `my $gp_expected_nonce = '${nonce}';`,
    "my $gp_stopped = 0;",
    "my $gp_pid_record_stopped = 0;",
    "my ($gp_stopped_pid, $gp_stopped_nonce, $gp_stopped_start);",
    "my $gp_owner_record = gp_owner_record_if_locked($gp_owner_file);",
    "if (defined $gp_owner_record && $gp_owner_record =~ /\"pid\"\\s*:\\s*([0-9]+)/) {",
    "  my $gp_pid = $1;",
    "  my $gp_cmd = gp_read_file(\"/proc/$gp_pid/cmdline\", 16384) // '';",
    "  my @gp_args = grep { length $_ } split /\\0/, $gp_cmd;",
    "  my @gp_nonces = map { /^--ghostprov-launch-nonce=([A-Za-z0-9][A-Za-z0-9._:-]{0,127})$/ ? $1 : () } @gp_args;",
    "  my $gp_start = gp_process_start($gp_pid);",
    "  if (@gp_nonces == 1 && $gp_nonces[0] eq $gp_expected_nonce && $gp_start && gp_owner_record_is_locked($gp_owner_file, $gp_owner_record)) {",
    "    $gp_stopped = gp_stop_if_current($gp_owner_file, $gp_owner_record, 1, $gp_pid, $gp_nonces[0], $gp_start, $gp_expected);",
    "    ($gp_stopped_pid, $gp_stopped_nonce, $gp_stopped_start) = ($gp_pid, $gp_nonces[0], $gp_start) if $gp_stopped;",
    "  }",
    "}",
    "my $gp_pid_record = gp_read_file($gp_pid_file, 4096);",
    "if (defined $gp_pid_record) {",
    "  my ($gp_pid, $gp_nonce, $gp_start) = $gp_pid_record =~ /^([0-9]+) ([A-Za-z0-9][A-Za-z0-9._:-]{0,127}) ([0-9]+)\\n?$/;",
    "  if ($gp_stopped && defined($gp_start) && $gp_pid eq $gp_stopped_pid && $gp_nonce eq $gp_stopped_nonce && $gp_start eq $gp_stopped_start) {",
    "    $gp_pid_record_stopped = 1;",
    "  } elsif (!$gp_stopped && defined $gp_start && $gp_nonce eq $gp_expected_nonce) {",
    "    $gp_stopped = gp_stop_if_current($gp_pid_file, $gp_pid_record, 0, $gp_pid, $gp_nonce, $gp_start, $gp_expected);",
    "    $gp_pid_record_stopped = $gp_stopped;",
    "  }",
    "}",
    "if ($gp_pid_record_stopped && defined $gp_pid_record) {",
    "  gp_unlink_pid_if_unowned_and_same($gp_owner_file, $gp_pid_file, $gp_pid_record);",
    "}",
    "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
    "",
  ].join("\n");
}

const MAIN_SOURCE = [
  "\\documentclass{article}",
  "\\begin{document}",
  "GhostProv V2 launcher inactive.",
  "\\end{document}",
  "",
].join("\n");

function bootstrapArtifactPaths(launchNonce: string): {
  outputPath: string;
} {
  if (!SAFE_ID.test(launchNonce)) throw new TypeError("launchNonce is invalid");
  return {
    outputPath: `.ghostprov-v2/bootstrap-output.${launchNonce}.tex`,
  };
}

function bootstrapMainSource(outputPath: string): string {
  return [
    "\\documentclass{article}",
    "\\begin{document}",
    `\\InputIfFileExists{${outputPath}}{}{}`,
    "GhostProv V2 sidecar bootstrap.",
    "\\end{document}",
    "",
  ].join("\n");
}

export interface V2BootstrapYDoc {
  readPath(path: string): Promise<string | null>;
  writePath(path: string, content: string): Promise<void>;
  tombstonePath(path: string): Promise<boolean>;
  renderSyncState(): Promise<RenderSyncState>;
}

export interface V2BootstrapGateway {
  syncWorkspace(signal?: AbortSignal): Promise<void>;
  triggerRender(request: {
    mainDocument: string;
    clientStateVector: string;
    clientDeleteSetUpdate: string;
  }, signal?: AbortSignal): Promise<void>;
  fetchText(
    suffix: string,
    init?: RequestInit,
  ): Promise<{ status: number; text: string; headers: Headers }>;
}

export type V2BootstrapAwareness = AwarenessChannel<HostFrame, SidecarFrame>;

export interface V2ReadyDescriptor {
  instanceId: string;
  protocolRevision: V2SidecarRevision;
  launchNonce: string;
  buildHash: string;
  schemaHash: string;
  leaderGeneration: number;
  capacity: number;
}

export type V2BootstrapErrorKind =
  | "deploy"
  | "render"
  | "hello"
  | "crypto"
  | "readiness"
  | "timeout";

export interface V2SidecarFatalDiagnostic {
  readonly errorName: string;
  readonly errorCode: string;
  readonly causeName: string | null;
  readonly phase?: string;
  readonly permission?: string | null;
}

export class V2BootstrapError extends Error {
  readonly name = "V2BootstrapError";
  readonly operation: "bootstrap" | "render";
  readonly sidecarDiagnostic: V2SidecarFatalDiagnostic | null;

  constructor(
    message: string,
    readonly kind: V2BootstrapErrorKind,
    readonly attempt: number,
    options?: ErrorOptions & { sidecarDiagnostic?: V2SidecarFatalDiagnostic },
  ) {
    super(message, options);
    this.operation = kind === "render" ? "render" : "bootstrap";
    this.sidecarDiagnostic = options?.sidecarDiagnostic ?? null;
  }
}

export interface V2BootstrapControllerOptions {
  projectId: ProjectId;
  workerIndex: number;
  sandboxSessionId: string;
  leaderGeneration: number;
  ySweet: YSweetEnvelope;
  ydoc: V2BootstrapYDoc;
  gateway: V2BootstrapGateway;
  awareness: V2BootstrapAwareness;
  assets: V2SidecarAssets;
  /** Selected immutable V2 revision. Omission derives only from the supplied assets. */
  revision?: V2SidecarRevision | string;
  /** Required for R2 so bootstrap and steady transport share one exact launch fence. */
  launchNonce?: string;
  attempts?: number;
  timeoutMs?: number;
  pollMs?: number;
  physicalMax?: number;
  acceptedMax?: number;
  initialActivePerSandbox?: number;
  maxActivePerSandbox?: number;
  memoryBudgetBytes?: number;
  diagnostics?: { enabled: boolean; intervalMs: number };
  batching?: boolean;
  clock?: Clock;
  random?: RandomSource;
}

interface BootstrapHello {
  version: 1;
  binding: BootstrapBinding;
  instanceId: string;
  publicJwk: JsonWebKey;
}

function exactKeys(
  value: Record<string, unknown>,
  expected: readonly string[],
): boolean {
  const actual = Object.keys(value).sort();
  const sorted = [...expected].sort();
  return actual.length === sorted.length &&
    actual.every((key, index) => key === sorted[index]);
}

function temporaryLauncher(
  entrypointPath: string,
  launchNonce: string,
  revision: V2SidecarRevision,
  staticConfig: string,
): string {
  const workspaceEntrypoint = `/workspace/${entrypointPath}`;
  const expected = perlSingleQuoted(workspaceEntrypoint);
  const nonce = perlSingleQuoted(launchNonce);
  const expectedStatic = perlSingleQuoted(staticConfig);
  const artifacts = bootstrapArtifactPaths(launchNonce);
  const logPath = perlSingleQuoted(`/tmp/ghostprov-v2/sidecar.${launchNonce}.log`);
  const outputPath = perlSingleQuoted(`/workspace/${artifacts.outputPath}`);
  const helloExpression = revision === "r2"
    ? '"$gp_dir/bootstrap-hello.$gp_launch_nonce.$gp_pid"'
    : '"$gp_dir/bootstrap-hello"';
  const childHelloExpression = revision === "r2"
    ? '"$gp_dir/bootstrap-hello.$gp_launch_nonce.$$"'
    : '"$gp_dir/bootstrap-hello"';
  return [
    "# GhostProv V2 temporary bootstrap launcher",
    "$pdf_mode = 1;",
    ...processInterlockHelpers(),
    "my $gp_dir = '/tmp/ghostprov-v2';",
    "my $gp_pid_file = '/tmp/ghostprov-v2/sidecar.pid';",
    "my $gp_owner_file = '/tmp/ghostprov-v2/sidecar.owner.lock';",
    `my $gp_expected = '${expected}';`,
    `my $gp_launch_nonce = '${nonce}';`,
    `my $gp_expected_static = '${expectedStatic}';`,
    "my $gp_static_path = '/workspace/.ghostprov-v2/static.json';",
    `my $gp_require_owner = ${revision === "r2" ? 1 : 0};`,
    "my $gp_hello = '';",
    "my $gp_candidate_pid_file = '';",
    "my $gp_candidate_record = '';",
    "my $gp_child_pid = 0;",
    "my $gp_child_start = '';",
    "my $gp_reused_owner = 0;",
    "my $gp_repeated_attempt = 0;",
    "my $gp_attempt_claimed = 0;",
    `my $gp_attempt_claim_limit = ${BOOTSTRAP_ATTEMPT_CLAIM_LIMIT};`,
    "my $gp_launch_error = '';",
    "my $gp_authoritative = 1;",
    `my $gp_log = '${logPath}';`,
    `my $gp_tex = '${outputPath}';`,
    "my $gp_hello_seen = 0;",
    "my $gp_marker = '';",
    "my $gp_secondary_marker = '';",
    "my $gp_attempt_path = \"$gp_dir/bootstrap-attempt.$gp_launch_nonce\";",
    "mkdir $gp_dir, 0700 unless -d $gp_dir;",
    // A delayed render must not retire the owner chosen by a newer binding.
    // The canonical static config contains the project/session/generation,
    // release hashes, protocol revision, launch nonce, and runtime limits.
    "my $gp_current_static = gp_read_file($gp_static_path, 16384);",
    "if (!defined($gp_current_static) || $gp_current_static ne $gp_expected_static) { $gp_authoritative = 0; $gp_launch_error = 'bootstrap launcher binding is no longer current'; }",
    "my $gp_deno = '';",
    "for my $gp_candidate (grep { defined $_ && length $_ } ($ENV{DENO_BIN}, '/usr/local/bin/deno', '/usr/bin/deno', '/root/.deno/bin/deno', '/code/crixet/deno')) { if (-x $gp_candidate) { $gp_deno = $gp_candidate; last; } }",
    "if (!$gp_deno) { my $gp_path_deno = `command -v deno 2>/dev/null`; chomp $gp_path_deno; $gp_deno = $gp_path_deno if $gp_path_deno && -x $gp_path_deno; }",
    "if (!$gp_deno && !$gp_launch_error) { open my $gp_missing, '>', $gp_log or die 'sidecar log'; print $gp_missing 'Deno executable not found on PATH or standard paths'; close $gp_missing; }",
    // A host restart does not imply a sandbox restart. Retire a prior immutable
    // R1 or R2 owner before forking the selected release; otherwise tryLock
    // correctly makes every new child lose forever. The locked owner record,
    // canonical PID record, Linux start tick, process group/session, immutable
    // entrypoint, and argv nonce are all revalidated immediately before signal.
    "my $gp_prior_owner = $gp_deno && !$gp_launch_error ? gp_owner_record_if_locked($gp_owner_file) : undef;",
    "my $gp_prior_pid_record = $gp_deno && !$gp_launch_error ? gp_read_file($gp_pid_file, 4096) : undef;",
    "my ($gp_prior_pid, $gp_prior_nonce, $gp_prior_start) = defined($gp_prior_pid_record) ? $gp_prior_pid_record =~ /^([0-9]+) ([A-Za-z0-9][A-Za-z0-9._:-]{0,127}) ([0-9]+)\\n$/ : ();",
    "if ($gp_deno && !$gp_launch_error && defined $gp_prior_owner) {",
    "    my ($gp_owner_instance, $gp_owner_nonce, $gp_owner_pid, $gp_owner_time) = $gp_prior_owner =~ /^\\{\"version\":1,\"instanceId\":\"([A-Za-z0-9][A-Za-z0-9._:-]{0,127})\",\"nonce\":\"([A-Za-z0-9][A-Za-z0-9._:-]{0,127})\",\"pid\":([0-9]+),\"acquiredAtMs\":([0-9]+)\\}$/;",
    "    my $gp_prior_entrypoint = defined($gp_prior_pid) ? gp_exact_sidecar_entrypoint($gp_prior_pid) : undef;",
    "    if (!defined($gp_owner_pid) || !defined($gp_owner_nonce) || !defined($gp_prior_start) || !defined($gp_prior_entrypoint) || $gp_owner_pid ne $gp_prior_pid || $gp_owner_pid < 2) {",
    "      $gp_launch_error = 'existing owned sidecar identity is incomplete';",
    "    } elsif (!gp_process_matches($gp_prior_pid, $gp_prior_nonce, $gp_prior_start, $gp_prior_entrypoint)) {",
    "      $gp_launch_error = 'existing owned sidecar process identity is invalid';",
    "    } elsif ($gp_prior_nonce eq $gp_launch_nonce && $gp_prior_entrypoint eq $gp_expected) {",
    // bootstrap.ts intentionally removes its hello after credential handoff.
    // A delayed duplicate render or a same-binding retry must therefore adopt
    // the strictly proven owner without requiring that one-shot file or ever
    // signalling the healthy process. The host only accepts this marker when
    // it still holds the decoded hello for the exact same binding.
    "      $gp_reused_owner = 1;",
    "      $gp_marker = \"GPV2_OWNER_REUSED $gp_launch_nonce\";",
    "      my $gp_reused_diag = gp_read_tail($gp_log, 4096);",
    "      $gp_secondary_marker = gp_hex_diag($gp_reused_diag) if defined($gp_reused_diag) && length($gp_reused_diag);",
    "    } else {",
    "      my $gp_prior_stopped = gp_stop_if_current($gp_owner_file, $gp_prior_owner, 1, $gp_prior_pid, $gp_prior_nonce, $gp_prior_start, $gp_prior_entrypoint, $gp_static_path, $gp_expected_static);",
    "      if (!$gp_prior_stopped && !gp_static_authority_is_current($gp_static_path, $gp_expected_static)) {",
    "        $gp_authoritative = 0;",
    "        $gp_launch_error = 'bootstrap launcher binding is no longer current';",
    "      } elsif (!$gp_prior_stopped) {",
    "        $gp_launch_error = 'existing owned sidecar could not be retired safely';",
    "      } else {",
    "        gp_unlink_pid_if_unowned_and_same($gp_owner_file, $gp_pid_file, $gp_prior_pid_record);",
    "        $gp_launch_error = 'existing sidecar ownership lock did not release' if defined gp_owner_record_if_locked($gp_owner_file);",
    "      }",
    "    }",
    // The canonical PID is only promoted after hello ownership proof. Its
    // exact nonce (or the atomic attempt directory used by newer launchers)
    // proves this nonce has already forked. If its owner has since exited,
    // preserve the log and report it; never fork the same nonce a second time.
    "} elsif ($gp_deno && !$gp_launch_error && (-d $gp_attempt_path || (defined($gp_prior_start) && $gp_prior_nonce eq $gp_launch_nonce))) {",
    "  $gp_repeated_attempt = 1;",
    "  my $gp_repeated_diag = gp_read_tail($gp_log, 4096);",
    "  $gp_repeated_diag = 'same-nonce sidecar no longer has a live owner' unless defined($gp_repeated_diag) && length($gp_repeated_diag);",
    "  $gp_marker = gp_hex_diag($gp_repeated_diag);",
    "} elsif ($gp_deno && !$gp_launch_error) {",
    "  if (!open my $gp_preflight_owner_fh, '+>>', $gp_owner_file) {",
    "    $gp_launch_error = 'unable to open sidecar ownership interlock';",
    "  } elsif (!flock($gp_preflight_owner_fh, LOCK_EX | LOCK_NB)) {",
    "    close $gp_preflight_owner_fh;",
    "    $gp_launch_error = 'sidecar ownership changed during startup preflight';",
    "  } else {",
    "    chmod 0600, $gp_owner_file;",
    "    my $gp_unowned_pid_record = gp_read_file($gp_pid_file, 4096);",
    "    if (defined $gp_unowned_pid_record) {",
    "      my ($gp_unowned_pid, $gp_unowned_nonce, $gp_unowned_start) = $gp_unowned_pid_record =~ /^([0-9]+) ([A-Za-z0-9][A-Za-z0-9._:-]{0,127}) ([0-9]+)\\n$/;",
    "      if (!defined($gp_unowned_start) || $gp_unowned_pid < 2) {",
    "        $gp_launch_error = 'unowned sidecar PID identity is malformed';",
    "      } else {",
    "        my ($gp_live_state, $gp_live_start) = gp_process_state_start($gp_unowned_pid);",
    "        if (!defined($gp_live_state)) {",
    "          if (-d \"/proc/$gp_unowned_pid\") { $gp_launch_error = 'unowned sidecar process identity cannot be inspected'; }",
    "          else { unlink $gp_pid_file if gp_record_is($gp_pid_file, $gp_unowned_pid_record); }",
    "        } elsif ($gp_live_state eq 'Z' || $gp_live_state eq 'X' || $gp_live_state eq 'x' || $gp_live_start ne $gp_unowned_start) {",
    "          unlink $gp_pid_file if gp_record_is($gp_pid_file, $gp_unowned_pid_record);",
    "        } else {",
    "          my $gp_unowned_entrypoint = gp_exact_sidecar_entrypoint($gp_unowned_pid);",
    "          if (!defined($gp_unowned_entrypoint) || !gp_process_matches($gp_unowned_pid, $gp_unowned_nonce, $gp_unowned_start, $gp_unowned_entrypoint)) {",
    "            $gp_launch_error = 'unowned live sidecar identity is not an immutable GhostProv release';",
    "          } else {",
    "            my $gp_unowned_stopped = gp_stop_if_current($gp_pid_file, $gp_unowned_pid_record, 0, $gp_unowned_pid, $gp_unowned_nonce, $gp_unowned_start, $gp_unowned_entrypoint, $gp_static_path, $gp_expected_static);",
    "            if (!$gp_unowned_stopped && !gp_static_authority_is_current($gp_static_path, $gp_expected_static)) { $gp_authoritative = 0; $gp_launch_error = 'bootstrap launcher binding is no longer current'; }",
    "            elsif (!$gp_unowned_stopped) { $gp_launch_error = 'unowned sidecar could not be retired safely'; }",
    "            elsif (gp_record_is($gp_pid_file, $gp_unowned_pid_record)) { unlink $gp_pid_file; }",
    "          }",
    "        }",
    "      }",
    "    }",
    "    flock($gp_preflight_owner_fh, LOCK_UN);",
    "    close $gp_preflight_owner_fh;",
    "  }",
    "}",
    // mkdir is the atomic once-per-nonce fork claim. A delayed duplicate which
    // raced through preflight loses this claim and becomes diagnostic-only.
    // Claims are intentionally permanent for the sandbox lifetime, so cap the
    // exact protected names and fail closed into sandbox recovery rather than
    // leak inodes forever or delete a fence a delayed launcher may still need.
    "if ($gp_deno && !$gp_launch_error && !$gp_reused_owner && !$gp_repeated_attempt) {",
    "  my $gp_attempt_claim_count = 0;",
    "  if (!opendir my $gp_attempt_dir, $gp_dir) {",
    "    $gp_launch_error = 'unable to inspect bootstrap attempt claims';",
    "  } else {",
    "    while (my $gp_attempt_name = readdir $gp_attempt_dir) {",
    "      next unless $gp_attempt_name =~ /^bootstrap-attempt\\.[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;",
    "      my $gp_attempt_candidate = \"$gp_dir/$gp_attempt_name\";",
    "      next if -l $gp_attempt_candidate;",
    "      $gp_attempt_claim_count++ if -d $gp_attempt_candidate;",
    "      last if $gp_attempt_claim_count >= $gp_attempt_claim_limit;",
    "    }",
    "    closedir $gp_attempt_dir;",
    "  }",
    "  if (!$gp_launch_error && $gp_attempt_claim_count >= $gp_attempt_claim_limit) {",
    "    $gp_launch_error = 'bootstrap attempt claim capacity is exhausted';",
    "  } elsif (!$gp_launch_error && gp_static_authority_is_current($gp_static_path, $gp_expected_static) && mkdir $gp_attempt_path, 0700) {",
    "    $gp_attempt_claimed = 1;",
    "  } elsif (!$gp_launch_error && !gp_static_authority_is_current($gp_static_path, $gp_expected_static)) {",
    "    $gp_authoritative = 0;",
    "    $gp_launch_error = 'bootstrap launcher binding is no longer current';",
    "  } elsif (!$gp_launch_error) {",
    "    $gp_repeated_attempt = 1;",
    "    my $gp_repeated_diag = gp_read_tail($gp_log, 4096);",
    "    $gp_repeated_diag = 'same-nonce bootstrap attempt is already claimed' unless defined($gp_repeated_diag) && length($gp_repeated_diag);",
    "    $gp_marker = gp_hex_diag($gp_repeated_diag);",
    "  }",
    "}",
    "if ($gp_deno && !$gp_launch_error && $gp_attempt_claimed && !gp_static_authority_is_current($gp_static_path, $gp_expected_static)) {",
    "  $gp_authoritative = 0;",
    "  $gp_launch_error = 'bootstrap launcher binding is no longer current';",
    "}",
    "if ($gp_deno && !$gp_launch_error && $gp_attempt_claimed) {",
    "  unlink $gp_log if -e $gp_log;",
    "  unlink $gp_tex if -e $gp_tex;",
    "}",
    "if ($gp_deno && !$gp_launch_error && $gp_attempt_claimed && !gp_static_authority_is_current($gp_static_path, $gp_expected_static)) {",
    "  $gp_authoritative = 0;",
    "  $gp_launch_error = 'bootstrap launcher binding is no longer current';",
    "}",
    "if ($gp_deno && !$gp_launch_error && $gp_attempt_claimed) {",
    "  my $gp_pid = fork();",
    "  die 'GhostProv V2 fork failed' unless defined $gp_pid;",
    "  if ($gp_pid > 0) {",
    "    $gp_child_pid = $gp_pid;",
    `    $gp_hello = ${helloExpression};`,
    "    $gp_candidate_pid_file = \"$gp_dir/sidecar.pid.$gp_launch_nonce.$gp_pid.candidate\";",
    "    for (my $gp_try = 0; $gp_try < 50 && !$gp_child_start; $gp_try++) { $gp_child_start = gp_process_start($gp_pid); select undef, undef, undef, 0.001 unless $gp_child_start; }",
    "    if (!$gp_child_start) {",
    "      $gp_launch_error = 'unable to record exact candidate process identity';",
    "      my $gp_signalled = gp_signal_if_authoritative('TERM', $gp_pid, $gp_static_path, $gp_expected_static);",
    "      if (!$gp_signalled && !gp_static_authority_is_current($gp_static_path, $gp_expected_static)) { $gp_authoritative = 0; $gp_launch_error = 'bootstrap launcher binding is no longer current'; }",
    "    }",
    "    else {",
    "      $gp_candidate_record = \"$gp_pid $gp_launch_nonce $gp_child_start\\n\";",
    "      if (open my $gp_pid_fh, '>', $gp_candidate_pid_file) { print $gp_pid_fh $gp_candidate_record; close $gp_pid_fh; chmod 0600, $gp_candidate_pid_file; }",
    "      else {",
    "        $gp_launch_error = 'unable to write candidate process identity';",
    "        my $gp_signalled = gp_signal_if_authoritative('TERM', $gp_pid, $gp_static_path, $gp_expected_static);",
    "        if (!$gp_signalled && !gp_static_authority_is_current($gp_static_path, $gp_expected_static)) { $gp_authoritative = 0; $gp_launch_error = 'bootstrap launcher binding is no longer current'; }",
    "      }",
    "    }",
    "  }",
    "  if ($gp_pid == 0) {",
    "    my $gp_child_candidate = \"$gp_dir/sidecar.pid.$gp_launch_nonce.$$.candidate\";",
    "    my $gp_child_candidate_ready = 0;",
    "    my $gp_child_verified_start = '';",
    "    for (my $gp_try = 0; $gp_try < 500 && !$gp_child_candidate_ready; $gp_try++) {",
    "      my $gp_child_record = gp_read_file($gp_child_candidate, 4096);",
    "      my $gp_self_start = gp_process_start($$);",
    "      my ($gp_recorded_start) = defined($gp_child_record) ? $gp_child_record =~ /^\\Q$$ $gp_launch_nonce \\E([0-9]+)\\n$/ : ();",
    "      if (defined($gp_recorded_start) && $gp_self_start && $gp_recorded_start eq $gp_self_start) { $gp_child_verified_start = $gp_self_start; $gp_child_candidate_ready = 1; }",
    "      select undef, undef, undef, 0.01 unless $gp_child_candidate_ready;",
    "    }",
    "    exit 126 unless $gp_child_candidate_ready;",
    `    my $gp_child_hello = ${childHelloExpression};`,
    "    unlink $gp_child_hello if -e $gp_child_hello;",
    "    open STDOUT, '>', $gp_log or die 'stdout';",
    "    open STDERR, '>&STDOUT' or die 'stderr';",
    `    exec 'setsid', $gp_deno, 'run', '--cached-only', "--cert=$ENV{PROXY_CA_CERT_PATH}", '--allow-env=OPENAI_BASE_URL,OPENAI_API_KEY,PROXY_CA_CERT_PATH,NODE_ENV,PRODUCTION,FORCE_COLOR,NO_COLOR,COLORTERM,TERM', '--allow-net', '--allow-read=/workspace/.ghostprov-v2,/tmp/ghostprov-v2', '--allow-write=/tmp/ghostprov-v2', '${expected}', "--ghostprov-launch-nonce=$gp_launch_nonce", "--ghostprov-process-start=$gp_child_verified_start";`,
    "    exit 127;",
    "  }",
    "}",
    // Keep the complete launcher (prior-owner stop + child hello + promotion)
    // strictly below the host's 45-second attempt deadline.
    "for (my $gp_try = 0; $gp_deno && !$gp_launch_error && $gp_attempt_claimed && !$gp_hello_seen && $gp_try < 500; $gp_try++) {",
    "  if (open my $gp_fh, '<', $gp_hello) {",
    "    while (my $gp_line = <$gp_fh>) { if ($gp_line =~ /^(GPV2_BOOTSTRAP [A-Za-z0-9_-]+)/) { $gp_marker = $1; $gp_hello_seen = 1; } }",
    "    close $gp_fh;",
    "    last;",
    "  }",
    "  if ($gp_child_pid) {",
    "    my $gp_waited = waitpid($gp_child_pid, 1);",
    "    if ($gp_waited == $gp_child_pid || ($gp_child_start && !gp_process_is_same_live($gp_child_pid, $gp_child_start))) { $gp_launch_error = 'sidecar exited before bootstrap hello'; last; }",
    "  }",
    "  select undef, undef, undef, 0.05;",
    "}",
    "if ($gp_hello_seen) {",
    "  my $gp_promoted = 0;",
    "  if ($gp_require_owner) {",
    "    for (my $gp_try = 0; $gp_try < 300 && !$gp_promoted; $gp_try++) {",
    "      my $gp_owner_record = gp_owner_record_if_locked($gp_owner_file);",
    "      my $gp_owner_matches = defined($gp_owner_record) && $gp_owner_record =~ /\"pid\"\\s*:\\s*\\Q$gp_child_pid\\E(?:\\D|$)/;",
    "      $gp_promoted = gp_record_is($gp_pid_file, $gp_candidate_record) && gp_process_matches($gp_child_pid, $gp_launch_nonce, $gp_child_start, $gp_expected) && $gp_owner_matches && gp_owner_record_is_locked($gp_owner_file, $gp_owner_record);",
    "      my $gp_waited = waitpid($gp_child_pid, 1);",
    "      if (!$gp_promoted && ($gp_waited == $gp_child_pid || !gp_process_is_same_live($gp_child_pid, $gp_child_start))) { $gp_launch_error = 'sidecar exited before owned PID promotion proof'; last; }",
    "      select undef, undef, undef, 0.01 unless $gp_promoted;",
    "    }",
    "    $gp_launch_error ||= 'owned sidecar did not atomically promote its process identity' unless $gp_promoted;",
    "  } else {",
    "    if (!gp_record_is($gp_candidate_pid_file, $gp_candidate_record) || !gp_process_matches($gp_child_pid, $gp_launch_nonce, $gp_child_start, $gp_expected)) {",
    "      $gp_launch_error = 'candidate hello did not prove the exact sidecar process';",
    "    } elsif (!rename $gp_candidate_pid_file, $gp_pid_file) {",
    "      $gp_launch_error = 'unable to atomically promote candidate process identity';",
    "    } elsif (!gp_record_is($gp_pid_file, $gp_candidate_record) || !gp_process_matches($gp_child_pid, $gp_launch_nonce, $gp_child_start, $gp_expected)) {",
    "      $gp_launch_error = 'sidecar process changed during identity promotion';",
    "    } else { chmod 0600, $gp_pid_file; $gp_promoted = 1; }",
    "  }",
    "}",
    "if (!$gp_reused_owner && !$gp_repeated_attempt && (!$gp_hello_seen || $gp_launch_error)) {",
    "  my $gp_stopped = 0;",
    "  my $gp_reaped = $gp_child_pid && waitpid($gp_child_pid, 1) == $gp_child_pid;",
    "  if ($gp_child_pid && $gp_child_start && $gp_candidate_record) {",
    "    my $gp_identity_path = -e $gp_candidate_pid_file ? $gp_candidate_pid_file : $gp_pid_file;",
    "    $gp_stopped = gp_stop_if_current($gp_identity_path, $gp_candidate_record, 0, $gp_child_pid, $gp_launch_nonce, $gp_child_start, $gp_expected, $gp_static_path, $gp_expected_static);",
    "    unlink $gp_candidate_pid_file if ($gp_reaped || $gp_stopped || !gp_group_alive($gp_child_pid)) && gp_record_is($gp_candidate_pid_file, $gp_candidate_record);",
    "  }",
    "  unlink $gp_hello if $gp_hello && -e $gp_hello;",
    "  my $gp_diag = $gp_launch_error || 'sidecar log is missing';",
    "  if ($gp_child_pid) { my $gp_sidecar_diag = gp_read_tail($gp_log, 4096); $gp_diag = $gp_sidecar_diag if defined($gp_sidecar_diag) && length($gp_sidecar_diag); }",
    "  $gp_diag = substr($gp_diag // '', 0, 4096);",
    "  $gp_marker = gp_hex_diag($gp_diag);",
    "}",
    "if ($gp_authoritative && length($gp_marker) && gp_static_authority_is_current($gp_static_path, $gp_expected_static)) {",
    "  open my $gp_tex_fh, '>', $gp_tex or die 'GhostProv V2 bootstrap output';",
    "  print $gp_tex_fh \"\\\\typeout{$gp_marker}\\n\";",
    "  print $gp_tex_fh \"\\\\typeout{$gp_secondary_marker}\\n\" if length($gp_secondary_marker);",
    "  close $gp_tex_fh;",
    "}",
    "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
    "",
  ].join("\n");
}

function sanitizeSidecarDiagnostic(value: string): string {
  const sanitized = value
    .replace(/\bBearer\s+[^\s]+/gi, "Bearer [REDACTED]")
    .replace(/\b(?:sk-|gAAAA|eyJ)[A-Za-z0-9._=-]{16,}/g, "[REDACTED]")
    .replace(/([?&](?:token|key|secret|authorization)=)[^&\s]+/gi, "$1[REDACTED]")
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .trim()
    .slice(0, 512);
  return sanitized || "unknown sidecar launch failure";
}

interface DecodedSidecarDiagnostic {
  readonly message: string;
  readonly fatal: V2SidecarFatalDiagnostic | null;
}

function parseSidecarFatalDiagnostic(value: string): V2SidecarFatalDiagnostic | null {
  const lines = value.split(/\r?\n/);
  for (let index = lines.length - 1; index >= 0; index--) {
    const line = lines[index]!.trim();
    if (!line.startsWith("{") || !line.endsWith("}") || line.length > 1_024) continue;
    try {
      const parsed = JSON.parse(line) as Record<string, unknown>;
      const legacy = exactKeys(parsed, ["event", "error_name", "error_code", "cause_name"]);
      const detailed = exactKeys(parsed, [
        "event", "error_name", "error_code", "cause_name", "phase", "permission",
      ]);
      if (
        (!legacy && !detailed) ||
        parsed.event !== "ghostprov_v2_sidecar_fatal" ||
        typeof parsed.error_name !== "string" ||
        !SAFE_ID.test(parsed.error_name) ||
        typeof parsed.error_code !== "string" ||
        !SAFE_ID.test(parsed.error_code) ||
        !(
          parsed.cause_name === null ||
          (typeof parsed.cause_name === "string" && SAFE_ID.test(parsed.cause_name))
        ) ||
        (detailed && (typeof parsed.phase !== "string" || !SAFE_ID.test(parsed.phase))) ||
        (detailed && !(
          parsed.permission === null ||
          (typeof parsed.permission === "string" && /^(?:read|write|env|net|sys|run|ffi)$/.test(parsed.permission))
        ))
      ) continue;
      return Object.freeze({
        errorName: parsed.error_name,
        errorCode: parsed.error_code,
        causeName: parsed.cause_name as string | null,
        ...(detailed ? {
          phase: parsed.phase as string,
          permission: parsed.permission as string | null,
        } : {}),
      });
    } catch {
      // Only the sidecar's exact bounded JSON fatal record is structured.
    }
  }
  return null;
}

function decodeSidecarDiagnostic(value: string): DecodedSidecarDiagnostic {
  if (value.length < 2 || value.length > 8192 || value.length % 2 !== 0 || !/^[0-9a-f]+$/i.test(value)) {
    return { message: "malformed sidecar launch diagnostic", fatal: null };
  }
  const bytes = new Uint8Array(value.length / 2);
  for (let index = 0; index < bytes.length; index++) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  let decoded: string;
  try {
    decoded = utf8Text(bytes);
  } catch {
    return { message: "malformed sidecar launch diagnostic", fatal: null };
  }
  return {
    message: sanitizeSidecarDiagnostic(decoded),
    fatal: parseSidecarFatalDiagnostic(decoded),
  };
}

function renderLogText(value: string): string {
  try {
    const parsed = JSON.parse(value) as Record<string, unknown>;
    const logs = [parsed.pdfTexLog, parsed.latexmkLog, parsed.bibTexLog]
      .filter((entry): entry is string => typeof entry === "string");
    if (logs.length) return logs.join("\n");
  } catch {
    // Test doubles and legacy proxies may return the log body directly.
  }
  return value;
}

function extractWrappedMarker(
  log: string,
  prefix: string,
  alphabet: RegExp,
  maximumLength: number,
): string | null {
  const marker = log.lastIndexOf(prefix);
  if (marker < 0) return null;
  const lines = log.slice(marker + prefix.length).split(/\r?\n/);
  let value = "";
  let complete = false;
  for (const line of lines) {
    const trimmed = line.trim();
    const match = alphabet.exec(trimmed);
    if (!match) {
      complete = value.length > 0;
      break;
    }
    value += match[0];
    if (value.length > maximumLength) return null;
    if (match[0].length !== trimmed.length) {
      complete = true;
      break;
    }
  }
  return complete && value ? value : null;
}

function parseHelloCandidate(
  encoded: string,
  expected: BootstrapBinding,
  attempt: number,
): BootstrapHello {
  let value: unknown;
  try {
    value = JSON.parse(utf8Text(base64UrlDecode(encoded)));
  } catch (cause) {
    throw new V2BootstrapError("Sidecar bootstrap hello is malformed", "hello", attempt, { cause });
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new V2BootstrapError("Sidecar bootstrap hello must be an object", "hello", attempt);
  }
  const raw = value as Record<string, unknown>;
  if (!exactKeys(raw, ["version", "binding", "instanceId", "publicJwk"]) || raw.version !== 1) {
    throw new V2BootstrapError("Sidecar bootstrap hello fields are invalid", "hello", attempt);
  }
  if (
    !raw.binding ||
    typeof raw.binding !== "object" ||
    Array.isArray(raw.binding) ||
    canonicalJson(raw.binding) !== canonicalJson(expected)
  ) {
    throw new V2BootstrapError("Sidecar bootstrap hello binding is stale", "hello", attempt);
  }
  if (typeof raw.instanceId !== "string" || !SAFE_ID.test(raw.instanceId)) {
    throw new V2BootstrapError("Sidecar bootstrap instance is invalid", "hello", attempt);
  }
  if (
    !raw.publicJwk ||
    typeof raw.publicJwk !== "object" ||
    Array.isArray(raw.publicJwk) ||
    (raw.publicJwk as JsonWebKey).kty !== "EC" ||
    (raw.publicJwk as JsonWebKey).crv !== "P-256" ||
    typeof (raw.publicJwk as JsonWebKey).x !== "string" ||
    typeof (raw.publicJwk as JsonWebKey).y !== "string" ||
    (raw.publicJwk as JsonWebKey).d !== undefined
  ) {
    throw new V2BootstrapError("Sidecar bootstrap public key is invalid", "hello", attempt);
  }
  return {
    version: 1,
    binding: expected,
    instanceId: raw.instanceId,
    publicJwk: raw.publicJwk as JsonWebKey,
  };
}

function matchesReadiness(
  envelope: AwarenessEnvelope<SidecarFrame>,
  binding: BootstrapBinding,
  instanceId: string,
): boolean {
  const r2 = binding.protocolRevision === "r2";
  const r2Envelope = envelope as R2AwarenessEnvelope<SidecarFrame>;
  return envelope.protocol === "gp-awareness-v2" &&
    envelope.schemaVersion === (r2 ? GP_V2_R2_SCHEMA_VERSION : GP_V2_R1_SCHEMA_VERSION) &&
    (!r2 || (
      r2Envelope.protocolRevision === "r2" &&
      r2Envelope.launchNonce === binding.nonce &&
      r2Envelope.targetInstanceId === binding.nonce
    )) &&
    envelope.projectId === binding.projectId &&
    envelope.sessionId === binding.sandboxSessionId &&
    envelope.leaderGeneration === binding.leaderGeneration &&
    envelope.buildHash === binding.buildHash &&
    envelope.schemaHash === binding.schemaHash &&
    envelope.instanceId === instanceId &&
    envelope.peerId === "sidecar" &&
    !envelope.draining &&
    Number.isSafeInteger(envelope.credits) &&
    envelope.credits > 0 &&
    envelope.credits <= 128;
}

function matchesBoundInstance(
  envelope: AwarenessEnvelope<SidecarFrame>,
  binding: BootstrapBinding,
  instanceId: string,
): boolean {
  const r2 = binding.protocolRevision === "r2";
  const r2Envelope = envelope as R2AwarenessEnvelope<SidecarFrame>;
  return envelope.protocol === "gp-awareness-v2" &&
    envelope.schemaVersion === (r2 ? GP_V2_R2_SCHEMA_VERSION : GP_V2_R1_SCHEMA_VERSION) &&
    (!r2 || (
      r2Envelope.protocolRevision === "r2" &&
      r2Envelope.launchNonce === binding.nonce &&
      r2Envelope.targetInstanceId === binding.nonce
    )) &&
    envelope.projectId === binding.projectId &&
    envelope.sessionId === binding.sandboxSessionId &&
    envelope.leaderGeneration === binding.leaderGeneration &&
    envelope.buildHash === binding.buildHash &&
    envelope.schemaHash === binding.schemaHash &&
    envelope.instanceId === instanceId &&
    envelope.peerId === "sidecar";
}

interface WorkerMarker {
  protocol: "gp-awareness-v2";
  workerIndex: number;
  buildHash: string;
  createdAtMs: number;
}

function parseWorkerMarker(
  content: string,
  workerIndex: number,
): WorkerMarker | null {
  try {
    const value = JSON.parse(content) as Record<string, unknown>;
    if (!(
      exactKeys(value, ["protocol", "workerIndex", "buildHash", "createdAtMs"]) &&
      value.protocol === "gp-awareness-v2" &&
      value.workerIndex === workerIndex &&
      typeof value.buildHash === "string" &&
      /^[a-f0-9]{64}$/.test(value.buildHash) &&
      Number.isSafeInteger(value.createdAtMs) &&
      (value.createdAtMs as number) >= 0
    )) return null;
    return value as unknown as WorkerMarker;
  } catch {
    return null;
  }
}

function safeReason(value: string): string {
  const normalized = value.replace(/[\r\n\0]/g, " ").trim().slice(0, 256);
  return normalized || "shutdown";
}

function abortable<T>(source: Promise<T>, signal: AbortSignal | undefined): Promise<T> {
  if (!signal) return source;
  if (signal.aborted) return Promise.reject(new DOMException("V2 bootstrap aborted", "AbortError"));
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (operation: () => void) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", onAbort);
      operation();
    };
    const onAbort = () => finish(() => reject(
      signal.reason instanceof Error
        ? signal.reason
        : new DOMException("V2 bootstrap aborted", "AbortError"),
    ));
    signal.addEventListener("abort", onAbort, { once: true });
    source.then(
      value => finish(() => resolve(value)),
      error => finish(() => reject(error)),
    );
  });
}

type DiagnosticStepResult<T> =
  | { readonly completed: true; readonly value: T }
  | { readonly completed: false };

/**
 * Bounds even operations which ignore AbortSignal. Every diagnostic stage is
 * raced against the same monotonic wall-clock deadline, while the operation
 * signal is still aborted to release cooperative gateway work. Late promise
 * settlement is observed by `completed`, so it cannot become unhandled.
 */
async function runDiagnosticStep<T>(
  deadlineMonoMs: number,
  signal: AbortSignal | undefined,
  operation: (signal: AbortSignal) => Promise<T>,
): Promise<DiagnosticStepResult<T>> {
  if (signal?.aborted) {
    throw signal.reason instanceof Error
      ? signal.reason
      : new DOMException("V2 bootstrap aborted", "AbortError");
  }
  const remainingMs = deadlineMonoMs - performance.now();
  if (remainingMs <= 0) return { completed: false };

  const operationController = new AbortController();
  const completed = Promise.resolve()
    .then(() => operation(operationController.signal))
    .then(
      value => ({ kind: "completed" as const, value }),
      () => ({ kind: "failed" as const }),
    );
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<{ readonly kind: "timeout" }>(resolve => {
    timer = setTimeout(() => resolve({ kind: "timeout" }), remainingMs);
  });
  try {
    const outcome = await abortable(Promise.race([completed, timeout]), signal);
    if (outcome.kind !== "completed") return { completed: false };
    return { completed: true, value: outcome.value };
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    operationController.abort(new DOMException(
      "V2 post-hello diagnostic deadline expired",
      "TimeoutError",
    ));
  }
}

export class V2BootstrapController {
  readonly #projectId: ProjectId;
  readonly #workerIndex: number;
  readonly #sandboxSessionId: string;
  readonly #leaderGeneration: number;
  readonly #ySweet: YSweetEnvelope;
  readonly #ydoc: V2BootstrapYDoc;
  readonly #gateway: V2BootstrapGateway;
  readonly #awareness: V2BootstrapAwareness;
  readonly #assets: V2SidecarAssets;
  readonly #selectedRevision: V2SidecarRevision;
  readonly #launchNonce: string | null;
  readonly #attempts: number;
  readonly #timeoutMs: number;
  readonly #pollMs: number;
  readonly #physicalMax: number;
  readonly #acceptedMax: number;
  readonly #initialActivePerSandbox: number;
  readonly #maxActivePerSandbox: number;
  readonly #memoryBudgetBytes: number;
  readonly #diagnostics: { enabled: boolean; intervalMs: number };
  readonly #batching: boolean;
  readonly #clock: Clock;
  readonly #random: RandomSource;
  #ready: V2ReadyDescriptor | null = null;
  #readyBinding: BootstrapBinding | null = null;
  #rotationKey: string | null = null;
  #ensuring: Promise<V2ReadyDescriptor> | null = null;
  #retiring: Promise<void> | null = null;
  #forceRetiring: Promise<void> | null = null;
  readonly #lifecycleAbort = new AbortController();
  #closed = false;
  #launchAttempted = false;
  #lastLaunchNonce: string | null = null;
  #reusableHello: BootstrapHello | null = null;
  #reusableRotationKey: string | null = null;
  readonly #transientArtifactPaths = new Set<string>();
  #awarenessRevision = 0;
  #hostSequence = 0;
  #sidecarAck = { epoch: 1, sequence: -1 };

  constructor(options: V2BootstrapControllerOptions) {
    const attempts = options.attempts ?? 3;
    const timeoutMs = options.timeoutMs ?? 45_000;
    const pollMs = options.pollMs ?? 100;
    const physicalMax = options.physicalMax ?? 128;
    const acceptedMax = options.acceptedMax ?? 256;
    const initialActivePerSandbox = options.initialActivePerSandbox ?? 8;
    const maxActivePerSandbox = options.maxActivePerSandbox ?? V2_TERMINAL_SPOOL_CAPACITY;
    const memoryBudgetBytes = options.memoryBudgetBytes ?? 512 * 1024 * 1024;
    const diagnostics = options.diagnostics ?? { enabled: false, intervalMs: 1_000 };
    const batching = options.batching ?? false;
    if (!Number.isSafeInteger(options.workerIndex) || options.workerIndex < 1) {
      throw new RangeError("workerIndex must be a positive integer");
    }
    if (!SAFE_ID.test(options.sandboxSessionId)) {
      throw new TypeError("sandboxSessionId is invalid");
    }
    if (!Number.isSafeInteger(options.leaderGeneration) || options.leaderGeneration < 1) {
      throw new RangeError("leaderGeneration must be a positive integer");
    }
    if (!Number.isInteger(attempts) || attempts < 1 || attempts > 3) {
      throw new RangeError("attempts must be an integer from 1 through 3");
    }
    if (!Number.isFinite(timeoutMs) || timeoutMs < 1) {
      throw new RangeError("timeoutMs must be positive");
    }
    if (!Number.isFinite(pollMs) || pollMs < 1 || pollMs > timeoutMs) {
      throw new RangeError("pollMs must be positive and no greater than timeoutMs");
    }
    if (!Number.isSafeInteger(physicalMax) || physicalMax < 1 || physicalMax > 128) {
      throw new RangeError("physicalMax must be an integer from 1 through 128");
    }
    if (!Number.isSafeInteger(acceptedMax) || acceptedMax < physicalMax) {
      throw new RangeError("acceptedMax must be at least physicalMax");
    }
    if (
      !Number.isSafeInteger(maxActivePerSandbox) ||
      maxActivePerSandbox < 1 ||
      maxActivePerSandbox > physicalMax ||
      maxActivePerSandbox > V2_TERMINAL_SPOOL_CAPACITY
    ) {
      throw new RangeError(
        `maxActivePerSandbox must be within physical and terminal-spool capacity (${V2_TERMINAL_SPOOL_CAPACITY})`,
      );
    }
    if (
      !Number.isSafeInteger(initialActivePerSandbox) ||
      initialActivePerSandbox < 1 ||
      initialActivePerSandbox > maxActivePerSandbox
    ) throw new RangeError("initialActivePerSandbox must be within the adaptive maximum");
    if (!Number.isSafeInteger(memoryBudgetBytes)) {
      throw new RangeError("memoryBudgetBytes must be a safe integer");
    }
    deriveV2SidecarEventMemorySizing({
      totalBytes: memoryBudgetBytes,
      responseMaxBytes: 64 * 1024 * 1024,
    });
    if (typeof diagnostics.enabled !== "boolean" || !Number.isSafeInteger(diagnostics.intervalMs) || diagnostics.intervalMs < 100 || diagnostics.intervalMs > 60_000) throw new RangeError("diagnostics interval must be 100 through 60000");
    if (options.ySweet.docId !== options.projectId) {
      throw new TypeError("Y-Sweet credential belongs to another project");
    }
    if (options.ySweet.authorization !== "full") {
      throw new TypeError("V2 bootstrap requires full Y-Sweet authorization");
    }
    if (options.awareness.projectId !== options.projectId) {
      throw new TypeError("Awareness channel belongs to another project");
    }
    this.#projectId = options.projectId;
    this.#workerIndex = options.workerIndex;
    this.#sandboxSessionId = options.sandboxSessionId;
    this.#leaderGeneration = options.leaderGeneration;
    this.#ySweet = options.ySweet;
    this.#ydoc = options.ydoc;
    this.#gateway = options.gateway;
    this.#awareness = options.awareness;
    this.#assets = options.assets;
    this.#selectedRevision = resolveV2SidecarRevision(
      options.revision ?? options.assets.revision,
    );
    if (this.#assets.revision !== this.#selectedRevision) {
      throw new TypeError(
        `Selected V2 revision ${this.#selectedRevision} does not match supplied assets ${this.#assets.revision}`,
      );
    }
    if (options.launchNonce !== undefined && !SAFE_ID.test(options.launchNonce)) {
      throw new TypeError("launchNonce is invalid");
    }
    if ((this.#selectedRevision as string) === "r2" && options.launchNonce === undefined) {
      throw new TypeError("R2 bootstrap requires an explicit launchNonce");
    }
    this.#launchNonce = options.launchNonce ?? null;
    this.#attempts = attempts;
    this.#timeoutMs = timeoutMs;
    this.#pollMs = pollMs;
    this.#physicalMax = physicalMax;
    this.#acceptedMax = acceptedMax;
    this.#initialActivePerSandbox = initialActivePerSandbox;
    this.#maxActivePerSandbox = maxActivePerSandbox;
    this.#memoryBudgetBytes = memoryBudgetBytes;
    this.#diagnostics = { ...diagnostics };
    this.#batching = batching;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
  }

  ensureReady(signal?: AbortSignal): Promise<V2ReadyDescriptor> {
    if (signal?.aborted) {
      return Promise.reject(new DOMException("V2 bootstrap aborted", "AbortError"));
    }
    if (this.#closed) {
      return Promise.reject(new V2BootstrapError(
        "V2 bootstrap controller is retired",
        "readiness",
        1,
      ));
    }
    if (this.#ready) return Promise.resolve(this.#ready);
    if (this.#ensuring) return abortable(this.#ensuring, signal);
    const ensuring = this.#ensureReady(this.#lifecycleAbort.signal).finally(() => {
      if (this.#ensuring === ensuring) this.#ensuring = null;
    });
    this.#ensuring = ensuring;
    return abortable(ensuring, signal);
  }

  async sealCredentialRotation(
    ySweet: YSweetEnvelope,
    rotationId: string,
  ): Promise<string> {
    const descriptor = this.#ready;
    const binding = this.#readyBinding;
    const rotationKey = this.#rotationKey;
    if (!descriptor || !binding || !rotationKey) {
      throw new V2BootstrapError("Sidecar is not ready for credential rotation", "readiness", 1);
    }
    const rotationBinding = binding.protocolRevision === "r2"
      ? {
          projectId: binding.projectId,
          sandboxSessionId: binding.sandboxSessionId,
          leaderGeneration: binding.leaderGeneration,
          buildHash: binding.buildHash,
          schemaHash: binding.schemaHash,
          instanceId: descriptor.instanceId,
          rotationId,
          protocolRevision: "r2" as const,
          schemaVersion: GP_V2_R2_SCHEMA_VERSION,
          launchNonce: binding.nonce,
        }
      : {
          projectId: binding.projectId,
          sandboxSessionId: binding.sandboxSessionId,
          leaderGeneration: binding.leaderGeneration,
          buildHash: binding.buildHash,
          schemaHash: binding.schemaHash,
          instanceId: descriptor.instanceId,
          rotationId,
        };
    const sealed = await sealCredentialRotation({
      rotationKey,
      binding: rotationBinding,
      credential: {
        url: ySweet.url,
        baseUrl: ySweet.baseUrl,
        docId: ySweet.docId,
        token: ySweet.token.reveal(),
        authorization: ySweet.authorization,
      },
    });
    const serialized = canonicalJson(sealed);
    if (serialized.length > 10_000) {
      throw new V2BootstrapError("Encrypted credential rotation is too large", "crypto", 1);
    }
    return serialized;
  }

  async ping(signal?: AbortSignal): Promise<number> {
    const descriptor = this.#ready;
    const binding = this.#readyBinding;
    if (!descriptor || !binding) {
      throw new V2BootstrapError("Sidecar is not ready for ping", "readiness", 1);
    }
    const sentAtMs = this.#clock.now();
    const frame = this.#controlFrame({ kind: "ping", sentAtMs });
    const seen: AwarenessEnvelope<SidecarFrame>[] = [];
    const unsubscribe = this.#awareness.subscribe(update => seen.push(update.envelope));
    try {
      this.#publishControl(binding, [frame], false);
      const deadline = this.#clock.now() + this.#timeoutMs;
      for (;;) {
        for (const envelope of seen) {
          if (
            !matchesBoundInstance(envelope, binding, descriptor.instanceId) ||
            envelope.ack.epoch !== frame.epoch ||
            envelope.ack.sequence < frame.sequence
          ) continue;
          const pong = envelope.frames.find(candidate => (
            candidate.kind === "pong" && candidate.sentAtMs === sentAtMs
          ));
          if (!pong) continue;
          this.#sidecarAck = { epoch: pong.epoch, sequence: pong.sequence };
          return Math.max(0, this.#clock.now() - sentAtMs);
        }
        if (signal?.aborted) throw new DOMException("V2 ping aborted", "AbortError");
        if (this.#clock.now() >= deadline) {
          throw new V2BootstrapError("Sidecar ping timed out", "timeout", 1);
        }
        await this.#clock.sleep(Math.min(this.#pollMs, deadline - this.#clock.now()), signal);
      }
    } finally {
      unsubscribe();
    }
  }

  drainAndStop(reason: string, signal?: AbortSignal): Promise<void> {
    return abortable(this.#beginRetirement(false, reason), signal);
  }

  /**
   * Forces the sandbox launcher inert and terminates the exact Deno process
   * group recorded by the bootstrap launcher. This is the fallback when the
   * steady transport cannot prove a clean drain.
   */
  forceStop(reason: string, signal?: AbortSignal): Promise<void> {
    if (!this.#closed) return abortable(this.#beginRetirement(true, reason), signal);
    if (!this.#forceRetiring) {
      const forced = this.#forceRetireAfterClose(reason);
      this.#forceRetiring = forced;
    }
    return abortable(this.#forceRetiring, signal);
  }

  async #ensureReady(signal?: AbortSignal): Promise<V2ReadyDescriptor> {
    try {
      await this.#deployAssets();
      await this.#awareness.connect(signal);
    } catch (cause) {
      if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
      if (cause instanceof V2BootstrapError) throw cause;
      throw new V2BootstrapError("V2 sidecar asset deployment failed", "deploy", 1, { cause });
    }

    let lastError: V2BootstrapError | null = null;
    for (let attempt = 1; attempt <= this.#attempts; attempt++) {
      if (signal?.aborted) throw new DOMException("V2 bootstrap aborted", "AbortError");
      const baseBinding = {
        projectId: this.#projectId,
        sandboxSessionId: this.#sandboxSessionId,
        leaderGeneration: this.#leaderGeneration,
        buildHash: this.#assets.buildHash,
        schemaHash: this.#assets.schemaHash,
        nonce: this.#launchNonce ?? this.#random.uuid(),
      };
      const binding: BootstrapBinding = (this.#selectedRevision as string) === "r2"
        ? {
            ...baseBinding,
            protocolRevision: "r2",
            schemaVersion: GP_V2_R2_SCHEMA_VERSION,
          }
        : baseBinding;
      this.#lastLaunchNonce = binding.nonce;
      this.#rotationKey = null;
      try {
        const descriptor = await this.#attempt(binding, attempt, signal);
        await this.#cleanupTransient(true);
        if (this.#closed || signal?.aborted) {
          throw new DOMException("V2 bootstrap retired", "AbortError");
        }
        this.#ready = descriptor;
        this.#readyBinding = binding;
        return descriptor;
      } catch (cause) {
        await this.#cleanupTransient(false);
        if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
        lastError = cause instanceof V2BootstrapError
          ? cause
          : new V2BootstrapError("V2 bootstrap attempt failed", "deploy", attempt, { cause });
      }
    }
    throw lastError ?? new V2BootstrapError("V2 bootstrap failed", "timeout", this.#attempts);
  }

  async #deployAssets(): Promise<void> {
    await assertV2SidecarAssets(this.#assets, this.#selectedRevision);
    for (const obsoletePath of this.#assets.obsoletePaths) {
      await this.#ydoc.tombstonePath(obsoletePath);
    }
    for (const asset of [...this.#assets.files, this.#assets.manifest]) {
      const existing = await this.#ydoc.readPath(asset.path);
      if (existing === null) {
        await this.#ydoc.writePath(asset.path, asset.content);
      } else if (existing !== asset.content) {
        throw new V2BootstrapError(
          `Immutable V2 sidecar asset collision: ${asset.path}`,
          "deploy",
          1,
        );
      }
      if (await this.#ydoc.readPath(asset.path) !== asset.content) {
        throw new V2BootstrapError(
          `V2 sidecar asset did not persist exactly: ${asset.path}`,
          "deploy",
          1,
        );
      }
    }
    const selectedRevision = canonicalJson({
      protocol: "gp-awareness-v2",
      version: 1,
      revision: this.#selectedRevision,
      buildHash: this.#assets.buildHash,
      schemaHash: this.#assets.schemaHash,
      manifestPath: this.#assets.manifest.path,
      entrypointPath: this.#assets.entrypointPath,
    });
    if (await this.#ydoc.readPath(REVISION_PATH) !== selectedRevision) {
      await this.#ydoc.writePath(REVISION_PATH, selectedRevision);
    }
    if (await this.#ydoc.readPath(REVISION_PATH) !== selectedRevision) {
      throw new V2BootstrapError(
        "Selected V2 sidecar revision did not persist exactly",
        "deploy",
        1,
      );
    }
    const existingMarker = await this.#ydoc.readPath(WORKER_PATH);
    if (existingMarker === null) {
      await this.#ydoc.writePath(WORKER_PATH, canonicalJson({
        protocol: "gp-awareness-v2",
        workerIndex: this.#workerIndex,
        buildHash: this.#assets.buildHash,
        createdAtMs: Math.floor(this.#clock.now()),
      }));
    } else {
      const marker = parseWorkerMarker(existingMarker, this.#workerIndex);
      if (!marker) {
        throw new V2BootstrapError("Existing GhostProv worker marker does not match", "deploy", 1);
      }
      if (marker.buildHash !== this.#assets.buildHash) {
        await this.#ydoc.writePath(WORKER_PATH, canonicalJson({
          ...marker,
          buildHash: this.#assets.buildHash,
        }));
      }
    }
    if (await this.#ydoc.readPath(MAIN_PATH) !== MAIN_SOURCE) {
      await this.#ydoc.writePath(MAIN_PATH, MAIN_SOURCE);
    }
  }

  async #attempt(
    binding: BootstrapBinding,
    attempt: number,
    signal?: AbortSignal,
  ): Promise<V2ReadyDescriptor> {
    const artifacts = bootstrapArtifactPaths(binding.nonce);
    this.#transientArtifactPaths.add(artifacts.outputPath);
    const staticConfig = canonicalJson({
      version: 1,
      binding,
      physicalMax: this.#physicalMax,
      acceptedMax: this.#acceptedMax,
      initialActivePerSandbox: this.#initialActivePerSandbox,
      maxActivePerSandbox: this.#maxActivePerSandbox,
      memoryBudgetBytes: this.#memoryBudgetBytes,
      diagnostics: this.#diagnostics,
      batching: this.#batching,
    });
    const launcher = temporaryLauncher(
      this.#assets.entrypointPath,
      binding.nonce,
      this.#selectedRevision,
      staticConfig,
    );
    const mainSource = bootstrapMainSource(artifacts.outputPath);
    try {
      await this.#ydoc.writePath(STATIC_PATH, staticConfig);
      await this.#ydoc.writePath(LAUNCHER_PATH, launcher);
      await this.#ydoc.writePath(MAIN_PATH, mainSource);
      await this.#gateway.syncWorkspace(signal);
    } catch (cause) {
      throw new V2BootstrapError("Unable to stage V2 bootstrap files", "deploy", attempt, { cause });
    }

    try {
      this.#launchAttempted = true;
      await this.#gateway.triggerRender({
        mainDocument: MAIN_PATH,
        ...await this.#ydoc.renderSyncState(),
      }, signal);
    } catch (cause) {
      throw new V2BootstrapError("Unable to trigger V2 sidecar render", "render", attempt, { cause });
    }

    const hello = await this.#waitForHello(
      binding,
      attempt,
      staticConfig,
      launcher,
      mainSource,
      signal,
    );
    const rotationKey = this.#reusableRotationKey ?? createCredentialRotationKey();
    this.#reusableRotationKey = rotationKey;
    let sealed: SealedBootstrapCredential;
    try {
      sealed = await sealBootstrapCredential({
        binding,
        sidecarPublicJwk: hello.publicJwk,
        credential: {
          url: this.#ySweet.url,
          baseUrl: this.#ySweet.baseUrl,
          docId: this.#ySweet.docId,
          token: this.#ySweet.token.reveal(),
          authorization: this.#ySweet.authorization,
          rotationKey,
        },
      });
    } catch (cause) {
      throw new V2BootstrapError("Unable to encrypt V2 bootstrap credential", "crypto", attempt, { cause });
    }

    const readiness: AwarenessEnvelope<SidecarFrame>[] = [];
    const unsubscribe = this.#awareness.subscribe(update => readiness.push(update.envelope));
    try {
      await this.#ydoc.writePath(CIPHERTEXT_PATH, canonicalJson(sealed));
      await this.#gateway.syncWorkspace(signal);
      const descriptor = await this.#waitForReadiness(
        binding,
        hello.instanceId,
        readiness,
        attempt,
        MAIN_PATH,
        signal,
      );
      this.#rotationKey = rotationKey;
      return descriptor;
    } catch (cause) {
      if (cause instanceof V2BootstrapError) throw cause;
      if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
      throw new V2BootstrapError("Unable to publish V2 bootstrap ciphertext", "deploy", attempt, { cause });
    } finally {
      unsubscribe();
    }
  }

  async #waitForHello(
    binding: BootstrapBinding,
    attempt: number,
    expectedStatic: string,
    expectedLauncher: string,
    expectedMain: string,
    signal?: AbortSignal,
  ): Promise<BootstrapHello> {
    const deadline = this.#clock.now() + this.#timeoutMs;
    let nextAuthorityCheckMs = this.#clock.now();
    let lastError: V2BootstrapError | null = null;
    for (;;) {
      if (signal?.aborted) throw new DOMException("V2 bootstrap aborted", "AbortError");
      let response: Awaited<ReturnType<V2BootstrapGateway["fetchText"]>>;
      try {
        response = await this.#gateway.fetchText("logs", { method: "GET", signal });
      } catch (cause) {
        lastError = new V2BootstrapError("Unable to read bootstrap render logs", "render", attempt, { cause });
        response = { status: 0, text: "", headers: new Headers() };
      }
      if (response.status >= 200 && response.status < 300) {
        const log = renderLogText(response.text);
        const hello = extractWrappedMarker(log, HELLO_PREFIX, /^[A-Za-z0-9_-]+/, 8_192);
        if (hello) {
          try {
            const parsed = parseHelloCandidate(hello, binding, attempt);
            if (
              this.#reusableHello &&
              canonicalJson(this.#reusableHello) !== canonicalJson(parsed)
            ) this.#reusableRotationKey = null;
            this.#reusableHello = parsed;
            return parsed;
          } catch (cause) {
            lastError = cause as V2BootstrapError;
          }
        }
        const reusedOwner = extractWrappedMarker(
          log,
          OWNER_REUSED_PREFIX,
          /^[A-Za-z0-9._:-]+/,
          128,
        );
        if (reusedOwner) {
          const cached = this.#reusableHello;
          if (
            reusedOwner === binding.nonce &&
            cached &&
            canonicalJson(cached.binding) === canonicalJson(binding)
          ) return cached;
          throw new V2BootstrapError(
            reusedOwner === binding.nonce
              ? "Already-running V2 sidecar cannot be adopted without its cached bootstrap hello"
              : "Already-running V2 sidecar reuse marker is stale",
            "hello",
            attempt,
          );
        }
        const diagnostic = extractWrappedMarker(log, SIDECAR_HEX_PREFIX, /^[0-9a-fA-F]+/, 8_192);
        if (diagnostic) {
          const decoded = decodeSidecarDiagnostic(diagnostic);
          throw new V2BootstrapError(
            `V2 sidecar failed before bootstrap hello: ${decoded.message}`,
            "hello",
            attempt,
            decoded.fatal ? { sidecarDiagnostic: decoded.fatal } : undefined,
          );
        }
      }
      const now = this.#clock.now();
      if (now >= nextAuthorityCheckMs) {
        const [currentStatic, currentLauncher, currentMain] = await Promise.all([
          this.#ydoc.readPath(STATIC_PATH),
          this.#ydoc.readPath(LAUNCHER_PATH),
          this.#ydoc.readPath(MAIN_PATH),
        ]);
        if (
          currentStatic !== expectedStatic ||
          currentLauncher !== expectedLauncher ||
          currentMain !== expectedMain
        ) {
          throw new V2BootstrapError(
            "V2 bootstrap workspace authority was replaced by another generation",
            "deploy",
            attempt,
          );
        }
        nextAuthorityCheckMs = now + Math.min(1_000, this.#timeoutMs);
      }
      if (this.#clock.now() >= deadline) {
        throw lastError ?? new V2BootstrapError(
          "Timed out waiting for the V2 bootstrap hello",
          "timeout",
          attempt,
        );
      }
      await this.#clock.sleep(Math.min(this.#pollMs, deadline - this.#clock.now()), signal);
    }
  }

  async #waitForReadiness(
    binding: BootstrapBinding,
    instanceId: string,
    readiness: readonly AwarenessEnvelope<SidecarFrame>[],
    attempt: number,
    mainPath: string,
    signal?: AbortSignal,
  ): Promise<V2ReadyDescriptor> {
    const deadline = this.#clock.now() + this.#timeoutMs;
    // Reserve a bounded tail of the existing readiness budget for one
    // best-effort render of sidecar.log. Diagnostics never extend the attempt.
    const diagnosticWindowMs = Math.min(5_000, Math.max(1, this.#timeoutMs / 4));
    const observationDeadline = deadline - diagnosticWindowMs;
    const diagnosticBudgetMs = Math.max(1, diagnosticWindowMs * 0.8);
    for (;;) {
      const envelope = readiness.find(candidate => matchesReadiness(candidate, binding, instanceId));
      if (envelope) {
        return {
          instanceId,
          protocolRevision: this.#selectedRevision,
          launchNonce: binding.nonce,
          buildHash: binding.buildHash,
          schemaHash: binding.schemaHash,
          leaderGeneration: binding.leaderGeneration,
          capacity: this.#maxActivePerSandbox,
        };
      }
      if (signal?.aborted) throw new DOMException("V2 bootstrap aborted", "AbortError");
      if (this.#clock.now() >= observationDeadline) {
        const diagnostic = await this.#capturePostHelloDiagnostic(
          diagnosticBudgetMs,
          mainPath,
          signal,
        );
        const lateEnvelope = readiness.find(candidate => (
          matchesReadiness(candidate, binding, instanceId)
        ));
        if (lateEnvelope) {
          return {
            instanceId,
            protocolRevision: this.#selectedRevision,
            launchNonce: binding.nonce,
            buildHash: binding.buildHash,
            schemaHash: binding.schemaHash,
            leaderGeneration: binding.leaderGeneration,
            capacity: this.#maxActivePerSandbox,
          };
        }
        throw new V2BootstrapError(
          diagnostic
            ? `Sidecar readiness failed after hello: ${diagnostic.message}`
            : "Sidecar readiness did not match the bootstrap binding and instance",
          "readiness",
          attempt,
          diagnostic?.fatal ? { sidecarDiagnostic: diagnostic.fatal } : undefined,
        );
      }
      await this.#clock.sleep(
        Math.min(this.#pollMs, observationDeadline - this.#clock.now()),
        signal,
      );
    }
  }

  async #capturePostHelloDiagnostic(
    budgetMs: number,
    mainPath: string,
    signal?: AbortSignal,
  ): Promise<DecodedSidecarDiagnostic | null> {
    const deadlineMonoMs = performance.now() + budgetMs;
    try {
      // Re-render the already staged, binding-fenced temporary launcher. Its
      // same-owner/same-attempt branches emit a bounded log tail without a
      // second fork or signalling/replacing the owner. Avoiding a diagnostic
      // write/sync is critical:
      // a signal-unaware late YDoc write could otherwise overwrite the inert
      // launcher or a newer generation after this deadline has returned.
      const syncState = await runDiagnosticStep(deadlineMonoMs, signal, () => (
        this.#ydoc.renderSyncState()
      ));
      if (!syncState.completed) return null;
      const render = await runDiagnosticStep(deadlineMonoMs, signal, stepSignal => (
        this.#gateway.triggerRender({
          mainDocument: mainPath,
          ...syncState.value,
        }, stepSignal)
      ));
      if (!render.completed) return null;
      for (;;) {
        if (signal?.aborted) throw new DOMException("V2 bootstrap aborted", "AbortError");
        const fetched = await runDiagnosticStep(deadlineMonoMs, signal, stepSignal => (
          this.#gateway.fetchText("logs", { method: "GET", signal: stepSignal })
        ));
        if (!fetched.completed) return null;
        const response = fetched.value;
        if (response.status >= 200 && response.status < 300) {
          const diagnostic = extractWrappedMarker(
            renderLogText(response.text),
            SIDECAR_HEX_PREFIX,
            /^[0-9a-fA-F]+/,
            8_192,
          );
          if (diagnostic) return decodeSidecarDiagnostic(diagnostic);
        }
        const remainingMs = deadlineMonoMs - performance.now();
        if (remainingMs <= 0) return null;
        const slept = await runDiagnosticStep(deadlineMonoMs, signal, stepSignal => (
          this.#clock.sleep(Math.min(this.#pollMs, remainingMs), stepSignal)
        ));
        if (!slept.completed) return null;
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") throw error;
      return null;
    }
  }

  #beginRetirement(force: boolean, reason: string): Promise<void> {
    if (this.#retiring) return this.#retiring;
    if (this.#closed) return Promise.resolve();
    this.#closed = true;
    this.#lifecycleAbort.abort(new DOMException("V2 bootstrap retired", "AbortError"));
    const descriptor = this.#ready;
    const binding = this.#readyBinding;
    this.#ready = null;
    this.#readyBinding = null;
    this.#rotationKey = null;
    this.#reusableHello = null;
    this.#reusableRotationKey = null;
    const ensuring = this.#ensuring;
    const retiring = this.#retire(
      force,
      safeReason(reason),
      descriptor,
      binding,
      this.#launchAttempted,
    );
    this.#retiring = retiring;
    if (force) this.#forceRetiring = retiring;
    // An abort-ignoring bootstrap attempt is fenced by #closed before it can
    // publish readiness. Re-apply inert cleanup after its eventual settlement.
    if (ensuring) {
      void ensuring.finally(() => this.#cleanupTransient(false)).catch(() => undefined);
    }
    return retiring;
  }

  #forceRetireAfterClose(reason: string): Promise<void> {
    return this.#retire(
      true,
      safeReason(reason),
      null,
      null,
      this.#launchAttempted,
    );
  }

  async #retire(
    force: boolean,
    reason: string,
    descriptor: V2ReadyDescriptor | null,
    binding: BootstrapBinding | null,
    mayHaveLaunched: boolean,
  ): Promise<void> {
    let unsubscribe: (() => void) | null = null;
    let failure: unknown = null;
    try {
      if (force && mayHaveLaunched) {
        const launchNonce = binding?.nonce ?? this.#lastLaunchNonce;
        if (!launchNonce) {
          throw new V2BootstrapError(
            "Unable to identify the V2 sidecar launch for termination",
            "readiness",
            1,
          );
        }
        await this.#forceTerminateSidecar(launchNonce);
      } else if (descriptor && binding) {
        const frame = this.#controlFrame({ kind: "drain", reason });
        const seen: AwarenessEnvelope<SidecarFrame>[] = [];
        unsubscribe = this.#awareness.subscribe(update => seen.push(update.envelope));
        this.#publishControl(binding, [frame], true);
        const deadline = this.#clock.now() + this.#timeoutMs;
        for (;;) {
          const drained = seen.find(envelope => (
            matchesBoundInstance(envelope, binding, descriptor.instanceId) &&
            envelope.ack.epoch === frame.epoch &&
            envelope.ack.sequence >= frame.sequence &&
            envelope.frames.some(candidate => (
              candidate.kind === "drained" && candidate.active === 0 && candidate.queued === 0
            ))
          ));
          if (drained) break;
          if (this.#clock.now() >= deadline) {
            throw new V2BootstrapError("Sidecar did not prove drain completion", "timeout", 1);
          }
          await this.#clock.sleep(Math.min(this.#pollMs, deadline - this.#clock.now()));
        }
      }
    } catch (error) {
      failure = error;
    } finally {
      unsubscribe?.();
      this.#awareness.clear();
      try {
        await this.#cleanupTransient(true);
      } catch (cleanupError) {
        failure = failure === null
          ? cleanupError
          : new AggregateError([failure, cleanupError], "V2 sidecar retirement failed");
      }
    }
    if (failure !== null) throw failure;
  }

  async #forceTerminateSidecar(launchNonce: string): Promise<void> {
    try {
      await this.#ydoc.writePath(
        LAUNCHER_PATH,
        terminationLauncher(this.#assets.entrypointPath, launchNonce),
      );
      await this.#gateway.syncWorkspace();
      await this.#gateway.triggerRender({
        mainDocument: MAIN_PATH,
        ...await this.#ydoc.renderSyncState(),
      });
    } catch (cause) {
      throw new V2BootstrapError(
        "Unable to terminate the V2 sidecar process",
        "render",
        1,
        { cause },
      );
    }
  }

  async #cleanupTransient(required: boolean, signal?: AbortSignal): Promise<void> {
    try {
      await this.#ydoc.tombstonePath(CIPHERTEXT_PATH);
      for (const path of this.#transientArtifactPaths) {
        await this.#ydoc.tombstonePath(path);
      }
      await this.#ydoc.writePath(LAUNCHER_PATH, INERT_LAUNCHER);
      await this.#gateway.syncWorkspace(signal);
    } catch (cause) {
      if (required) {
        throw new V2BootstrapError("Unable to restore the inert V2 launcher", "deploy", 1, { cause });
      }
    }
  }

  #controlFrame(frame: HostFrame): ReliableFrame<HostFrame> {
    return {
      ...frame,
      epoch: 1,
      sequence: this.#hostSequence++,
    } as ReliableFrame<HostFrame>;
  }

  #publishControl(
    binding: BootstrapBinding,
    frames: readonly ReliableFrame<HostFrame>[],
    draining: boolean,
  ): void {
    const common = {
      protocol: "gp-awareness-v2",
      schemaHash: binding.schemaHash,
      buildHash: binding.buildHash,
      projectId: this.#projectId,
      sessionId: binding.sandboxSessionId,
      peerId: "host-bootstrap",
      instanceId: binding.nonce,
      leaderGeneration: binding.leaderGeneration,
      epoch: 1,
      revision: this.#awarenessRevision++,
      ack: this.#sidecarAck,
      credits: 0,
      active: 0,
      queued: 0,
      draining,
      frames,
    } as const;
    this.#awareness.publish(binding.protocolRevision === "r2"
      ? {
          ...common,
          protocolRevision: "r2",
          schemaVersion: GP_V2_R2_SCHEMA_VERSION,
          launchNonce: binding.nonce,
          targetInstanceId: this.#ready?.instanceId ?? "sidecar-unbound",
        }
      : {
          ...common,
          schemaVersion: GP_V2_R1_SCHEMA_VERSION,
        });
  }
}
