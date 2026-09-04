/**
 * Sandbox-relay launch assets (R9).
 *
 * The Prism render VM no longer runs sidecars, so the fleet stages these
 * POSIX shell scripts into the workspace via the Y-doc data plane and
 * executes them inside the sandbox container (via the platform exec API or
 * the Codex agent). The sidecar bundle itself is unchanged on the wire:
 * these scripts only reproduce, in `sh`, what the render-VM `.latexmkrc`
 * launcher used to do in Perl.
 *
 * Pure policy module: no I/O, clocks, or environment reads. All scripts are
 * bounded, single-quoted where interpolated, and refuse unsafe inputs.
 */

const SAFE_NONCE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SAFE_ENTRYPOINT = /^[A-Za-z0-9.][A-Za-z0-9._/-]{0,255}$/;

/** Workspace-relative staging paths for the relay assets. */
export const RELAY_WATCHER_PATH = ".ghostprov-v2/relay/watcher.sh" as const;
export const RELAY_LAUNCH_PATH = ".ghostprov-v2/relay/bootstrap-launch.sh" as const;
export const RELAY_SIDECAR_LOG = "/tmp/ghostprov-relay/sidecar.log" as const;
/** Workspace-relative hello read-back file, suffixed per launch nonce. */
export const RELAY_HELLO_PREFIX = ".ghostprov-v2/relay/hello." as const;
export const RELAY_HELLO_SUFFIX = ".txt" as const;

/** Upper bound for any single generated relay script. */
export const MAX_RELAY_SCRIPT_BYTES = 8_192;
/** Upper bound for a hello read-back file the render printer will emit. */
export const MAX_RELAY_HELLO_BYTES = 8_192;

export interface RelaySidecarLaunchOptions {
  /** Immutable workspace-relative bundle path, e.g. from `V2SidecarAssets`. */
  readonly entrypointPath: string;
  /** Exact launch nonce bound to this bootstrap attempt. */
  readonly nonce: string;
}

function shSingleQuoted(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function assertRelayNonce(nonce: unknown): asserts nonce is string {
  if (typeof nonce !== "string" || !SAFE_NONCE.test(nonce)) {
    throw new TypeError("Relay launch nonce is invalid");
  }
}

/** Workspace-relative path of the hello read-back file for one launch nonce. */
export function relayHelloPath(nonce: string): string {
  assertRelayNonce(nonce);
  return `${RELAY_HELLO_PREFIX}${nonce}${RELAY_HELLO_SUFFIX}`;
}

function assertRelayLaunchOptions(options: RelaySidecarLaunchOptions): void {
  if (!options || typeof options !== "object") throw new TypeError("Relay launch options are invalid");
  if (typeof options.entrypointPath !== "string" || !SAFE_ENTRYPOINT.test(options.entrypointPath)) {
    throw new TypeError("Relay launch entrypoint path is invalid");
  }
  if (options.entrypointPath.includes("..")) {
    throw new TypeError("Relay launch entrypoint path must not traverse");
  }
  assertRelayNonce(options.nonce);
}

/**
 * Generates the sandbox-container sidecar launcher executed by the relay
 * watcher (or directly through the platform exec API). It forks the R9
 * bundle under Deno with the exact capability allow-list, publishes the
 * candidate PID record the sidecar promotes before its hello, then prints
 * the hello line for the host to seal against.
 */
export function relaySidecarLaunchScript(options: RelaySidecarLaunchOptions): string {
  assertRelayLaunchOptions(options);
  const entrypoint = shSingleQuoted(`/workspace/${options.entrypointPath}`);
  const nonce = shSingleQuoted(options.nonce);
  const helloCopy = shSingleQuoted(`/workspace/${relayHelloPath(options.nonce)}`);
  const script = [
    "#!/bin/sh",
    "# GhostProv R9 sandbox-relay sidecar launcher (generated, do not hand-edit)",
    "set -u",
    "STATE=/tmp/ghostprov-relay",
    `EP=${entrypoint}`,
    `NONCE=${nonce}`,
    "START=$(date -u +%s)",
    `HELLO_COPY=${helloCopy}`,
    'mkdir -p "$STATE" /tmp/ghostprov-v2',
    "DENO=${DENO_BIN:-}",
    'if [ -z "$DENO" ]; then',
    "  for candidate in /usr/local/bin/deno /usr/bin/deno /root/.deno/bin/deno; do",
    '    if [ -x "$candidate" ]; then DENO="$candidate"; break; fi',
    "  done",
    "fi",
    'if [ -z "$DENO" ]; then DENO=$(command -v deno 2>/dev/null || true); fi',
    'if [ -z "$DENO" ] || [ ! -x "$DENO" ]; then',
    '  echo "relay: deno executable not found" >> "$STATE/sidecar.log"',
    "  exit 1",
    "fi",
    "if pgrep -f 'sidecar.bundle.js' >/dev/null 2>&1; then",
    '  echo "relay: sidecar already running" >> "$STATE/sidecar.log"',
    "  exit 0",
    "fi",
    // Detach the sidecar into its own session: the launcher itself runs inside
    // the platform exec turn, whose process group is reaped when the turn ends.
    // Bare nohup does not survive that; without setsid the sidecar dies
    // silently seconds after publishing its hello (proven live).
    "setsid nohup \"$DENO\" run --cached-only --cert=\"${PROXY_CA_CERT_PATH}\" \\",
    "  --allow-env=OPENAI_BASE_URL,OPENAI_API_KEY,PROXY_CA_CERT_PATH,NODE_ENV,PRODUCTION,FORCE_COLOR,NO_COLOR,COLORTERM,TERM \\",
    "  --allow-net --allow-read=/workspace/.ghostprov-v2,/tmp/ghostprov-v2 --allow-write=/tmp/ghostprov-v2 \\",
    "  \"$EP\" --ghostprov-launch-nonce=\"$NONCE\" --ghostprov-process-start=\"$START\" \\",
    `  >> ${shSingleQuoted(RELAY_SIDECAR_LOG)} 2>&1 &`,
    "child=$!",
    'echo "$(date -u +%FT%TZ) sidecar forked pid $child" >> "$STATE/sidecar.log"',
    "i=0",
    "while [ $i -lt 40 ]; do",
    '  if [ -d "/proc/$child" ]; then',
    '    printf "%s %s %s\\n" "$child" "$NONCE" "$START" > "/tmp/ghostprov-v2/sidecar.pid.$NONCE.$child.candidate" 2>/dev/null && break',
    "  fi",
    "  i=$((i + 1)); sleep 0.05",
    "done",
    "j=0",
    "while [ $j -lt 300 ]; do",
    '  hello=$(ls /tmp/ghostprov-v2/bootstrap-hello."$NONCE".* 2>/dev/null | head -1)',
    '  if [ -n "$hello" ]; then',
    '    cp "$hello" "$HELLO_COPY" 2>/dev/null',
    "    echo HELLO-BEGIN",
    '    cat "$hello"',
    "    echo HELLO-END",
    "    exit 0",
    "  fi",
    "  j=$((j + 1)); sleep 0.1",
    "done",
    'echo "relay: hello timeout" >> "$STATE/sidecar.log"',
    "exit 1",
    "",
  ].join("\n");
  if (script.length > MAX_RELAY_SCRIPT_BYTES) {
    throw new RangeError("Relay launch script exceeds its byte bound");
  }
  return script;
}

/**
 * Generates the relay watcher: polls the staged launch payload every 200 ms
 * and executes a new-hash payload exactly once (persistent `executed.sha`
 * guard plus `watcher-version` fencing, as proven in the PoC).
 */
export function relayWatcherScript(): string {
  const script = [
    "#!/bin/sh",
    "# GhostProv R9 relay watcher (generated, do not hand-edit)",
    "set -u",
    "STATE=/tmp/ghostprov-relay",
    "RELAY=/workspace/.ghostprov-v2/relay",
    'mkdir -p "$STATE"',
    'LOG="$STATE/watcher.log"',
    'echo "$(date -u +%FT%TZ) watcher started (pid $$)" >> "$LOG"',
    'echo relay-v1 > "$STATE/watcher-version"',
    'last=""',
    "while :; do",
    '  trigger="$RELAY/bootstrap-launch.sh"',
    '  if [ -f "$trigger" ]; then',
    '    sig=$(md5sum "$trigger" 2>/dev/null | cut -d" " -f1)',
    '    done_sig=$(cat "$STATE/executed.sha" 2>/dev/null || true)',
    '    if [ -n "$sig" ] && [ "$sig" != "$last" ] && [ "$sig" != "$done_sig" ]; then',
    '      echo "$(date -u +%FT%TZ) payload detected ($sig)" >> "$LOG"',
    '      last="$sig"',
    '      echo "$sig" > "$STATE/executed.sha"',
    '      cp "$trigger" "$STATE/bootstrap-launch.current.sh" 2>/dev/null',
    '      echo "$(date -u +%FT%TZ) executing relocated payload" >> "$LOG"',
    '      setsid sh "$STATE/bootstrap-launch.current.sh" >> "$STATE/launch.log" 2>&1',
    "    fi",
    "  fi",
    "  sleep 0.2",
    "done",
    "",
  ].join("\n");
  if (script.length > MAX_RELAY_SCRIPT_BYTES) {
    throw new RangeError("Relay watcher script exceeds its byte bound");
  }
  return script;
}

function perlSingleQuotedRelay(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

/**
 * Nonce alphabet safe inside a TeX file-name scan. UUID nonces satisfy this;
 * the wider relay alphabet (`_`, `:`) does not survive TeX catcodes.
 */
const SAFE_TEX_NONCE = /^[A-Za-z0-9][A-Za-z0-9.-]{0,127}$/;

function assertTexNonce(nonce: unknown): asserts nonce is string {
  if (!isRelayMainNonce(nonce)) {
    throw new TypeError("Relay TeX nonce is invalid");
  }
}

/** Whether a launch nonce survives a TeX file-name scan (UUIDs qualify). */
export function isRelayMainNonce(nonce: unknown): nonce is string {
  return typeof nonce === "string" && SAFE_TEX_NONCE.test(nonce);
}

const EXEC_HELLO_PREFIX = "GPV2_BOOTSTRAP ";
const EXEC_HELLO_MARKERS = new Set(["HELLO-BEGIN", "HELLO-END"]);
const EXEC_HELLO_ALPHABET = /^[A-Za-z0-9_-]+$/;
const MAX_EXEC_HELLO_CHARS = 8_192;

/**
 * Extracts the sidecar hello token from exec-channel output (the launcher's
 * printed `HELLO-BEGIN … HELLO-END` block, possibly wrapped by the agent).
 * Unlike the render-log extractor, launcher marker lines are skipped instead
 * of consumed (`HELLO-END` is valid base64 alphabet and would otherwise be
 * appended to the token), and extraction stops at the first line that is
 * neither a full base64 run nor a marker. Returns null when no candidate
 * exists. Callers must still validate the token (parseHelloCandidate).
 */
export function extractRelayExecHello(output: string): string | null {
  if (typeof output !== "string" || output.length === 0) return null;
  const lines = output.slice(0, MAX_EXEC_HELLO_CHARS * 2).split(/\r?\n/);
  let token: string | null = null;
  for (const raw of lines) {
    const line = raw.trim();
    if (token === null) {
      if (!line.startsWith(EXEC_HELLO_PREFIX)) continue;
      const rest = line.slice(EXEC_HELLO_PREFIX.length);
      if (!rest || !EXEC_HELLO_ALPHABET.test(rest)) continue;
      token = rest;
      continue;
    }
    if (EXEC_HELLO_MARKERS.has(line)) continue;
    if (!line || !EXEC_HELLO_ALPHABET.test(line)) break;
    token += line;
    if (token.length > MAX_EXEC_HELLO_CHARS) return null;
  }
  return token && token.length <= MAX_EXEC_HELLO_CHARS ? token : null;
}

/**
 * Generates the relay-mode `main.tex`. The render compiles this document, so
 * its `\typeout` lines land in `pdfTexLog` by construction — a second
 * read-back channel beside the `.latexmkrc` printer. When the hello file has
 * not reached the workspace disk yet, the document reports
 * `RELAY-HELLO-MISSING`, which distinguishes "render compiled without hello"
 * from "render never compiled".
 */
export function relayMainSource(nonce: string): string {
  assertTexNonce(nonce);
  const script = [
    "\\documentclass{article}",
    "\\begin{document}",
    "GhostProv V2 relay bootstrap.",
    "\\newread\\gphello",
    `\\openin\\gphello=.ghostprov-v2/relay/hello.${nonce}.txt`,
    "\\ifeof\\gphello",
    "\\typeout{RELAY-HELLO-MISSING}",
    "\\else",
    "\\endlinechar=-1",
    "\\read\\gphello to\\gpline",
    "\\typeout{\\gpline}",
    "\\loop\\unless\\ifeof\\gphello",
    "\\read\\gphello to\\gpline",
    "\\typeout{\\gpline}",
    "\\repeat",
    "\\fi",
    "\\closein\\gphello",
    "\\end{document}",
    "",
  ].join("\n");
  if (script.length > MAX_RELAY_SCRIPT_BYTES) {
    throw new RangeError("Relay main source exceeds its byte bound");
  }
  return script;
}

/**
 * Generates the render-VM `.latexmkrc` for relay mode. The render VM cannot
 * fork the sidecar (no upstream environment) and cannot reach the sandbox
 * container (isolated network namespace, deny-by-default egress), so this
 * printer does the one thing renders still can: report the hello read-back
 * file the sandbox-side launcher copied into `/workspace` into the render
 * log, where the host's existing hello extraction picks it up.
 */
export function relayHelloPrinterLatexmkrc(nonce: string): string {
  assertRelayNonce(nonce);
  const helloPath = perlSingleQuotedRelay(`/workspace/${relayHelloPath(nonce)}`);
  const script = [
    "# GhostProv R9 relay hello printer (generated, do not hand-edit)",
    "$pdf_mode = 1;",
    `my $gp_hello_path = ${helloPath};`,
    "my $gp_hello = '';",
    "if (open my $gp_fh, '<', $gp_hello_path) {",
    "  my $gp_size = -s $gp_fh;",
    `  if (defined($gp_size) && $gp_size > 0 && $gp_size <= ${MAX_RELAY_HELLO_BYTES}) {`,
    "    local $/;",
    "    my $gp_raw = <$gp_fh>;",
    "    if (defined($gp_raw)) { $gp_hello = $gp_raw; }",
    "  }",
    "  close $gp_fh;",
    "}",
    "if ($gp_hello ne '') {",
    "  print \"RELAY-HELLO-BEGIN\\n\";",
    "  print $gp_hello;",
    "  print \"\\nRELAY-HELLO-END\\n\";",
    "}",
    "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
    "1;",
    "",
  ].join("\n");
  if (script.length > MAX_RELAY_SCRIPT_BYTES) {
    throw new RangeError("Relay hello printer exceeds its byte bound");
  }
  return script;
}
