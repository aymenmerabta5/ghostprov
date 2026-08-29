$ErrorActionPreference = 'Stop'

function Deny-ProtectedPortCommand {
    param([string]$Reason)

    @{
        hookSpecificOutput = @{
            hookEventName = 'PreToolUse'
            permissionDecision = 'deny'
            permissionDecisionReason = $Reason
        }
    } | ConvertTo-Json -Compress -Depth 4
    exit 0
}

$protectedReason = 'Blocked: port 18787 is the GhostProv production gateway. Production startup is allowed, but lifecycle, replacement, kill, and unapproved live commands remain denied.'
$raw = [Console]::In.ReadToEnd()
try {
    $payload = $raw | ConvertFrom-Json
} catch {
    Deny-ProtectedPortCommand 'Blocked: malformed shell-tool input cannot be proven safe for protected port 18787.'
}

if ($payload.tool_name -ne 'Bash' -and $payload.tool_name -ne 'PowerShell') {
    Deny-ProtectedPortCommand 'Blocked: unexpected tool input cannot be proven safe for protected port 18787.'
}
$command = [string]$payload.tool_input.command
if ([string]::IsNullOrWhiteSpace($command) -or $command.Length -gt 32768) {
    Deny-ProtectedPortCommand 'Blocked: missing or oversized shell command cannot be proven safe for protected port 18787.'
}
$command = $command.Trim()

# No command composition, redirection, interpolation, subshells, encoded payloads,
# or multiline scripts are part of the allowlisted grammar.
if (
    $command -match '[\x00-\x1f]' -or
    $command -match '[;&|<>`]' -or
    $command -match '\$\(' -or
    $command -match '\$\{' -or
    $command -match '(?i)(?:encodedcommand|frombase64string|invoke-expression|\biex\b)'
) {
    Deny-ProtectedPortCommand $protectedReason
}

$offlineRun = '(?:typecheck|test:unit|test:integration|build:v2-sidecar-release|check:v2-sidecar|check:v2-sidecar-release|check:v2-sidecar-deno|test:v2-deno-fs|test:v2-deno-upstream|benchmark:v2-research)'
$allowed = @(
    '^curl(?:\.exe)?\s+(?:--fail\s+)?(?:--silent\s+)?(?:--show-error\s+)?http://(?:127\.0\.0\.1|localhost):18888/health$',
    '^git(?:\.exe)?\s+status(?:\s+(?:--short|--porcelain(?:=v1)?|--branch))*$',
    '^git(?:\.exe)?\s+diff(?:\s+(?:--check|--stat|--name-only|--numstat|--cached|--staged))*$',
    '^git(?:\.exe)?\s+diff(?:\s+(?:--check|--stat|--name-only|--numstat|--cached|--staged))*\s+--\s+[A-Za-z0-9_./\\-]+(?:\s+[A-Za-z0-9_./\\-]+)*$',
    '^git(?:\.exe)?\s+branch\s+--show-current$',
    '^git(?:\.exe)?\s+rev-parse\s+(?:--show-toplevel|--show-prefix|--abbrev-ref\s+HEAD)$',
    '^git(?:\.exe)?\s+--no-pager\s+(?:log|show)(?:\s+--oneline)?$',
    '^bun(?:\.exe)?\s+--version$',
    '^bun(?:\.exe)?\s+(?:run\s+)?dev$',
    '^bun(?:\.exe)?\s+src[/\\]server\.ts$',
    '^bun(?:\.exe)?\s+--no-env-file\s+test\s+--isolate(?:\s+tests[/\\][A-Za-z0-9_.@/\\-]+\.test\.ts)*$',
    "^bun(?:\.exe)?\s+run\s+$offlineRun$",
    '^bun(?:\.exe)?\s+--no-env-file\s+node_modules[/\\]typescript[/\\]bin[/\\]tsc\s+--project\s+tsconfig\.scripts\.json$',
    '^bun(?:\.exe)?\s+run\s+verify:persisted-continuation$',
    '^bun(?:\.exe)?\s+--env-file=\.ghostprov[/\\]continuation-live\.env\s+scripts[/\\]verify_persisted_continuation\.ts$',
    '^codex(?:\.exe)?\s+--help$',
    '^codex(?:\.exe)?\s+exec\s+--help$',
    '^codex(?:\.exe)?\s+exec\s+resume\s+--help$'
)

foreach ($pattern in $allowed) {
    if ($command -match $pattern) { exit 0 }
}

# A single read-only health request is allowed; no arbitrary URL, header, body,
# method override, proxy, or shell wrapper is accepted.
if ($command -match '^curl(?:\.exe)?\s+(?:--fail\s+)?(?:--silent\s+)?(?:--show-error\s+)?https?://(?:127\.0\.0\.1|localhost):18787/health$') {
    exit 0
}

Deny-ProtectedPortCommand $protectedReason
