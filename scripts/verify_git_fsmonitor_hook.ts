import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { makeGitFsmonitorPlan, parseGitFsmonitorResult } from "../src/v2/supervisor/git-fsmonitor";
import { proveSignedShutdown } from "./lib/git-fsmonitor-proof";

const root = path.resolve(".ghostprov/git-fsmonitor-offline");
await mkdir(root, { recursive: true });
const directory = await mkdtemp(path.join(root, "run-"));
type Case = { id: string; criterion: string; scope: string; passed: boolean; evidence: Record<string, unknown> };
const cases: Case[] = [];
const record = (id: string, criterion: string, passed: boolean, evidence: Record<string, unknown>) => cases.push({ id, criterion, scope: "offline_typescript_policy", passed, evidence });
const sessionId = "11111111-1111-4111-8111-111111111111";
const submitted = { version: 1, sessionId, outcome: "submitted", configRestored: true, lockReleased: true, launchSubmitted: true };
record("strict_submitted_receipt", "Submitted receipt carries restoration and ownership proof", parseGitFsmonitorResult(submitted, sessionId).outcome === "submitted", { accepted: true });
const bad = [null, [], {}, { ...submitted, sessionId: sessionId + "\n" }, { ...submitted, configRestored: false },
  { ...submitted, lockReleased: false }, { ...submitted, launchSubmitted: false }, { ...submitted, outcome: "expired" }, { ...submitted, extra: true }];
let rejected = 0;
for (const value of bad) { try { parseGitFsmonitorResult(value, sessionId); } catch { rejected++; } }
record("receipt_rejections", "Malformed and mismatched receipts cannot become launch proof", rejected === bad.length, { rejected, tested: bad.length });
const badIds = ["", sessionId + "\n", "../" + sessionId, sessionId + "/extra", "1".repeat(36)];
let badIdsRejected = 0;
for (const value of badIds) { try { makeGitFsmonitorPlan(value, 1); } catch { badIdsRejected++; } }
record("path_identity_rejections", "Only an exact private UUID forms filesystem paths", badIdsRejected === badIds.length, { rejected: badIdsRejected, tested: badIds.length });
let calls = 0;
const unopened = { instanceId: "pending", async shutdown() { calls++; } };
const noProof = await proveSignedShutdown(unopened, null, "offline", new AbortController().signal);
record("no_false_signed_shutdown", "An unopened supervisor cannot produce signed shutdown evidence", !noProof && calls === 0, { signedShutdown: noProof, shutdownCalls: calls });
const authenticated = { instanceId: sessionId, async shutdown() { calls++; } };
const proof = await proveSignedShutdown(authenticated, sessionId, "offline", new AbortController().signal);
record("matching_shutdown_only", "Only the recorded authenticated instance is allowed to shut down", proof && calls === 1, { signedShutdown: proof, shutdownCalls: calls });
let rejectedShutdown = false;
try { await proveSignedShutdown({ instanceId: sessionId, async shutdown() { throw new Error("Synthetic failed acknowledgement"); } }, sessionId, "offline", new AbortController().signal); }
catch { rejectedShutdown = true; }
record("failed_shutdown_not_proof", "A failed shutdown acknowledgement cannot become success", rejectedShutdown, { rejected: rejectedShutdown });

const linuxPath = (value: string) => {
  const absolute = path.resolve(value).replaceAll("\\", "/");
  if (!/^[A-Za-z]:\//.test(absolute)) throw new Error("Expected a Windows test path");
  return `/mnt/${absolute[0]!.toLowerCase()}${absolute.slice(2)}`;
};
const command = process.platform === "win32"
  ? ["wsl.exe", "--distribution", "Ubuntu-24.04", "--exec", "/usr/bin/python3",
      linuxPath("scripts/lib/verify-git-fsmonitor-hook.py"), linuxPath(directory), linuxPath("src/v2/supervisor/git-fsmonitor-hook.py")]
  : ["python3", path.resolve("scripts/lib/verify-git-fsmonitor-hook.py"), directory, path.resolve("src/v2/supervisor/git-fsmonitor-hook.py")];
const linux = Bun.spawn(command, { stdout: "pipe", stderr: "pipe" });
const [exitCode, stdout, stderr] = await Promise.all([linux.exited, new Response(linux.stdout).text(), new Response(linux.stderr).text()]);
if (stdout.length + stderr.length > 128 * 1024) throw new Error("Linux test output exceeded its bound");
if (exitCode !== 0) throw new Error(`Linux test failed (${exitCode}): ${stderr.slice(0, 1500)}`);
cases.push(...(JSON.parse(stdout) as { cases: Case[] }).cases);
const report = { candidate: "git-fsmonitor", live: false, passed: cases.filter(item => item.passed).length,
  failed: cases.filter(item => !item.passed).length, success: cases.every(item => item.passed), cases };
await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify({ event: "git_fsmonitor.offline_acceptance", directory, ...report }, null, 2));
if (!report.success) process.exitCode = 1;
