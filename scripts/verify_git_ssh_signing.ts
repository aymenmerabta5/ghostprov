import { mkdir, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { GIT_SSH_SIGNING_HOOK_SHA256, GIT_SSH_SIGNER_SHA256, loadGitSshSigningSources } from "../src/v2/supervisor/git-ssh-signing";
import { gitSshSigningMarkerLauncher } from "./lib/git-ssh-signing-marker";

function linuxPath(value: string): string {
  const absolute = path.resolve(value).replaceAll("\\", "/");
  if (process.platform !== "win32") return absolute;
  if (!/^[A-Za-z]:\//.test(absolute)) throw new Error("The Linux verifier requires a local Windows drive path");
  return `/mnt/${absolute[0]!.toLowerCase()}${absolute.slice(2)}`;
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      if (!next.value.byteLength) continue;
      bytes += next.value.byteLength;
      if (bytes > 128 * 1024 || chunks.length >= 256) throw new Error("Offline verifier output exceeded its bound");
      chunks.push(next.value);
    }
    return Buffer.concat(chunks).toString("utf8");
  } finally { await reader.cancel().catch(() => undefined); reader.releaseLock(); }
}

async function run(script: string, arguments_: readonly string[]): Promise<void> {
  const command = process.platform === "win32"
    ? ["wsl.exe", "--distribution", "Ubuntu-24.04", "--exec", "/usr/bin/python3", linuxPath(script), ...arguments_]
    : ["python3", path.resolve(script), ...arguments_];
  const child = Bun.spawn(command, { stdout: "pipe", stderr: "pipe",
    env: { PATH: process.env.PATH ?? "", SystemRoot: process.env.SystemRoot ?? "", LANG: "C" } });
  const timeout = setTimeout(() => { child.kill(); }, 120_000);
  try {
    const [exitCode, stdout, stderr] = await Promise.all([child.exited, collect(child.stdout), collect(child.stderr)]);
    if (exitCode !== 0) throw new Error(`Offline Git signing verifier failed (${exitCode}): ${stderr.slice(0, 2048)} ${stdout.slice(0, 2048)}`);
  } catch (error) {
    child.kill();
    await child.exited.catch(() => undefined);
    throw error;
  } finally { clearTimeout(timeout); }
}

async function readProof(filename: string, minimum: number, expectedHook: boolean) {
  if ((await stat(filename)).size > 128 * 1024) throw new Error("Offline proof exceeded its byte bound");
  const value: unknown = JSON.parse(await readFile(filename, "utf8"));
  if (!value || typeof value !== "object" || !("live" in value) || value.live !== false ||
      !("passed" in value) || typeof value.passed !== "number" || !Number.isSafeInteger(value.passed) || value.passed < minimum ||
      !("failed" in value) || value.failed !== 0 || !("cases" in value) || !Array.isArray(value.cases) || value.cases.length !== value.passed ||
      !value.cases.every(item => item && typeof item === "object" && item.passed === true) ||
      !("signerSha256" in value) || value.signerSha256 !== GIT_SSH_SIGNER_SHA256 ||
      (expectedHook && (!("sourceSha256" in value) || value.sourceSha256 !== GIT_SSH_SIGNING_HOOK_SHA256))) {
    throw new Error("Incomplete or mismatched offline Git signing proof");
  }
  return value;
}

export async function verifyGitSshSigning(): Promise<void> {
  await loadGitSshSigningSources();
  const runtime = process.env.GP_GIT_SSH_DENO_PATH ?? (process.platform === "win32" ? "/tmp/ghostprov-deno/deno" : "/usr/local/bin/deno");
  if (runtime !== "/tmp/ghostprov-deno/deno" && runtime !== "/usr/local/bin/deno") throw new Error("Use the installed pinned Linux Deno at a supported fixture path");
  const root = path.resolve(".ghostprov/git-ssh-signing-offline");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(path.join(root, "run-"));
  const marker = path.join(directory, "marker.sh");
  await writeFile(marker, gitSshSigningMarkerLauncher("11111111-1111-4111-8111-111111111111"));
  const hook = linuxPath("src/v2/supervisor/git-ssh-signing-hook.py");
  const signer = linuxPath("src/v2/supervisor/git-sshsig.mjs");
  const cryptoReport = path.join(directory, "sshsig.json");
  const gitReport = path.join(directory, "linux-git.json");
  await run("scripts/lib/verify-git-sshsig.py", [signer, runtime, linuxPath(cryptoReport), GIT_SSH_SIGNER_SHA256]);
  const cryptoProof = await readProof(cryptoReport, 43, false);
  await run("scripts/lib/verify-git-ssh-signing.py", [hook, signer, linuxPath(marker), runtime, linuxPath(gitReport),
    GIT_SSH_SIGNING_HOOK_SHA256, GIT_SSH_SIGNER_SHA256]);
  const gitProof = await readProof(gitReport, 51, true);
  const report = { live: false, method: "git-ssh-signing", success: true, hookSha256: GIT_SSH_SIGNING_HOOK_SHA256,
    signerSha256: GIT_SSH_SIGNER_SHA256, gitAcceptance: { passed: gitProof.passed, failed: 0 },
    interoperability: { passed: cryptoProof.passed, failed: 0 } };
  await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ event: "git_ssh_signing.offline_acceptance", directory, ...report }));
}

if (import.meta.main) await verifyGitSshSigning();
