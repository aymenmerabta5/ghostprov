import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { GIT_CLEAN_FILTER_HOOK_SHA256, loadGitCleanFilterHook } from "../src/v2/supervisor/git-clean-filter";

import { GIT_TEXTCONV_INPUT_SHA256, loadGitTextconvInput } from "../src/v2/supervisor/git-textconv";
import { verifyGitCleanFilter } from "./verify_git_clean_filter";

function linuxPath(value: string): string {
  const absolute = path.resolve(value).replaceAll("\\", "/");
  if (process.platform !== "win32") return absolute;
  if (!/^[A-Za-z]:\//.test(absolute)) throw new Error("A local Windows drive path is required");
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

export async function verifyGitTextconv(): Promise<void> {
  await verifyGitCleanFilter();
  await loadGitCleanFilterHook();
  await loadGitTextconvInput();
  const script = "scripts/lib/verify-git-textconv.py";
  const hook = "src/v2/supervisor/git-clean-filter-hook.py";
  const relayFile = "src/v2/supervisor/git-textconv-input.py";
  const command = process.platform === "win32"
    ? ["wsl.exe", "--distribution", "Ubuntu-24.04", "--user", "nobody", "--exec", "/usr/bin/python3",
        linuxPath(script), linuxPath(hook), linuxPath(relayFile)]
    : ["python3", path.resolve(script), path.resolve(hook), path.resolve(relayFile)];
  const child = Bun.spawn(command, { stdout: "pipe", stderr: "pipe",
    env: { PATH: process.env.PATH ?? "", SystemRoot: process.env.SystemRoot ?? "", LANG: "C" } });
  const timeout = setTimeout(() => child.kill(), 120000);
  let output: string;
  try {
    const [code, stdout, stderr] = await Promise.all([child.exited, collect(child.stdout), collect(child.stderr)]);
    if (code !== 0) throw new Error(`Offline textconv checks failed (${code}): ${stderr.slice(0, 2048)} ${stdout.slice(0, 2048)}`);
    output = stdout;
  } catch (error) {
    child.kill();
    await child.exited.catch(() => undefined);
    throw error;
  } finally { clearTimeout(timeout); }
  const report: unknown = JSON.parse(output);
  if (!report || typeof report !== "object" || !("live" in report) || report.live !== false ||
      !("coreSha256" in report) || report.coreSha256 !== GIT_CLEAN_FILTER_HOOK_SHA256 ||
      !("relaySha256" in report) || report.relaySha256 !== GIT_TEXTCONV_INPUT_SHA256 ||
      !("passed" in report) || typeof report.passed !== "number" || !Number.isSafeInteger(report.passed) || report.passed < 30 || report.passed > 128 ||
      !("failed" in report) || report.failed !== 0 || !("cases" in report) || !Array.isArray(report.cases) || report.cases.length !== report.passed ||
      !report.cases.every(value => value && typeof value === "object" && value.passed === true) ||
      !("uid" in report) || typeof report.uid !== "number" || report.uid <= 0 || !("euid" in report) || report.euid !== report.uid) {
    throw new Error("Incomplete or mismatched unprivileged textconv proof");
  }
  const root = path.resolve(".ghostprov/git-textconv-offline");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(path.join(root, "run-"));
  await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ event: "git_textconv.offline_acceptance", directory, passed: report.passed,
    failed: 0, live: false, uid: report.uid, hookSha256: GIT_CLEAN_FILTER_HOOK_SHA256, relaySha256: GIT_TEXTCONV_INPUT_SHA256 }));
}

if (import.meta.main) await verifyGitTextconv();
