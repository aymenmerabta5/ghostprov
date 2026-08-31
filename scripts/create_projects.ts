import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { PrismAccountSession } from "../src/prism/account-session";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { projectId } from "../src/sandbox/types";
import { SandboxYDoc } from "../src/sandbox/ydoc";

if (process.env.GP_LIVE_CREATE_PROJECTS !== "1") {
  throw new Error("Set GP_LIVE_CREATE_PROJECTS=1 to create dedicated Prism projects");
}

const origin = (process.env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, "");
const cookieFile = process.env.PRISM_COOKIE_FILE?.trim();
const initialCookieHeader = process.env.PRISM_COOKIE?.trim();
if (!cookieFile && !initialCookieHeader) throw new Error("PRISM_COOKIE_FILE or PRISM_COOKIE is required");
const count = Number(process.env.GP_LIVE_PROJECT_COUNT ?? 4);
if (!Number.isInteger(count) || count < 1 || count > 32) throw new Error("GP_LIVE_PROJECT_COUNT must be 1..32");
const titlePrefix = process.env.GP_LIVE_PROJECT_TITLE?.trim() || "GhostProv";

const INERT_LAUNCHER = [
  "# GhostProv V2 launcher inactive",
  "$pdf_mode = 1;",
  "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
  "",
].join("\n");
const MAIN_SOURCE = [
  "\\documentclass{article}",
  "\\begin{document}",
  "GhostProv V2 launcher inactive.",
  "\\end{document}",
  "",
].join("\n");

const account = await PrismAccountSession.create({ origin, cookieFile, initialCookieHeader });
const projects = new PrismProjectsClient(account);
const provisioning = new PrismProvisioningClient({ account });
const timestamp = new Date().toISOString();
const created: Array<{ projectId: string; title: string; seeded: string[]; createdAt: string }> = [];
for (let index = 0; index < count; index++) {
  const projectUuid = crypto.randomUUID();
  const title = `${titlePrefix} ${timestamp} #${index + 1}`;
  const summary = await projects.create({ projectId: projectId(projectUuid), title });
  const id = summary.projectId;
  const sandbox = await provisioning.acquireSandbox();
  const credentials = await provisioning.provisionAcquired(id, sandbox);
  const ydoc = new SandboxYDoc({
    projectId: id,
    mintYSweet: async () => credentials.ySweet,
  });
  try {
    await ydoc.connect();
    await ydoc.writePath("main.tex", MAIN_SOURCE);
    await ydoc.writePath(".latexmkrc", INERT_LAUNCHER);
    const main = await ydoc.readPath("main.tex");
    const launcher = await ydoc.readPath(".latexmkrc");
    if (main !== MAIN_SOURCE || launcher !== INERT_LAUNCHER) {
      throw new Error(`Seeded project files did not round-trip: ${id}`);
    }
  } finally {
    await ydoc.destroy().catch(() => undefined);
  }
  created.push({ projectId: id, title, seeded: ["main.tex", ".latexmkrc"], createdAt: timestamp });
}
await account.flush();

const listed = await projects.list();
const missing = created.filter(item => {
  const summary = listed.find(entry => entry.projectId === item.projectId);
  return !summary || summary.deleted;
});
if (missing.length > 0) {
  throw new Error(`Created projects are not visible to this account: ${missing.map(item => item.projectId).join(", ")}`);
}

const ids = created.map(item => item.projectId).join(",");
const envLine = `PRISM_PROJECT_IDS=${ids}`;
const envFile = path.resolve(process.env.GP_PROJECT_ENV_FILE?.trim() || ".env");
let envSource: string;
try {
  envSource = await readFile(envFile, "utf8");
} catch (error) {
  const code = (error as { code?: unknown }).code;
  if (code !== "ENOENT") throw error;
  envSource = "";
}
const existingLines = [...envSource.matchAll(/^PRISM_PROJECT_IDS=.*$/gm)];
if (existingLines.length > 1) {
  throw new Error(`Refusing to update ${envFile}: multiple PRISM_PROJECT_IDS lines exist`);
}
const updatedEnv = existingLines.length === 1
  ? envSource.replace(existingLines[0][0], envLine)
  : `${envSource}${envSource && !envSource.endsWith("\n") ? "\n" : ""}${envLine}\n`;
const envTemporary = `${envFile}.${process.pid}.${crypto.randomUUID()}.tmp`;
await writeFile(envTemporary, updatedEnv);
await rename(envTemporary, envFile);

const directory = path.resolve(process.env.GP_PROJECT_STATE_DIR ?? ".ghostprov");
const manifest = path.join(directory, "projects.json");
await mkdir(directory, { recursive: true });
const temporary = `${manifest}.${process.pid}.${crypto.randomUUID()}.tmp`;
await writeFile(temporary, JSON.stringify({ created, envFile, envLine }, null, 2));
await rename(temporary, manifest);
console.log(JSON.stringify({ created: created.length, envFile, manifest, envLine }, null, 2));
