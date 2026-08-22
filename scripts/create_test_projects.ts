import { mkdir, rename, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { PrismAccountSession } from "../src/prism/account-session";

if (process.env.GP_LIVE_CREATE_TEST_PROJECTS !== "1") {
  throw new Error("Set GP_LIVE_CREATE_TEST_PROJECTS=1 to create dedicated Prism test projects");
}

const origin = (process.env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, "");
const cookieFile = process.env.PRISM_COOKIE_FILE?.trim();
const initialCookieHeader = process.env.PRISM_COOKIE?.trim();
if (!cookieFile && !initialCookieHeader) throw new Error("PRISM_COOKIE_FILE or PRISM_COOKIE is required");
const account = await PrismAccountSession.create({ origin, cookieFile, initialCookieHeader });
const count = Number(process.env.GP_LIVE_PROJECT_COUNT ?? 4);
if (!Number.isInteger(count) || count < 1 || count > 16) throw new Error("GP_LIVE_PROJECT_COUNT must be 1..16");
const created: Array<{ projectId: string; title: string; createdAt: string }> = [];
const timestamp = new Date().toISOString();
for (let index = 0; index < count; index++) {
  const projectUuid = crypto.randomUUID();
  const title = `GhostProv Load Test ${timestamp} #${index + 1}`;
  const response = await account.request("/api/projects", {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ project_uuid: projectUuid, title, file_uuids: [] }),
  });
  const body = await response.json() as any;
  if (!response.ok || typeof body?.uuid !== "string") {
    throw new Error(`Project creation failed at index ${index + 1} (${response.status})`);
  }
  created.push({ projectId: body.uuid, title, createdAt: timestamp });
}
const directory = path.resolve(process.env.GP_TEST_STATE_DIR ?? ".ghostprov");
const manifest = path.join(directory, "test-projects.json");
await mkdir(directory, { recursive: true });
const temporary = `${manifest}.${process.pid}.${crypto.randomUUID()}.tmp`;
await writeFile(temporary, JSON.stringify({ created }, null, 2));
await rename(temporary, manifest);
console.log(JSON.stringify({ created: created.length, manifest }, null, 2));
