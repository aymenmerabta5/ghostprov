import "dotenv/config";
import { createBrowserlessSystem } from "./server/system";

export { createBrowserlessSystem } from "./server/system";
export { createGhostProvApp } from "./server/app";
export { createGhostProvServer } from "./server/runtime";

const isMain = process.argv[1] && (
  process.argv[1].endsWith("server.ts") ||
  process.argv[1].endsWith("server.js")
);

if (isMain) {
  const system = await createBrowserlessSystem(process.env);
  await system.start();
  console.log(
    `ghostprov browserless router listening on http://127.0.0.1:${system.config.port} ` +
    `projects=${system.config.projectIds.length}`,
  );

  let closing = false;
  const close = async () => {
    if (closing) return;
    closing = true;
    await system.close();
  };
  process.once("SIGINT", () => void close());
  process.once("SIGTERM", () => void close());
}
