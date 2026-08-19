import { Hono } from "hono";
import { relayCreate } from "./relay/client";
const app = new Hono();
app.get("/health", (c) => c.json({ ok: true }));
app.post("/v1/responses", async (c) => {
  const body = await c.req.json();
  const res = await relayCreate(body);
  return c.json(res);
});
export default app;
if (import.meta.main) {
  const port = Number(process.env.PORT || 8787);
  console.log(`ghostprov listening on http://localhost:${port}`);
  Bun.serve({ fetch: app.fetch, port });
}
