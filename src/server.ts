import { Hono } from "hono";
const app = new Hono();
app.get("/health", (c) => c.json({ ok: true }));
export default app;
if (import.meta.main) {
  const port = Number(process.env.PORT || 8787);
  console.log(`ghostprov listening on http://localhost:${port}`);
  Bun.serve({ fetch: app.fetch, port });
}
