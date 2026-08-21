/**
 * Write NEW .latexmkrc into the doc, then watch doc vs disk (via server's
 * fresh-token debug endpoint) to catch land/revert behavior.
 */
import "dotenv/config";

const SERVER = "http://localhost:8787";
const NEW = [
  "$pdf_mode = 1;",
  "system('rm -f main.pdf main.aux main.fls main.fdb_latexmk; rm -f /workspace/response.json');",
  "$pdflatex = 'pgrep -f gp_daemon.py >/dev/null 2>&1 || nohup python gp_daemon.py > /tmp/gp_daemon.log 2>&1 & nohup python runner.py > /tmp/gp_bg.log 2>&1 & pdflatex -interaction=nonstopmode %O %S'",
].join("\n") + "\n";

const { writeDocFile } = await import("../src/prism/ydoc");

async function disk(): Promise<string | null> {
  try {
    const r = await fetch(`${SERVER}/__debug/disk?name=.latexmkrc`);
    if (!r.ok) return null;
    const j = await r.json();
    return j.content ?? null;
  } catch { return null; }
}
async function docView(): Promise<string | null> {
  try {
    const r = await fetch(`${SERVER}/__debug/doc?name=.latexmkrc`);
    if (!r.ok) return null;
    const j = await r.json();
    return j.content ?? null;
  } catch { return null; }
}

console.log("writing NEW .latexmkrc via ydoc…");
await writeDocFile(".latexmkrc", NEW);

let last = "";
const t0 = Date.now();
for (let i = 0; i < 45; i++) {
  const [d, dv] = await Promise.all([disk(), docView()]);
  const ds = d === null ? "null" : d.includes("gp_daemon") ? "NEW" : "OLD";
  const dvs = dv === null ? "null" : dv.includes("gp_daemon") ? "NEW" : "OLD";
  const state = `doc=${dvs} disk=${ds}`;
  if (state !== last) {
    console.log(`t+${String(Date.now() - t0).padStart(6)}ms ${state}`);
    last = state;
  }
  if (ds === "NEW" && i > 10) { console.log("landed and stable ✓"); break; }
  await new Promise((r) => setTimeout(r, 1000));
}
process.exit(0);
