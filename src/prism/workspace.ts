/**
 * Workspace writer — sets file contents through the live Prism tab
 * (monaco models → Yjs → sandbox disk) and VERIFIES sync via /zip before
 * returning. Unverified writes are the #1 source of stale-render bugs.
 *
 * Evaluate bodies are raw strings (tsx/esbuild __name injection breaks
 * function-form evaluates — see src/prism/gateway.ts).
 */
import { config } from "../config";
import { readTrackedFile, deleteFile, evalInPage } from "./gateway";

const WRITE_SRC = `
async (arg) => {
  const monaco = globalThis.monaco;
  const models = (monaco && monaco.editor && monaco.editor.getModels ? monaco.editor.getModels() : []) || [];
  let target = null;
  for (const m of models) {
    let val = "";
    try { val = m.getValue(); } catch (e) { continue; }
    if (arg.name === ".latexmkrc" && val.includes("pdf_mode")) { target = m; break; }
    if (arg.name === "job.json") {
      try {
        const j = JSON.parse(val);
        if (j && typeof j === "object" && typeof j.model === "string") { target = m; break; }
      } catch (e) {}
    }
    if (arg.name === "main.tex" && val.includes("documentclass")) { target = m; break; }
    if (arg.name === "runner.py" && val.includes("import pathlib")) { target = m; break; }
  }
  if (!target) return false;
  target.setValue(arg.content);
  return true;
}
`;

/**
 * Write a tracked workspace file via monaco. Files are identified by content
 * shape because Yjs URIs are opaque UUIDs.
 */
export async function writeTrackedFile(name: string, content: string): Promise<void> {
  const ok = await evalInPage<boolean>(WRITE_SRC, { name, content });
  if (!ok) throw new Error(`no editor model matched ${name}`);
}

/** Wait until the sandbox-side copy of `name` contains `marker`. */
export async function waitSync(name: string, marker: string, timeoutMs = config.syncTimeoutMs): Promise<void> {
  const t0 = Date.now();
  for (;;) {
    const txt = await readTrackedFile(name).catch(() => null);
    if (txt !== null && txt.includes(marker)) return;
    if (Date.now() - t0 > timeoutMs) throw new Error(`sync timeout waiting for ${name}`);
    await new Promise((r) => setTimeout(r, 500));
  }
}

export { deleteFile };
