/** Touch main.tex (content change) then caller renders. */
import "dotenv/config";
import { readDocFile, writeDocFile } from "../src/prism/ydoc";

const cur = (await readDocFile("main.tex")) ?? "\\documentclass{article}\n\\begin{document}\nGhostProv v2\n\\end{document}\n";
const next = cur.replace(/\s*% gp-touch \w+\s*\n?$/, "\n") + `% gp-touch ${Date.now().toString(36)}\n`;
await writeDocFile("main.tex", next);
console.log("main.tex touched:", JSON.stringify(next.slice(-60)));
process.exit(0);
