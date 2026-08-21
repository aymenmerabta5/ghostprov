/**
 * Setup static main.tex (\input{rendered.tex}) + create rendered.tex.
 * All dynamic writes/diagnostics go through rendered.tex from now on.
 */
import "dotenv/config";
import { readDocFile, writeDocFile } from "../src/prism/ydoc";

const MAIN = "\\documentclass{article}\n\\begin{document}\n\\input{rendered.tex}\n\\end{document}\n";

if ((await readDocFile("rendered.tex")) === null) {
  await writeDocFile("rendered.tex", "GhostProv v2\n");
  console.log("created rendered.tex");
  await new Promise((r) => setTimeout(r, 800));
}
const curMain = await readDocFile("main.tex");
if (curMain !== MAIN) {
  await writeDocFile("main.tex", MAIN);
  console.log("main.tex set to \\input{rendered.tex}");
}
console.log("done");
process.exit(0);
