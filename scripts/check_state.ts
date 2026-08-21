/** Quick state check of loop files in the doc. */
import "dotenv/config";
import { readDocFile, listDocFiles } from "../src/prism/ydoc";

const files = await listDocFiles();
console.log("entries:", files.filter((f) => !f.deleted).map((f) => f.filename).join(", "));
for (const name of ["gp_status.json", ".latexmkrc", "job_s1.json", "resp_s1.json", "job_s2.json", "resp_s2.json"]) {
  const c = await readDocFile(name);
  console.log(`--- ${name}: ${c === null ? "MISSING" : `${c.length} chars`} ${c ? c.slice(0, 200).replace(/\n/g, "\\n") : ""}`);
}
const daemon = await readDocFile("gp_daemon.py");
console.log(`--- gp_daemon.py: ${daemon === null ? "MISSING" : `${daemon.length} chars`}`);
process.exit(0);
