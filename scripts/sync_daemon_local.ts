/** Pull live gp_daemon.py from sandbox disk into the local repo (single source of truth sync). */
import * as fs from "node:fs";

const r = await fetch("http://localhost:8787/__debug/disk?name=gp_daemon.py");
const j: any = await r.json();
if (!j.content) { console.log("MISSING on disk:", JSON.stringify(j)); process.exit(1); }
fs.writeFileSync("D:/MyWork/AI/ghostprov/sandbox/gp_daemon.py", j.content);
console.log("synced", j.content.length, "chars; flock:", j.content.includes("flock"), "slots30:", j.content.includes("30"));
process.exit(0);
