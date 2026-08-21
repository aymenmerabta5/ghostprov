/** Check deployed gp_daemon.py for paste-corruption via server debug endpoint. */
const r = await fetch("http://localhost:8787/__debug/disk?name=gp_daemon.py");
const j = await r.json();
if (!j.content) { console.log("not on disk:", JSON.stringify(j)); process.exit(0); }
const c = j.content as string;
console.log("size:", c.length);
console.log("contains 'def ask(' (ask.py merge):", c.includes("def ask("));
console.log("contains 'ThreadPoolExecutor':", c.includes("ThreadPoolExecutor"));
console.log("contains 'runner-state' (runner.py merge):", c.includes("runner-state"));
console.log("first 100:", JSON.stringify(c.slice(0, 100)));
console.log("last 80:", JSON.stringify(c.slice(-80)));
process.exit(0);
