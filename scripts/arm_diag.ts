/** Arm diagnostic .latexmkrc dumping into rendered.tex, render, read back. */
import "dotenv/config";
import { writeDocFile } from "../src/prism/ydoc";

const DIAG = [
  "$pdf_mode = 1;",
  "$pdflatex = 'pgrep -f gp_daemon.py >/dev/null 2>&1 || nohup python gp_daemon.py > /tmp/gp_daemon.log 2>&1 & nohup python runner.py > /tmp/gp_bg.log 2>&1 & pdflatex -interaction=nonstopmode %O %S; { echo ===GPDIAG===; date; id; echo ---DAEMON_LOG; cat /tmp/gp_daemon.log 2>&1; echo ---BG_LOG; cat /tmp/gp_bg.log 2>&1; echo ---STATUS_FILE; cat /workspace/gp_status.json 2>&1; echo; echo ---PROCS; ps aux | grep -E \"gp_daemon|runner|python\" | grep -v grep; echo ---JOBS; ls -la /workspace/job_s*.json /workspace/resp_s*.json 2>&1; } > /workspace/rendered.tex 2>&1'",
].join("\n") + "\n";

await writeDocFile(".latexmkrc", DIAG);
console.log("DIAG armed");
process.exit(0);
