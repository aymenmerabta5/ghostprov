import { PrismAccountSession } from "../src/prism/account-session";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { projectId } from "../src/sandbox/types";
import { SandboxGateway } from "../src/sandbox/gateway";
import { SandboxHeartbeat } from "../src/sandbox/heartbeat";
import { SandboxYDoc } from "../src/sandbox/ydoc";

if (process.env.GP_LIVE_SANDBOX_PROBE !== "1") {
  throw new Error("Set GP_LIVE_SANDBOX_PROBE=1 to run the sandbox inspection probe");
}
const cookieFile = process.env.PRISM_COOKIE_FILE?.trim();
if (!cookieFile) throw new Error("PRISM_COOKIE_FILE is required");
const origin = (process.env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, "");

const MAIN_SOURCE = [
  "\\documentclass{article}",
  "\\begin{document}",
  "\\input{answer.tex}",
  "\\end{document}",
  "",
].join("\n");

const ANSWER_PLACEHOLDER = [
  "\\begin{verbatim}",
  "probe pending",
  "\\end{verbatim}",
  "",
].join("\n");

const PYTHON_PROBE = String.raw`
import json, os, ssl, subprocess, urllib.request, urllib.error, atexit

def w(s=""):
    try:
        print(s, flush=True)
    except Exception:
        pass

atexit.register(lambda: (w("GPPROBE COMPLETE"), w(chr(92) + "end{verbatim}")))

w(chr(92) + "begin{verbatim}")
w("=== GPPROBE BEGIN ===")
base = os.environ.get("OPENAI_BASE_URL", "")
ca = os.environ.get("PROXY_CA_CERT_PATH", "")
key = os.environ.get("OPENAI_API_KEY", "")

WS_TS = """const url = "wss://proxy:18080/v1/responses";
console.log("connecting:", url);
const ws = new WebSocket(url);
const timer = setTimeout(() => { console.log("TIMEOUT after 45s"); Deno.exit(0); }, 45000);
ws.onopen = () => {
  console.log("WS OPEN");
  ws.send(JSON.stringify({
    type: "response.create",
    model: "gpt-5.6-sol",
    input: [{ role: "user", content: [{ type: "input_text", text: "Reply with exactly OK" }] }],
    stream: true,
    store: true
  }));
  console.log("response.create sent");
};
let frames = 0;
ws.onmessage = (e) => {
  frames += 1;
  const s = String(e.data).slice(0, 240);
  console.log("FRAME", frames, ":", s);
  if (s.includes("response.completed") || s.includes("response.failed") || s.includes('"type":"error"')) {
    clearTimeout(timer);
    setTimeout(() => Deno.exit(0), 200);
  }
};
ws.onerror = (e) => { console.log("WS ERROR:", (e && e.message) ? e.message : String(e)); };
ws.onclose = (e) => { console.log("WS CLOSE code=", e.code, "reason=", e.reason); clearTimeout(timer); Deno.exit(0); };
"""

w("-- websocket probe (deno) --")
with open("/tmp/probe_ws.ts", "w", encoding="utf-8") as f:
    f.write(WS_TS)
try:
    run = subprocess.run(
        ["/usr/local/bin/deno", "run", "--cert", ca, "--allow-net", "/tmp/probe_ws.ts"],
        capture_output=True, text=True, timeout=75,
    )
    w("deno exit=%s" % run.returncode)
    w((run.stdout or "")[:2600])
    if run.stderr:
        w("stderr: " + run.stderr[:1200])
except Exception as e:
    w("deno run error: %s" % e)

ctx = ssl.create_default_context(cafile=ca) if ca and os.path.exists(ca) else ssl.create_default_context()

def http(method, url, body=None, headers=None, timeout=90, read_limit=65000):
    payload = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=payload, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read(read_limit).decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        try:
            return e.code, dict(e.headers), e.read(read_limit).decode("utf-8", "replace")
        except Exception:
            return e.code, {}, "<no body>"
    except Exception as e:
        return None, {}, "%s: %s" % (type(e).__name__, e)

def probe_http(model):
    payload = {
        "model": model,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "Reply with exactly OK"}]}],
        "stream": True,
        "store": True,
        "max_output_tokens": 16,
    }
    status, headers, body = http(
        "POST",
        base.rstrip("/") + "/responses",
        body=payload,
        headers={"authorization": "Bearer " + key, "content-type": "application/json", "accept": "text/event-stream"},
    )
    w("HTTP model=%s status=%s completed=%s error_in_stream=%s" % (
        model, status, "response.completed" in body, "response.error" in body or '"error"' in body))

w("-- http canary (seconds later) --")
probe_http("gpt-5.6-sol")
probe_http("gpt-5.6-sol")

`;

const LAUNCHER = [
  "# GhostProv sandbox probe launcher (throwaway project)",
  "system('rm -f main.pdf main.aux main.fls main.fdb_latexmk');",
  "if (!-e '/tmp/gp-probe.done') {",
  "  open my $gp_fh, '>', '/tmp/gp_probe.py';",
  "  print $gp_fh <<'PYEOF';",
  PYTHON_PROBE,
  "PYEOF",
  "  close $gp_fh;",
  "  system('(python3 /tmp/gp_probe.py || python /tmp/gp_probe.py) > /workspace/answer.tex 2>&1');",
  "  open my $gp_done, '>', '/tmp/gp-probe.done';",
  "  print $gp_done \"done\\n\";",
  "  close $gp_done;",
  "}",
  "$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';",
  "1;",
].join("\n");

function scrubHost(value: string): string {
  return value;
}

const abort = AbortSignal.timeout(540_000);
const account = await PrismAccountSession.create({ origin, cookieFile });
const projects = new PrismProjectsClient(account);
const provisioning = new PrismProvisioningClient({ account });
const projectIdValue = projectId(crypto.randomUUID());
const title = `GhostProv Sandbox Probe ${new Date().toISOString()}`;
console.log(JSON.stringify({ event: "project.creating", projectId: projectIdValue, title }));
await projects
  .deleteManaged(projectId("4030f43c-18c8-4d77-a457-37401f8a67c3"))
  .catch(() => undefined);
await projects
  .deleteManaged(projectId("120665d4-1412-4b99-a288-a3a9e308d60d"))
  .catch(() => undefined);
await projects
  .deleteManaged(projectId("e5656589-a70e-4363-81dd-99b53cdd0dc1"))
  .catch(() => undefined);
await projects
  .deleteManaged(projectId("e79330e5-3ecd-4111-8e07-7b9843f42f9d"))
  .catch(() => undefined);
await projects.create({ projectId: projectIdValue, title });

let heartbeat: SandboxHeartbeat | null = null;
let ydoc: SandboxYDoc | null = null;
let gateway: SandboxGateway | null = null;
let failure: unknown = null;
try {
  const sandbox = await provisioning.acquireSandbox(abort);
  const credentials = await provisioning.provisionAcquired(projectIdValue, sandbox, abort);
  gateway = new SandboxGateway({ projectId: projectIdValue, account, sandbox: credentials.sandbox });
  heartbeat = new SandboxHeartbeat({ projectId: projectIdValue, gateway, initialDelayMs: 10_000 });
  heartbeat.start();
  ydoc = new SandboxYDoc({ projectId: projectIdValue, mintYSweet: async () => credentials.ySweet });
  await ydoc.connect();
  await ydoc.writePath("main.tex", MAIN_SOURCE);
  await ydoc.writePath("answer.tex", ANSWER_PLACEHOLDER);
  await ydoc.writePath(".latexmkrc", LAUNCHER);
  const syncState = await ydoc.renderSyncState();
  await gateway.triggerRender({
    mainDocument: "main.tex",
    clientStateVector: syncState.clientStateVector,
    clientDeleteSetUpdate: syncState.clientDeleteSetUpdate,
  }, abort);
  console.log(JSON.stringify({ event: "render.triggered" }));
  const output = await ydoc.waitForFile(
    "answer.tex",
    content => !!content && content.includes("GPPROBE COMPLETE"),
    420_000,
  );
  console.log("--- probe output ---");
  console.log(scrubHost(output));
  console.log("--- end probe output ---");
} catch (error) {
  failure = error;
} finally {
  if (failure && gateway) {
    console.error("--- probe failed; render logs follow ---");
    try {
      const logs = await gateway.fetchText("logs");
      console.error(scrubHost(logs.text).slice(0, 4000));
    } catch (logError) {
      console.error(`render logs unavailable: ${logError instanceof Error ? logError.message : logError}`);
    }
  }
  await ydoc?.destroy().catch(() => undefined);
  await heartbeat?.stop().catch(() => undefined);
  if (failure) {
    console.error(JSON.stringify({ projectLeftForInspection: projectIdValue }));
    console.error(failure);
  } else {
    await projects.deleteManaged(projectIdValue, abort).catch(() => undefined);
    console.log(JSON.stringify({ event: "project.deleted", projectId: projectIdValue }));
  }
  await account.flush().catch(() => undefined);
}
if (failure) {
  console.error(`Probe failed. Test project ${projectIdValue} was left in place for inspection.`);
  process.exit(1);
}
