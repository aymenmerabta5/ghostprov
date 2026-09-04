import { describe, expect, it } from "bun:test";
import {
  extractRelayExecHello,
  isRelayMainNonce,
  MAX_RELAY_HELLO_BYTES,
  MAX_RELAY_SCRIPT_BYTES,
  RELAY_HELLO_PREFIX,
  RELAY_HELLO_SUFFIX,
  RELAY_LAUNCH_PATH,
  RELAY_WATCHER_PATH,
  relayHelloPath,
  relayHelloPrinterLatexmkrc,
  relayMainSource,
  relaySidecarLaunchScript,
  relayWatcherScript,
} from "../src/v2/bootstrap/relay-launcher";

const NONCE = "00000000-0000-4000-8000-000000000001";
const OPTIONS = {
  entrypointPath: ".ghostprov-v2/revisions/r9/abc/sidecar.bundle.js",
  nonce: NONCE,
};

describe("R9 sandbox-relay launcher", () => {
  it("stages under stable relay paths with bounded bytes", () => {
    expect(RELAY_WATCHER_PATH).toBe(".ghostprov-v2/relay/watcher.sh");
    expect(RELAY_LAUNCH_PATH).toBe(".ghostprov-v2/relay/bootstrap-launch.sh");
    expect(MAX_RELAY_SCRIPT_BYTES).toBe(8_192);
  });

  it("generates a fenced single-flight launcher carrying the exact launch identity", () => {
    const script = relaySidecarLaunchScript(OPTIONS);
    expect(script.length).toBeGreaterThan(500);
    expect(script.length).toBeLessThanOrEqual(MAX_RELAY_SCRIPT_BYTES);
    expect(script).toContain("'/workspace/.ghostprov-v2/revisions/r9/abc/sidecar.bundle.js'");
    expect(script).toContain("NONCE='00000000-0000-4000-8000-000000000001'");
    expect(script).toContain("START=$(date -u +%s)");
    expect(script).toContain("--ghostprov-launch-nonce=\"$NONCE\"");
    expect(script).toContain("--ghostprov-process-start=\"$START\"");
    expect(script).toContain("sidecar.pid.$NONCE.$child.candidate");
    expect(script).toContain("HELLO-BEGIN");
    expect(script).toContain("HELLO-END");
    expect(script).toContain("sidecar already running");
    expect(script).toContain('setsid nohup "$DENO"');
    expect(script).toContain("--allow-read=/workspace/.ghostprov-v2,/tmp/ghostprov-v2");
    expect(script).toContain("--allow-write=/tmp/ghostprov-v2");
    expect(script.endsWith("\n")).toBe(true);
    expect(script).not.toContain("\r");
  });

  it("fails closed on traversal and nonce abuse", () => {
    expect(() => relaySidecarLaunchScript({ ...OPTIONS, entrypointPath: "../evil.js" }))
      .toThrow(/traverse|invalid/i);
    expect(() => relaySidecarLaunchScript({ ...OPTIONS, entrypointPath: "/abs.js" }))
      .toThrow(/invalid/i);
    expect(() => relaySidecarLaunchScript({ ...OPTIONS, nonce: "bad nonce!" }))
      .toThrow(/nonce/i);
    expect(() => relaySidecarLaunchScript({ ...OPTIONS, nonce: "" }))
      .toThrow(/nonce/i);
  });

  it("copies the hello into the workspace read-back file for the render printer", () => {
    expect(relayHelloPath(NONCE)).toBe(`.ghostprov-v2/relay/hello.${NONCE}.txt`);
    expect(() => relayHelloPath("bad nonce!")).toThrow(/nonce/i);
    const script = relaySidecarLaunchScript(OPTIONS);
    expect(script).toContain(`HELLO_COPY='/workspace/.ghostprov-v2/relay/hello.${NONCE}.txt'`);
    expect(script).toContain('cp "$hello" "$HELLO_COPY"');
  });

  it("generates a bounded render printer bound to the exact launch nonce", () => {
    expect(RELAY_HELLO_PREFIX).toBe(".ghostprov-v2/relay/hello.");
    expect(RELAY_HELLO_SUFFIX).toBe(".txt");
    expect(MAX_RELAY_HELLO_BYTES).toBe(8_192);
    const printer = relayHelloPrinterLatexmkrc(NONCE);
    expect(printer.length).toBeLessThanOrEqual(MAX_RELAY_SCRIPT_BYTES);
    expect(printer).toContain(`'/workspace/.ghostprov-v2/relay/hello.${NONCE}.txt'`);
    expect(printer).toContain("RELAY-HELLO-BEGIN");
    expect(printer).toContain("RELAY-HELLO-END");
    expect(printer).toContain("$pdf_mode = 1;");
    expect(printer).toContain("$pdflatex = 'pdflatex -interaction=nonstopmode %O %S';");
    expect(printer.endsWith("\n")).toBe(true);
    expect(printer).not.toContain("\r");
    expect(printer).not.toContain("sidecar.bundle.js");
    expect(printer).not.toContain("Deno");
    expect(() => relayHelloPrinterLatexmkrc("bad nonce!")).toThrow(/nonce/i);
    expect(() => relayHelloPrinterLatexmkrc("")).toThrow(/nonce/i);
  });

  it("single-quotes shell interpolations so paths cannot break out", () => {
    const script = relaySidecarLaunchScript({
      ...OPTIONS,
      entrypointPath: "a/b-c_d.js",
    });
    expect(script).toContain("EP='/workspace/a/b-c_d.js'");
  });

  it("generates a restart-safe watcher with a persistent executed-hash guard", () => {
    const script = relayWatcherScript();
    expect(script.length).toBeLessThanOrEqual(MAX_RELAY_SCRIPT_BYTES);
    // Persistent guard first: the hash is recorded before execution so a
    // restart cannot replay the payload (the PoC's self-upgrade loop fix).
    const guardAt = script.indexOf("executed.sha");
    const execAt = script.indexOf("bootstrap-launch.current.sh");
    expect(guardAt).toBeGreaterThan(-1);
    expect(execAt).toBeGreaterThan(guardAt);
    expect(script).toContain("watcher-version");
    expect(script).toContain("sleep 0.2");
    expect(script).toContain("setsid sh");
    expect(script.endsWith("\n")).toBe(true);
    expect(script).not.toContain("\r");
  });

  it("generates a TeX read-back document bound to the exact launch nonce", () => {
    expect(isRelayMainNonce(NONCE)).toBe(true);
    expect(isRelayMainNonce("bad nonce!")).toBe(false);
    expect(isRelayMainNonce("has_underscore")).toBe(false);
    expect(isRelayMainNonce("")).toBe(false);
    const main = relayMainSource(NONCE);
    expect(main.length).toBeLessThanOrEqual(MAX_RELAY_SCRIPT_BYTES);
    expect(main).toContain("\\documentclass{article}");
    expect(main).toContain(`\\openin\\gphello=.ghostprov-v2/relay/hello.${NONCE}.txt`);
    expect(main).toContain("\\typeout{RELAY-HELLO-MISSING}");
    expect(main).toContain("\\typeout{\\gpline}");
    expect(main).toContain("\\loop\\unless\\ifeof\\gphello");
    expect(main).toContain("\\end{document}");
    expect(main.endsWith("\n")).toBe(true);
    expect(main).not.toContain("\r");
    expect(main).not.toContain("sidecar.bundle.js");
    expect(() => relayMainSource("bad nonce!")).toThrow(/TeX nonce/i);
    expect(() => relayMainSource("has_underscore")).toThrow(/TeX nonce/i);
  });

  it("keeps both scripts deterministic for identical inputs", () => {
    expect(relaySidecarLaunchScript(OPTIONS)).toBe(relaySidecarLaunchScript(OPTIONS));
    expect(relayWatcherScript()).toBe(relayWatcherScript());
  });

  it("extracts exec hellos without consuming launcher markers", () => {
    const token = `abcDEF123-_${"x".repeat(64)}`;
    expect(extractRelayExecHello(
      `some agent prose\nHELLO-BEGIN\nGPV2_BOOTSTRAP ${token}\nHELLO-END\ntrailing prose here`,
    )).toBe(token);
    // Agent-wrapped base64 continues across full-alphabet lines.
    const wrapped = `${token.slice(0, 32)}\n${token.slice(32)}`;
    expect(extractRelayExecHello(`HELLO-BEGIN\nGPV2_BOOTSTRAP ${wrapped}\nHELLO-END\n`)).toBe(token);
    // A prose mention of the prefix must not poison a later real hello.
    expect(extractRelayExecHello(
      `the GPV2_BOOTSTRAP line is below\nGPV2_BOOTSTRAP ${token}\nall done!`,
    )).toBe(token);
    expect(extractRelayExecHello("launcher ran, no marker here")).toBeNull();
    expect(extractRelayExecHello("")).toBeNull();
    expect(extractRelayExecHello(`GPV2_BOOTSTRAP \nHELLO-END`)).toBeNull();
    expect(extractRelayExecHello(`GPV2_BOOTSTRAP ${token}\nHELLO-END extra words`)).toBe(token);
  });
});
