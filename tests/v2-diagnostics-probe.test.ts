import { describe, expect, it } from "bun:test";
import { createDiagnosticEvent, resolveDiagnosticProbeOptions } from "../scripts/diagnose_v2_concurrency";

describe("V2 concurrency diagnostic probe guards",()=>{
 it("requires explicit opt-in and refuses protected/non-loopback roots",()=>{
  expect(()=>resolveDiagnosticProbeOptions({})).toThrow("GP_LIVE_V2_DIAGNOSTICS=1");
  expect(()=>resolveDiagnosticProbeOptions({GP_LIVE_V2_DIAGNOSTICS:"1",GP_V2_DIAGNOSTIC_ROOT:"http://127.0.0.1:8787"})).toThrow("8787");
  expect(()=>resolveDiagnosticProbeOptions({GP_LIVE_V2_DIAGNOSTICS:"1",GP_V2_DIAGNOSTIC_ROOT:"https://example.com"})).toThrow("loopback");
 });
 it("builds one immutable body with implicit websocket streaming",()=>{
  const left=createDiagnosticEvent();const right=createDiagnosticEvent();
  expect(JSON.stringify(left)).toBe(JSON.stringify(right));
  expect(left).not.toHaveProperty("stream");
  expect(Object.isFrozen(left)).toBe(true);
  expect(JSON.stringify(left).length).toBeGreaterThan(256*1024);
 });
});
