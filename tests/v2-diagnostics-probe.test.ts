import { describe, expect, it } from "bun:test";
import { createDiagnosticEvent, metricsFromSnapshot, resolveDiagnosticProbeOptions } from "../scripts/diagnose_v2_concurrency";

describe("V2 concurrency diagnostic probe guards",()=>{
 it("requires explicit opt-in and refuses protected/non-loopback roots",()=>{
 expect(()=>resolveDiagnosticProbeOptions({})).toThrow("GP_LIVE_V2_DIAGNOSTICS=1");
  expect(()=>resolveDiagnosticProbeOptions({GP_LIVE_V2_DIAGNOSTICS:"1",GHOSTPROV_API_KEY:"test-ghostprov-key-0123456789abcdef",GP_V2_DIAGNOSTIC_ROOT:"http://127.0.0.1:18787"})).toThrow("18787");
  expect(()=>resolveDiagnosticProbeOptions({GP_LIVE_V2_DIAGNOSTICS:"1",GHOSTPROV_API_KEY:"test-ghostprov-key-0123456789abcdef",GP_V2_DIAGNOSTIC_ROOT:"https://example.com"})).toThrow("loopback");
  expect(resolveDiagnosticProbeOptions({GP_LIVE_V2_DIAGNOSTICS:"1",GHOSTPROV_API_KEY:"test-ghostprov-key-0123456789abcdef"}).root).toBe("http://127.0.0.1:18888");
 });
 it("builds one immutable body with implicit websocket streaming",()=>{
  const left=createDiagnosticEvent();const right=createDiagnosticEvent();
  expect(JSON.stringify(left)).toBe(JSON.stringify(right));
  expect(left).not.toHaveProperty("stream");
  expect(Object.isFrozen(left)).toBe(true);
  expect(JSON.stringify(left).length).toBeGreaterThan(256*1024);
 });
 it("extracts sandbox resource and ACK pressure without copying payload data",()=>{
  const value=metricsFromSnapshot({runtimes:[{diagnostics:{host:{ackProgressAgeMs:250},sidecar:{resource:{processCpuPercent:80,normalizedLoad1:0.5,eventLoopP95Ms:12,systemTotalBytes:1000,systemFreeBytes:200,processRssBytes:100},transport:{ackProgressAgeMs:300}}}}]}, {firstEventMs:{p95:400},maxGapMs:{p95:50}} as any);
  expect(value).toMatchObject({sidecarCpuP95:80,sidecarEventLoopP95Ms:12,sidecarFreeMemoryRatio:0.2,ackProgressAgeP95Ms:300,transportResidualP95Ms:400});
 });
});
