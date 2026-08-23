import { describe, expect, it } from "bun:test";
import { encodedDiagnosticBytes } from "../src/v2/diagnostics/types";
import { SidecarResourceSampler } from "../src/v2/diagnostics/sidecar-sampler";
import { safeDiagnostics } from "./v2-diagnostics-types.test";

describe("V2 diagnostics observer-effect budgets",()=>{
 it("stays inside fixed envelope and sampler memory ceilings",async()=>{
  expect(encodedDiagnosticBytes(safeDiagnostics())).toBeLessThanOrEqual(2_048);
  const sampler=new SidecarResourceSampler({enabled:true,intervalMs:1_000,probe:{memory:()=>({rss:1,heapUsed:1,heapTotal:1,external:1}),systemMemory:()=>null,processCpuPercent:()=>null,normalizedLoad1:()=>null}});
  expect(sampler.estimatedBytes).toBeLessThanOrEqual(1024*1024);
  await sampler.close();
 });
 it("omits every diagnostic field while disabled",async()=>{
  const sampler=new SidecarResourceSampler({enabled:false,intervalMs:1_000});
  expect(sampler.snapshot()).toEqual({processRssBytes:0,heapUsedBytes:0,heapTotalBytes:0,externalBytes:0,systemTotalBytes:null,systemFreeBytes:null,processCpuPercent:null,normalizedLoad1:null,eventLoopP50Ms:0,eventLoopP95Ms:0,eventLoopMaxMs:0});
  await sampler.close();
 });
});
