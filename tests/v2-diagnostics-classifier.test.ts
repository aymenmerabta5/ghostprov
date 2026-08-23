import { describe, expect, it } from "bun:test";
import { classifyV2Diagnostics, type V2DiagnosticReport } from "../src/v2/diagnostics/classify";

const base=(overrides:Partial<V2DiagnosticReport["current"]>={}):V2DiagnosticReport=>({baseline:{sidecarCpuP95:20,sidecarEventLoopP95Ms:2,sidecarFreeMemoryRatio:0.5,sidecarRssBudgetRatio:0.2,ackProgressAgeP95Ms:100,transportResidualP95Ms:100,upstreamFirstEventP95Ms:100,webSocketSendP95Ms:5,webSocketDeliveryP95Ms:10},current:{sidecarCpuP95:20,sidecarEventLoopP95Ms:2,sidecarFreeMemoryRatio:0.5,sidecarRssBudgetRatio:0.2,ackProgressAgeP95Ms:100,transportResidualP95Ms:100,upstreamFirstEventP95Ms:100,webSocketSendP95Ms:5,webSocketDeliveryP95Ms:10,...overrides}});

describe("V2 diagnostic classifier",()=>{
 it("classifies each measured boundary",()=>{
  expect(classifyV2Diagnostics(base({sidecarCpuP95:90})).primary).toBe("sandbox_resource_pressure");
  expect(classifyV2Diagnostics(base({ackProgressAgeP95Ms:250})).primary).toBe("awareness_transport_pressure");
  expect(classifyV2Diagnostics(base({upstreamFirstEventP95Ms:250})).primary).toBe("upstream_provider_pressure");
  expect(classifyV2Diagnostics(base({webSocketSendP95Ms:80})).primary).toBe("client_backpressure");
  expect(classifyV2Diagnostics(base()).primary).toBe("no_pressure_detected");
 });
});
