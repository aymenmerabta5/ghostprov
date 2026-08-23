export interface V2DiagnosticMetrics { sidecarCpuP95:number|null;sidecarEventLoopP95Ms:number;sidecarFreeMemoryRatio:number|null;sidecarRssBudgetRatio:number|null;ackProgressAgeP95Ms:number;transportResidualP95Ms:number;upstreamFirstEventP95Ms:number;webSocketSendP95Ms:number;webSocketDeliveryP95Ms:number }
export interface V2DiagnosticReport { baseline:V2DiagnosticMetrics;current:V2DiagnosticMetrics }
export interface V2DiagnosticClassification { primary:"sandbox_resource_pressure"|"awareness_transport_pressure"|"upstream_provider_pressure"|"client_backpressure"|"mixed_pressure"|"no_pressure_detected";triggered:readonly string[];evidence:Readonly<Record<string,number|null>> }
const ratio=(value:number,base:number)=>base>0?value/base:(value>0?Number.POSITIVE_INFINITY:1);
export function classifyV2Diagnostics(report:V2DiagnosticReport):V2DiagnosticClassification{
 const b=report.baseline,c=report.current;const transportRatio=Math.max(ratio(c.ackProgressAgeP95Ms,b.ackProgressAgeP95Ms),ratio(c.transportResidualP95Ms,b.transportResidualP95Ms));
 const sandbox=(c.sidecarCpuP95??0)>=85||c.sidecarEventLoopP95Ms>=20||((c.sidecarFreeMemoryRatio??1)<0.1&&transportRatio>=2)||((c.sidecarRssBudgetRatio??0)>=0.8&&transportRatio>=2);
 const transport=!sandbox&&transportRatio>=2;
 const upstream=ratio(c.upstreamFirstEventP95Ms,b.upstreamFirstEventP95Ms)>=2&&transportRatio<1.25&&ratio(c.webSocketDeliveryP95Ms,b.webSocketDeliveryP95Ms)<1.25;
 const client=c.webSocketSendP95Ms>50;
 const triggered=[...(sandbox?["sandbox_resource_pressure"]:[]),...(transport?["awareness_transport_pressure"]:[]),...(upstream?["upstream_provider_pressure"]:[]),...(client?["client_backpressure"]:[])];
 return {primary:triggered.length===0?"no_pressure_detected":triggered.length>1?"mixed_pressure":triggered[0] as V2DiagnosticClassification["primary"],triggered,evidence:{transportRatio,upstreamRatio:ratio(c.upstreamFirstEventP95Ms,b.upstreamFirstEventP95Ms),webSocketRatio:ratio(c.webSocketDeliveryP95Ms,b.webSocketDeliveryP95Ms),sidecarCpuP95:c.sidecarCpuP95}};
}
