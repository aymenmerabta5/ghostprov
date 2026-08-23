import type { RunDiagnostics } from "../../relay/run-handle";
import type { V2FirstEventDiagnostics, V2RequestAckDiagnostics, V2TransportDiagnostics } from "./types";

export class HostRequestDiagnosticsObserver {
  readonly #now: () => number; readonly #start: number; #prepared:number|null=null; #published:number|null=null; #committed:number|null=null;
  readonly #value: RunDiagnostics = { hostPrepareMs:null,hostSchedulerWaitMs:null,hostPublishToAckMs:null,sidecarFirstFrameToCommitMs:null,sidecarClaimSyncMs:null,sidecarAdmissionWaitMs:null,sidecarCommitProcessingMs:null,transportCommitResidualMs:null,upstreamFirstEventMs:null,sidecarFirstEventSchedulerMs:null,hostAckToFirstFrameMs:null,hostAssemblyToDeliveryMs:null,peakTransport:null };
  constructor(now:()=>number=()=>performance.now()){this.#now=now;this.#start=now();}
  prepared():void{this.#prepared=this.#now();this.#value.hostPrepareMs=Math.max(0,this.#prepared-this.#start);}
  published():void{if(this.#published!==null)return;this.#published=this.#now();this.#value.hostSchedulerWaitMs=this.#prepared===null?null:Math.max(0,this.#published-this.#prepared);}
  committed(sidecar?:V2RequestAckDiagnostics):void{this.#committed=this.#now();const host=this.#published===null?null:Math.max(0,this.#committed-this.#published);this.#value.hostPublishToAckMs=host;if(sidecar){this.#value.sidecarFirstFrameToCommitMs=sidecar.firstFrameToCommitMs;this.#value.sidecarClaimSyncMs=sidecar.claimSyncMs;this.#value.sidecarAdmissionWaitMs=sidecar.admissionWaitMs;this.#value.sidecarCommitProcessingMs=sidecar.commitProcessingMs;this.#value.transportCommitResidualMs=host===null?null:Math.max(0,host-sidecar.commitProcessingMs);}}
  firstFrame(sidecar?:V2FirstEventDiagnostics):void{this.#value.hostAckToFirstFrameMs=this.#committed===null?null:Math.max(0,this.#now()-this.#committed);if(sidecar){this.#value.upstreamFirstEventMs=sidecar.upstreamFirstEventMs;this.#value.sidecarFirstEventSchedulerMs=sidecar.schedulerWaitMs;}}
  assemblyDelivered(ms:number):void{this.#value.hostAssemblyToDeliveryMs=Math.max(0,ms);}
  peak(value:V2TransportDiagnostics):void{this.#value.peakTransport={...value};}
  value():RunDiagnostics{return this.#value;}
}
