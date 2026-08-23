import type { SidecarResourceSampler } from "./sidecar-sampler";
import type { V2EnvelopeDiagnostics, V2FirstEventDiagnostics, V2RequestAckDiagnostics, V2TerminalDiagnostics, V2TransportDiagnostics } from "./types";

interface RequestTiming { opened: number; commit?: number; assembly?: number; claimMs: number; admissionMs: number; fetch?: number; upstreamStart?: number; schedulerPeakFrames: number; schedulerPeakBytes: number; ackAgePeak: number }

const emptyTransport = (): V2TransportDiagnostics => ({ windowFrames: 0, windowBytes: 0, schedulerFrames: 0, schedulerBytes: 0, schedulerStreams: 0, oldestQueuedFrameMs: 0, ackProgressAgeMs: 0, retransmissionAttempt: 0, peakRetransmissionAttempt: 0, retransmissionsTotal: 0, publicationsTotal: 0, publishedBytesTotal: 0, admittedUncommitted: 0, roundTripEwmaMs: null, pacedPublicationsTotal: 0, urgentPublicationsTotal: 0, logicalEventsTotal: 0, responseBatchFramesTotal: 0, batchedEventsTotal: 0 });

export class SidecarDiagnosticsObserver {
  readonly #enabled: boolean; readonly #now: () => number; readonly #sampler: SidecarResourceSampler;
  readonly #requests = new Map<string, RequestTiming>(); #transport = emptyTransport(); #malformed = 0;
  constructor(options: { enabled: boolean; now?: () => number; sampler: SidecarResourceSampler }) { this.#enabled = options.enabled; this.#now = options.now ?? (() => performance.now()); this.#sampler = options.sampler; }
  get enabled(): boolean { return this.#enabled; }
  requestOpened(id: string): void { if (this.#enabled && !this.#requests.has(id)) this.#requests.set(id, { opened: this.#now(), claimMs: 0, admissionMs: 0, schedulerPeakFrames: 0, schedulerPeakBytes: 0, ackAgePeak: 0 }); }
  commitReceived(id: string): void { const r=this.#requests.get(id); if(r) r.commit=this.#now(); }
  assemblyCompleted(id: string): void { const r=this.#requests.get(id); if(r) r.assembly=this.#now(); }
  claimCompleted(id: string, ms: number): void { const r=this.#requests.get(id); if(r) r.claimMs=Math.max(0,ms); }
  admissionCompleted(id: string, ms: number): void { const r=this.#requests.get(id); if(r) r.admissionMs=Math.max(0,ms); }
  fetchStarted(id: string): void { const r=this.#requests.get(id); if(r) r.fetch=this.#now(); }
  upstreamEvent(id: string, index: number): V2FirstEventDiagnostics | undefined { const r=this.#requests.get(id); if(!r || index!==0 || r.fetch===undefined) return; return { upstreamFirstEventMs: Math.max(0,this.#now()-r.fetch), schedulerWaitMs:0 }; }
  requestAck(id:string):V2RequestAckDiagnostics|undefined { const r=this.#requests.get(id); if(!r||r.commit===undefined) return; const now=this.#now(); return { firstFrameToCommitMs:Math.max(0,r.commit-r.opened), commitToAssemblyMs:Math.max(0,(r.assembly??r.commit)-r.commit), claimSyncMs:r.claimMs, admissionWaitMs:r.admissionMs, commitProcessingMs:Math.max(0,now-r.commit) }; }
  requestTerminal(id:string):V2TerminalDiagnostics|undefined { const r=this.#requests.get(id); if(!r) return; return { upstreamTotalMs:r.fetch===undefined?0:Math.max(0,this.#now()-r.fetch), schedulerPeakFrames:r.schedulerPeakFrames, schedulerPeakBytes:r.schedulerPeakBytes, ackProgressAgePeakMs:r.ackAgePeak }; }
  transport(value: Partial<V2TransportDiagnostics>):void { if(!this.#enabled)return; this.#transport={...this.#transport,...value}; for(const r of this.#requests.values()){r.schedulerPeakFrames=Math.max(r.schedulerPeakFrames,this.#transport.schedulerFrames);r.schedulerPeakBytes=Math.max(r.schedulerPeakBytes,this.#transport.schedulerBytes);r.ackAgePeak=Math.max(r.ackAgePeak,this.#transport.ackProgressAgeMs);} }
  malformed():void { this.#malformed++; }
  envelope():V2EnvelopeDiagnostics { return { version:1, resource:this.#sampler.snapshot(), transport:{...this.#transport}, malformedCount:this.#malformed }; }
  removeRequest(id:string):void { this.#requests.delete(id); }
}
