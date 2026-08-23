import { BoundedDistribution, type DistributionSnapshot } from "../v2/diagnostics/statistics";

export interface WebSocketTelemetrySnapshot { activeSessions:number; activeResponses:number; sendQueueDepthPeak:number; bufferedBytesPeak:number; delayedByBackpressure:number; enqueueToSend:DistributionSnapshot; sendCallback:DistributionSnapshot }
export class WebSocketTelemetry {
  readonly #queue:BoundedDistribution;readonly #callback:BoundedDistribution;#sessions=0;#responses=0;#queuePeak=0;#bufferPeak=0;#delayed=0;
  constructor(options:{capacity?:number}={}){this.#queue=new BoundedDistribution(options.capacity??256);this.#callback=new BoundedDistribution(options.capacity??256);}
  sessionOpened():void{this.#sessions++;} sessionClosed():void{this.#sessions=Math.max(0,this.#sessions-1);}
  responseStarted():void{this.#responses++;} responseFinished():void{this.#responses=Math.max(0,this.#responses-1);}
  sendQueued(depth:number):void{this.#queuePeak=Math.max(this.#queuePeak,depth);}
  recordSend(value:{queueMs:number;callbackMs:number;bufferedBytes:number;delayed:boolean}):void{this.#queue.record(value.queueMs);this.#callback.record(value.callbackMs);this.#bufferPeak=Math.max(this.#bufferPeak,Math.max(0,value.bufferedBytes));if(value.delayed)this.#delayed++;}
  snapshotAndReset():WebSocketTelemetrySnapshot{const value={activeSessions:this.#sessions,activeResponses:this.#responses,sendQueueDepthPeak:this.#queuePeak,bufferedBytesPeak:this.#bufferPeak,delayedByBackpressure:this.#delayed,enqueueToSend:this.#queue.snapshotAndReset(),sendCallback:this.#callback.snapshotAndReset()};this.#queuePeak=0;this.#bufferPeak=0;this.#delayed=0;return value;}
}
