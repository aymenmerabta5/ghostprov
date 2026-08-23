export interface CongestionSample {schedulerFrames:number;schedulerBytes:number;oldestQueuedFrameMs:number;ackProgressAgeMs:number;windowFrames:number}
export class TransportCongestion {#congested=false;#healthy=0;
 update(v:CongestionSample):boolean{const high=v.schedulerFrames>=128||v.schedulerBytes>=256*1024||v.oldestQueuedFrameMs>=2000||(v.windowFrames>0&&v.ackProgressAgeMs>=2000);if(high){this.#congested=true;this.#healthy=0;return true;}if(this.#congested){const low=v.schedulerFrames<=32&&v.schedulerBytes<=64*1024&&v.oldestQueuedFrameMs<=500&&(v.windowFrames===0||v.ackProgressAgeMs<=500);this.#healthy=low?this.#healthy+1:0;if(this.#healthy>=2){this.#congested=false;this.#healthy=0;}}return this.#congested;}
 get congested():boolean{return this.#congested;}
}
