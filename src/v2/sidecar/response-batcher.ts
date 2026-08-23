import type { SseFrame } from "../../relay/sse";
import { base64UrlEncode, utf8Bytes, type SidecarFrame } from "../protocol/shared";

interface BatchState { firstEventIndex:number;items:Array<{event:string;dataBase64Url:string}>;timer:ReturnType<typeof setTimeout>|null }
export class ResponseBatcher {
 readonly #emit:(frame:Extract<SidecarFrame,{kind:"response_batch"}>)=>void;readonly #flushMs:number;readonly #targetBytes:number;readonly #states=new Map<string,BatchState>();
 constructor(options:{emit:(frame:Extract<SidecarFrame,{kind:"response_batch"}>)=>void;flushMs?:number;targetBytes?:number}){this.#emit=options.emit;this.#flushMs=options.flushMs??5;this.#targetBytes=options.targetBytes??14*1024;if(this.#flushMs<1||this.#targetBytes<512||this.#targetBytes>=16*1024)throw new RangeError("invalid response batch limits");}
 push(requestId:string,eventIndex:number,event:SseFrame):boolean{
  const item={event:event.event,dataBase64Url:base64UrlEncode(utf8Bytes(event.data))};let state=this.#states.get(requestId);
  if(state&&state.firstEventIndex+state.items.length!==eventIndex){this.flush(requestId);state=undefined;}
  const candidate=(items:Array<{event:string;dataBase64Url:string}>,first:number)=>({kind:"response_batch" as const,requestId,firstEventIndex:first,items});
  if(state&&utf8Bytes(JSON.stringify(candidate([...state.items,item],state.firstEventIndex))).byteLength>this.#targetBytes){this.flush(requestId);state=undefined;}
  if(!state){if(utf8Bytes(JSON.stringify(candidate([item],eventIndex))).byteLength>this.#targetBytes)return false;state={firstEventIndex:eventIndex,items:[],timer:null};this.#states.set(requestId,state);state.timer=setTimeout(()=>this.flush(requestId),this.#flushMs);(state.timer as any).unref?.();}
  state.items.push(item);if(state.items.length>=32)this.flush(requestId);return true;
 }
 flush(requestId:string):void{const state=this.#states.get(requestId);if(!state)return;this.#states.delete(requestId);if(state.timer)clearTimeout(state.timer);if(state.items.length)this.#emit({kind:"response_batch",requestId,firstEventIndex:state.firstEventIndex,items:Object.freeze([...state.items])});}
 cancel(requestId:string):void{const state=this.#states.get(requestId);if(!state)return;this.#states.delete(requestId);if(state.timer)clearTimeout(state.timer);}
 flushAll():void{for(const id of [...this.#states.keys()])this.flush(id);}
}
