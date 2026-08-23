import { describe,expect,it } from "bun:test";
import { parseFrame } from "../src/v2/protocol/validate";

const batch=(items=2)=>({kind:"response_batch",requestId:"req-1",firstEventIndex:0,items:Array.from({length:items},(_,i)=>({event:"response.output_text.delta",dataBase64Url:btoa(String(i)).replace(/=+$/g,"")})),epoch:1,sequence:0});
describe("V2 response batches",()=>{
 it("accepts at most 32 ordered logical events",()=>{
  expect((parseFrame(batch(),"sidecar") as any).items).toHaveLength(2);
  expect(()=>parseFrame(batch(33),"sidecar")).toThrow("32");
 });
 it("rejects unsafe names and oversized physical frames",()=>{
  expect(()=>parseFrame({...batch(),items:[{event:"bad event",dataBase64Url:"eA"}]},"sidecar")).toThrow("event");
  expect(()=>parseFrame({...batch(),items:[{event:"response.output_text.delta",dataBase64Url:"eA".repeat(10_000)}]},"sidecar")).toThrow("frame exceeds");
 });
});
