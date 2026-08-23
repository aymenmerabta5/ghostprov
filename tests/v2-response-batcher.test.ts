import { describe,expect,it } from "bun:test";
import { ResponseBatcher } from "../src/v2/sidecar/response-batcher";
import type { SidecarFrame } from "../src/v2/protocol/shared";

describe("ResponseBatcher",()=>{
 it("packs ordered small events and flushes explicitly",()=>{
  const frames:SidecarFrame[]=[];const b=new ResponseBatcher({emit:f=>frames.push(f),flushMs:5});
  for(let i=0;i<3;i++)expect(b.push("r",i,{event:"response.output_text.delta",data:JSON.stringify({delta:String(i)})})).toBe(true);
  b.flush("r");expect(frames).toHaveLength(1);expect(frames[0]).toMatchObject({kind:"response_batch",requestId:"r",firstEventIndex:0});expect((frames[0] as any).items).toHaveLength(3);
 });
 it("flushes at 32 events and leaves large events for segmentation",()=>{
  const frames:SidecarFrame[]=[];const b=new ResponseBatcher({emit:f=>frames.push(f),flushMs:5});
  for(let i=0;i<32;i++)expect(b.push("r",i,{event:"response.output_text.delta",data:"x"})).toBe(true);
  expect(frames).toHaveLength(1);expect(b.push("r",32,{event:"response.output_text.delta",data:"x".repeat(20_000)})).toBe(false);
 });
 it("uses a twenty-five millisecond default deadline",async()=>{
  const frames:SidecarFrame[]=[];const b=new ResponseBatcher({emit:f=>frames.push(f)});b.push("r",0,{event:"response.created",data:"{}"});await Bun.sleep(10);expect(frames).toHaveLength(0);await Bun.sleep(25);expect(frames).toHaveLength(1);
 });
});
