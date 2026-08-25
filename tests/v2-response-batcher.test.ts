import { describe,expect,it } from "bun:test";
import { ResponseBatcher,streamResponseEventParts } from "../src/v2/sidecar/response-batcher";
import { base64UrlDecode,type SidecarFrame } from "../src/v2/protocol/shared";

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
 it("rejects a jumbo event in preflight and still batches the following small event",()=>{
  const frames:SidecarFrame[]=[];const b=new ResponseBatcher({emit:f=>frames.push(f),flushMs:60_000});
  const jumbo={event:"response.output_text.delta",data:"x".repeat(2*1024*1024)};
  expect(b.canBatch("r",0,jumbo)).toBe(false);
  expect(b.push("r",0,jumbo)).toBe(false);
  expect(frames).toEqual([]);
  expect(b.push("r",0,{event:"response.output_text.delta",data:"small"})).toBe(true);
  b.flush("r");
  expect(frames).toHaveLength(1);
  expect(b.push("r",1,{event:".invalid",data:"x"})).toBe(false);
 });
 it("segments UTF-8 across byte boundaries with bounded scratch",async()=>{
  const data=`a🌍b${"z".repeat(100)}\ud800end`;const parts:any[]=[];let scratch=0;
  const result=await streamResponseEventParts(data,part=>{parts.push(part);},{partBytes:7,onScratchBytes:value=>{scratch=value;}});
  const decoded=parts.map(part=>base64UrlDecode(part.dataBase64Url));const total=decoded.reduce((sum,value)=>sum+value.byteLength,0);const bytes=new Uint8Array(total);let offset=0;for(const value of decoded){bytes.set(value,offset);offset+=value.byteLength;}
  expect(bytes).toEqual(new TextEncoder().encode(data));
  expect(parts.map(part=>part.offset)).toEqual(parts.map((_part,index)=>index*7));
  expect(result).toEqual({byteCount:bytes.byteLength,partCount:parts.length});expect(scratch).toBe(7);
 });
 it("supports empty data, bounds wire parts/count, and yields during jumbo encoding",async()=>{
  const empty:any[]=[];await streamResponseEventParts("",part=>{empty.push(part);});expect(empty).toEqual([{partIndex:0,partCount:1,offset:0,dataBase64Url:""}]);
  await expect(streamResponseEventParts("x",()=>{},{partBytes:8193})).rejects.toBeInstanceOf(RangeError);
  await expect(streamResponseEventParts("x".repeat(65_537),()=>{},{partBytes:1})).rejects.toBeInstanceOf(RangeError);
  let yields=0;await streamResponseEventParts("x".repeat(200_000),()=>{},{byteCount:200_000,yieldTask:async()=>{yields++;}});expect(yields).toBeGreaterThanOrEqual(3);
  expect(()=>new ResponseBatcher({emit:()=>{},targetBytes:14*1024+1})).toThrow(RangeError);
 });
});
