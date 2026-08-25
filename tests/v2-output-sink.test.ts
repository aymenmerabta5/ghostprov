import { describe,expect,it } from "bun:test";
import { OutputBackpressure } from "../src/v2/sidecar/output-sink";

const state=(frames:number,bytes=0)=>({frames,bytes});
describe("OutputBackpressure",()=>{
 it("pauses at high water and wakes FIFO below low water",async()=>{
  const gate=new OutputBackpressure({highFrames:4,highBytes:100,lowFrames:1,lowBytes:25,hardFrames:8,hardBytes:200});gate.update(state(4));
  const order:number[]=[];const a=gate.wait().then(()=>order.push(1));const b=gate.wait().then(()=>order.push(2));await Bun.sleep(0);expect(order).toEqual([]);
  gate.update(state(1));await Promise.all([a,b]);expect(order).toEqual([1,2]);
 });
 it("removes an aborted waiter",async()=>{
  const gate=new OutputBackpressure({highFrames:1,highBytes:1,lowFrames:0,lowBytes:0,hardFrames:2,hardBytes:2});gate.update(state(1));const c=new AbortController();const waiting=gate.wait(c.signal);c.abort();await expect(waiting).rejects.toMatchObject({name:"AbortError"});
 });
 it("closes idempotently and rejects every paused waiter",async()=>{
  const gate=new OutputBackpressure({highFrames:1,highBytes:1,lowFrames:0,lowBytes:0,hardFrames:2,hardBytes:2});gate.update(state(1));
  const first=gate.wait();const second=gate.wait();gate.close();gate.close();
  await expect(first).rejects.toThrow("output backpressure closed");await expect(second).rejects.toThrow("output backpressure closed");
  expect(gate.snapshot()).toMatchObject({closed:true,paused:false,waiters:0});
  await expect(gate.wait()).rejects.toThrow("output backpressure closed");
 });
});
