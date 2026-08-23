import { describe,expect,it } from "bun:test";
import { TransportCongestion } from "../src/v2/transport/congestion";

const s=(v:Partial<{schedulerFrames:number;schedulerBytes:number;oldestQueuedFrameMs:number;ackProgressAgeMs:number;windowFrames:number}>={})=>({schedulerFrames:0,schedulerBytes:0,oldestQueuedFrameMs:0,ackProgressAgeMs:0,windowFrames:0,...v});
describe("TransportCongestion",()=>{
 it("enters on any high threshold and clears after two healthy samples",()=>{
  const c=new TransportCongestion();expect(c.update(s())).toBe(false);expect(c.update(s({schedulerFrames:128}))).toBe(true);expect(c.update(s())).toBe(true);expect(c.update(s())).toBe(false);
 });
 it("uses byte, queue-age and active-window ACK thresholds",()=>{
  for(const value of [s({schedulerBytes:256*1024}),s({oldestQueuedFrameMs:2000}),s({windowFrames:1,ackProgressAgeMs:2000})]){const c=new TransportCongestion();expect(c.update(value)).toBe(true);}
  expect(new TransportCongestion().update(s({windowFrames:0,ackProgressAgeMs:9999}))).toBe(false);
 });
});
