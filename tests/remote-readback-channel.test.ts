import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import * as encoding from "lib0/encoding";
import type WebSocket from "ws";
import { ServerReadbackChannel } from "../src/sandbox/remote-readback-channel";
import { MAX_REMOTE_PATH_UPDATE_BYTES } from "../src/sandbox/remote-path-readback";

class Socket extends EventEmitter {
  readyState = 1;
  bufferedAmount = 0;
  readonly sent: Uint8Array[] = [];
  send(data: Uint8Array, _options: unknown, callback: (error?: Error) => void) { this.sent.push(data); callback(); }
  terminate() { this.readyState = 3; this.emit("close"); }
}
function frame(update: Uint8Array, inner = 1, outer = 0) {
  const e = encoding.createEncoder(); encoding.writeVarUint(e, outer); encoding.writeVarUint(e, inner); encoding.writeVarUint8Array(e, update); return encoding.toUint8Array(e);
}
const socketType = (s: Socket) => s as unknown as WebSocket;

describe("bounded socket-origin full document query", () => {
  it("sends only an empty-vector Step1 and consumes a Step2 before ordinary listeners", async () => {
    const s = new Socket(), channel = new ServerReadbackChannel(() => socketType(s)), order: string[] = [];
    s.on("message", () => order.push("ordinary"));
    const result = channel.request({ signal: new AbortController().signal, acceptUpdate: bytes => { expect([...bytes]).toEqual([7, 8]); order.push("verified"); return true; } });
    expect([...s.sent[0]!]).toEqual([0, 0, 1, 0]);
    s.emit("message", frame(new Uint8Array([7, 8])), true);
    await result;
    expect(order).toEqual(["verified", "ordinary"]);
    expect(s.sent).toHaveLength(1);
    expect(s.listenerCount("message")).toBe(1);
    expect(s.listenerCount("close")).toBe(0);
  });

  it("does not accept regular updates, awareness, or local events as readback", async () => {
    const s = new Socket(), stop = new AbortController(); let calls = 0;
    const result = new ServerReadbackChannel(() => socketType(s)).request({ signal: stop.signal, acceptUpdate: () => { calls++; return true; } }).then(() => null, error => error);
    s.emit("message", frame(new Uint8Array([0]), 2), true);
    s.emit("message", frame(new Uint8Array([0]), 1, 1), true);
    s.emit("sync", true);
    stop.abort(); expect(await result).toMatchObject({ reason: "aborted" }); expect(calls).toBe(0); expect(s.sent).toHaveLength(1);
  });

  it("allows a later full reply after one stale candidate without another query", async () => {
    const s = new Socket(); let calls = 0;
    const result = new ServerReadbackChannel(() => socketType(s)).request({ signal: new AbortController().signal, acceptUpdate: () => ++calls === 2 });
    s.emit("message", frame(new Uint8Array([0])), true); s.emit("message", frame(new Uint8Array([1])), true);
    await result; expect(calls).toBe(2); expect(s.sent).toHaveLength(1);
  });

  it("rejects trailing bytes and overlarge replies", async () => {
    for (const payload of [new Uint8Array([...frame(new Uint8Array([1])), 0]), frame(new Uint8Array(MAX_REMOTE_PATH_UPDATE_BYTES + 1))]) {
      const s = new Socket(); let calls = 0;
      const result = new ServerReadbackChannel(() => socketType(s)).request({ signal: new AbortController().signal, acceptUpdate: () => { calls++; return true; } }).then(() => null, error => error);
      s.emit("message", payload, true); expect(await result).toBeInstanceOf(Error);
      expect(calls).toBe(0); expect(s.listenerCount("message")).toBe(0);
    }
  });

  it("bounds stale candidate count", async () => {
    const s = new Socket(), channel = new ServerReadbackChannel(() => socketType(s));
    const result = channel.request({ signal: new AbortController().signal, acceptUpdate: () => false }).then(() => null, error => error);
    for (let n = 0; n < 3; n++) s.emit("message", frame(new Uint8Array([0])), true);
    expect(await result).toMatchObject({ reason: "wire_overflow" }); expect(s.sent).toHaveLength(1);
  });

  it("fails closed on socket replacement or closure and removes every listener", async () => {
    for (const mode of ["replace", "close"] as const) {
      const s = new Socket(); let current = s;
      const result = new ServerReadbackChannel(() => socketType(current)).request({ signal: new AbortController().signal, acceptUpdate: () => true }).then(() => null, error => error);
      if (mode === "replace") { current = new Socket(); s.emit("message", frame(new Uint8Array([0])), true); } else s.emit("close");
      expect(await result).toBeInstanceOf(Error); expect(s.listenerCount("message")).toBe(0); expect(s.listenerCount("error")).toBe(0); expect(s.listenerCount("close")).toBe(0);
    }
  });

  it("permits only one flight and fences the source on abort", async () => {
    const s = new Socket(), channel = new ServerReadbackChannel(() => socketType(s)), stop = new AbortController();
    const first = channel.request({ signal: stop.signal, acceptUpdate: () => true }).then(() => null, error => error);
    await expect(channel.request({ signal: new AbortController().signal, acceptUpdate: () => true })).rejects.toMatchObject({ reason: "busy" });
    stop.abort(); expect(await first).toMatchObject({ reason: "aborted" });
    await expect(channel.request({ signal: new AbortController().signal, acceptUpdate: () => true })).rejects.toMatchObject({ reason: "disconnected" });
    expect(s.sent).toHaveLength(1); expect(s.readyState).toBe(3);
  });
});
