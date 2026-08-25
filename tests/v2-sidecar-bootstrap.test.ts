import { describe, expect, it } from "bun:test";
import { abortableSleep } from "../src/v2/bootstrap/sleep";
import { resolveSidecarBootstrapHelloPath } from "../sandbox/v2/bootstrap";

class TrackingSignal {
  aborted = false;
  added = 0;
  removed = 0;

  addEventListener(_event: string, _listener: EventListenerOrEventListenerObject): void {
    this.added++;
  }

  removeEventListener(_event: string, _listener: EventListenerOrEventListenerObject): void {
    this.removed++;
  }
}

describe("Deno sidecar bootstrap helpers", () => {
  it("removes the abort listener after a normal polling sleep", async () => {
    const signal = new TrackingSignal();

    await abortableSleep(0, signal as unknown as AbortSignal);

    expect(signal.added).toBe(1);
    expect(signal.removed).toBe(1);
  });

  it("binds overlapping R2 hello files to the exact launch nonce and fork PID", () => {
    const launchNonce = "launch-nonce-7";
    const first = resolveSidecarBootstrapHelloPath({
      launchNonce,
      pid: 401,
      args: [
        `--ghostprov-launch-nonce=${launchNonce}`,
        "--ghostprov-process-start=9001",
      ],
    });
    const second = resolveSidecarBootstrapHelloPath({
      launchNonce,
      pid: 402,
      args: [
        `--ghostprov-launch-nonce=${launchNonce}`,
        "--ghostprov-process-start=9002",
      ],
    });

    expect(first).toEqual({
      helloPath: "/tmp/ghostprov-v2/bootstrap-hello.launch-nonce-7.401",
      processStart: "9001",
    });
    expect(second).toEqual({
      helloPath: "/tmp/ghostprov-v2/bootstrap-hello.launch-nonce-7.402",
      processStart: "9002",
    });
    expect(first).not.toBe(second);
  });

  it("rejects a missing, stale, duplicated, or malformed launcher identity", () => {
    const valid = {
      launchNonce: "launch-nonce-8",
      pid: 501,
    };
    expect(() => resolveSidecarBootstrapHelloPath({ ...valid, args: [] }))
      .toThrow(/launch argument/i);
    expect(() => resolveSidecarBootstrapHelloPath({
      ...valid,
      args: ["--ghostprov-launch-nonce=stale", "--ghostprov-process-start=1"],
    })).toThrow(/launch argument/i);
    expect(() => resolveSidecarBootstrapHelloPath({
      ...valid,
      args: [
        "--ghostprov-launch-nonce=launch-nonce-8",
        "--ghostprov-process-start=1",
        "--ghostprov-process-start=1",
      ],
    })).toThrow(/launch argument/i);
    expect(() => resolveSidecarBootstrapHelloPath({
      launchNonce: "../../escape",
      pid: 501,
      args: ["--ghostprov-launch-nonce=../../escape", "--ghostprov-process-start=1"],
    })).toThrow(/nonce/i);
    expect(() => resolveSidecarBootstrapHelloPath({
      launchNonce: "launch-nonce-8",
      pid: 1,
      args: ["--ghostprov-launch-nonce=launch-nonce-8", "--ghostprov-process-start=1"],
    })).toThrow(/PID/i);
  });
});
