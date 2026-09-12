import { describe, expect, it } from "bun:test";
import { stopGhostProv } from "../scripts/stop_ghostprov";

describe("verified PC shutdown command", () => {
  it("waits for affirmative cleanup before requesting poweroff", async () => {
    const events: string[] = [];
    const cleanup = Promise.withResolvers<void>();
    const stopping = stopGhostProv(["--poweroff"], {
      cwd: "offline-workspace", platform: "win32", log: () => {},
      requestShutdown: async (cwd, requestOptions) => {
        expect(cwd).toBe("offline-workspace"); events.push("cleanup"); await cleanup.promise;
        await requestOptions.afterCleanup?.({} as never);
      },
      powerOff: async () => { events.push("poweroff"); },
    });
    await Promise.resolve();
    expect(events).toEqual(["cleanup"]);
    cleanup.resolve();
    await stopping;
    expect(events).toEqual(["cleanup", "poweroff"]);
  });

  it.each(["timeout", "missing receipt", "failed cleanup", "changed instance"])("does not power off after %s", async message => {
    let powerOffs = 0;
    await expect(stopGhostProv(["--poweroff"], {
      platform: "win32", log: () => {},
      requestShutdown: async () => { throw new Error(message); },
      powerOff: async () => { powerOffs++; },
    })).rejects.toThrow(message);
    expect(powerOffs).toBe(0);
  });

  it("stops without powering off by default", async () => {
    let stops = 0, powerOffs = 0;
    await stopGhostProv([], {
      log: () => {}, requestShutdown: async () => { stops++; },
      powerOff: async () => { powerOffs++; },
    });
    expect(stops).toBe(1);
    expect(powerOffs).toBe(0);
  });

  it("rejects unsupported platforms or malformed arguments before stopping", async () => {
    let stops = 0;
    const options = { platform: "linux" as const, log: () => {}, requestShutdown: async () => { stops++; } };
    await expect(stopGhostProv(["--poweroff"], options)).rejects.toThrow("only on Windows");
    await expect(stopGhostProv(["--force"], options)).rejects.toThrow("Usage");
    await expect(stopGhostProv(["--poweroff", "--poweroff"], options)).rejects.toThrow("Usage");
    expect(stops).toBe(0);
  });
});
