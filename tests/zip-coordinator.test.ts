import { describe, expect, it, vi } from "bun:test";
import { ZipPollCoordinator } from "../src/prism/zip-coordinator";

const bytes = (value: string) => new TextEncoder().encode(value);

describe("ZipPollCoordinator", () => {
  it("coalesces simultaneous filename reads into one archive fetch", async () => {
    const fetcher = vi.fn(async (names: string[]) => new Map(names.map(name => [name, bytes(name)])));
    const coordinator = new ZipPollCoordinator(fetcher, 5);
    const reads = Array.from({ length: 8 }, (_, index) => coordinator.read([`resp_s${index + 1}.json`]));
    const results = await Promise.all(reads);

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0][0]).toEqual(Array.from({ length: 8 }, (_, index) => `resp_s${index + 1}.json`));
    expect(results.map((result, index) => new TextDecoder().decode(result.get(`resp_s${index + 1}.json`)))).toEqual(
      Array.from({ length: 8 }, (_, index) => `resp_s${index + 1}.json`),
    );
  });

  it("coalesces duplicate names and returns only each caller's requested subset", async () => {
    const fetcher = vi.fn(async (names: string[]) => new Map(names.map(name => [name, bytes(name)])));
    const coordinator = new ZipPollCoordinator(fetcher, 0);
    const [left, right] = await Promise.all([
      coordinator.read(["shared", "left"]),
      coordinator.read(["shared", "right"]),
    ]);
    expect(fetcher.mock.calls[0][0]).toEqual(["shared", "left", "right"]);
    expect([...left.keys()]).toEqual(["shared", "left"]);
    expect([...right.keys()]).toEqual(["shared", "right"]);
  });

  it("isolates a failed batch from the next batch", async () => {
    const fetcher = vi.fn()
      .mockRejectedValueOnce(new Error("zip unavailable"))
      .mockResolvedValueOnce(new Map([["next", bytes("ok")]]));
    const coordinator = new ZipPollCoordinator(fetcher, 0);
    await expect(coordinator.read(["first"])).rejects.toThrow("zip unavailable");
    await expect(coordinator.read(["next"])).resolves.toEqual(new Map([["next", bytes("ok")]]));
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
});
