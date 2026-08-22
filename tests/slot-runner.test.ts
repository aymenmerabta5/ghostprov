import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import type { Clock } from "../src/core/ports";
import { SandboxSlotRunner } from "../src/relay/slot-runner";
import { SlotPool } from "../src/sandbox/slot-pool";
import { projectId } from "../src/sandbox/types";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const B = projectId("69288e8b-c183-45f3-9131-d64b05b98928");

class FastClock implements Clock {
  value = 0;
  now(): number { return this.value; }
  async sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new DOMException("aborted", "AbortError");
    this.value += ms;
    await Promise.resolve();
  }
  async waitUntil(deadline: number, signal?: AbortSignal): Promise<void> {
    await this.sleep(Math.max(0, deadline - this.value), signal);
  }
}

class FakeGateway {
  response = "";
  reads = 0;
  async readFiles(names: string[]): Promise<Map<string, Uint8Array>> {
    this.reads++;
    return this.response && names.some(name => name.startsWith("resp_s"))
      ? new Map([[names.find(name => name.startsWith("resp_s"))!, new TextEncoder().encode(this.response)]])
      : new Map();
  }
}

class FakeYDoc {
  files = new Map<string, string>();
  writes: string[] = [];
  folder = "gp-folder";
  onWrite?: (name: string, content: string) => void;
  async readFile(name: string): Promise<string | null> { return this.files.get(name) ?? null; }
  async writeFile(name: string, content: string): Promise<void> {
    this.files.set(name, content);
    this.writes.push(name);
    this.onWrite?.(name, content);
  }
  async deleteFile(name: string): Promise<boolean> { return this.files.delete(name); }
  async ensureFolder(): Promise<string> { return this.folder; }
}

function sha12(text: string): string {
  return createHash("sha256").update(text).digest("hex").slice(0, 12);
}

function terminal(ack: string, id = "resp_test", transportId?: string): string {
  const final = { id, object: "response", status: "completed", output: [] };
  const sse = `event: response.completed\ndata: ${JSON.stringify({ type: "response.completed", response: final })}\n\n`;
  return JSON.stringify({
    gp_final: final,
    gp_ack: ack,
    gp_sse: sse,
    gp_total: sse.length,
    ...(transportId ? { gp_transport: { v: 6.3, id: transportId } } : {}),
  });
}

function runner(
  project = A,
  capacity = 2,
  clock = new FastClock(),
  extra: Record<string, unknown> = {},
) {
  const ydoc = new FakeYDoc();
  const gateway = new FakeGateway();
  let ensureCalls = 0;
  const value = new SandboxSlotRunner({
    projectId: project,
    capacity,
    ydoc,
    gateway,
    daemon: {
      ensureReady: async () => {
        ensureCalls++;
        return { build: "v-test", pid: 1, capacity, protocols: ["segmented-v6.3-embedded-commit"] };
      },
    },
    clock,
    pollMinMs: 1,
    pollMaxMs: 5,
    unlatchTimeoutMs: 10,
    stallTimeoutMs: 1_000,
    rescuePollMs: 1,
    quarantineRecoveryMaxPolls: 0,
    ...extra,
  });
  return { runner: value, ydoc, gateway, ensureCalls: () => ensureCalls, clock };
}

function completeOnPublish(value: ReturnType<typeof runner>, responseId: string): void {
  value.ydoc.onWrite = (name, content) => {
    if (/^job_s\d+\.json$/.test(name) && content) {
      value.gateway.response = terminal(sha12(content), responseId);
      return;
    }
    if (/^job_s\d+\.json\.[^.]+\.0$/.test(name)) {
      const newline = content.indexOf("\n");
      const header = JSON.parse(content.slice("GP63 ".length, newline));
      value.gateway.response = terminal(header.sha12, responseId, header.id);
    }
  };
}

describe("SlotPool", () => {
  it("never leases more than its physical capacity and keeps quarantines unavailable", async () => {
    const pool = new SlotPool(2);
    const first = await pool.acquire();
    const second = await pool.acquire();
    const third = pool.acquire();
    expect(pool.snapshot()).toMatchObject({ active: 2, available: 0, queued: 1 });
    first.quarantine();
    first.release();
    expect(pool.snapshot()).toMatchObject({ active: 1, quarantined: 1, available: 0 });
    first.recover();
    const acquired = await third;
    expect(acquired.slot).toBe(first.slot);
    second.release();
    acquired.release();
  });
});

describe("SandboxSlotRunner", () => {
  it("uses independent physical slot one in two projects", async () => {
    const left = runner(A, 1);
    const right = runner(B, 1);
    completeOnPublish(left, "resp_left");
    completeOnPublish(right, "resp_right");

    const [leftHandle, rightHandle] = await Promise.all([
      left.runner.run({ model: "gpt-5.6-sol", input: "left", stream: true }),
      right.runner.run({ model: "gpt-5.6-sol", input: "right", stream: true }),
    ]);
    const [leftFinal, rightFinal] = await Promise.all([leftHandle.final, rightHandle.final]);

    expect(leftFinal.id).toBe("resp_left");
    expect(rightFinal.id).toBe("resp_right");
    expect(left.ydoc.writes).toContain("job_s1.json");
    expect(right.ydoc.writes).toContain("job_s1.json");
    expect(left.runner.snapshot().active).toBe(0);
    expect(right.runner.snapshot().active).toBe(0);
  });

  it("prepares large v6.3 gzip payloads off the slot hot path", async () => {
    let gzipCalls = 0;
    const value = runner(A, 1, new FastClock(), {
      gzip: async (bytes: Uint8Array) => {
        gzipCalls++;
        return new Uint8Array(gzipSync(bytes, { level: 1 }));
      },
    });
    completeOnPublish(value, "resp_large");
    const handle = await value.runner.run({
      model: "gpt-5.6-sol",
      input: "compressible-context-".repeat(40_000),
      stream: true,
    });
    await expect(handle.final).resolves.toMatchObject({ id: "resp_large" });
    expect(gzipCalls).toBe(1);
    expect(value.ydoc.writes.some(name => /^job_s1\.json\.[^.]+\.0$/.test(name))).toBe(true);
  });

  it("revalidates the daemon before a no-ACK republish", async () => {
    const value = runner(A, 1);
    let publications = 0;
    value.ydoc.onWrite = (name, content) => {
      if (name !== "job_s1.json" || !content) return;
      publications++;
      if (publications === 2) value.gateway.response = terminal(sha12(content), "resp_recovered");
    };

    const handle = await value.runner.run({ model: "gpt-5.6-sol", input: "recover", stream: true });
    await expect(handle.final).resolves.toMatchObject({ id: "resp_recovered" });
    expect(publications).toBe(2);
    expect(value.ensureCalls()).toBe(2);
  });

  it("ignores a stale terminal envelope until the matching terminal arrives", async () => {
    const value = runner(A, 1);
    let ack = "";
    value.ydoc.onWrite = (name, content) => {
      if (name === "job_s1.json" && content) {
        ack = sha12(content);
        value.gateway.response = terminal("stale-ack", "resp_stale");
      }
    };
    const handle = await value.runner.run({ model: "gpt-5.6-sol", input: "match", stream: true });
    await Promise.resolve();
    value.gateway.response = terminal(ack, "resp_match");
    await expect(handle.final).resolves.toMatchObject({ id: "resp_match" });
  });

  it("quarantines a published lane when no matching terminal can be proven", async () => {
    const value = runner(A, 1);
    value.ydoc.onWrite = (name, content) => {
      if (name === "job_s1.json" && content) value.gateway.response = terminal("wrong-ack", "resp_wrong");
    };
    const handle = await value.runner.run({ model: "gpt-5.6-sol", input: "quarantine", stream: true });
    await expect(handle.final).rejects.toBeDefined();
    expect(value.runner.snapshot().quarantined).toBe(1);
  });
});
