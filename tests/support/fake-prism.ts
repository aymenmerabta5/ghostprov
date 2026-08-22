import { createHash } from "node:crypto";
import type { RunHandle } from "../../src/relay/run-handle";
import { SandboxSlotRunner } from "../../src/relay/slot-runner";
import type { RoutableSandboxRuntime, RoutableSandboxSnapshot } from "../../src/sandbox/registry";
import { projectId, type ProjectId } from "../../src/sandbox/types";

function sha12(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 12);
}

interface Lane {
  ack: string;
  transportId: string | null;
  responseId: string;
  terminal: boolean;
}

class FakeProjectTransport {
  readonly files = new Map<string, string>();
  readonly lanes = new Map<number, Lane>();
  readonly executionIds = new Set<string>();
  readonly duplicateIds: string[] = [];
  maxActive = 0;
  folderId = "gp";

  async readFile(name: string): Promise<string | null> { return this.files.get(name) ?? null; }
  async ensureFolder(): Promise<string> { return this.folderId; }
  async deleteFile(name: string): Promise<boolean> { return this.files.delete(name); }
  async writeFile(name: string, content: string): Promise<void> {
    this.files.set(name, content);
    if (!content) return;
    const legacy = /^job_s(\d+)\.json$/.exec(name);
    const commit = /^job_s(\d+)\.json\.([^.]+)\.0$/.exec(name);
    if (!legacy && !commit) return;
    const slot = Number((legacy ?? commit)![1]);
    let ack: string;
    let transportId: string | null = null;
    if (commit) {
      const newline = content.indexOf("\n");
      const header = JSON.parse(content.slice("GP63 ".length, newline));
      ack = header.sha12;
      transportId = header.id;
    } else {
      ack = sha12(content);
    }
    const executionId = `${slot}:${transportId ?? ack}`;
    if (this.executionIds.has(executionId)) this.duplicateIds.push(executionId);
    else this.executionIds.add(executionId);
    this.lanes.set(slot, {
      ack,
      transportId,
      responseId: `resp_${executionId.replace(/[^a-zA-Z0-9]/g, "_")}`,
      terminal: false,
    });
    this.maxActive = Math.max(this.maxActive, this.lanes.size);
  }

  async readFiles(names: string[]): Promise<Map<string, Uint8Array>> {
    const output = new Map<string, Uint8Array>();
    for (const name of names) {
      const response = /^resp_s(\d+)\.json$/.exec(name);
      if (!response) continue;
      const lane = this.lanes.get(Number(response[1]));
      if (!lane) continue;
      const envelope = lane.terminal ? this.#terminal(lane) : {
        streaming: true,
        ack: lane.ack,
        total: 0,
        start: 0,
        tail: "",
        ...(lane.transportId ? { transport: { v: 6.3, id: lane.transportId } } : {}),
      };
      output.set(name, new TextEncoder().encode(JSON.stringify(envelope)));
    }
    return output;
  }

  completeAll(): void {
    for (const lane of this.lanes.values()) lane.terminal = true;
  }

  #terminal(lane: Lane): Record<string, unknown> {
    const final = { id: lane.responseId, object: "response", status: "completed", output: [] };
    const sse = `event: response.completed\ndata: ${JSON.stringify({ type: "response.completed", response: final })}\n\n`;
    return {
      gp_final: final,
      gp_ack: lane.ack,
      gp_sse: sse,
      gp_total: sse.length,
      ...(lane.transportId ? { gp_transport: { v: 6.3, id: lane.transportId } } : {}),
    };
  }
}

export class FakePrismRuntime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  readonly transport = new FakeProjectTransport();
  readonly runner: SandboxSlotRunner;
  state = "ready";

  constructor(index: number, capacity = 30) {
    this.projectId = projectId(`00000000-0000-4000-8000-${String(index).padStart(12, "0")}`);
    this.runner = new SandboxSlotRunner({
      projectId: this.projectId,
      capacity,
      ydoc: this.transport,
      gateway: this.transport,
      daemon: {
        ensureReady: async () => ({
          build: "v-test",
          pid: index,
          capacity,
          protocols: ["segmented-v6.3-embedded-commit"],
        }),
      },
      pollMinMs: 1,
      pollMaxMs: 2,
      unlatchTimeoutMs: 10_000,
      stallTimeoutMs: 60_000,
      quarantineRecoveryMaxPolls: 0,
    });
  }

  async start(): Promise<void> {}
  async stop(): Promise<void> {}
  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle> {
    return this.runner.run(body, signal);
  }
  snapshot(): RoutableSandboxSnapshot {
    const lanes = this.runner.snapshot();
    return {
      projectId: this.projectId,
      state: this.state,
      capacity: lanes.capacity,
      active: lanes.active,
      available: lanes.available,
      quarantined: lanes.quarantined,
      heartbeatAgeMs: 1_000,
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: 1_000,
      completionEwmaMs: 10_000,
      recentFailureRate: 0,
      queueDepth: lanes.queued,
      memoryPressure: 0,
    };
  }
  completeAll(): void { this.transport.completeAll(); }
}
