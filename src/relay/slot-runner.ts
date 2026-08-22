import { createHash } from "node:crypto";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type { RunHandle } from "./pipeline";
import {
  planV63Job,
  type PlannedV63Job,
} from "./segments";
import {
  TerminalBackedSseExtractor,
  sanitizeFinal,
  type SseFrame,
} from "./sse";
import { matchesTerminalEnvelope } from "./terminal-rescue";
import { recoverUnlatchedPublication } from "./unlatch-recovery";
import { SlotPool, type SlotPoolSnapshot } from "../sandbox/slot-pool";
import type { DaemonDescriptor } from "../sandbox/daemon-controller";
import type { ProjectId, SlotId } from "../sandbox/types";

export interface SlotRunnerYDoc {
  readFile(name: string): Promise<string | null>;
  writeFile(name: string, content: string, options?: { folderId?: string }): Promise<void>;
  deleteFile(name: string): Promise<boolean>;
  ensureFolder(name: string): Promise<string>;
}

export interface SlotRunnerGateway {
  readFiles(names: string[]): Promise<Map<string, Uint8Array>>;
}

export interface SlotRunnerDaemon {
  ensureReady(signal?: AbortSignal): Promise<DaemonDescriptor>;
}

export interface SandboxSlotRunnerOptions {
  projectId: ProjectId;
  capacity: number;
  ydoc: SlotRunnerYDoc;
  gateway: SlotRunnerGateway;
  daemon: SlotRunnerDaemon;
  clock?: Clock;
  pollMinMs?: number;
  pollMaxMs?: number;
  unlatchTimeoutMs?: number;
  stallTimeoutMs?: number;
  rescuePollMs?: number;
  quarantineRecoveryMaxPolls?: number;
  segmentThresholdBytes?: number;
  gzip?: (bytes: Uint8Array) => Promise<Uint8Array>;
}

export interface SlotRunnerSnapshot extends SlotPoolSnapshot {
  projectId: ProjectId;
}

export class SlotRunnerError extends Error {
  constructor(
    message: string,
    readonly kind: "sync" | "timeout" | "upstream" | "capacity",
    readonly details?: unknown,
  ) {
    super(message);
    this.name = "SlotRunnerError";
  }
}

interface PollState {
  terminal: boolean;
  published: boolean;
  ack: string;
  requestId: string | null;
  responseName: string;
  jobName: string;
  plan: PlannedV63Job | null;
  serialized: string;
  artifacts: string[];
  frames: SseFrame[];
  notify: (() => void) | null;
  closed: boolean;
}

function sha12(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 12);
}

function decode(bytes: Uint8Array | undefined): string | null {
  return bytes ? new TextDecoder().decode(bytes) : null;
}

function parseEnvelope(raw: string | null): any | null {
  if (!raw || raw.length < 2) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

export class SandboxSlotRunner {
  readonly projectId: ProjectId;
  readonly #capacity: number;
  readonly #ydoc: SlotRunnerYDoc;
  readonly #gateway: SlotRunnerGateway;
  readonly #daemon: SlotRunnerDaemon;
  readonly #clock: Clock;
  readonly #pool: SlotPool;
  readonly #pollMinMs: number;
  readonly #pollMaxMs: number;
  readonly #unlatchTimeoutMs: number;
  readonly #stallTimeoutMs: number;
  readonly #rescuePollMs: number;
  readonly #quarantineRecoveryMaxPolls: number;
  readonly #segmentThresholdBytes: number;
  readonly #gzip?: (bytes: Uint8Array) => Promise<Uint8Array>;
  #folderId: string | null = null;
  #provisioning: Promise<void> | null = null;

  constructor(options: SandboxSlotRunnerOptions) {
    this.projectId = options.projectId;
    this.#capacity = options.capacity;
    this.#ydoc = options.ydoc;
    this.#gateway = options.gateway;
    this.#daemon = options.daemon;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#pool = new SlotPool(options.capacity);
    this.#pollMinMs = options.pollMinMs ?? 60;
    this.#pollMaxMs = options.pollMaxMs ?? 250;
    this.#unlatchTimeoutMs = options.unlatchTimeoutMs ?? 45_000;
    this.#stallTimeoutMs = options.stallTimeoutMs ?? 900_000;
    this.#rescuePollMs = options.rescuePollMs ?? 500;
    this.#quarantineRecoveryMaxPolls = options.quarantineRecoveryMaxPolls ?? Number.POSITIVE_INFINITY;
    this.#segmentThresholdBytes = options.segmentThresholdBytes ?? 512 * 1024;
    this.#gzip = options.gzip;
  }

  snapshot(): SlotRunnerSnapshot {
    return { projectId: this.projectId, ...this.#pool.snapshot() };
  }

  async run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle> {
    const serialized = JSON.stringify(body);
    const ack = sha12(serialized);
    const isSegmented = Buffer.byteLength(serialized, "utf8") > this.#segmentThresholdBytes;
    const gzipBytes = isSegmented && this.#gzip
      ? await this.#gzip(Buffer.from(serialized, "utf8"))
      : undefined;
    if (signal?.aborted) throw new DOMException("Relay aborted", "AbortError");
    const descriptor = await this.#daemon.ensureReady(signal);
    if (descriptor.capacity < 1) throw new SlotRunnerError("Daemon reported no capacity", "capacity");
    await this.#ensureLaneEntries();
    const lease = await this.#pool.acquire(signal);
    if (lease.slot > descriptor.capacity) {
      lease.release();
      throw new SlotRunnerError(
        `Daemon capacity ${descriptor.capacity} does not include slot ${lease.slot}`,
        "capacity",
      );
    }

    const jobName = `job_s${lease.slot}.json`;
    const responseName = `resp_s${lease.slot}.json`;
    const plan = isSegmented
      ? planV63Job(jobName, serialized, gzipBytes ? { gzipBytes } : { gzip: true })
      : null;
    const state: PollState = {
      terminal: false,
      published: false,
      ack,
      requestId: plan?.reqid ?? null,
      responseName,
      jobName,
      plan,
      serialized,
      artifacts: [],
      frames: [],
      notify: null,
      closed: false,
    };
    const stats = { bytes: 0, firstByteMs: null as number | null, frames: 0 };
    try {
      await this.#publish(state);
      state.published = true;
    } catch (error) {
      lease.release();
      throw error;
    }

    const final = this.#poll(state, stats, signal).then(
      async value => {
        await this.#clearLane(lease.slot, state);
        lease.release();
        state.closed = true;
        state.notify?.();
        return value;
      },
      async error => {
        state.closed = true;
        state.notify?.();
        if (!state.published || state.terminal) {
          await this.#clearLane(lease.slot, state).catch(() => undefined);
          lease.release();
        } else {
          lease.quarantine();
          lease.release();
          void this.#recoverQuarantine(lease, state).catch(() => undefined);
        }
        throw error;
      },
    );

    async function* events(): AsyncGenerator<SseFrame> {
      let index = 0;
      for (;;) {
        while (index < state.frames.length) yield state.frames[index++];
        if (state.closed) return;
        await new Promise<void>(resolve => { state.notify = resolve; });
        state.notify = null;
      }
    }

    return { events, final, stats };
  }

  async #ensureLaneEntries(): Promise<void> {
    if (!this.#provisioning) {
      this.#provisioning = (async () => {
        for (let slot = 1; slot <= this.#capacity; slot++) {
          for (const name of [`job_s${slot}.json`, `resp_s${slot}.json`]) {
            if (await this.#ydoc.readFile(name) === null) await this.#ydoc.writeFile(name, "");
          }
        }
      })().finally(() => { this.#provisioning = null; });
    }
    return this.#provisioning;
  }

  async #folder(): Promise<string> {
    if (!this.#folderId) this.#folderId = await this.#ydoc.ensureFolder("gp");
    return this.#folderId;
  }

  async #publish(state: PollState): Promise<void> {
    if (!state.plan) {
      await this.#ydoc.writeFile(state.jobName, state.serialized);
      return;
    }
    const folderId = await this.#folder();
    for (const part of state.plan.publishOrder) {
      await this.#ydoc.writeFile(part.base, part.text, { folderId });
      state.artifacts.push(part.base);
    }
  }

  async #republish(state: PollState): Promise<void> {
    if (!state.plan) {
      await this.#ydoc.writeFile(state.jobName, state.serialized);
      return;
    }
    await this.#ydoc.writeFile(
      state.plan.parts[0].base,
      state.plan.parts[0].text,
      { folderId: await this.#folder() },
    );
  }

  async #poll(
    state: PollState,
    stats: RunHandle["stats"],
    signal?: AbortSignal,
  ): Promise<any> {
    const extractor = new TerminalBackedSseExtractor();
    const started = this.#clock.now();
    let lastProgress = started;
    let unlatchDeadline = started + this.#unlatchTimeoutMs;
    let unlatchAttempts = 0;
    let interval = this.#pollMinMs;
    let lastRaw: string | null = null;

    for (;;) {
      if (signal?.aborted) throw new DOMException("Relay aborted", "AbortError");
      const now = this.#clock.now();
      if (now - lastProgress > this.#stallTimeoutMs) {
        throw new SlotRunnerError("Stream stalled without terminal proof", "timeout");
      }
      if (stats.firstByteMs === null && now > unlatchDeadline) {
        const recovery = await recoverUnlatchedPublication({
          attempt: unlatchAttempts,
          maxAttempts: 2,
          ensureDaemonReady: async () => {
            const descriptor = await this.#daemon.ensureReady(signal);
            if (descriptor.capacity < Number(state.jobName.match(/job_s(\d+)/)?.[1] ?? 0)) {
              throw new SlotRunnerError("Daemon capacity shrank during recovery", "capacity");
            }
          },
          republish: () => this.#republish(state),
        });
        if (!recovery.recovered) {
          throw new SlotRunnerError("Daemon never accepted the published job", "sync");
        }
        unlatchAttempts = recovery.nextAttempt;
        unlatchDeadline = this.#clock.now() + this.#unlatchTimeoutMs;
      }

      const files = await this.#gateway.readFiles([state.responseName]).catch(() => null);
      const raw = decode(files?.get(state.responseName));
      if (raw && raw !== lastRaw) {
        lastRaw = raw;
        const envelope = parseEnvelope(raw);
        if (envelope && matchesTerminalEnvelope(envelope, state.ack, state.requestId)) {
          lastProgress = this.#clock.now();
          if (stats.firstByteMs === null) stats.firstByteMs = lastProgress - started;
          if (envelope.streaming === true && typeof envelope.tail === "string") {
            const fresh = extractor.feedProgress(raw);
            for (const frame of fresh) state.frames.push(frame);
            stats.frames += fresh.length;
            stats.bytes += fresh.reduce((sum, frame) => sum + frame.data.length, 0);
            state.notify?.();
          }
          if (envelope.gp_final !== undefined || envelope.status === "failed") {
            state.terminal = true;
            if (envelope.status === "failed") {
              throw new SlotRunnerError("Upstream request failed", "upstream", envelope.error);
            }
            if (typeof envelope.gp_sse === "string" && envelope.gp_sse.length) {
              const fresh = extractor.replayTerminal(
                envelope.gp_sse,
                Number.isFinite(envelope.gp_total) ? envelope.gp_total : undefined,
              );
              for (const frame of fresh) state.frames.push(frame);
              stats.frames += fresh.length;
              stats.bytes += fresh.reduce((sum, frame) => sum + frame.data.length, 0);
              state.notify?.();
            }
            return sanitizeFinal(envelope.gp_final ?? envelope);
          }
        }
      }
      await this.#clock.sleep(interval, signal);
      interval = Math.min(this.#pollMaxMs, Math.max(this.#pollMinMs, interval * 1.3));
    }
  }

  async #clearLane(slot: SlotId, state: PollState): Promise<void> {
    await Promise.allSettled([
      this.#ydoc.writeFile(`job_s${slot}.json`, ""),
      this.#ydoc.writeFile(`resp_s${slot}.json`, ""),
      ...state.artifacts.map(name => this.#ydoc.deleteFile(name)),
    ]);
  }

  async #recoverQuarantine(
    lease: { recover(): void; slot: SlotId },
    state: PollState,
  ): Promise<void> {
    for (let polls = 0; polls < this.#quarantineRecoveryMaxPolls; polls++) {
      const files = await this.#gateway.readFiles([state.responseName]).catch(() => null);
      const envelope = parseEnvelope(decode(files?.get(state.responseName)));
      if (envelope && matchesTerminalEnvelope(envelope, state.ack, state.requestId) &&
          (envelope.gp_final !== undefined || envelope.status === "failed")) {
        state.terminal = true;
        await this.#clearLane(lease.slot, state);
        lease.recover();
        return;
      }
      await this.#clock.sleep(this.#rescuePollMs);
    }
  }
}
