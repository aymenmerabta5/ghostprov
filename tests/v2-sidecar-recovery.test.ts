import { describe, expect, it } from "bun:test";
import { projectId } from "../src/sandbox/types";
import { FakeAwarenessNetwork } from "./support/fake-awareness";
import {
  GP_V2_R2_SCHEMA_VERSION,
  base64UrlEncode,
  utf8Bytes,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type R2RequestCommitFrame,
  type SidecarFrame,
} from "../src/v2/protocol/shared";
import { TranscriptProof } from "../src/v2/protocol/transcript";
import { V2TransportSession } from "../src/v2/transport/session";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");

function binding(launchNonce: string): ProtocolBinding {
  return {
    protocolRevision: "r2",
    launchNonce,
    projectId: PROJECT,
    sessionId: "same-sandbox-session",
    leaderGeneration: 7,
    buildHash: "a".repeat(64),
    schemaHash: "b".repeat(64),
  };
}

async function within<T>(value: Promise<T>, label: string): Promise<T> {
  return Promise.race([
    value,
    Bun.sleep(1_000).then(() => { throw new Error(`timed out: ${label}`); }),
  ]);
}

class RecoveryLink {
  readonly network = new FakeAwarenessNetwork<HostFrame, SidecarFrame>();
  readonly host;
  readonly sidecar;
  readonly session: V2TransportSession;
  readonly hostFrames: ReliableFrame<HostFrame>[] = [];
  hostUpdates = 0;
  credits = 1;
  #sidecarSequence = 0;
  #sidecarRevision = 0;
  #hostEpoch = 1;
  #hostAck = -1;

  constructor(
    readonly binding: ProtocolBinding,
    readonly sidecarInstanceId: string,
    readonly hostInstanceId: string,
    eventMemoryPool?: AtomicBytePermitPool,
  ) {
    this.host = this.network.connectHost(hostInstanceId, PROJECT);
    this.sidecar = this.network.connectSidecar(sidecarInstanceId, PROJECT);
    this.sidecar.subscribe(update => {
      this.hostUpdates++;
      for (const frame of update.envelope.frames) {
        if (frame.epoch !== this.#hostEpoch) {
          this.#hostEpoch = frame.epoch;
          this.#hostAck = -1;
          this.hostFrames.length = 0;
        }
        if (!this.hostFrames.some(value => value.sequence === frame.sequence)) {
          this.hostFrames.push(frame);
        }
        this.#hostAck = Math.max(this.#hostAck, frame.sequence);
      }
    });
    this.session = new V2TransportSession({
      binding,
      channel: this.host,
      hostInstanceId,
      expectedSidecarInstanceId: sidecarInstanceId,
      hostCeiling: 1,
      maxResponseBytes: 2 * 1024 * 1024,
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      eventMemoryPool,
    });
  }

  async connect(): Promise<void> {
    await this.sidecar.connect();
    await this.session.connect();
    this.publish([], "control");
    await this.settle();
  }

  publish(
    frames: readonly SidecarFrame[],
    lane: "data" | "control" = "data",
    instanceId = this.sidecarInstanceId,
    hostAck = this.#hostAck,
  ): void {
    const foreign = instanceId !== this.sidecarInstanceId;
    let foreignSequence = 0;
    const reliable = frames.map(frame => ({
      ...frame,
      epoch: 1,
      sequence: foreign ? foreignSequence++ : this.#sidecarSequence++,
    })) as ReliableFrame<SidecarFrame>[];
    const envelope: AwarenessEnvelope<SidecarFrame> = {
      protocol: "gp-awareness-v2",
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      schemaHash: this.binding.schemaHash,
      buildHash: this.binding.buildHash,
      projectId: this.binding.projectId,
      sessionId: this.binding.sessionId,
      peerId: "sidecar",
      instanceId,
      targetInstanceId: this.hostInstanceId,
      launchNonce: this.binding.launchNonce,
      leaderGeneration: this.binding.leaderGeneration,
      epoch: 1,
      revision: this.#sidecarRevision++,
      ack: { epoch: this.#hostEpoch, sequence: hostAck },
      credits: this.credits,
      active: 0,
      queued: 0,
      draining: false,
      frames: reliable,
      lane,
    };
    this.sidecar.publish(envelope, lane);
  }

  async settle(turns = 20): Promise<void> {
    for (let index = 0; index < turns; index++) {
      await this.network.flushAll();
      await Bun.sleep(1);
    }
  }

  async hostFrame<K extends HostFrame["kind"]>(kind: K): Promise<Extract<
    ReliableFrame<HostFrame>, { kind: K }
  >> {
    for (let turn = 0; turn < 100; turn++) {
      const frame = this.hostFrames.find(value => value.kind === kind);
      if (frame) return frame as Extract<ReliableFrame<HostFrame>, { kind: K }>;
      await this.settle(2);
      this.publish([], "control");
    }
    throw new Error(`missing host frame ${kind}`);
  }
}

async function committedRequest(link: RecoveryLink, signal?: AbortSignal): Promise<{
  handle: Awaited<ReturnType<V2TransportSession["run"]>>;
  requestId: string;
  requestDigest: string;
}> {
  const pending = link.session.run({ model: "gpt-5.6-sol", input: "recover" }, signal);
  const commit = await within(link.hostFrame("request_commit"), "commit") as ReliableFrame<R2RequestCommitFrame>;
  link.publish([{ ...commit, kind: "request_prepared" }]);
  await link.settle();
  const start = await within(link.hostFrame("request_start"), "start");
  link.publish([{
    kind: "request_ack",
    requestId: start.requestId,
    requestDigest: start.requestDigest,
    committed: true,
  }, {
    kind: "upstream_prelude",
    requestId: start.requestId,
    status: 200,
    contentType: "text/event-stream",
    upstreamRequestId: "upstream-original",
  }]);
  await link.settle();
  return {
    handle: await within(pending, "request ack"),
    requestId: start.requestId,
    requestDigest: start.requestDigest,
  };
}

describe("R2 same-sandbox sidecar recovery", () => {
  it("semantically ACKs a recovered terminal orphaned by a fresh host", async () => {
    const eventMemoryPool = new AtomicBytePermitPool({
      ordinaryBytes: 4 * 1024 * 1024,
      jumboBytes: 4 * 1024 * 1024,
      maxWaiters: 4,
    });
    const link = new RecoveryLink(
      binding("fresh-orphan-nonce"),
      "fresh-orphan-sidecar",
      "fresh-orphan-host",
      eventMemoryPool,
    );
    link.credits = 0;
    await link.connect();
    const controller = new AbortController();
    const queued = link.session.run({
      model: "gpt-5.6-sol",
      input: "queued before recovered replay",
    }, controller.signal);
    void queued.catch(() => undefined);
    await link.settle(2);
    expect(link.session.snapshot().queued).toBe(1);
    const requestId = "req-fresh-recovery-orphan";
    const requestDigest = "c".repeat(64);
    const completed = {
      event: "response.completed",
      data: JSON.stringify({
        type: "response.completed",
        response: { id: "resp_fresh_orphan", status: "completed", output: [] },
      }),
    };
    const transcript = new TranscriptProof(requestId, requestDigest);
    transcript.append(completed);
    const terminal = await transcript.createTerminal();

    link.publish([{
      kind: "request_ack",
      requestId,
      requestDigest,
      committed: true,
    }, {
      kind: "response_batch",
      requestId,
      firstEventIndex: 0,
      items: [{
        event: completed.event,
        dataBase64Url: base64UrlEncode(utf8Bytes(completed.data)),
      }],
    }]);
    await link.settle();
    expect(link.hostFrames.some(frame => frame.kind === "response_ack")).toBe(false);
    // Full proof metadata is derived immediately; no parsed final content is
    // retained while waiting for the separate response_terminal frame.
    expect(eventMemoryPool.snapshot().activePermits).toBe(0);

    link.publish([{ ...terminal, requestId, requestDigest }]);
    const receipt = await within(link.hostFrame("response_ack"), "fresh orphan response ACK");
    expect(receipt).toMatchObject({
      requestId,
      terminalDigest: terminal.transcriptSha256,
    });

    controller.abort();
    await expect(queued).rejects.toMatchObject({
      name: "V2PreCommitError",
      code: "cancelled_precommit",
      committed: false,
    });
    expect(eventMemoryPool.snapshot().activePermits).toBe(0);
    link.publish([], "control");
    await link.settle();
    await link.session.close();
    eventMemoryPool.close();
  });

  it("never ACKs a truncated orphan recovery terminal", async () => {
    const link = new RecoveryLink(
      binding("truncated-orphan-nonce"),
      "truncated-orphan-sidecar",
      "truncated-orphan-host",
    );
    link.credits = 0;
    await link.connect();
    const requestId = "req-truncated-recovery-orphan";
    const requestDigest = "d".repeat(64);
    const completed = {
      event: "response.completed",
      data: JSON.stringify({
        type: "response.completed",
        response: { id: "resp_truncated_orphan", status: "completed", output: [] },
      }),
    };
    const transcript = new TranscriptProof(requestId, requestDigest);
    transcript.append(completed);
    const terminal = await transcript.createTerminal();

    link.publish([{
      kind: "request_ack",
      requestId,
      requestDigest,
      committed: true,
    }, { ...terminal, requestId, requestDigest }]);
    await link.settle();

    expect(link.hostFrames.some(frame => frame.kind === "response_ack")).toBe(false);
    expect(link.session.snapshot().connected).toBe(false);
    await link.session.close();
  });

  it("never ACKs a tampered orphan recovery transcript", async () => {
    const eventMemoryPool = new AtomicBytePermitPool({
      ordinaryBytes: 4 * 1024 * 1024,
      jumboBytes: 4 * 1024 * 1024,
      maxWaiters: 4,
    });
    const link = new RecoveryLink(
      binding("tampered-orphan-nonce"),
      "tampered-orphan-sidecar",
      "tampered-orphan-host",
      eventMemoryPool,
    );
    link.credits = 0;
    await link.connect();
    const requestId = "req-tampered-recovery-orphan";
    const requestDigest = "e".repeat(64);
    const completed = {
      event: "response.completed",
      data: JSON.stringify({
        type: "response.completed",
        response: { id: "resp_tampered_orphan", status: "completed", output: [] },
      }),
    };
    const transcript = new TranscriptProof(requestId, requestDigest);
    transcript.append(completed);
    const terminal = await transcript.createTerminal();

    link.publish([{
      kind: "request_ack",
      requestId,
      requestDigest,
      committed: true,
    }, {
      kind: "response_batch",
      requestId,
      firstEventIndex: 0,
      items: [{
        event: completed.event,
        dataBase64Url: base64UrlEncode(utf8Bytes(completed.data)),
      }],
    }]);
    await link.settle();
    expect(eventMemoryPool.snapshot().activePermits).toBe(0);

    link.publish([{
      ...terminal,
      requestId,
      requestDigest,
      transcriptSha256: "f".repeat(64),
    }]);
    await link.settle();

    expect(link.hostFrames.some(frame => frame.kind === "response_ack")).toBe(false);
    expect(link.session.snapshot().connected).toBe(false);
    expect(eventMemoryPool.snapshot().activePermits).toBe(0);
    await link.session.close();
    eventMemoryPool.close();
  });

  it("never mistakes a locally owned recovery conflict for an orphan terminal", async () => {
    const source = new RecoveryLink(
      binding("owned-conflict-old"),
      "owned-conflict-sidecar-old",
      "owned-conflict-host-old",
    );
    await source.connect();
    const request = await committedRequest(source);
    const capsule = await source.session.suspendSidecarRecovery("owned conflict");
    const target = new RecoveryLink(
      binding("owned-conflict-new"),
      "owned-conflict-sidecar-new",
      "owned-conflict-host-new",
    );
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await target.connect();
    const completed = {
      event: "response.completed",
      data: JSON.stringify({
        type: "response.completed",
        response: { id: "resp_owned_conflict", status: "completed", output: [] },
      }),
    };
    const transcript = new TranscriptProof(request.requestId, request.requestDigest);
    transcript.append(completed);
    const terminal = await transcript.createTerminal();

    target.publish([{
      kind: "response_error",
      requestId: request.requestId,
      code: "definitive_recovery_error",
      message: "The retained request already settled definitively",
      ambiguous: false,
    }, {
      kind: "request_ack",
      requestId: request.requestId,
      requestDigest: request.requestDigest,
      committed: true,
    }, { ...terminal, requestId: request.requestId, requestDigest: request.requestDigest }, {
      kind: "recovery_complete",
    }]);
    await target.settle();

    await expect(request.handle.final).rejects.toMatchObject({
      code: "definitive_recovery_error",
      ambiguous: false,
    });
    expect(target.hostFrames.some(frame => frame.kind === "response_ack")).toBe(false);
    await expect(within(barrier, "owned conflict recovery barrier")).rejects.toMatchObject({
      code: "owned_terminal_conflict",
      ambiguous: true,
    });
    await target.session.close();
  });

  it("preserves the original handle, suppresses replayed prefix, and never restarts fetch", async () => {
    const oldBinding: ProtocolBinding = {
      protocolRevision: "r2",
      launchNonce: "old-launch-nonce",
      projectId: PROJECT,
      sessionId: "same-sandbox-session",
      leaderGeneration: 7,
      buildHash: "a".repeat(64),
      schemaHash: "b".repeat(64),
    };
    const source = new RecoveryLink(oldBinding, "sidecar-old", "host-old");
    await source.connect();
    expect(source.session.snapshot().credits).toBe(1);
    const pending = source.session.run({ model: "gpt-5.6-sol", input: "recover" });
    const commit = await within(source.hostFrame("request_commit"), "source commit") as ReliableFrame<R2RequestCommitFrame>;
    source.publish([{
      ...commit,
      kind: "request_prepared",
    }]);
    await source.settle();
    const start = await within(source.hostFrame("request_start"), "source start");
    source.publish([{
      kind: "request_ack",
      requestId: start.requestId,
      requestDigest: start.requestDigest,
      committed: true,
    }, {
      kind: "upstream_prelude",
      requestId: start.requestId,
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: "upstream-original",
    }]);
    await source.settle();
    const handle = await within(pending, "source request ack");
    const prefix = { event: "response.output_text.delta", data: JSON.stringify({
      type: "response.output_text.delta", delta: "prefix",
    }) };
    source.publish([{
      kind: "response_batch",
      requestId: start.requestId,
      firstEventIndex: 0,
      items: [{ event: prefix.event, dataBase64Url: base64UrlEncode(utf8Bytes(prefix.data)) }],
    }]);
    await source.settle();
    const iterator = handle.events();
    expect((await within(iterator.next(), "source prefix")).value).toEqual(prefix);

    const capsule = await within(source.session.suspendSidecarRecovery("test"), "source suspend");
    const nextBinding = { ...oldBinding, launchNonce: "fresh-launch-nonce" };
    const target = new RecoveryLink(nextBinding, "sidecar-new", "host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await within(target.connect(), "target connect");
    const suffix = { event: "response.completed", data: JSON.stringify({
      type: "response.completed",
      response: { id: "resp_recovered", status: "completed", output: [] },
    }) };
    const transcript = new TranscriptProof(start.requestId, start.requestDigest);
    transcript.append(prefix);
    transcript.append(suffix);
    const terminal = await transcript.createTerminal();
    target.publish([{
      kind: "request_ack",
      requestId: start.requestId,
      requestDigest: start.requestDigest,
      committed: true,
    }, {
      kind: "upstream_prelude",
      requestId: start.requestId,
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: null,
    }, {
      kind: "response_batch",
      requestId: start.requestId,
      firstEventIndex: 0,
      items: [prefix, suffix].map(frame => ({
        event: frame.event,
        dataBase64Url: base64UrlEncode(utf8Bytes(frame.data)),
      })),
    }, {
      ...terminal,
      requestId: start.requestId,
      requestDigest: start.requestDigest,
    }, { kind: "recovery_complete" }]);
    await target.settle();
    expect((await within(iterator.next(), "target suffix")).value).toEqual(suffix);
    expect((await within(iterator.next(), "target done")).done).toBe(true);
    await expect(handle.final).resolves.toMatchObject({ id: "resp_recovered" });
    expect(target.hostFrames.some(frame => frame.kind === "request_start")).toBe(false);

    await within(target.hostFrame("response_ack"), "target response ack");
    target.publish([], "control");
    await target.settle();
    await expect(within(barrier, "target recovery barrier")).resolves.toBeUndefined();
    await target.session.close();
  }, 10_000);

  it("turns a missing journal outcome at the recovery marker into explicit ambiguity", async () => {
    const source = new RecoveryLink(binding("missing-old-nonce"), "missing-old", "missing-host-old");
    await source.connect();
    const request = await committedRequest(source);
    const capsule = await source.session.suspendSidecarRecovery("missing-journal");
    const target = new RecoveryLink(binding("missing-new-nonce"), "missing-new", "missing-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await target.connect();
    target.publish([{ kind: "recovery_complete" }]);
    await target.settle();

    await expect(request.handle.final).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "sidecar_recovery_unresolved",
    });
    await expect(within(barrier, "missing-journal barrier")).resolves.toBeUndefined();
    expect(target.hostFrames.some(frame => frame.kind === "request_start")).toBe(false);
    await target.session.close();
  });

  it("retains a terminal-before-ACK receipt and completes without another fetch", async () => {
    const source = new RecoveryLink(binding("receipt-old-nonce"), "receipt-old", "receipt-host-old");
    await source.connect();
    const request = await committedRequest(source);
    const completed = { event: "response.completed", data: JSON.stringify({
      type: "response.completed",
      response: { id: "resp_receipt", status: "completed", output: [] },
    }) };
    const transcript = new TranscriptProof(request.requestId, request.requestDigest);
    transcript.append(completed);
    const terminal = await transcript.createTerminal();
    source.publish([{
      kind: "response_batch",
      requestId: request.requestId,
      firstEventIndex: 0,
      items: [{
        event: completed.event,
        dataBase64Url: base64UrlEncode(utf8Bytes(completed.data)),
      }],
    }, { ...terminal, requestId: request.requestId, requestDigest: request.requestDigest }]);
    await source.settle();
    await expect(request.handle.final).resolves.toMatchObject({ id: "resp_receipt" });
    await within(source.hostFrame("response_ack"), "source unacknowledged receipt");

    const capsule = await source.session.suspendSidecarRecovery("terminal-before-ack");
    const target = new RecoveryLink(binding("receipt-new-nonce"), "receipt-new", "receipt-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await target.connect();
    target.publish([
      { ...terminal, requestId: request.requestId, requestDigest: request.requestDigest },
      { kind: "recovery_complete" },
    ]);
    await target.settle();
    await within(target.hostFrame("response_ack"), "replayed terminal receipt");
    expect(target.hostFrames.some(frame => frame.kind === "request_start")).toBe(false);
    target.publish([], "control");
    await target.settle();
    await expect(within(barrier, "terminal receipt barrier")).resolves.toBeUndefined();
    await target.session.close();
  });

  it("preserves FETCH_STARTING ambiguity without issuing another request_start", async () => {
    const source = new RecoveryLink(binding("fetch-old-nonce"), "fetch-old", "fetch-host-old");
    await source.connect();
    const request = await committedRequest(source);
    const capsule = await source.session.suspendSidecarRecovery("fetch-starting");
    const target = new RecoveryLink(binding("fetch-new-nonce"), "fetch-new", "fetch-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await target.connect();
    target.publish([{
      kind: "request_ack",
      requestId: request.requestId,
      requestDigest: request.requestDigest,
      committed: true,
    }, {
      kind: "response_error",
      requestId: request.requestId,
      code: "recovered_ambiguous",
      message: "Fetch started without a proven terminal",
      ambiguous: true,
    }, { kind: "recovery_complete" }]);
    await target.settle();

    await expect(request.handle.final).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "recovered_ambiguous",
    });
    await expect(within(barrier, "fetch-starting barrier")).resolves.toBeUndefined();
    expect(target.hostFrames.some(frame => frame.kind === "request_start")).toBe(false);
    await target.session.close();
  });

  it("rejects a mismatched replay prefix before exposing duplicate output", async () => {
    const source = new RecoveryLink(binding("mismatch-old-nonce"), "mismatch-old", "mismatch-host-old");
    await source.connect();
    const request = await committedRequest(source);
    const original = { event: "response.output_text.delta", data: "original-prefix" };
    source.publish([{
      kind: "response_batch",
      requestId: request.requestId,
      firstEventIndex: 0,
      items: [{ event: original.event, dataBase64Url: base64UrlEncode(utf8Bytes(original.data)) }],
    }]);
    await source.settle();
    const iterator = request.handle.events();
    expect((await within(iterator.next(), "original prefix")).value).toEqual(original);
    const capsule = await source.session.suspendSidecarRecovery("prefix-mismatch");
    const target = new RecoveryLink(binding("mismatch-new-nonce"), "mismatch-new", "mismatch-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await target.connect();
    target.publish([{
      kind: "request_ack",
      requestId: request.requestId,
      requestDigest: request.requestDigest,
      committed: true,
    }, {
      kind: "upstream_prelude",
      requestId: request.requestId,
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: null,
    }, {
      kind: "response_batch",
      requestId: request.requestId,
      firstEventIndex: 0,
      items: [{
        event: original.event,
        dataBase64Url: base64UrlEncode(utf8Bytes("tampered-prefix")),
      }],
    }, { kind: "recovery_complete" }]);
    await target.settle();

    await expect(request.handle.final).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "recovery_prefix_mismatch",
    });
    await expect(within(barrier, "prefix mismatch barrier")).resolves.toBeUndefined();
    await target.session.close();
  });

  it("ignores a stale old sidecar and waits for the elected recovery marker", async () => {
    const source = new RecoveryLink(binding("stale-old-nonce"), "stale-old", "stale-host-old");
    await source.connect();
    const capsule = await source.session.suspendSidecarRecovery("stale-peer");
    const target = new RecoveryLink(binding("stale-new-nonce"), "stale-new", "stale-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await target.connect();
    target.publish([{ kind: "recovery_complete" }], "data", "stale-old");
    await target.settle();
    expect(await Promise.race([
      barrier.then(() => "settled" as const),
      Promise.resolve("pending" as const),
    ])).toBe("pending");

    target.publish([{ kind: "recovery_complete" }]);
    await target.settle();
    await expect(within(barrier, "elected marker")).resolves.toBeUndefined();
    await target.session.close();
  });

  it("retains request_start risk until recovered journal output decides the pending run", async () => {
    const source = new RecoveryLink(binding("start-risk-old"), "start-risk-sidecar-old", "start-risk-host-old");
    await source.connect();
    const pending = source.session.run({ model: "gpt-5.6-sol", input: "start-risk" });
    void pending.catch(() => undefined);
    const commit = await source.hostFrame("request_commit") as ReliableFrame<R2RequestCommitFrame>;
    source.publish([{ ...commit, kind: "request_prepared" }]);
    await source.settle();
    const start = await source.hostFrame("request_start");

    const capsule = await source.session.suspendSidecarRecovery("start-risk-before-ack");
    expect(await Promise.race([
      pending.then(() => "settled" as const, () => "settled" as const),
      Promise.resolve("pending" as const),
    ])).toBe("pending");

    const target = new RecoveryLink(binding("start-risk-new"), "start-risk-sidecar-new", "start-risk-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await target.connect();
    target.publish([{
      kind: "request_ack",
      requestId: start.requestId,
      requestDigest: start.requestDigest,
      committed: true,
    }, {
      kind: "response_error",
      requestId: start.requestId,
      code: "provider_declined",
      message: "Recovered provider rejection",
      ambiguous: false,
    }, { kind: "recovery_complete" }]);
    await target.settle();

    const handle = await within(pending, "recovered pending run ACK");
    await expect(handle.final).rejects.toMatchObject({
      committed: true,
      ambiguous: false,
      code: "provider_declined",
    });
    await expect(within(barrier, "start-risk barrier")).resolves.toBeUndefined();
    expect(target.hostFrames.some(frame => frame.kind === "request_start")).toBe(false);
    await target.session.close();
  });

  it("carries ambiguity tombstones across repeated in-place sidecar generations", async () => {
    const source = new RecoveryLink(binding("tombstone-old"), "tombstone-sidecar-old", "tombstone-host-old");
    await source.connect();
    const request = await committedRequest(source);
    source.publish([{
      kind: "response_error",
      requestId: request.requestId,
      code: "unknown_fetch_outcome",
      message: "Execution outcome is ambiguous",
      ambiguous: true,
    }]);
    await source.settle();
    await expect(request.handle.final).rejects.toMatchObject({ ambiguous: true });
    expect(source.session.snapshot().ambiguousQuarantined).toBe(1);

    const capsule = await source.session.suspendSidecarRecovery("carry-tombstone");
    const target = new RecoveryLink(binding("tombstone-new"), "tombstone-sidecar-new", "tombstone-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    expect(target.session.snapshot().ambiguousQuarantined).toBe(1);
    await target.connect();
    target.publish([{ kind: "recovery_complete" }]);
    await target.settle();
    await expect(within(barrier, "tombstone barrier")).resolves.toBeUndefined();
    expect(target.session.snapshot().ambiguousQuarantined).toBe(1);
    await target.session.close();
  });

  it("ACKs a verified recovered terminal for a client-cancelled cleanup-only state", async () => {
    const source = new RecoveryLink(binding("cancel-old"), "cancel-sidecar-old", "cancel-host-old");
    await source.connect();
    const controller = new AbortController();
    const request = await committedRequest(source, controller.signal);
    const cancelledFinal = request.handle.final;
    void cancelledFinal.catch(() => undefined);
    controller.abort();

    const capsule = await within(
      source.session.suspendSidecarRecovery("cancelled-cleanup"),
      "cancelled source suspend",
    );
    const target = new RecoveryLink(binding("cancel-new"), "cancel-sidecar-new", "cancel-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    await within(target.connect(), "cancelled target connect");
    const completed = { event: "response.completed", data: JSON.stringify({
      type: "response.completed",
      response: { id: "resp_cancel_cleanup", status: "completed", output: [] },
    }) };
    const transcript = new TranscriptProof(request.requestId, request.requestDigest);
    transcript.append(completed);
    const terminal = await transcript.createTerminal();
    target.publish([{
      kind: "request_ack",
      requestId: request.requestId,
      requestDigest: request.requestDigest,
      committed: true,
    }, {
      kind: "response_batch",
      requestId: request.requestId,
      firstEventIndex: 0,
      items: [{
        event: completed.event,
        dataBase64Url: base64UrlEncode(utf8Bytes(completed.data)),
      }],
    }, { ...terminal, requestId: request.requestId, requestDigest: request.requestDigest }, {
      kind: "recovery_complete",
    }]);
    await target.settle();
    await within(target.hostFrame("response_ack"), "cancelled terminal ACK");
    target.publish([], "control");
    await target.settle();
    await expect(within(barrier, "cancelled cleanup barrier")).resolves.toBeUndefined();
    await expect(cancelledFinal).rejects.toMatchObject({ code: "cancelled" });
    await within(target.session.close(), "cancelled target close");
  });

  it("does not finish graceful drain before a terminal receipt is cumulatively ACKed", async () => {
    const link = new RecoveryLink(binding("drain-receipt"), "drain-sidecar", "drain-host");
    await link.connect();
    const request = await committedRequest(link);
    const completed = { event: "response.completed", data: JSON.stringify({
      type: "response.completed",
      response: { id: "resp_drain_receipt", status: "completed", output: [] },
    }) };
    const transcript = new TranscriptProof(request.requestId, request.requestDigest);
    transcript.append(completed);
    const terminal = await transcript.createTerminal();
    link.publish([{
      kind: "response_batch",
      requestId: request.requestId,
      firstEventIndex: 0,
      items: [{
        event: completed.event,
        dataBase64Url: base64UrlEncode(utf8Bytes(completed.data)),
      }],
    }, { ...terminal, requestId: request.requestId, requestDigest: request.requestDigest }]);
    await link.settle();
    await expect(request.handle.final).resolves.toMatchObject({ id: "resp_drain_receipt" });
    const receipt = await link.hostFrame("response_ack");

    const draining = link.session.drain("receipt-barrier");
    void draining.catch(() => undefined);
    await link.settle();
    await link.hostFrame("drain");
    link.publish([{ kind: "drained", active: 0, queued: 0 }], "data", link.sidecarInstanceId, receipt.sequence - 1);
    await link.settle();
    expect(await Promise.race([
      draining.then(() => "settled" as const),
      Promise.resolve("pending" as const),
    ])).toBe("pending");

    link.publish([], "control");
    await link.settle();
    await expect(within(draining, "receipt-aware drain")).resolves.toBeUndefined();
  });

  it("freezes old publication atomically and completes a capsule after post-boundary abort", async () => {
    const source = new RecoveryLink(binding("atomic-suspend"), "atomic-sidecar", "atomic-host");
    await source.connect();
    let releaseClose!: () => void;
    const closeGate = new Promise<void>(resolve => { releaseClose = resolve; });
    const originalClose = source.host.close.bind(source.host);
    (source.host as { close(): Promise<void> }).close = async () => {
      await closeGate;
      await originalClose();
    };

    const pending = source.session.run({ model: "gpt-5.6-sol", input: "atomic" });
    void pending.catch(() => undefined);
    const commit = await source.hostFrame("request_commit") as ReliableFrame<R2RequestCommitFrame>;
    source.publish([{ ...commit, kind: "request_prepared" }]);
    const flushingPrepared = source.network.flushAll();
    const publicationsAtBoundary = source.hostUpdates;
    const controller = new AbortController();
    const recovery = source.session.suspendSidecarRecovery("atomic-boundary", controller.signal);
    controller.abort();
    await flushingPrepared;
    await Bun.sleep(5);
    expect(source.hostUpdates).toBe(publicationsAtBoundary);

    releaseClose();
    const capsule = await within(recovery, "post-boundary capsule");
    await expect(pending).rejects.toMatchObject({
      committed: false,
      code: "sidecar_recovery",
    });
    source.session.failSidecarRecovery(capsule, new Error("test cleanup"));
  });

  it("invalidates queued frame processing before close can recreate a terminal receipt", async () => {
    const link = new RecoveryLink(binding("close-epoch"), "close-epoch-sidecar", "close-epoch-host");
    await link.connect();
    const request = await committedRequest(link);
    void request.handle.final.catch(() => undefined);
    const completed = { event: "response.completed", data: JSON.stringify({
      type: "response.completed",
      response: { id: "resp_close_epoch", status: "completed", output: [] },
    }) };
    const transcript = new TranscriptProof(request.requestId, request.requestDigest);
    transcript.append(completed);
    const terminal = await transcript.createTerminal();
    link.publish([{
      kind: "response_batch",
      requestId: request.requestId,
      firstEventIndex: 0,
      items: [{ event: completed.event, dataBase64Url: base64UrlEncode(utf8Bytes(completed.data)) }],
    }, { ...terminal, requestId: request.requestId, requestDigest: request.requestDigest }]);
    const flushing = link.network.flushAll();
    const closing = link.session.close();
    await flushing;
    await within(closing, "epoch close");
    await Bun.sleep(5);
    expect(link.hostFrames.some(frame => frame.kind === "response_ack")).toBe(false);
    await expect(request.handle.final).rejects.toMatchObject({ code: "closed" });
  });

  it("captures ambiguity classified while the frozen processing tail drains", async () => {
    const source = new RecoveryLink(binding("tail-tombstone-old"), "tail-sidecar-old", "tail-host-old");
    await source.connect();
    const request = await committedRequest(source);
    void request.handle.final.catch(() => undefined);
    source.publish([{
      kind: "response_error",
      requestId: request.requestId,
      code: "tail_ambiguity",
      message: "Ambiguity arrived at the suspension boundary",
      ambiguous: true,
    }]);
    const flushing = source.network.flushAll();
    const capsulePromise = source.session.suspendSidecarRecovery("tail-tombstone");
    await flushing;
    const capsule = await within(capsulePromise, "tail tombstone capsule");
    await expect(request.handle.final).rejects.toMatchObject({ code: "tail_ambiguity" });

    const target = new RecoveryLink(binding("tail-tombstone-new"), "tail-sidecar-new", "tail-host-new");
    const barrier = target.session.adoptRecoveredSidecar(capsule);
    expect(target.session.snapshot().ambiguousQuarantined).toBe(1);
    await target.connect();
    target.publish([{ kind: "recovery_complete" }]);
    await target.settle();
    await expect(within(barrier, "tail tombstone barrier")).resolves.toBeUndefined();
    await target.session.close();
  });

  it("zeroes retired windows and scheduler state after capsule transfer", async () => {
    const source = new RecoveryLink(binding("retired-zero"), "retired-sidecar", "retired-host");
    await source.connect();
    const pending = source.session.run({ model: "gpt-5.6-sol", input: "sensitive prompt" });
    void pending.catch(() => undefined);
    await source.hostFrame("request_commit");
    const rotating = source.session.rotateCredential(
      "retired-rotation",
      "sealed-sensitive-credential",
    );
    void rotating.catch(() => undefined);

    const capsule = await source.session.suspendSidecarRecovery("retired-zero");
    await expect(pending).rejects.toMatchObject({ code: "sidecar_recovery" });
    await expect(rotating).rejects.toMatchObject({ code: "instance_changed" });
    expect(source.session.snapshot().diagnostics?.host).toMatchObject({
      windowFrames: 0,
      windowBytes: 0,
      schedulerFrames: 0,
      schedulerBytes: 0,
      schedulerStreams: 0,
    });
    await source.session.close();
    source.session.failSidecarRecovery(capsule, new Error("test cleanup"));
  });

  it("routes protocol failure through full channel teardown", async () => {
    const link = new RecoveryLink(binding("protocol-teardown"), "protocol-sidecar", "protocol-host");
    await link.connect();
    const pending = link.session.run({ model: "gpt-5.6-sol", input: "protocol" });
    void pending.catch(() => undefined);
    await link.hostFrame("request_commit");
    link.sidecar.publish({
      protocol: "gp-awareness-v2",
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      schemaHash: "f".repeat(64),
      buildHash: link.binding.buildHash,
      projectId: link.binding.projectId,
      sessionId: link.binding.sessionId,
      peerId: "sidecar",
      instanceId: link.sidecarInstanceId,
      targetInstanceId: link.hostInstanceId,
      launchNonce: link.binding.launchNonce,
      leaderGeneration: link.binding.leaderGeneration,
      epoch: 1,
      revision: 999,
      ack: { epoch: 1, sequence: -1 },
      credits: 0,
      active: 0,
      queued: 0,
      draining: false,
      frames: [],
      lane: "control",
    });
    await link.network.flushAll();
    for (let turn = 0; turn < 100 && link.session.snapshot().connected; turn++) {
      await Bun.sleep(1);
    }
    expect(link.session.snapshot().connected).toBe(false);
    expect(link.sidecar.peerIds()).toEqual([]);
    await expect(pending).rejects.toMatchObject({ code: "protocol" });
  });
});
