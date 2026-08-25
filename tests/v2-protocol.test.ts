import { describe, expect, it } from "bun:test";
import {
  GP_V2_FRAME_MAX_BYTES,
  GP_V2_PROTOCOL,
  GP_V2_REQUEST_MAX_BYTES,
  GP_V2_R1_TRANSPORT_LIMITS,
  GP_V2_SCHEMA_DESCRIPTOR,
  GP_V2_SCHEMA_VERSION,
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  canonicalJson,
  protocolSchemaHash,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
} from "../src/v2/protocol/shared";
import {
  V2ProtocolError,
  parseAwarenessEnvelope,
  parseFrame,
} from "../src/v2/protocol/validate";

const PROJECT = "e631d12c-d917-4434-b360-41b4715d6679";
const BINDING: ProtocolBinding = {
  protocolRevision: "r1",
  launchNonce: "launch-nonce-r1",
  projectId: PROJECT,
  sessionId: "session-1",
  leaderGeneration: 7,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};

function ping(sequence = 0): ReliableFrame<HostFrame> {
  return {
    kind: "ping",
    sentAtMs: 1_000,
    epoch: 3,
    sequence,
  };
}

function validEnvelope(): AwarenessEnvelope<HostFrame> {
  return {
    protocol: GP_V2_PROTOCOL,
    schemaVersion: GP_V2_SCHEMA_VERSION,
    schemaHash: BINDING.schemaHash,
    buildHash: BINDING.buildHash,
    projectId: BINDING.projectId,
    sessionId: BINDING.sessionId,
    peerId: "host-1",
    instanceId: "host-instance-1",
    leaderGeneration: BINDING.leaderGeneration,
    epoch: 3,
    revision: 1,
    ack: { epoch: 2, sequence: 9 },
    credits: 16,
    active: 1,
    queued: 2,
    draining: false,
    frames: [ping()],
  };
}

describe("V2 shared protocol", () => {
  it("publishes the exact hard transport limits and deterministic schema hash", async () => {
    expect(GP_V2_FRAME_MAX_BYTES).toBe(16 * 1024);
    expect(GP_V2_WINDOW_MAX_FRAMES).toBe(128);
    expect(GP_V2_WINDOW_MAX_BYTES).toBe(512 * 1024);
    expect(GP_V2_REQUEST_MAX_BYTES).toBe(64 * 1024 * 1024);
    expect(await protocolSchemaHash("r1")).toMatch(/^[a-f0-9]{64}$/);
    expect(await protocolSchemaHash("r1")).toBe(await protocolSchemaHash("r1"));
    expect(canonicalJson(GP_V2_SCHEMA_DESCRIPTOR)).toContain(
      '"response_event":["requestId","eventIndex","event","partIndex","partCount","offset","dataBase64Url","diagnostics?"]',
    );
    expect(canonicalJson(GP_V2_SCHEMA_DESCRIPTOR)).toContain(
      '"livenessFields":{"ping":["sentAtMs"],"pong":["sentAtMs"]}',
    );
  });

  it("parses a fully bound host envelope", () => {
    expect(parseAwarenessEnvelope(validEnvelope(), BINDING, "host")).toEqual(validEnvelope());
  });

  it("accepts only the two independent Awareness lanes", () => {
    expect(parseAwarenessEnvelope({
      ...validEnvelope(),
      lane: "control",
    }, BINDING, "host")).toMatchObject({ lane: "control" });
    expect(parseAwarenessEnvelope({
      ...validEnvelope(),
      lane: "data",
    }, BINDING, "host")).toMatchObject({ lane: "data" });
    expect(() => parseAwarenessEnvelope({
      ...validEnvelope(),
      lane: "invalid",
    }, BINDING, "host")).toThrow("lane");
  });

  it("validates directional out-of-band liveness on frame-free control envelopes", () => {
    const hostProbe = {
      ...validEnvelope(),
      lane: "control",
      frames: [],
      liveness: { kind: "ping", sentAtMs: 1_000.25 },
    };
    expect(parseAwarenessEnvelope(hostProbe, BINDING, "host")).toMatchObject({
      lane: "control",
      frames: [],
      liveness: { kind: "ping", sentAtMs: 1_000.25 },
    });

    const sidecarProbe = {
      ...hostProbe,
      peerId: "sidecar",
      instanceId: "sidecar-instance-1",
      liveness: { kind: "pong", sentAtMs: 1_000.25 },
    };
    expect(parseAwarenessEnvelope(sidecarProbe, BINDING, "sidecar")).toMatchObject({
      lane: "control",
      frames: [],
      liveness: { kind: "pong", sentAtMs: 1_000.25 },
    });
  });

  it("rejects liveness on the wrong lane, with frames, in the wrong direction, or malformed", () => {
    const probe = {
      ...validEnvelope(),
      lane: "control",
      frames: [],
      liveness: { kind: "ping", sentAtMs: 1_000 },
    };
    const invalidHostProbes: unknown[] = [
      { ...probe, lane: undefined },
      { ...probe, lane: "data" },
      { ...probe, frames: [ping()] },
      { ...probe, liveness: { kind: "pong", sentAtMs: 1_000 } },
      { ...probe, liveness: { kind: "ping", sentAtMs: -1 } },
      { ...probe, liveness: { kind: "ping", sentAtMs: Number.NaN } },
      { ...probe, liveness: { kind: "ping", sentAtMs: Number.POSITIVE_INFINITY } },
      { ...probe, liveness: { kind: "ping", sentAtMs: "1000" } },
      { ...probe, liveness: { kind: "ping" } },
      { ...probe, liveness: { kind: "ping", sentAtMs: 1_000, sequence: 0 } },
    ];
    for (const mutation of invalidHostProbes) {
      expect(() => parseAwarenessEnvelope(mutation, BINDING, "host"))
        .toThrow(V2ProtocolError);
    }
    expect(() => parseAwarenessEnvelope({
      ...probe,
      liveness: { kind: "ping", sentAtMs: 1_000 },
    }, BINDING, "sidecar")).toThrow("must be pong");
  });

  it("rejects wrong project, schema, build, leader, and frame kinds", () => {
    const envelope = validEnvelope();
    const mutations: unknown[] = [
      { ...envelope, projectId: "69288e8b-c183-45f3-9131-d64b05b98928" },
      { ...envelope, schemaHash: "c".repeat(64) },
      { ...envelope, buildHash: "d".repeat(64) },
      { ...envelope, leaderGeneration: 8 },
      (({ revision: _revision, ...withoutRevision }) => withoutRevision)(envelope),
      { ...envelope, frames: [{ kind: "unknown", epoch: 3, sequence: 0 }] },
      { ...envelope, unexpected: true },
    ];
    for (const mutation of mutations) {
      expect(() => parseAwarenessEnvelope(mutation, BINDING, "host")).toThrow(V2ProtocolError);
    }
  });

  it("enforces exact frame and window limits", () => {
    const oversized = {
      kind: "request_data",
      requestId: "req-1",
      partIndex: 0,
      offset: 0,
      dataBase64Url: "eA".repeat(16 * 1024),
      epoch: 3,
      sequence: 0,
    };
    expect(() => parseFrame(oversized, "host", "r1")).toThrow("frame exceeds");

    const tooMany = {
      ...validEnvelope(),
      frames: Array.from({ length: 129 }, (_, index) => ping(index)),
    };
    expect(() => parseAwarenessEnvelope(tooMany, BINDING, "host")).toThrow("128-frame");

    const overFrozenR1Flight = {
      ...validEnvelope(),
      frames: Array.from({ length: 33 }, (_, index) => ping(index)),
    };
    expect(() => parseAwarenessEnvelope(overFrozenR1Flight, BINDING, "host"))
      .toThrow(`${GP_V2_R1_TRANSPORT_LIMITS.flightMaxFrames}-frame flight`);
  });

  it("validates byte-part fields and rejects conflicting structural keys", () => {
    const frame = {
      kind: "request_data",
      requestId: "req-1",
      partIndex: 0,
      offset: 0,
      dataBase64Url: "8J-Zgg",
      epoch: 3,
      sequence: 0,
    } as const;
    expect(parseFrame(frame, "host", "r1")).toEqual(frame);
    expect(() => parseFrame(frame, "host", "r3" as any)).toThrow("Unsupported");
    expect(() => parseFrame({ ...frame, dataBase64Url: "not+url/base64" }, "host", "r1")).toThrow("base64url");
    expect(() => parseFrame({ ...frame, dataBase64Url: "A" }, "host", "r1"))
      .toThrow("valid base64url");
    expect(() => parseFrame({ ...frame, dataBase64Url: "AB" }, "host", "r1"))
      .toThrow("canonical base64url tail bits");
    expect(parseFrame({ ...frame, dataBase64Url: "AA" }, "host", "r1"))
      .toMatchObject({ dataBase64Url: "AA" });
    expect(() => parseFrame({ ...frame, extra: 1 }, "host", "r1")).toThrow("unknown field");
  });

  it("accepts only a bound reliable cancellation-complete frame from the sidecar", () => {
    const frame = {
      kind: "request_cancelled",
      requestId: "req-cancelled-1",
      epoch: 3,
      sequence: 0,
    } as const;
    expect(parseFrame(frame, "sidecar", "r1")).toEqual(frame);
    expect(() => parseFrame({ ...frame, unexpected: true }, "sidecar", "r1"))
      .toThrow("unknown field");
  });

  it("accepts valid empty SSE data in event and batch frames", () => {
    const event = {
      kind: "response_event",
      requestId: "req-empty-data",
      eventIndex: 0,
      event: "response.output_text.delta",
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: "",
      epoch: 3,
      sequence: 0,
    } as const;
    const batch = {
      kind: "response_batch",
      requestId: "req-empty-data",
      firstEventIndex: 0,
      items: [{ event: "response.output_text.delta", dataBase64Url: "" }],
      epoch: 3,
      sequence: 1,
    } as const;
    expect(parseFrame(event, "sidecar", "r1")).toEqual(event);
    expect(parseFrame(batch, "sidecar", "r1")).toEqual(batch);
  });

  it("canonicalizes object keys recursively without changing array order", () => {
    expect(canonicalJson({ z: 1, a: { y: 2, x: [3, 1] } })).toBe(
      '{"a":{"x":[3,1],"y":2},"z":1}',
    );
    expect(() => canonicalJson({ invalid: Number.NaN })).toThrow("finite");
  });
});
