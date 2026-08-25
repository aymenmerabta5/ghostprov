import { describe, expect, it } from "bun:test";
import {
  GP_V2_R2_TRANSPORT_LIMITS,
  GP_V2_R2_SCHEMA_VERSION,
  protocolSchemaHash,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type R2RequestCommitFrame,
  type R2RequestPreparedFrame,
  type R2RequestStartFrame,
  type ReliableFrame,
  type SidecarFrame,
} from "../src/v2/protocol/shared";
import {
  parseAwarenessEnvelope,
  parseFrame,
  V2ProtocolError,
} from "../src/v2/protocol/validate";
import { protocolBindingFromBootstrap } from "../src/v2/bootstrap/binding";
import {
  createBootstrapKeyPair,
  createCredentialRotationKey,
  openBootstrapCredential,
  sealBootstrapCredential,
} from "../src/v2/bootstrap/crypto";

const PROJECT = "e631d12c-d917-4434-b360-41b4715d6679";
const BINDING: ProtocolBinding = {
  protocolRevision: "r2",
  projectId: PROJECT,
  sessionId: "sandbox-session-r2",
  leaderGeneration: 9,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  launchNonce: "launch-r2-1",
};

function commitment(
  overrides: Partial<R2RequestCommitFrame> = {},
): ReliableFrame<R2RequestCommitFrame> {
  return {
    kind: "request_commit",
    requestId: "req-r2-1",
    protocolRevision: "r2",
    schemaVersion: GP_V2_R2_SCHEMA_VERSION,
    projectId: PROJECT,
    sessionId: BINDING.sessionId,
    leaderGeneration: BINDING.leaderGeneration,
    buildHash: BINDING.buildHash,
    schemaHash: BINDING.schemaHash,
    launchNonce: BINDING.launchNonce,
    hostInstanceId: "host-r2-1",
    targetInstanceId: "sidecar-r2-1",
    requestDigest: "c".repeat(64),
    decodedBytes: 2,
    encodedBytes: 2,
    encoding: "identity",
    partCount: 1,
    epoch: 1,
    sequence: 0,
    ...overrides,
  };
}

function start(
  overrides: Partial<R2RequestStartFrame> = {},
): ReliableFrame<R2RequestStartFrame> {
  const commit = commitment();
  return { ...commit, kind: "request_start", ...overrides };
}

function prepared(
  overrides: Partial<R2RequestPreparedFrame> = {},
): ReliableFrame<R2RequestPreparedFrame> {
  const commit = commitment();
  return { ...commit, kind: "request_prepared", ...overrides };
}

function hostEnvelope(
  overrides: Partial<AwarenessEnvelope<HostFrame>> = {},
): AwarenessEnvelope<HostFrame> {
  return {
    protocol: "gp-awareness-v2",
    protocolRevision: "r2",
    schemaVersion: GP_V2_R2_SCHEMA_VERSION,
    schemaHash: BINDING.schemaHash,
    buildHash: BINDING.buildHash,
    projectId: BINDING.projectId,
    sessionId: BINDING.sessionId,
    peerId: "host",
    instanceId: "host-r2-1",
    targetInstanceId: "sidecar-r2-1",
    launchNonce: BINDING.launchNonce,
    leaderGeneration: BINDING.leaderGeneration,
    epoch: 1,
    revision: 1,
    ack: { epoch: 1, sequence: -1 },
    credits: 16,
    active: 0,
    queued: 0,
    draining: false,
    frames: [commitment()],
    lane: "data",
    ...overrides,
  } as AwarenessEnvelope<HostFrame>;
}

describe("V2-R2 fenced wire protocol", () => {
  it("accepts only the explicit R2 fixed-flight bounds", () => {
    const ping = (sequence: number): ReliableFrame<HostFrame> => ({
      kind: "ping",
      sentAtMs: 1_000 + sequence,
      epoch: 1,
      sequence,
    });
    expect(parseAwarenessEnvelope(hostEnvelope({
      frames: Array.from(
        { length: GP_V2_R2_TRANSPORT_LIMITS.flightMaxFrames },
        (_, index) => ping(index),
      ),
    }), BINDING, "host").frames).toHaveLength(32);
    expect(() => parseAwarenessEnvelope(hostEnvelope({
      frames: Array.from(
        { length: GP_V2_R2_TRANSPORT_LIMITS.flightMaxFrames + 1 },
        (_, index) => ping(index),
      ),
    }), BINDING, "host")).toThrow("32-frame flight");

    const responseFrames = Array.from({ length: 12 }, (_, sequence) => ({
      kind: "response_event" as const,
      requestId: "req-r2-response",
      eventIndex: sequence,
      event: "response.output_text.delta",
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: "eA".repeat(6_000),
      epoch: 1,
      sequence,
    }));
    const sidecarEnvelope = (frames: readonly ReliableFrame<SidecarFrame>[]) => ({
      ...hostEnvelope({ frames: [] }),
      peerId: "sidecar",
      instanceId: "sidecar-r2-1",
      targetInstanceId: "host-r2-1",
      frames,
    }) as AwarenessEnvelope<SidecarFrame>;
    expect(parseAwarenessEnvelope(
      sidecarEnvelope(responseFrames.slice(0, 10)),
      BINDING,
      "sidecar",
    ).frames).toHaveLength(10);
    expect(() => parseAwarenessEnvelope(
      sidecarEnvelope(responseFrames),
      BINDING,
      "sidecar",
    )).toThrow("131072-byte flight");
  });

  it("parses only an exact complete R2 envelope and commit fence", () => {
    expect(parseAwarenessEnvelope(hostEnvelope(), BINDING, "host")).toEqual(hostEnvelope());
    expect(parseFrame(commitment(), "host", "r2")).toEqual(commitment());

    for (const frame of [
      commitment({ launchNonce: "stale-launch" }),
      commitment({ hostInstanceId: "foreign-host" }),
      commitment({ targetInstanceId: "foreign-sidecar" }),
      commitment({ buildHash: "d".repeat(64) }),
      commitment({ schemaHash: "e".repeat(64) }),
      commitment({ leaderGeneration: 10 }),
    ]) {
      expect(() => parseAwarenessEnvelope(
        hostEnvelope({ frames: [frame] }),
        BINDING,
        "host",
      )).toThrow(V2ProtocolError);
    }
  });

  it("parses exact full-fence prepared/start phases and cross-checks both envelopes", () => {
    const requestStart = start();
    expect(parseFrame(requestStart, "host", "r2")).toEqual(requestStart);
    expect(parseAwarenessEnvelope(
      hostEnvelope({ frames: [requestStart] }),
      BINDING,
      "host",
    ).frames).toEqual([requestStart]);

    const requestPrepared = prepared();
    const sidecarEnvelope = {
      ...hostEnvelope({ frames: [] }),
      peerId: "sidecar",
      instanceId: "sidecar-r2-1",
      targetInstanceId: "host-r2-1",
      frames: [requestPrepared],
    } as AwarenessEnvelope<SidecarFrame>;
    expect(parseFrame(requestPrepared, "sidecar", "r2")).toEqual(requestPrepared);
    expect(parseAwarenessEnvelope(sidecarEnvelope, BINDING, "sidecar").frames)
      .toEqual([requestPrepared]);

    expect(() => parseAwarenessEnvelope(hostEnvelope({
      frames: [start({ targetInstanceId: "other-sidecar" })],
    }), BINDING, "host")).toThrow("request_start fence");
    expect(() => parseAwarenessEnvelope({
      ...sidecarEnvelope,
      frames: [prepared({ hostInstanceId: "other-host" })],
    }, BINDING, "sidecar")).toThrow("request_prepared fence");
    expect(() => parseAwarenessEnvelope({
      ...sidecarEnvelope,
      frames: [prepared({ targetInstanceId: "other-sidecar" })],
    }, BINDING, "sidecar")).toThrow("request_prepared fence");

    expect(() => parseFrame(requestStart, "host", "r1")).toThrow("not part of R1");
    expect(() => parseFrame(requestPrepared, "sidecar", "r1")).toThrow("not part of R1");
  });

  it("never auto-falls back between the frozen R1 and fenced R2 parsers", () => {
    const r1Binding: ProtocolBinding = {
      ...BINDING,
      protocolRevision: "r1",
    };
    const r1Envelope: AwarenessEnvelope<HostFrame> = {
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: r1Binding.schemaHash,
      buildHash: r1Binding.buildHash,
      projectId: r1Binding.projectId,
      sessionId: r1Binding.sessionId,
      peerId: "host",
      instanceId: "host-r1",
      leaderGeneration: r1Binding.leaderGeneration,
      epoch: 1,
      revision: 1,
      ack: { epoch: 1, sequence: -1 },
      credits: 1,
      active: 0,
      queued: 0,
      draining: false,
      frames: [],
    };
    expect(() => parseAwarenessEnvelope(hostEnvelope(), r1Binding, "host"))
      .toThrow(V2ProtocolError);
    expect(() => parseAwarenessEnvelope(r1Envelope, BINDING, "host"))
      .toThrow(V2ProtocolError);
    expect(() => parseFrame({ kind: "request_commit", requestId: "req", epoch: 1, sequence: 0 }, "host", "r2"))
      .toThrow(V2ProtocolError);
  });

  it("accepts the R2 upstream prelude only on schema 2", () => {
    const prelude: ReliableFrame<SidecarFrame> = {
      kind: "upstream_prelude",
      requestId: "req-r2-1",
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: "req_upstream_1",
      epoch: 1,
      sequence: 0,
    };
    expect(parseFrame(prelude, "sidecar", "r2")).toEqual(prelude);
    expect(() => parseFrame(prelude, "sidecar", "r1")).toThrow("not part of R1");
  });

  it("accepts the reliable recovery barrier only on R2", () => {
    const marker: ReliableFrame<SidecarFrame> = {
      kind: "recovery_complete",
      epoch: 1,
      sequence: 0,
    };
    expect(parseFrame(marker, "sidecar", "r2")).toEqual(marker);
    expect(() => parseFrame(marker, "sidecar", "r1")).toThrow("R2-only");
  });

  it("retains the authenticated R2 launch nonce in the steady binding", async () => {
    expect(protocolBindingFromBootstrap({
      projectId: PROJECT,
      sandboxSessionId: BINDING.sessionId,
      leaderGeneration: BINDING.leaderGeneration,
      buildHash: BINDING.buildHash,
      schemaHash: await protocolSchemaHash("r2"),
      nonce: BINDING.launchNonce,
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
    }, "r2")).toMatchObject({
      protocolRevision: "r2",
      launchNonce: BINDING.launchNonce,
    });
    expect(await protocolSchemaHash("r2")).not.toBe(await protocolSchemaHash("r1"));
  });

  it("cryptographically authenticates the R2 revision and launch fence", async () => {
    const binding = {
      projectId: PROJECT,
      sandboxSessionId: BINDING.sessionId,
      leaderGeneration: BINDING.leaderGeneration,
      buildHash: BINDING.buildHash,
      schemaHash: await protocolSchemaHash("r2"),
      nonce: BINDING.launchNonce,
      protocolRevision: "r2" as const,
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
    };
    const sidecar = await createBootstrapKeyPair(binding);
    const sealed = await sealBootstrapCredential({
      binding,
      sidecarPublicJwk: sidecar.publicJwk,
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "secret-y-token",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
    });
    await expect(openBootstrapCredential({
      sealed,
      sidecarPrivateKey: sidecar.privateKey,
      expectedBinding: binding,
    })).resolves.toMatchObject({ token: "secret-y-token" });
    await expect(openBootstrapCredential({
      sealed,
      sidecarPrivateKey: sidecar.privateKey,
      expectedBinding: { ...binding, nonce: "stale-launch" },
    })).rejects.toThrow("binding does not match");
  });
});
