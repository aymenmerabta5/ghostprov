import type { HostFrame } from "../protocol/shared";
import type { V2TransportDiagnostics } from "./types";

/** Receive recovery requires a recently sampled exact-instance control state. */
export const V2_RECEIVE_EVIDENCE_MAX_AGE_MS = 20_000;

/** Content-free host frame categories; never retain the frame or its identity. */
export const HOST_TRANSPORT_FRAME_KINDS = [
  "request_open", "request_data", "request_commit", "request_start",
  "request_cancel", "response_ack", "credential_rotate", "drain", "ping",
] as const satisfies readonly HostFrame["kind"][];

/** Host snapshots only. These fields are not part of the sidecar wire schema. */
export interface V2HostTransportDiagnostics extends V2TransportDiagnostics {
  /** Optional for older/synthetic host snapshot producers. */
  readonly retainedPrecommitCancellations?: number;
  /** Last successful CONTROL publication, not a sidecar receipt or current policy. */
  readonly lastPublishedHostCreditCeiling?: number | null;
  readonly oldestOutstandingFrameKind?: HostFrame["kind"] | null;
  /** Null until a reliable flight has been published, and after its ACK. */
  readonly outstandingFlightAgeMs?: number | null;
  /** Age at snapshot time of the last identity- and revision-validated lane message. */
  readonly dataPeerAgeMs?: number | null;
  readonly controlPeerAgeMs?: number | null;
  /** Last validated application revision per lane; null before its first message. */
  readonly lastDataRevision?: number | null;
  readonly lastControlRevision?: number | null;
  readonly inboundAcceptedEpoch?: number;
  readonly inboundAcceptedSequence?: number;
  readonly inboundProcessedEpoch?: number;
  readonly inboundProcessedSequence?: number;
  readonly inboundAcceptedProgressAgeMs?: number | null;
  readonly inboundProcessedProgressAgeMs?: number | null;
  /** Includes frames deliberately waiting for client or event-memory capacity. */
  readonly inboundProcessingFrames?: number;
  /** Cached/repeated sidecar diagnostics never refresh this age. */
  readonly sidecarDiagnosticsAgeMs?: number | null;
  /** Continuous sampled pending work while the local receive cursor is drained. */
  readonly sidecarPendingAgeMs?: number | null;
  /** Peer data publications since this continuously pending interval began. */
  readonly sidecarPendingDataPublications?: number;
}
