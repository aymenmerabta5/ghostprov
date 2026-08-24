import type { SseFrame } from "./sse";
import type { V2TransportDiagnostics } from "../v2/diagnostics/types";
import type { RateLimitHint } from "../v2/rate-limit";

export interface RunDiagnostics {
  hostPrepareMs: number | null; hostSchedulerWaitMs: number | null; hostPublishToAckMs: number | null;
  sidecarFirstFrameToCommitMs: number | null; sidecarClaimSyncMs: number | null; sidecarAdmissionWaitMs: number | null; sidecarCommitProcessingMs: number | null;
  transportCommitResidualMs: number | null; upstreamFirstEventMs: number | null; sidecarFirstEventSchedulerMs: number | null;
  hostAckToFirstFrameMs: number | null; hostAssemblyToDeliveryMs: number | null; peakTransport: V2TransportDiagnostics | null;
}

export interface RunHandle {
  events(): AsyncGenerator<SseFrame>;
  final: Promise<any>;
  /** Latest upstream rate-limit headers, kept out of the public Responses body. */
  rateLimit?: RateLimitHint;
  stats: {
    bytes: number;
    firstByteMs: number | null;
    frames: number;
    diagnostics?: RunDiagnostics;
  };
}

export class RelayError extends Error {
  constructor(
    message: string,
    readonly kind: "sync" | "render" | "timeout" | "no-bridge" | "upstream" | "exhausted",
    readonly details?: unknown,
  ) {
    super(message);
    this.name = "RelayError";
  }
}
