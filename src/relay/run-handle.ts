import type { SseFrame } from "./sse";

export interface RunHandle {
  events(): AsyncGenerator<SseFrame>;
  final: Promise<any>;
  stats: {
    bytes: number;
    firstByteMs: number | null;
    frames: number;
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
