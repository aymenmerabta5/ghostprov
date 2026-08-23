import { createHash, type Hash } from "node:crypto";

export type StreamBlockKind = "thinking" | "text" | "tool_use";

export type StreamBlock = {
  kind: StreamBlockKind;
  itemId: string | null;
  outputIndex: number;
  contentIndex: number | null;
  started: boolean;
  stopped: boolean;
  done: boolean;
  suppressed: boolean;
  forceStart: boolean;
  index: number | null;
  pending: string[];
  pendingBytes: number;
  emittedUnits: number;
  fingerprint: Hash;
  seenUnits: number;
  signature: string | null;
  signatureEmitted: boolean;
  toolId: string | null;
  toolName: string | null;
  argumentDeltaSeen: boolean;
  argumentUnits: number;
  argumentBytes: number;
};

export function streamBlockIdentity(
  kind: StreamBlockKind,
  itemId: string | null,
  outputIndex: number,
  contentIndex: number | null,
): string {
  return `${kind}:${itemId ?? `@${outputIndex}`}:${contentIndex ?? "-"}`;
}

export function createStreamBlock(
  kind: StreamBlockKind,
  itemId: string | null,
  outputIndex: number,
  contentIndex: number | null,
): StreamBlock {
  return {
    kind,
    itemId,
    outputIndex,
    contentIndex,
    started: false,
    stopped: false,
    done: false,
    suppressed: false,
    forceStart: false,
    index: null,
    pending: [],
    pendingBytes: 0,
    emittedUnits: 0,
    fingerprint: createHash("sha256"),
    seenUnits: 0,
    signature: null,
    signatureEmitted: false,
    toolId: null,
    toolName: null,
    argumentDeltaSeen: false,
    argumentUnits: 0,
    argumentBytes: 0,
  };
}
