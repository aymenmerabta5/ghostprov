import { createHash, type Hash } from "node:crypto";
import type { IncrementalSha256Hasher } from "../protocol/transcript";

/** Bun-host SHA-256. Hash.copy() makes digest snapshots non-destructive. */
export class NativeIncrementalSha256 implements IncrementalSha256Hasher {
  #hash: Hash = createHash("sha256");
  #bytesHashed = 0;

  get bytesHashed(): number {
    return this.#bytesHashed;
  }

  update(value: Uint8Array): this {
    if (!(value instanceof Uint8Array)) {
      throw new TypeError("Native incremental SHA-256 input must be Uint8Array");
    }
    if (this.#bytesHashed + value.byteLength > Number.MAX_SAFE_INTEGER) {
      throw new RangeError("Native incremental SHA-256 input is too large");
    }
    this.#hash.update(value);
    this.#bytesHashed += value.byteLength;
    return this;
  }

  digestHex(): string {
    return this.#hash.copy().digest("hex");
  }
}

export function createNativeTranscriptHasher(): IncrementalSha256Hasher {
  return new NativeIncrementalSha256();
}
