import { ApiError } from "../http/errors";
import type { ReleasableLease } from "../router/types";
import {
  atomicBytePermitOf,
  releaseAtomicBytePermit,
} from "../v2/memory/permit-carrier";

/** Synchronous, unqueued reservation from the existing global governor. */
export type ImageResultMemoryAllocator = (bytes: number) => ReleasableLease | null;

export class ImageResultMemoryError extends ApiError {
  constructor() {
    super(503, "Completed image results exceed the available response memory budget.", {
      type: "server_error",
      code: "image_result_memory_exhausted",
    });
  }
}

/** Per-batch ownership, transferred to the downstream JSON response on success. */
export class ImageBatchResultMemory {
  readonly #leases: ReleasableLease[] = [];
  #released = false;

  constructor(
    readonly maxResults: number,
    private readonly reserve?: ImageResultMemoryAllocator,
  ) {
    if (!Number.isSafeInteger(maxResults) || maxResults < 1 || maxResults > 10) {
      throw new RangeError("Image result memory requires a batch size between 1 and 10");
    }
  }

  /** Called only after authoritative final proof and complete event drainage. */
  retain(final: unknown): void {
    const permit = atomicBytePermitOf(final);
    if (!permit || permit.region !== "jumbo") return;
    if (this.#released || this.#leases.length >= this.maxResults) {
      throw new ImageResultMemoryError();
    }
    let lease: ReleasableLease | null;
    try {
      lease = this.reserve?.(permit.bytes) ?? null;
    } catch {
      throw new ImageResultMemoryError();
    }
    if (!lease) throw new ImageResultMemoryError();
    if (this.#released) {
      lease.release();
      throw new ImageResultMemoryError();
    }
    // Keep the full existing physical charge, including JSON/string overlap.
    // Reserve before releasing the exclusive assembly slot; never wait while
    // holding that slot or move retained results into another event lane.
    this.#leases.push(lease);
    releaseAtomicBytePermit(final);
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    for (const lease of this.#leases) lease.release();
    this.#leases.length = 0;
  }
}
