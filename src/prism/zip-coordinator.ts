export type ZipFetcher = (names: string[]) => Promise<Map<string, Uint8Array>>;

interface PendingRead {
  names: string[];
  resolve: (files: Map<string, Uint8Array>) => void;
  reject: (error: unknown) => void;
}

/** Coalesces same-tick archive reads without caching results across polls. */
export class ZipPollCoordinator {
  private readonly pending: PendingRead[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly fetcher: ZipFetcher,
    private readonly batchWindowMs = 10,
  ) {
    if (!Number.isFinite(batchWindowMs) || batchWindowMs < 0) throw new RangeError("zip batch window must be non-negative");
  }

  read(names: string[]): Promise<Map<string, Uint8Array>> {
    const uniqueNames = [...new Set(names)];
    if (!uniqueNames.length) return Promise.resolve(new Map());
    return new Promise((resolve, reject) => {
      this.pending.push({ names: uniqueNames, resolve, reject });
      if (this.timer === null) {
        this.timer = setTimeout(() => void this.flush(), this.batchWindowMs);
      }
    });
  }

  private async flush(): Promise<void> {
    this.timer = null;
    const batch = this.pending.splice(0);
    if (!batch.length) return;
    const names = [...new Set(batch.flatMap(request => request.names))];
    try {
      const files = await this.fetcher(names);
      for (const request of batch) {
        const subset = new Map<string, Uint8Array>();
        for (const name of request.names) {
          const value = files.get(name);
          if (value) subset.set(name, value);
        }
        request.resolve(subset);
      }
    } catch (error) {
      for (const request of batch) request.reject(error);
    }
  }
}
