export interface ReleasableLease {
  release(): void;
}

export interface ResourceSizing {
  totalBytes: number;
  startupFreeBytes: number;
  startupRssBytes: number;
  systemReserveBytes: number;
  processCeilingBytes: number;
  reservationCapacityBytes: number;
  pauseFreeBytes: number;
  resumeFreeBytes: number;
  cpuTransformWorkers: number;
}

export interface ResourceGovernorSnapshot extends ResourceSizing {
  paused: boolean;
  reservedBytes: number;
  queued: number;
  cpuActive: number;
  cpuQueued: number;
}
