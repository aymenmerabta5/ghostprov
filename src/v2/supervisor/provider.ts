export interface SupervisorAwarenessLike {
  getStates(): Map<number, Record<string, unknown>>;
  setLocalState(value: Record<string, unknown> | null): void;
  setLocalStateField(field: string, value: unknown): void;
  on(event: "change", listener: SupervisorAwarenessChangeListener): void;
  off(event: "change", listener: SupervisorAwarenessChangeListener): void;
}

export type SupervisorAwarenessChangeListener = (change: {
  added: number[];
  updated: number[];
  removed: number[];
}) => void;

export interface SupervisorAwarenessProviderLike {
  readonly awareness: SupervisorAwarenessLike;
  on(event: string, listener: (...args: any[]) => void): void;
  off(event: string, listener: (...args: any[]) => void): void;
  connect(): void;
  disconnect(): void;
  destroy(): void;
}
