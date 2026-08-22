import type { ProjectId } from "../../src/sandbox/types";
import type {
  AwarenessChannel,
  AwarenessPeerUpdate,
} from "../../src/v2/awareness/channel";
import type { AwarenessEnvelope } from "../../src/v2/protocol/shared";

interface FaultOptions {
  drop?: readonly number[];
  duplicate?: readonly number[];
  reverseEvery?: number;
}

interface Endpoint {
  role: "host" | "sidecar";
  peerId: string;
  projectId: ProjectId;
  envelope: AwarenessEnvelope<any> | null;
  listeners: Set<(update: AwarenessPeerUpdate<any>) => void>;
  closed: boolean;
}

class FakeChannel<TLocal, TRemote> implements AwarenessChannel<TLocal, TRemote> {
  constructor(
    readonly network: FakeAwarenessNetwork<any, any>,
    readonly endpoint: Endpoint,
  ) {}

  get projectId(): ProjectId { return this.endpoint.projectId; }
  get localPeerId(): string { return this.endpoint.peerId; }

  async connect(): Promise<void> {}

  publish(envelope: AwarenessEnvelope<TLocal>): void {
    this.network.publish(this.endpoint, envelope);
  }

  clear(): void {
    this.network.clear(this.endpoint);
  }

  subscribe(listener: (update: AwarenessPeerUpdate<TRemote>) => void): () => void {
    this.endpoint.listeners.add(listener as (update: AwarenessPeerUpdate<any>) => void);
    return () => this.endpoint.listeners.delete(
      listener as (update: AwarenessPeerUpdate<any>) => void,
    );
  }

  peerIds(): readonly string[] {
    return this.network.peerIds(this.endpoint);
  }

  async close(): Promise<void> {
    this.network.close(this.endpoint);
  }
}

export class FakeAwarenessNetwork<THost, TSidecar> {
  readonly persistentWrites = 0;
  readonly #endpoints = new Map<string, Endpoint>();
  readonly #pending: Array<() => void> = [];
  readonly #drop: Set<number>;
  readonly #duplicate: Set<number>;
  readonly #reverseEvery: number;
  #publication = 0;

  constructor(options: FaultOptions = {}) {
    this.#drop = new Set(options.drop ?? []);
    this.#duplicate = new Set(options.duplicate ?? []);
    this.#reverseEvery = options.reverseEvery ?? 0;
  }

  connectHost(
    peerId: string,
    projectId: ProjectId,
  ): AwarenessChannel<THost, TSidecar> {
    return new FakeChannel(this, this.#add("host", peerId, projectId));
  }

  connectSidecar(
    peerId: string,
    projectId: ProjectId,
  ): AwarenessChannel<TSidecar, THost> {
    return new FakeChannel(this, this.#add("sidecar", peerId, projectId));
  }

  publish<T>(endpoint: Endpoint, envelope: AwarenessEnvelope<T>): void {
    if (endpoint.closed) throw new Error("Fake Awareness channel is closed");
    endpoint.envelope = structuredClone(envelope);
    const publication = ++this.#publication;
    if (this.#drop.has(publication)) return;
    for (const target of this.#endpoints.values()) {
      if (
        target.closed ||
        target.role === endpoint.role ||
        target.projectId !== endpoint.projectId
      ) continue;
      const update: AwarenessPeerUpdate<T> = {
        peerClientId: endpoint.peerId,
        envelope: structuredClone(envelope),
      };
      this.#pending.push(() => {
        for (const listener of target.listeners) listener(update);
      });
      if (this.#duplicate.has(publication)) {
        this.#pending.push(() => {
          for (const listener of target.listeners) listener(structuredClone(update));
        });
      }
    }
  }

  clear(endpoint: Endpoint): void {
    endpoint.envelope = null;
  }

  close(endpoint: Endpoint): void {
    endpoint.envelope = null;
    endpoint.closed = true;
    this.#endpoints.delete(endpoint.peerId);
    endpoint.listeners.clear();
  }

  peerIds(endpoint: Endpoint): readonly string[] {
    return [...this.#endpoints.values()]
      .filter(candidate => (
        !candidate.closed &&
        candidate.role !== endpoint.role &&
        candidate.projectId === endpoint.projectId &&
        candidate.envelope !== null
      ))
      .map(candidate => candidate.peerId)
      .sort();
  }

  async flushAll(): Promise<void> {
    while (this.#pending.length) {
      const count = this.#reverseEvery > 1
        ? Math.min(this.#reverseEvery, this.#pending.length)
        : 1;
      const deliveries = this.#pending.splice(0, count);
      if (this.#reverseEvery > 1) deliveries.reverse();
      for (const deliver of deliveries) deliver();
      await Promise.resolve();
    }
  }

  #add(
    role: "host" | "sidecar",
    peerId: string,
    projectId: ProjectId,
  ): Endpoint {
    if (!peerId || this.#endpoints.has(peerId)) {
      throw new Error(`Duplicate or empty fake Awareness peer: ${peerId}`);
    }
    const endpoint: Endpoint = {
      role,
      peerId,
      projectId,
      envelope: null,
      listeners: new Set(),
      closed: false,
    };
    this.#endpoints.set(peerId, endpoint);
    return endpoint;
  }
}
