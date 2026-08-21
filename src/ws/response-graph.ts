export interface ResponseCreateEvent {
  type: "response.create";
  stream_id?: string;
  previous_response_id?: string | null;
  generate?: boolean;
  model?: string;
  store?: boolean;
  input?: unknown;
  include?: string[] | null;
  instructions?: string | null;
  [key: string]: unknown;
}

export interface ResolvedRelayRequest {
  relayBody: Record<string, unknown>;
  parentId?: string;
  clientPreviousResponseId?: string;
  inputDelta: unknown[];
  cacheLocally: boolean;
  exposeEncryptedReasoning: boolean;
}

interface ResponseNode {
  id: string;
  parentId?: string;
  children: Set<string>;
  input: unknown[];
  output: unknown[];
  model?: string;
  bytes: number;
  lastAccess: number;
  flattened?: unknown[];
}

export class PreviousResponseNotFoundError extends Error {
  readonly code = "previous_response_not_found";

  constructor(readonly responseId: string) {
    super(`Previous response '${responseId}' is not available on this WebSocket connection`);
    this.name = "PreviousResponseNotFoundError";
  }
}

export class SharedByteBudget {
  used = 0;

  constructor(readonly limit: number) {
    if (!Number.isFinite(limit) || limit < 1) throw new RangeError("shared byte limit must be positive");
  }

  canReserve(bytes: number): boolean {
    return this.used + bytes <= this.limit;
  }

  reserve(bytes: number): boolean {
    if (!this.canReserve(bytes)) return false;
    this.used += bytes;
    return true;
  }

  release(bytes: number): void {
    this.used = Math.max(0, this.used - bytes);
  }
}

export interface ResponseGraphOptions {
  maxNodes?: number;
  maxBytes?: number;
  sharedBudget?: SharedByteBudget;
}

const DEFAULT_MAX_NODES = positiveInt(process.env.GP_WS_CACHE_NODES, 256);
const DEFAULT_MAX_BYTES = positiveInt(process.env.GP_WS_CACHE_BYTES, 64 * 1024 * 1024);
export const processResponseGraphBudget = new SharedByteBudget(
  positiveInt(process.env.GP_WS_CACHE_TOTAL_BYTES, 512 * 1024 * 1024),
);

function positiveInt(raw: string | undefined, fallback: number): number {
  const value = Number(raw);
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function normalizeInput(input: unknown): unknown[] {
  if (input == null) return [];
  if (Array.isArray(input)) return clone(input);
  if (typeof input === "string") {
    return [{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: input }],
    }];
  }
  return [clone(input)];
}

function byteSize(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), "utf8");
}

function stripEncryptedContent(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stripEncryptedContent);
  if (!value || typeof value !== "object") return value;
  const output: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (key !== "encrypted_content") output[key] = stripEncryptedContent(child);
  }
  return output;
}

export class ResponseGraph {
  private readonly nodes = new Map<string, ResponseNode>();
  private readonly maxNodes: number;
  private readonly maxBytes: number;
  private readonly sharedBudget: SharedByteBudget;
  private bytes = 0;
  private clock = 0;

  constructor(options: ResponseGraphOptions = {}) {
    this.maxNodes = options.maxNodes ?? DEFAULT_MAX_NODES;
    this.maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
    this.sharedBudget = options.sharedBudget ?? processResponseGraphBudget;
  }

  resolve(event: ResponseCreateEvent): ResolvedRelayRequest {
    const relayBody: Record<string, unknown> = { ...event };
    delete relayBody.type;
    delete relayBody.stream_id;
    delete relayBody.generate;
    delete relayBody.background;
    delete relayBody.stream;

    const inputDelta = normalizeInput(event.input);
    const localContinuation = event.store === false && typeof event.previous_response_id === "string";
    let parentId: string | undefined;
    if (localContinuation) {
      parentId = event.previous_response_id as string;
      const parent = this.nodes.get(parentId);
      if (!parent) throw new PreviousResponseNotFoundError(parentId);
      parent.lastAccess = ++this.clock;
      relayBody.input = [...this.materialize(parent), ...clone(inputDelta)];
      delete relayBody.previous_response_id;
      if (relayBody.model == null && parent.model) relayBody.model = parent.model;
    } else {
      relayBody.input = clone(inputDelta);
      if (event.previous_response_id == null) delete relayBody.previous_response_id;
    }

    if (relayBody.model == null) delete relayBody.model;
    if (event.instructions === undefined) delete relayBody.instructions;
    relayBody.stream = true;

    const requestedIncludes = Array.isArray(event.include) ? event.include : [];
    const exposeEncryptedReasoning = requestedIncludes.includes("reasoning.encrypted_content");
    if (event.store === false) {
      relayBody.include = Array.from(new Set([...requestedIncludes, "reasoning.encrypted_content"]));
    }

    return {
      relayBody,
      parentId,
      clientPreviousResponseId: typeof event.previous_response_id === "string"
        ? event.previous_response_id
        : undefined,
      inputDelta,
      cacheLocally: event.store === false,
      exposeEncryptedReasoning,
    };
  }

  record(resolved: ResolvedRelayRequest, final: any): void {
    if (!resolved.cacheLocally || typeof final?.id !== "string") return;
    const output = Array.isArray(final.output) ? clone(final.output) : [];
    const node: ResponseNode = {
      id: final.id,
      parentId: resolved.parentId,
      children: new Set(),
      input: clone(resolved.inputDelta),
      output,
      model: typeof final.model === "string"
        ? final.model
        : typeof resolved.relayBody.model === "string"
          ? resolved.relayBody.model
          : undefined,
      bytes: 0,
      lastAccess: ++this.clock,
    };
    node.bytes = byteSize({ input: node.input, output: node.output, model: node.model });

    if (this.nodes.has(node.id)) this.removeNode(node.id);
    const protectedIds = this.ancestorsOf(node.parentId);
    if (!this.makeRoom(node.bytes, protectedIds)) return;
    if (
      this.nodes.size >= this.maxNodes ||
      this.bytes + node.bytes > this.maxBytes ||
      !this.sharedBudget.reserve(node.bytes)
    ) return;

    this.nodes.set(node.id, node);
    this.bytes += node.bytes;
    if (node.parentId) this.nodes.get(node.parentId)?.children.add(node.id);
  }

  sanitizeOutbound<T>(value: T, resolved: ResolvedRelayRequest): T {
    const sanitized = (resolved.exposeEncryptedReasoning ? clone(value) : stripEncryptedContent(value)) as any;
    if (resolved.clientPreviousResponseId && sanitized && typeof sanitized === "object") {
      if (sanitized.response && typeof sanitized.response === "object") {
        sanitized.response.previous_response_id = resolved.clientPreviousResponseId;
      } else if (sanitized.object === "response") {
        sanitized.previous_response_id = resolved.clientPreviousResponseId;
      }
    }
    return sanitized as T;
  }

  evict(id: string): void {
    const node = this.nodes.get(id);
    if (!node) return;
    for (const child of [...node.children]) this.evict(child);
    this.removeNode(id);
  }

  clear(): void {
    for (const node of this.nodes.values()) this.sharedBudget.release(node.bytes);
    this.nodes.clear();
    this.bytes = 0;
  }

  stats(): { nodes: number; bytes: number } {
    return { nodes: this.nodes.size, bytes: this.bytes };
  }

  private materialize(node: ResponseNode): unknown[] {
    if (node.flattened) return clone(node.flattened);
    const prefix = node.parentId ? this.materialize(this.nodes.get(node.parentId)!) : [];
    node.flattened = [...prefix, ...clone(node.input), ...clone(node.output)];
    return clone(node.flattened);
  }

  private makeRoom(incomingBytes: number, protectedIds: Set<string>): boolean {
    while (
      this.nodes.size >= this.maxNodes ||
      this.bytes + incomingBytes > this.maxBytes ||
      !this.sharedBudget.canReserve(incomingBytes)
    ) {
      const candidate = [...this.nodes.values()]
        .filter(node => node.children.size === 0 && !protectedIds.has(node.id))
        .sort((left, right) => left.lastAccess - right.lastAccess)[0];
      if (!candidate) return false;
      this.removeNode(candidate.id);
    }
    return true;
  }

  private ancestorsOf(id?: string): Set<string> {
    const ancestors = new Set<string>();
    let current = id ? this.nodes.get(id) : undefined;
    while (current) {
      ancestors.add(current.id);
      current = current.parentId ? this.nodes.get(current.parentId) : undefined;
    }
    return ancestors;
  }

  private removeNode(id: string): void {
    const node = this.nodes.get(id);
    if (!node) return;
    this.nodes.delete(id);
    this.bytes -= node.bytes;
    this.sharedBudget.release(node.bytes);
    if (node.parentId) this.nodes.get(node.parentId)?.children.delete(id);
  }
}
