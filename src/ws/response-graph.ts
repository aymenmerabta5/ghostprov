import { jsonEncodedByteLength } from "../router/workload";

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

interface MaterializationReservation {
  releasePayload(): void;
  releasePin(): void;
  release(): void;
}

const MATERIALIZATION_RESERVATIONS = new WeakMap<object, MaterializationReservation>();
const RESOLUTION_RESERVATIONS = new WeakMap<ResolvedRelayRequest, MaterializationReservation>();

export function releaseResponseGraphMaterialization(value: unknown): void {
  if (!value || typeof value !== "object") return;
  const reservation = MATERIALIZATION_RESERVATIONS.get(value);
  if (!reservation) return;
  MATERIALIZATION_RESERVATIONS.delete(value);
  reservation.release();
}

/** Releases only the payload-sized materialization charge after router handoff. */
export function releaseResponseGraphPayload(resolved: ResolvedRelayRequest): void {
  RESOLUTION_RESERVATIONS.get(resolved)?.releasePayload();
}

/** Releases every remaining materialization charge and lineage pin. */
export function releaseResponseGraphResolution(resolved: ResolvedRelayRequest): void {
  const reservation = RESOLUTION_RESERVATIONS.get(resolved);
  if (!reservation) return;
  RESOLUTION_RESERVATIONS.delete(resolved);
  reservation.release();
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
}

export class PreviousResponseNotFoundError extends Error {
  readonly code = "previous_response_not_found";

  constructor(readonly responseId: string) {
    super(`Previous response '${responseId}' is not available on this WebSocket connection`);
    this.name = "PreviousResponseNotFoundError";
  }
}

export class ResponseGraphCapacityError extends Error {
  readonly name = "ResponseGraphCapacityError";
  readonly code = "response_graph_capacity";
  readonly status = 503;

  constructor(message = "WebSocket continuation materialization capacity is exhausted") {
    super(message);
  }
}

export class SharedByteBudget {
  used = 0;

  constructor(readonly limit: number) {
    if (!Number.isFinite(limit) || limit < 1) throw new RangeError("shared byte limit must be positive");
  }

  canReserve(bytes: number): boolean {
    if (!Number.isSafeInteger(bytes) || bytes < 0) return false;
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

function freezeJson<T>(value: T, seen = new Set<object>()): T {
  if (!value || typeof value !== "object") return value;
  const object = value as object;
  if (seen.has(object)) return value;
  seen.add(object);
  for (const child of Object.values(object)) freezeJson(child, seen);
  return Object.freeze(value);
}

function cloneFrozen<T>(value: T): T {
  return freezeJson(structuredClone(value));
}

function normalizeInput(input: unknown): unknown[] {
  if (input == null) return freezeJson([] as unknown[]);
  if (Array.isArray(input)) return cloneFrozen(input);
  if (typeof input === "string") {
    return freezeJson([{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: input }],
    }]);
  }
  return freezeJson([cloneFrozen(input)]);
}

function byteSize(value: unknown): number {
  return jsonEncodedByteLength(value);
}

function stripEncryptedContent(value: unknown): unknown {
  if (Array.isArray(value)) {
    let output: unknown[] | null = null;
    for (let index = 0; index < value.length; index++) {
      const original = value[index];
      const sanitized = stripEncryptedContent(original);
      if (!output && sanitized !== original) output = value.slice(0, index);
      output?.push(sanitized);
    }
    return output ?? value;
  }
  if (!value || typeof value !== "object") return value;
  const source = value as Record<string, unknown>;
  let output: Record<string, unknown> | null = null;
  for (const key in source) {
    if (!Object.hasOwn(source, key)) continue;
    if (key === "encrypted_content") {
      output ??= { ...source };
      delete output[key];
      continue;
    }
    const original = source[key];
    const sanitized = stripEncryptedContent(original);
    if (sanitized !== original) {
      output ??= { ...source };
      output[key] = sanitized;
    }
  }
  return output ?? value;
}

export class ResponseGraph {
  private readonly nodes = new Map<string, ResponseNode>();
  private readonly maxNodes: number;
  private readonly maxBytes: number;
  private readonly sharedBudget: SharedByteBudget;
  private readonly activeMaterializations = new Map<string, number>();
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

    const localContinuation = event.store === false && typeof event.previous_response_id === "string";
    let parentId: string | undefined;
    let materialization: { lineage: ResponseNode[]; reservation: MaterializationReservation } | null = null;
    try {
      if (localContinuation) {
        parentId = event.previous_response_id as string;
        const parent = this.nodes.get(parentId);
        if (!parent) throw new PreviousResponseNotFoundError(parentId);
        parent.lastAccess = ++this.clock;
        materialization = this.reserveLineage(parent);
      }

      // No payload-scale lineage clone occurs before reserveLineage succeeds.
      const inputDelta = normalizeInput(event.input);
      if (localContinuation) {
        const history = this.materialize(materialization!.lineage);
        history.push(...inputDelta);
        relayBody.input = Object.freeze(history);
        delete relayBody.previous_response_id;
        const parent = materialization!.lineage[0]!;
        if (relayBody.model == null && parent.model) relayBody.model = parent.model;
      } else {
        relayBody.input = inputDelta;
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

      if (materialization) {
        MATERIALIZATION_RESERVATIONS.set(relayBody, materialization.reservation);
      }

      const resolved: ResolvedRelayRequest = {
        relayBody,
        parentId,
        clientPreviousResponseId: typeof event.previous_response_id === "string"
          ? event.previous_response_id
          : undefined,
        inputDelta,
        cacheLocally: event.store === false,
        exposeEncryptedReasoning,
      };
      if (materialization) {
        RESOLUTION_RESERVATIONS.set(resolved, materialization.reservation);
      }
      return resolved;
    } catch (error) {
      materialization?.reservation.release();
      throw error;
    }
  }

  record(resolved: ResolvedRelayRequest, final: any): void {
    try {
      if (!resolved.cacheLocally || typeof final?.id !== "string") return;
      if (resolved.parentId && !this.nodes.has(resolved.parentId)) {
        throw new ResponseGraphCapacityError(
          "WebSocket continuation parent became unavailable before cache commit",
        );
      }
      const output = Array.isArray(final.output) ? cloneFrozen(final.output) : Object.freeze([]);
      const node: ResponseNode = {
        id: final.id,
        parentId: resolved.parentId,
        children: new Set(),
        input: resolved.inputDelta,
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
      if (node.bytes > this.maxBytes || node.bytes > this.sharedBudget.limit) {
        throw new ResponseGraphCapacityError("WebSocket response exceeds cache capacity");
      }

      if (this.nodes.has(node.id)) {
        throw new ResponseGraphCapacityError("WebSocket response id is already cached");
      }
      const protectedIds = this.ancestorsOf(node.parentId);
      if (!this.makeRoom(node.bytes, protectedIds)) {
        throw new ResponseGraphCapacityError("WebSocket response cache capacity is exhausted");
      }
      if (
        this.nodes.size >= this.maxNodes ||
        this.bytes + node.bytes > this.maxBytes ||
        !this.sharedBudget.reserve(node.bytes)
      ) {
        throw new ResponseGraphCapacityError("WebSocket response cache admission failed");
      }

      this.nodes.set(node.id, node);
      this.bytes += node.bytes;
      if (node.parentId) this.nodes.get(node.parentId)!.children.add(node.id);
    } finally {
      releaseResponseGraphResolution(resolved);
    }
  }

  sanitizeOutbound<T>(value: T, resolved: ResolvedRelayRequest): T {
    let sanitized = (resolved.exposeEncryptedReasoning
      ? value
      : stripEncryptedContent(value)) as any;
    if (resolved.clientPreviousResponseId && sanitized && typeof sanitized === "object") {
      if (sanitized.response && typeof sanitized.response === "object") {
        sanitized = {
          ...sanitized,
          response: {
            ...sanitized.response,
            previous_response_id: resolved.clientPreviousResponseId,
          },
        };
      } else if (sanitized.object === "response") {
        sanitized = {
          ...sanitized,
          previous_response_id: resolved.clientPreviousResponseId,
        };
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
    for (const node of this.nodes.values()) {
      this.sharedBudget.release(node.bytes);
    }
    this.nodes.clear();
    this.bytes = 0;
  }

  stats(): { nodes: number; bytes: number } {
    return { nodes: this.nodes.size, bytes: this.bytes };
  }

  private reserveLineage(node: ResponseNode): {
    lineage: ResponseNode[];
    reservation: MaterializationReservation;
  } {
    const lineage: ResponseNode[] = [];
    const seen = new Set<string>();
    let lineageBytes = 0;
    let current: ResponseNode | undefined = node;
    while (current) {
      if (seen.has(current.id)) {
        throw new ResponseGraphCapacityError("Response graph lineage contains a cycle");
      }
      seen.add(current.id);
      lineage.push(current);
      if (lineage.length > this.maxNodes) {
        throw new ResponseGraphCapacityError("Response graph lineage exceeds its node limit");
      }
      lineageBytes += current.bytes;
      if (!Number.isSafeInteger(lineageBytes) || lineageBytes > this.maxBytes) {
        throw new ResponseGraphCapacityError("Response graph lineage exceeds its byte limit");
      }
      if (!current.parentId) break;
      const parent = this.nodes.get(current.parentId);
      if (!parent) throw new PreviousResponseNotFoundError(current.parentId);
      current = parent;
    }

    if (!this.sharedBudget.reserve(lineageBytes)) throw new ResponseGraphCapacityError();
    for (const entry of lineage) {
      this.activeMaterializations.set(
        entry.id,
        (this.activeMaterializations.get(entry.id) ?? 0) + 1,
      );
    }
    let payloadReleased = false;
    let pinReleased = false;
    const reservation: MaterializationReservation = {
      releasePayload: () => {
        if (payloadReleased) return;
        payloadReleased = true;
        this.sharedBudget.release(lineageBytes);
      },
      releasePin: () => {
        if (pinReleased) return;
        pinReleased = true;
        for (const entry of lineage) {
          const remaining = (this.activeMaterializations.get(entry.id) ?? 1) - 1;
          if (remaining <= 0) this.activeMaterializations.delete(entry.id);
          else this.activeMaterializations.set(entry.id, remaining);
        }
      },
      release: () => {
        reservation.releasePayload();
        reservation.releasePin();
      },
    };
    return { lineage, reservation };
  }

  private materialize(lineage: readonly ResponseNode[]): unknown[] {
    const history: unknown[] = [];
    for (let index = lineage.length - 1; index >= 0; index--) {
      const entry = lineage[index]!;
      history.push(...entry.input, ...entry.output);
    }
    return history;
  }

  private makeRoom(incomingBytes: number, protectedIds: Set<string>): boolean {
    const remaining = new Set(this.nodes.keys());
    const removals: string[] = [];
    let removedBytes = 0;
    while (
      remaining.size >= this.maxNodes ||
      this.bytes - removedBytes + incomingBytes > this.maxBytes ||
      this.sharedBudget.used - removedBytes + incomingBytes > this.sharedBudget.limit
    ) {
      const candidate = [...this.nodes.values()]
        .filter(node =>
          remaining.has(node.id) &&
          ![...node.children].some(childId => remaining.has(childId)) &&
          !protectedIds.has(node.id) &&
          !this.activeMaterializations.has(node.id)
        )
        .sort((left, right) => left.lastAccess - right.lastAccess)[0];
      if (!candidate) return false;
      remaining.delete(candidate.id);
      removals.push(candidate.id);
      removedBytes += candidate.bytes;
    }
    for (const id of removals) this.removeNode(id);
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
