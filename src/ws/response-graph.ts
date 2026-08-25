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
interface PendingInputReservation {
  readonly bytes: number;
  transfer(retainedBytes: number): void;
  release(): void;
}
const PENDING_INPUT_RESERVATIONS = new WeakMap<ResolvedRelayRequest, PendingInputReservation>();

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
  if (reservation) {
    RESOLUTION_RESERVATIONS.delete(resolved);
    reservation.release();
  }
  const pending = PENDING_INPUT_RESERVATIONS.get(resolved);
  if (pending) {
    PENDING_INPUT_RESERVATIONS.delete(resolved);
    pending.release();
  }
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

export interface ResponseGraphResolveOptions {
  /** Caller transfers an isolated JSON.parse-owned input graph. */
  inputOwnership?: "borrowed" | "owned";
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
  if (Array.isArray(object)) {
    for (let index = 0; index < object.length; index++) {
      const child = object[index];
      if (child && typeof child === "object") freezeJson(child, seen);
    }
  } else {
    for (const child of Object.values(object)) {
      if (child && typeof child === "object") freezeJson(child, seen);
    }
  }
  return Object.freeze(value);
}

function cloneFrozen<T>(value: T): T {
  return freezeJson(structuredClone(value));
}

function normalizeInput(input: unknown, owned: boolean): unknown[] {
  if (input == null) return freezeJson([] as unknown[]);
  if (Array.isArray(input)) return owned ? freezeJson(input) : cloneFrozen(input);
  if (typeof input === "string") {
    return freezeJson([{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: input }],
    }]);
  }
  return freezeJson([owned ? freezeJson(input) : cloneFrozen(input)]);
}

function byteSize(value: unknown): number {
  return jsonEncodedByteLength(value);
}

function normalizedInputItemCount(input: unknown): number {
  if (input == null) return 0;
  return Array.isArray(input) ? input.length : 1;
}

function normalizedInputByteSize(input: unknown): number {
  if (input == null) return byteSize([]);
  if (Array.isArray(input)) return byteSize(input);
  if (typeof input === "string") {
    return byteSize([{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: input }],
    }]);
  }
  return byteSize([input]);
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

  resolve(
    event: ResponseCreateEvent,
    options: ResponseGraphResolveOptions = {},
  ): ResolvedRelayRequest {
    const relayBody: Record<string, unknown> = { ...event };
    delete relayBody.type;
    delete relayBody.stream_id;
    delete relayBody.generate;
    delete relayBody.background;
    delete relayBody.stream;

    const localContinuation = event.store === false && typeof event.previous_response_id === "string";
    const cacheLocally = event.store === false;
    let parentId: string | undefined;
    let materialization: { lineage: ResponseNode[]; reservation: MaterializationReservation } | null = null;
    let pendingInput: PendingInputReservation | null = null;
    try {
      if (localContinuation) {
        parentId = event.previous_response_id as string;
        const parent = this.nodes.get(parentId);
        if (!parent) throw new PreviousResponseNotFoundError(parentId);
        parent.lastAccess = ++this.clock;
        materialization = this.reserveLineage(parent, normalizedInputItemCount(event.input));
      }
      if (cacheLocally) {
        const pendingBytes = normalizedInputByteSize(event.input);
        if (pendingBytes > this.maxBytes || pendingBytes > this.sharedBudget.limit) {
          throw new ResponseGraphCapacityError("WebSocket response input exceeds cache capacity");
        }
        if (!this.sharedBudget.reserve(pendingBytes)) {
          throw new ResponseGraphCapacityError("WebSocket response input capacity is exhausted");
        }
        let active = true;
        pendingInput = {
          bytes: pendingBytes,
          transfer: retainedBytes => {
            if (!active) throw new Error("Pending response input reservation is no longer active");
            active = false;
            if (retainedBytes < pendingBytes) {
              this.sharedBudget.release(pendingBytes - retainedBytes);
            }
          },
          release: () => {
            if (!active) return;
            active = false;
            this.sharedBudget.release(pendingBytes);
          },
        };
      }

      // No payload-scale lineage clone occurs before reserveLineage succeeds.
      const inputDelta = normalizeInput(event.input, options.inputOwnership === "owned");
      if (localContinuation) {
        const history = this.materialize(materialization!.lineage, inputDelta);
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
        cacheLocally,
        exposeEncryptedReasoning,
      };
      if (materialization) {
        RESOLUTION_RESERVATIONS.set(resolved, materialization.reservation);
      }
      if (pendingInput) {
        PENDING_INPUT_RESERVATIONS.set(resolved, pendingInput);
        pendingInput = null;
      }
      return resolved;
    } catch (error) {
      pendingInput?.release();
      materialization?.reservation.release();
      throw error;
    }
  }

  record(resolved: ResolvedRelayRequest, final: any): void {
    let additionalReservedBytes = 0;
    try {
      if (!resolved.cacheLocally || typeof final?.id !== "string") return;
      if (resolved.parentId && !this.nodes.has(resolved.parentId)) {
        throw new ResponseGraphCapacityError(
          "WebSocket continuation parent became unavailable before cache commit",
        );
      }
      if (this.nodes.has(final.id)) {
        throw new ResponseGraphCapacityError("WebSocket response id is already cached");
      }
      const outputSource = Array.isArray(final.output) ? final.output : [];
      const model = typeof final.model === "string"
        ? final.model
        : typeof resolved.relayBody.model === "string"
          ? resolved.relayBody.model
          : undefined;
      const nodeBytes = byteSize({ input: resolved.inputDelta, output: outputSource, model });
      if (nodeBytes > this.maxBytes || nodeBytes > this.sharedBudget.limit) {
        throw new ResponseGraphCapacityError("WebSocket response exceeds cache capacity");
      }
      const pending = PENDING_INPUT_RESERVATIONS.get(resolved);
      const pendingBytes = pending?.bytes ?? 0;
      const additionalBytes = Math.max(0, nodeBytes - pendingBytes);
      const protectedIds = this.ancestorsOf(resolved.parentId);
      if (!this.makeRoom(nodeBytes, additionalBytes, protectedIds)) {
        throw new ResponseGraphCapacityError("WebSocket response cache capacity is exhausted");
      }
      if (
        this.nodes.size >= this.maxNodes ||
        this.bytes + nodeBytes > this.maxBytes ||
        !this.sharedBudget.reserve(additionalBytes)
      ) {
        throw new ResponseGraphCapacityError("WebSocket response cache admission failed");
      }
      additionalReservedBytes = additionalBytes;
      const output = outputSource.length > 0 ? cloneFrozen(outputSource) : Object.freeze([]);
      const node: ResponseNode = {
        id: final.id,
        parentId: resolved.parentId,
        children: new Set(),
        input: resolved.inputDelta,
        output,
        model,
        bytes: nodeBytes,
        lastAccess: ++this.clock,
      };

      this.nodes.set(node.id, node);
      this.bytes += node.bytes;
      if (node.parentId) this.nodes.get(node.parentId)!.children.add(node.id);
      pending?.transfer(node.bytes);
      if (pending) PENDING_INPUT_RESERVATIONS.delete(resolved);
      additionalReservedBytes = 0;
    } finally {
      if (additionalReservedBytes > 0) this.sharedBudget.release(additionalReservedBytes);
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

  private reserveLineage(node: ResponseNode, additionalItems: number): {
    lineage: ResponseNode[];
    reservation: MaterializationReservation;
  } {
    const lineage: ResponseNode[] = [];
    const seen = new Set<string>();
    let lineageBytes = 0;
    let lineageItems = 0;
    let current: ResponseNode | undefined = node;
    while (current) {
      if (seen.has(current.id)) {
        throw new ResponseGraphCapacityError("Response graph lineage contains a cycle");
      }
      seen.add(current.id);
      lineage.push(current);
      lineageItems += current.input.length + current.output.length;
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

    // Materialization shares deeply frozen node items and allocates only the
    // new history array. Stored payload bytes are already charged once.
    const materializationBytes = 1_024 +
      (lineageItems + additionalItems) * 16 +
      lineage.length * 128;
    if (!Number.isSafeInteger(materializationBytes) ||
        !this.sharedBudget.reserve(materializationBytes)) {
      throw new ResponseGraphCapacityError();
    }
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
        this.sharedBudget.release(materializationBytes);
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

  private materialize(
    lineage: readonly ResponseNode[],
    inputDelta: readonly unknown[],
  ): unknown[] {
    let itemCount = inputDelta.length;
    for (const entry of lineage) itemCount += entry.input.length + entry.output.length;
    const history = new Array<unknown>(itemCount);
    let cursor = 0;
    for (let index = lineage.length - 1; index >= 0; index--) {
      const entry = lineage[index]!;
      for (const item of entry.input) history[cursor++] = item;
      for (const item of entry.output) history[cursor++] = item;
    }
    for (const item of inputDelta) history[cursor++] = item;
    return history;
  }

  private makeRoom(
    incomingBytes: number,
    additionalSharedBytes: number,
    protectedIds: Set<string>,
  ): boolean {
    const remaining = new Set(this.nodes.keys());
    const removals: string[] = [];
    let removedBytes = 0;
    while (
      remaining.size >= this.maxNodes ||
      this.bytes - removedBytes + incomingBytes > this.maxBytes ||
      this.sharedBudget.used - removedBytes + additionalSharedBytes > this.sharedBudget.limit
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
