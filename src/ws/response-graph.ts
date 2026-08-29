import { normalizeUpstreamResponsesRequest } from "../relay/upstream-policy";
import {
  createAuthenticatedTenantScope,
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";

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
  /** Opaque server-derived scope; never serialized into a provider request. */
  authorizationScope: AuthenticatedTenantScope;
  /** Present only when a synthetic warmup alias was expanded locally. */
  parentId?: string;
  /** Public alias restored on outbound response objects after local expansion. */
  clientPreviousResponseId?: string;
  inputDelta: unknown[];
  /** Generated responses are always provider-backed and are never cached locally. */
  cacheLocally: boolean;
  exposeEncryptedReasoning: boolean;
  /** True when relayBody.input includes locally accumulated warmup deltas. */
  materializedLocally: boolean;
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

/** Releases handoff scratch and lineage pins once router/transport owns the request. */
export function releaseResponseGraphPayload(resolved: ResolvedRelayRequest): void {
  const reservation = RESOLUTION_RESERVATIONS.get(resolved);
  reservation?.releasePayload();
  reservation?.releasePin();
}

/** Releases every remaining materialization charge and lineage pin. */
export function releaseResponseGraphResolution(resolved: ResolvedRelayRequest): void {
  const reservation = RESOLUTION_RESERVATIONS.get(resolved);
  if (!reservation) return;
  RESOLUTION_RESERVATIONS.delete(resolved);
  reservation.release();
}

interface WarmupAliasNode {
  id: string;
  authorizationScope: AuthenticatedTenantScope;
  parentId?: string;
  /** Provider-owned ancestor at the root of a local alias chain. */
  providerAncestorId?: string;
  children: Set<string>;
  input: unknown[];
  model?: string;
  bytes: number;
  lastAccess: number;
}

export const SYNTHETIC_WARMUP_ID_PREFIX = "resp_gp_warmup_";

function isSyntheticWarmupId(value: string): boolean {
  return value.startsWith(SYNTHETIC_WARMUP_ID_PREFIX);
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
  /** Maximum number of retained JSON values in one warmup alias. */
  maxRetainedItems?: number;
  /** Maximum container nesting below the normalized input root. */
  maxRetainedDepth?: number;
}

export interface ResponseGraphResolveOptions {
  /** Trusted server-side scope; never sourced from a client event. */
  authorizationScope?: AuthenticatedTenantScope;
  /** Caller transfers an isolated JSON.parse-owned input graph. */
  inputOwnership?: "borrowed" | "owned";
}

const DEFAULT_MAX_NODES = positiveInt(process.env.GP_WS_CACHE_NODES, 256);
const DEFAULT_MAX_BYTES = positiveInt(process.env.GP_WS_CACHE_BYTES, 64 * 1024 * 1024);
const DEFAULT_MAX_RETAINED_ITEMS = 65_536;
const DEFAULT_MAX_RETAINED_DEPTH = 64;
const ABSOLUTE_MAX_RETAINED_ITEMS = 1_000_000;
const ABSOLUTE_MAX_RETAINED_DEPTH = 256;
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

const RETAINED_ARRAY_HEADER_BYTES = 64;
const RETAINED_ARRAY_SLOT_BYTES = 16;
const RETAINED_OBJECT_HEADER_BYTES = 96;
const RETAINED_PROPERTY_BYTES = 48;
const RETAINED_STRING_HEADER_BYTES = 32;
const RETAINED_VALUE_BYTES = 16;
const RETAINED_TRAVERSAL_BYTES = 64;
const RETAINED_NODE_OVERHEAD_BYTES = 640;
const RETAINED_PARENT_EDGE_BYTES = 128;

function checkedAdd(left: number, right: number): number {
  const value = left + right;
  if (!Number.isSafeInteger(value)) {
    throw new ResponseGraphCapacityError("WebSocket warmup alias exceeds safe accounting capacity");
  }
  return value;
}

function checkedMultiply(left: number, right: number): number {
  const value = left * right;
  if (!Number.isSafeInteger(value)) {
    throw new ResponseGraphCapacityError("WebSocket warmup alias exceeds safe accounting capacity");
  }
  return value;
}

function alignRetainedBytes(value: number): number {
  const remainder = value % 8;
  return remainder === 0 ? value : checkedAdd(value, 8 - remainder);
}

function retainedStringByteSize(value: string): number {
  // V8/Bun may use one-byte strings for ASCII, but charging UTF-16 is a safe
  // fixed-governor upper estimate and avoids materializing an escaped JSON copy.
  return checkedAdd(
    RETAINED_STRING_HEADER_BYTES,
    alignRetainedBytes(checkedMultiply(value.length, 2)),
  );
}

/**
 * Conservatively estimates the resident graph retained by one warmup alias.
 * The normal source is JSON.parse, so non-plain containers are rejected rather
 * than silently retaining Map/Set/typed-array contents that enumeration misses.
 * An ancestor-only set keeps traversal scratch bounded by maxDepth; shared
 * subgraphs are deliberately charged once per reference.
 */
function retainedJsonGraphByteSize(
  value: unknown,
  maxItems: number,
  maxDepth: number,
): number {
  let bytes = 0;
  let items = 0;
  const ancestors = new Set<object>();
  const add = (amount: number) => {
    bytes = checkedAdd(bytes, amount);
  };
  const visit = (current: unknown, depth: number): void => {
    items++;
    if (items > maxItems) {
      throw new ResponseGraphCapacityError("WebSocket warmup alias exceeds its retained item limit");
    }
    add(RETAINED_TRAVERSAL_BYTES);
    if (typeof current === "string") {
      add(retainedStringByteSize(current));
      return;
    }
    if (!current || typeof current !== "object") {
      add(RETAINED_VALUE_BYTES);
      return;
    }
    if (depth > maxDepth) {
      throw new ResponseGraphCapacityError("WebSocket warmup alias exceeds its retained depth limit");
    }
    if (ancestors.has(current)) {
      throw new ResponseGraphCapacityError("WebSocket warmup alias must contain an acyclic JSON graph");
    }
    ancestors.add(current);
    try {
      if (Array.isArray(current)) {
        add(RETAINED_ARRAY_HEADER_BYTES);
        add(checkedMultiply(current.length, RETAINED_ARRAY_SLOT_BYTES));
        for (let index = 0; index < current.length; index++) {
          visit(current[index], depth + 1);
        }
        return;
      }
      const prototype = Object.getPrototypeOf(current);
      if (prototype !== Object.prototype && prototype !== null) {
        throw new ResponseGraphCapacityError("WebSocket warmup alias must contain only JSON containers");
      }
      add(RETAINED_OBJECT_HEADER_BYTES);
      const record = current as Record<string, unknown>;
      for (const key in record) {
        if (!Object.hasOwn(record, key)) continue;
        add(RETAINED_PROPERTY_BYTES);
        add(retainedStringByteSize(key));
        visit(record[key], depth + 1);
      }
    } finally {
      ancestors.delete(current);
    }
  };
  visit(value, 0);
  // Charge the bounded recursion/ancestor scratch throughout retention. This is
  // intentionally conservative so admission remains safe at a full byte budget.
  add(checkedMultiply(checkedAdd(maxDepth, 1), RETAINED_TRAVERSAL_BYTES));
  return bytes;
}

function normalizedInputItemCount(input: unknown): number {
  if (input == null) return 0;
  return Array.isArray(input) ? input.length : 1;
}

function normalizedInputRetainedByteSize(
  input: unknown,
  maxItems: number,
  maxDepth: number,
): number {
  if (input == null) return retainedJsonGraphByteSize([], maxItems, maxDepth);
  if (Array.isArray(input)) return retainedJsonGraphByteSize(input, maxItems, maxDepth);
  if (typeof input === "string") {
    return retainedJsonGraphByteSize([{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: input }],
    }], maxItems, maxDepth);
  }
  return retainedJsonGraphByteSize([input], maxItems, maxDepth);
}

function warmupAliasByteSize(
  id: string,
  input: unknown,
  parentId: string | undefined,
  providerAncestorId: string | undefined,
  model: string | undefined,
  maxItems: number,
  maxDepth: number,
): number {
  let bytes = normalizedInputRetainedByteSize(input, maxItems, maxDepth);
  bytes = checkedAdd(bytes, retainedStringByteSize(id));
  if (parentId) bytes = checkedAdd(bytes, retainedStringByteSize(parentId));
  if (providerAncestorId) {
    bytes = checkedAdd(bytes, retainedStringByteSize(providerAncestorId));
  }
  if (model) bytes = checkedAdd(bytes, retainedStringByteSize(model));
  bytes = checkedAdd(bytes, RETAINED_NODE_OVERHEAD_BYTES);
  if (parentId) bytes = checkedAdd(bytes, RETAINED_PARENT_EDGE_BYTES);
  return bytes;
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
  private readonly nodes = new Map<string, WarmupAliasNode>();
  private readonly maxNodes: number;
  private readonly maxBytes: number;
  private readonly maxRetainedItems: number;
  private readonly maxRetainedDepth: number;
  private readonly sharedBudget: SharedByteBudget;
  private readonly defaultAuthorizationScope = createAuthenticatedTenantScope();
  private readonly activeMaterializations = new Map<string, number>();
  /** Logically evicted aliases whose payload is still pinned by a router handoff. */
  private readonly retiredNodeBytes = new Map<string, number>();
  private bytes = 0;
  private clock = 0;

  constructor(options: ResponseGraphOptions = {}) {
    this.maxNodes = options.maxNodes ?? DEFAULT_MAX_NODES;
    this.maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
    this.maxRetainedItems = options.maxRetainedItems ?? DEFAULT_MAX_RETAINED_ITEMS;
    this.maxRetainedDepth = options.maxRetainedDepth ?? DEFAULT_MAX_RETAINED_DEPTH;
    this.sharedBudget = options.sharedBudget ?? processResponseGraphBudget;
    if (!Number.isSafeInteger(this.maxNodes) || this.maxNodes < 1) {
      throw new RangeError("maxNodes must be a positive safe integer");
    }
    if (!Number.isSafeInteger(this.maxBytes) || this.maxBytes < 1) {
      throw new RangeError("maxBytes must be a positive safe integer");
    }
    if (
      !Number.isSafeInteger(this.maxRetainedItems) ||
      this.maxRetainedItems < 1 ||
      this.maxRetainedItems > ABSOLUTE_MAX_RETAINED_ITEMS
    ) {
      throw new RangeError(
        `maxRetainedItems must be from 1 through ${ABSOLUTE_MAX_RETAINED_ITEMS}`,
      );
    }
    if (
      !Number.isSafeInteger(this.maxRetainedDepth) ||
      this.maxRetainedDepth < 1 ||
      this.maxRetainedDepth > ABSOLUTE_MAX_RETAINED_DEPTH
    ) {
      throw new RangeError(
        `maxRetainedDepth must be from 1 through ${ABSOLUTE_MAX_RETAINED_DEPTH}`,
      );
    }
  }

  /**
   * Resolves an ordinary generated request. Every generated turn is persisted
   * upstream. Local state participates only when previous_response_id names an
   * explicit synthetic warmup alias.
   */
  resolve(
    event: ResponseCreateEvent,
    options: ResponseGraphResolveOptions = {},
  ): ResolvedRelayRequest {
    // Apply the upstream persistence invariant before deciding whether any
    // connection-local warmup state needs expansion.
    const upstreamEvent = normalizeUpstreamResponsesRequest(event);
    const relayBody: Record<string, unknown> = { ...upstreamEvent };
    delete relayBody.type;
    delete relayBody.stream_id;
    delete relayBody.generate;
    delete relayBody.background;
    delete relayBody.stream;
    relayBody.stream = true;

    const authorizationScope = this.resolveAuthorizationScope(options?.authorizationScope);
    const inputDelta = normalizeInput(event.input, options.inputOwnership === "owned");
    relayBody.input = inputDelta;

    const previousResponseId = typeof event.previous_response_id === "string"
      ? event.previous_response_id
      : undefined;
    let parentId: string | undefined;
    let clientPreviousResponseId: string | undefined;
    let materializedLocally = false;
    let materialization: {
      lineage: WarmupAliasNode[];
      providerAncestorId?: string;
      inheritedModel?: string;
      reservation: MaterializationReservation;
    } | null = null;

    try {
      if (previousResponseId && isSyntheticWarmupId(previousResponseId)) {
        const parent = this.nodes.get(previousResponseId);
        if (!parent) throw new PreviousResponseNotFoundError(previousResponseId);
        parentId = previousResponseId;
        clientPreviousResponseId = previousResponseId;
        materializedLocally = true;
        materialization = this.reserveLineage(
          parent,
          normalizedInputItemCount(event.input),
          authorizationScope,
        );
        relayBody.input = Object.freeze(
          this.materialize(materialization.lineage, inputDelta),
        );
        if (materialization.providerAncestorId !== undefined) {
          relayBody.previous_response_id = materialization.providerAncestorId;
        } else {
          delete relayBody.previous_response_id;
        }
        if (relayBody.model == null && materialization.inheritedModel) {
          relayBody.model = materialization.inheritedModel;
        }
        MATERIALIZATION_RESERVATIONS.set(relayBody, materialization.reservation);
      }

      const requestedIncludes = Array.isArray(event.include) ? event.include : [];
      const resolved: ResolvedRelayRequest = {
        relayBody,
        authorizationScope,
        parentId,
        clientPreviousResponseId,
        inputDelta,
        cacheLocally: false,
        exposeEncryptedReasoning: requestedIncludes.includes("reasoning.encrypted_content"),
        materializedLocally,
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

  /**
   * Records one explicit generate:false alias. Provider IDs remain provider
   * ancestors; only IDs in GhostProv's reserved namespace can name local nodes.
   */
  recordWarmup(
    id: string,
    event: ResponseCreateEvent,
    options: ResponseGraphResolveOptions = {},
  ): void {
    if (event.generate !== false) {
      throw new TypeError("Only explicit generate:false requests may create warmup aliases");
    }
    if (!isSyntheticWarmupId(id)) {
      throw new TypeError(`Warmup alias IDs must start with '${SYNTHETIC_WARMUP_ID_PREFIX}'`);
    }
    if (this.nodes.has(id) || this.retiredNodeBytes.has(id)) {
      throw new ResponseGraphCapacityError("WebSocket warmup alias id is already cached");
    }
    const authorizationScope = this.resolveAuthorizationScope(options?.authorizationScope);

    const previousResponseId = typeof event.previous_response_id === "string"
      ? event.previous_response_id
      : undefined;
    let parentId: string | undefined;
    let providerAncestorId: string | undefined;
    if (previousResponseId && isSyntheticWarmupId(previousResponseId)) {
      const parent = this.nodes.get(previousResponseId);
      if (!parent || parent.authorizationScope !== authorizationScope) {
        throw new PreviousResponseNotFoundError(previousResponseId);
      }
      parentId = previousResponseId;
    } else {
      providerAncestorId = previousResponseId;
    }
    const model = typeof event.model === "string" ? event.model : undefined;
    const nodeBytes = warmupAliasByteSize(
      id,
      event.input,
      parentId,
      providerAncestorId,
      model,
      this.maxRetainedItems,
      this.maxRetainedDepth,
    );
    if (nodeBytes > this.maxBytes || nodeBytes > this.sharedBudget.limit) {
      throw new ResponseGraphCapacityError("WebSocket warmup alias exceeds cache capacity");
    }

    const input = normalizeInput(event.input, options.inputOwnership === "owned");
    const protectedIds = this.ancestorsOf(parentId);
    for (const protectedId of protectedIds) {
      const node = this.nodes.get(protectedId);
      if (node) node.lastAccess = ++this.clock;
    }
    if (!this.makeRoom(nodeBytes, protectedIds)) {
      throw new ResponseGraphCapacityError("WebSocket warmup alias cache capacity is exhausted");
    }
    if (
      this.nodes.size >= this.maxNodes ||
      this.bytes + nodeBytes > this.maxBytes ||
      !this.sharedBudget.reserve(nodeBytes)
    ) {
      throw new ResponseGraphCapacityError("WebSocket warmup alias cache admission failed");
    }

    try {
      const node: WarmupAliasNode = {
        id,
        authorizationScope,
        parentId,
        providerAncestorId,
        children: new Set(),
        input,
        model,
        bytes: nodeBytes,
        lastAccess: ++this.clock,
      };
      this.nodes.set(id, node);
      this.bytes += nodeBytes;
      if (parentId) this.nodes.get(parentId)!.children.add(id);
    } catch (error) {
      this.sharedBudget.release(nodeBytes);
      throw error;
    }
  }

  /** Generated provider responses deliberately never become local aliases. */
  record(resolved: ResolvedRelayRequest, _final: unknown): void {
    releaseResponseGraphResolution(resolved);
  }

  sanitizeOutbound<T>(value: T, resolved: ResolvedRelayRequest): T {
    let sanitized = (resolved.exposeEncryptedReasoning
      ? value
      : stripEncryptedContent(value)) as any;
    // Only locally expanded warmup aliases need a public-id restoration. An
    // ordinary provider previous_response_id is passed and returned untouched.
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
    for (const id of [...this.nodes.keys()]) this.removeNode(id);
    this.bytes = 0;
  }

  stats(): { nodes: number; bytes: number } {
    return { nodes: this.nodes.size, bytes: this.bytes };
  }

  private reserveLineage(
    node: WarmupAliasNode,
    additionalItems: number,
    authorizationScope: AuthenticatedTenantScope,
  ): {
    lineage: WarmupAliasNode[];
    providerAncestorId?: string;
    inheritedModel?: string;
    reservation: MaterializationReservation;
  } {
    const lineage: WarmupAliasNode[] = [];
    const seen = new Set<string>();
    let lineageBytes = 0;
    let lineageItems = 0;
    let inheritedModel: string | undefined;
    let current: WarmupAliasNode | undefined = node;
    while (current) {
      if (current.authorizationScope !== authorizationScope) {
        throw new PreviousResponseNotFoundError(node.id);
      }
      if (seen.has(current.id)) {
        throw new ResponseGraphCapacityError("Response graph lineage contains a cycle");
      }
      seen.add(current.id);
      lineage.push(current);
      lineageItems += current.input.length;
      inheritedModel ??= current.model;
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

    const providerAncestorId = lineage.at(-1)?.providerAncestorId;
    const materializationBytes = 1_024 +
      (lineageItems + additionalItems) * 16 +
      lineage.length * 128;
    if (!Number.isSafeInteger(materializationBytes) ||
        !this.sharedBudget.reserve(materializationBytes)) {
      throw new ResponseGraphCapacityError();
    }
    for (const entry of lineage) {
      entry.lastAccess = ++this.clock;
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
          if (remaining <= 0) {
            this.activeMaterializations.delete(entry.id);
            const retiredBytes = this.retiredNodeBytes.get(entry.id);
            if (retiredBytes !== undefined) {
              this.retiredNodeBytes.delete(entry.id);
              this.sharedBudget.release(retiredBytes);
            }
          } else {
            this.activeMaterializations.set(entry.id, remaining);
          }
        }
      },
      release: () => {
        reservation.releasePayload();
        reservation.releasePin();
      },
    };
    return {
      lineage,
      providerAncestorId,
      inheritedModel,
      reservation,
    };
  }

  private materialize(
    lineage: readonly WarmupAliasNode[],
    inputDelta: readonly unknown[],
  ): unknown[] {
    let itemCount = inputDelta.length;
    for (const entry of lineage) itemCount += entry.input.length;
    const input = new Array<unknown>(itemCount);
    let cursor = 0;
    for (let index = lineage.length - 1; index >= 0; index--) {
      const entry = lineage[index]!;
      for (const item of entry.input) input[cursor++] = item;
    }
    for (const item of inputDelta) input[cursor++] = item;
    return input;
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

  private resolveAuthorizationScope(
    value: AuthenticatedTenantScope | undefined,
  ): AuthenticatedTenantScope {
    if (value === undefined) return this.defaultAuthorizationScope;
    if (!isAuthenticatedTenantScope(value)) {
      throw new TypeError("authorizationScope must be an AuthenticatedTenantScope");
    }
    return value;
  }

  private removeNode(id: string): void {
    const node = this.nodes.get(id);
    if (!node) return;
    this.nodes.delete(id);
    this.bytes = Math.max(0, this.bytes - node.bytes);
    if (this.activeMaterializations.has(id)) {
      // The alias is no longer addressable, but an in-flight resolution still
      // references its frozen payload. Keep the global charge until its last pin.
      this.retiredNodeBytes.set(id, node.bytes);
    } else {
      this.sharedBudget.release(node.bytes);
    }
    if (node.parentId) this.nodes.get(node.parentId)?.children.delete(id);
  }
}
