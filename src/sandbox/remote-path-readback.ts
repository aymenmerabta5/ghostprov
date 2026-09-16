import { createHash } from "node:crypto";
import * as decoding from "lib0/decoding";
import * as Y from "yjs";

export const MAX_REMOTE_PATH_UPDATE_BYTES = 8 * 1024 * 1024;
const MAX_PATHS = 8, MAX_CONTENT_BYTES = 1024 * 1024, MAX_TOTAL_BYTES = 2 * 1024 * 1024;
const MAX_VECTOR_BYTES = 16 * 1024, MAX_CLIENTS = 1024, MAX_STRUCTS = 16_384;
const MAX_NODES = 4096, MAX_DEPTH = 32, MAX_PATH_BYTES = 1024;
const PREPARED = Symbol("prepared remote paths");
const UTF8 = new TextDecoder("utf-8", { fatal: true });

export interface RemotePathExpectation { readonly path: string; readonly content: string }
export interface PreparedRemotePathExpectations {
  readonly entries: readonly {
    readonly path: string;
    readonly segments: readonly string[];
    readonly bytes: number;
    readonly sha256: string;
  }[];
  readonly totalBytes: number;
  readonly [PREPARED]: true;
}
export type RemotePathReadbackReason = "expectations_invalid" | "expectations_bound" | "update_bound" |
  "vector_invalid" | "vector_bound" | "update_invalid" | "unsupported_update" | "structure_bound" |
  "node_bound" | "stale_snapshot" | "path_missing" | "path_conflict" | "content_mismatch";
export class RemotePathReadbackError extends Error {
  constructor(readonly reason: RemotePathReadbackReason) {
    super(reason);
    this.name = "RemotePathReadbackError";
  }
}
function fail(reason: RemotePathReadbackReason): never { throw new RemotePathReadbackError(reason); }
function requireThat(value: unknown, reason: RemotePathReadbackReason): asserts value { if (!value) fail(reason); }
function wellFormed(value: string): boolean {
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
    } else if (code >= 0xdc00 && code <= 0xdfff) return false;
  }
  return true;
}
const digest = (value: string) => createHash("sha256").update(value, "utf8").digest("hex");

/** Copies only validated path metadata and digests; never retains the supplied file contents. */
export function prepareRemotePathExpectations(paths: readonly RemotePathExpectation[]): PreparedRemotePathExpectations {
  requireThat(Array.isArray(paths), "expectations_invalid");
  requireThat(paths.length > 0 && paths.length <= MAX_PATHS, "expectations_bound");
  const seen = new Set<string>();
  let totalBytes = 0;
  const entries = paths.map((value: RemotePathExpectation) => {
    requireThat(value && typeof value.path === "string" && typeof value.content === "string", "expectations_invalid");
    const { path, content } = value;
    requireThat(path.length > 0 && path.length <= MAX_PATH_BYTES && Buffer.byteLength(path) <= MAX_PATH_BYTES && wellFormed(path) &&
      !/[\\\x00-\x1f\x7f]/.test(path), "expectations_invalid");
    const segments = path.split("/");
    requireThat(segments.length <= MAX_DEPTH && segments.every(part => part !== "" && part !== "." && part !== "..") &&
      !seen.has(path), "expectations_invalid");
    for (const previous of seen) requireThat(!path.startsWith(`${previous}/`) && !previous.startsWith(`${path}/`), "expectations_invalid");
    seen.add(path);
    requireThat(content.length <= MAX_CONTENT_BYTES, "expectations_bound");
    requireThat(wellFormed(content), "expectations_invalid");
    const bytes = Buffer.byteLength(content);
    totalBytes += bytes;
    requireThat(bytes <= MAX_CONTENT_BYTES && totalBytes <= MAX_TOTAL_BYTES, "expectations_bound");
    return Object.freeze({ path, segments: Object.freeze(segments), bytes, sha256: digest(content) });
  });
  return Object.freeze({ entries: Object.freeze(entries), totalBytes, [PREPARED]: true as const });
}

function take(decoder: decoding.Decoder, length: number): Uint8Array {
  requireThat(Number.isSafeInteger(length) && length >= 0 && length <= decoder.arr.length - decoder.pos, "update_invalid");
  const bytes = decoder.arr.subarray(decoder.pos, decoder.pos + length);
  decoder.pos += length;
  return bytes;
}
function uint(decoder: decoding.Decoder): number {
  let value = 0, factor = 1;
  for (let count = 0; count < 8; count++) {
    const byte = take(decoder, 1)[0]!;
    value += (byte & 127) * factor;
    requireThat(Number.isSafeInteger(value), "update_invalid");
    if ((byte & 128) === 0) {
      requireThat(count === 0 || byte !== 0, "update_invalid");
      return value;
    }
    factor *= 128;
  }
  return fail("update_invalid");
}
function add(left: number, right: number): number {
  const value = left + right;
  requireThat(Number.isSafeInteger(value), "update_invalid");
  return value;
}

/** Uses the pinned V1 interface, with bounded strings and nonrecursive binary reads. */
class BoundedDecoder extends Y.UpdateDecoderV1 {
  anyValues = 0;
  typeCount = 0;
  override readInfo(): number { return take(this.restDecoder, 1)[0]!; }
  override readClient(): number { return uint(this.restDecoder); }
  override readLen(): number { return uint(this.restDecoder); }
  override readDsClock(): number { return uint(this.restDecoder); }
  override readDsLen(): number { return uint(this.restDecoder); }
  override readLeftID(): Y.ID { return Y.createID(this.readClient(), this.readLen()); }
  override readRightID(): Y.ID { return this.readLeftID(); }
  override readParentInfo(): boolean {
    const value = uint(this.restDecoder);
    requireThat(value === 0 || value === 1, "update_invalid");
    return value === 1;
  }
  override readString(): string {
    const length = uint(this.restDecoder);
    requireThat(length <= MAX_CONTENT_BYTES, "structure_bound");
    return UTF8.decode(take(this.restDecoder, length));
  }
  override readKey(): string { return this.readString(); }
  override readTypeRef(): number {
    requireThat(++this.typeCount <= MAX_NODES, "node_bound");
    const type = uint(this.restDecoder);
    requireThat(type === 0 || type === 1 || type === 2, "unsupported_update"); // Y.Array / Y.Map / Y.Text only.
    return type;
  }
  override readAny(): unknown { return this.any(0); }
  private any(depth: number): unknown {
    requireThat(depth <= MAX_DEPTH && ++this.anyValues <= MAX_STRUCTS, "structure_bound");
    const tag = this.readInfo();
    switch (tag) {
      case 127: return undefined;
      case 126: return null;
      case 121: return false;
      case 120: return true;
      case 119: return this.readString();
      case 125: {
        const first = this.readInfo();
        let value = first & 63, factor = 64, byte = first, count = 0;
        while (byte & 128) {
          requireThat(++count <= 7, "update_invalid");
          byte = this.readInfo(); value += (byte & 127) * factor; factor *= 128;
          requireThat(Number.isSafeInteger(value), "update_invalid");
        }
        return (first & 64) ? -value : value;
      }
      case 124:
      case 123: {
        const bytes = take(this.restDecoder, tag === 124 ? 4 : 8);
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        const value = tag === 124 ? view.getFloat32(0, false) : view.getFloat64(0, false);
        requireThat(Number.isFinite(value), "unsupported_update");
        return value;
      }
      case 122: { const bytes = take(this.restDecoder, 8); return new DataView(bytes.buffer, bytes.byteOffset, 8).getBigInt64(0, false); }
      case 118: {
        const count = uint(this.restDecoder);
        requireThat(count <= MAX_STRUCTS - this.anyValues, "structure_bound");
        const value: Record<string, unknown> = Object.create(null);
        for (let i = 0; i < count; i++) {
          const key = this.readString();
          requireThat(key !== "__proto__" && !Object.hasOwn(value, key), "unsupported_update");
          value[key] = this.any(depth + 1);
        }
        return value;
      }
      case 117: {
        const count = uint(this.restDecoder);
        requireThat(count <= MAX_STRUCTS - this.anyValues, "structure_bound");
        const value: unknown[] = [];
        for (let i = 0; i < count; i++) value.push(this.any(depth + 1));
        return value;
      }
      default: return fail("unsupported_update"); // Includes binary buffers.
    }
  }
}

type Parent = string | Y.ID | null;
interface StructHeader { client: number; clock: number; end: number; kind: number; parent: Parent; origin: Y.ID | null; right: Y.ID | null }
function preflight(update: Uint8Array): Map<number, number> {
  const decoder = new BoundedDecoder(decoding.createDecoder(update));
  const clients = uint(decoder.restDecoder);
  requireThat(clients <= MAX_CLIENTS, "structure_bound");
  const clocks = new Map<number, number>(), histories = new Map<number, StructHeader[]>(), types: StructHeader[] = [];
  let structs = 0;
  for (let c = 0; c < clients; c++) {
    const count = uint(decoder.restDecoder);
    requireThat(count > 0 && count <= MAX_STRUCTS - structs, "structure_bound");
    structs += count;
    const client = decoder.readClient(), firstClock = uint(decoder.restDecoder);
    requireThat(!clocks.has(client) && firstClock === 0, "update_invalid");
    let clock = firstClock;
    const history: StructHeader[] = [];
    histories.set(client, history);
    for (let i = 0; i < count; i++) {
      const info = decoder.readInfo(), kind = info & 31;
      let length = 0, parent: Parent = null, origin: Y.ID | null = null, right: Y.ID | null = null;
      if (kind === 0) {
        requireThat(info === 0, "update_invalid");
        length = decoder.readLen();
      } else {
        requireThat(kind === 1 || kind === 4 || kind === 7 || kind === 8, "unsupported_update");
        if (info & 128) origin = decoder.readLeftID();
        if (info & 64) right = decoder.readRightID();
        if (!origin && !right) {
          parent = decoder.readParentInfo() ? decoder.readString() : decoder.readLeftID();
          if (info & 32) decoder.readString();
        }
        if (kind === 1) length = decoder.readLen();
        else if (kind === 4) length = decoder.readString().length; // Yjs clocks count UTF-16 units.
        else if (kind === 7) { decoder.readTypeRef(); length = 1; }
        else {
          length = decoder.readLen();
          requireThat(length <= MAX_STRUCTS - decoder.anyValues, "structure_bound");
          for (let j = 0; j < length; j++) decoder.readAny();
        }
      }
      requireThat(length > 0, "update_invalid");
      const header = { client, clock, end: add(clock, length), kind, parent, origin, right };
      history.push(header);
      if (kind === 7) types.push(header);
      clock = header.end;
    }
    clocks.set(client, clock);
  }
  const deleteClients = uint(decoder.restDecoder), deletedClients = new Set<number>();
  requireThat(deleteClients <= MAX_CLIENTS, "structure_bound");
  let ranges = 0;
  for (let c = 0; c < deleteClients; c++) {
    const client = uint(decoder.restDecoder), count = uint(decoder.restDecoder);
    requireThat(!deletedClients.has(client), "update_invalid"); deletedClients.add(client);
    requireThat(count <= MAX_STRUCTS - ranges, "structure_bound"); ranges += count;
    let end = 0;
    for (let i = 0; i < count; i++) {
      const clock = decoder.readDsClock(), length = decoder.readDsLen();
      requireThat(length > 0 && clock >= end, "update_invalid");
      end = add(clock, length);
      requireThat(end <= (clocks.get(client) ?? 0), "update_invalid");
    }
  }
  requireThat(decoder.restDecoder.pos === update.length, "update_invalid");
  // Bound historical shared-type nesting too, before Yjs can recursively delete types.
  const find = (id: Y.ID): StructHeader | undefined => {
    const history = histories.get(id.client); if (!history) return;
    let low = 0, high = history.length - 1;
    while (low <= high) {
      const mid = (low + high) >>> 1, item = history[mid]!;
      if (id.clock < item.clock) high = mid - 1;
      else if (id.clock >= item.end) low = mid + 1;
      else return item;
    }
  };
  for (const type of types) {
    let current: StructHeader | undefined = type, depth = 0;
    const seen = new Set<StructHeader>();
    while (current) {
      requireThat(++depth <= MAX_DEPTH && !seen.has(current), "structure_bound"); seen.add(current);
      let owner: StructHeader | undefined = current;
      for (let inherited = 0; owner?.parent === null; inherited++) {
        requireThat(inherited < MAX_DEPTH, "structure_bound");
        const origin = owner.origin ?? owner.right;
        if (!origin) { owner = undefined; break; }
        owner = find(origin);
      }
      if (!owner || typeof owner.parent === "string") break;
      requireThat(owner.parent !== null, "update_invalid");
      current = find(owner.parent);
      requireThat(current && current.kind === 7 && current.clock === owner.parent.clock, "update_invalid");
    }
  }
  return clocks;
}

function stateVector(bytes: Uint8Array): Map<number, number> {
  requireThat(bytes instanceof Uint8Array && bytes.length <= MAX_VECTOR_BYTES, "vector_bound");
  try {
    const decoder = decoding.createDecoder(bytes), count = uint(decoder), result = new Map<number, number>();
    requireThat(count <= MAX_CLIENTS, "vector_bound");
    for (let i = 0; i < count; i++) {
      const client = uint(decoder), clock = uint(decoder);
      requireThat(!result.has(client), "vector_invalid"); result.set(client, clock);
    }
    requireThat(decoder.pos === bytes.length, "vector_invalid");
    return result;
  } catch (error) {
    if (error instanceof RemotePathReadbackError && (error.reason === "vector_bound" || error.reason === "vector_invalid")) throw error;
    return fail("vector_invalid");
  }
}

interface LiveNode { id: string; filename: string; type: string; parent: string | null; map: Y.Map<unknown> }
function verifyPaths(doc: Y.Doc, expectations: PreparedRemotePathExpectations): void {
  const content = doc.getMap<unknown>("content"), nodes = new Map<string, LiveNode>(), children = new Map<string, Map<string, LiveNode>>();
  let root: LiveNode | undefined;
  for (const value of content.values()) {
    requireThat(value instanceof Y.Map, "path_conflict");
    if (value.get("deleted") === true) continue;
    requireThat(nodes.size < MAX_NODES, "node_bound");
    const id = value.get("id"), filename = value.get("filename"), type = value.get("type"), parent = value.get("inFolder");
    requireThat(typeof id === "string" && id.length > 0 && id.length <= MAX_PATH_BYTES && typeof filename === "string" &&
      filename.length > 0 && filename.length <= MAX_PATH_BYTES && typeof type === "string" &&
      (parent === undefined || parent === null || typeof parent === "string") && !nodes.has(id), "path_conflict");
    const node: LiveNode = { id, filename, type, parent: parent ?? null, map: value };
    nodes.set(id, node);
    if (filename === "root" && type === "folder" && node.parent === null) {
      requireThat(!root, "path_conflict"); root = node;
    } else {
      requireThat(typeof parent === "string", "path_conflict");
      const siblings = children.get(parent) ?? new Map<string, LiveNode>();
      requireThat(!siblings.has(filename), "path_conflict"); siblings.set(filename, node); children.set(parent, siblings);
    }
  }
  requireThat(root, "path_missing");
  for (const node of nodes.values()) {
    let current = node, depth = 0;
    const visited = new Set<string>();
    while (current !== root) {
      requireThat(++depth <= MAX_DEPTH && !visited.has(current.id), "path_conflict"); visited.add(current.id);
      const parent = current.parent === null ? undefined : nodes.get(current.parent);
      requireThat(parent?.type === "folder", "path_conflict"); current = parent;
    }
  }
  let total = 0;
  for (const expected of expectations.entries) {
    let node = root;
    for (let i = 0; i < expected.segments.length; i++) {
      const child = children.get(node.id)?.get(expected.segments[i]!);
      requireThat(child, "path_missing");
      requireThat(child.type === (i === expected.segments.length - 1 ? "text" : "folder"), "path_conflict"); node = child;
    }
    const value = node.map.get("content");
    requireThat(typeof value === "string" || value instanceof Y.Text, "path_conflict");
    if (value instanceof Y.Text) {
      requireThat(value.length <= MAX_CONTENT_BYTES, "content_mismatch");
      const delta: Array<{ insert?: unknown; attributes?: unknown }> = value.toDelta();
      requireThat(delta.length <= MAX_STRUCTS && delta.every(part => typeof part.insert === "string" && part.attributes === undefined), "path_conflict");
    }
    const text = typeof value === "string" ? value : value.toString(), bytes = Buffer.byteLength(text);
    total += bytes;
    requireThat(wellFormed(text) && bytes <= MAX_CONTENT_BYTES && total <= MAX_TOTAL_BYTES &&
      bytes === expected.bytes && digest(text) === expected.sha256, "content_mismatch");
  }
}

/** Caller reserves its bounded scratch-memory permit before entering this synchronous verifier. */
export function verifyRemotePathUpdate(update: Uint8Array, expectations: PreparedRemotePathExpectations, targetStateVector: Uint8Array): void {
  requireThat(expectations?.[PREPARED] === true, "expectations_invalid");
  requireThat(update instanceof Uint8Array && update.length > 0 && update.length <= MAX_REMOTE_PATH_UPDATE_BYTES, "update_bound");
  const target = stateVector(targetStateVector);
  let scratch: Y.Doc | undefined;
  try {
    const bytes = new Uint8Array(update), parsed = preflight(bytes);
    for (const [client, clock] of target) requireThat((parsed.get(client) ?? 0) >= clock, "stale_snapshot");
    scratch = new Y.Doc({ gc: false });
    Y.applyUpdate(scratch, bytes);
    requireThat(scratch.store.pendingStructs === null && scratch.store.pendingDs === null, "update_invalid");
    const actual = stateVector(Y.encodeStateVector(scratch));
    requireThat(actual.size === parsed.size && [...parsed].every(([client, clock]) => actual.get(client) === clock), "update_invalid");
    verifyPaths(scratch, expectations);
  } catch (error) {
    if (error instanceof RemotePathReadbackError) throw error;
    fail("update_invalid");
  } finally {
    try { scratch?.destroy(); } catch { /* No document or provider escapes this scope. */ }
  }
}
