/**
 * Segment Protocol v6.2 helpers — JOB SIDE ONLY ("nothing big lives in one
 * file"). Responses stay on the proven v5 windowed/terminal protocol.
 *
 * Wire format (shipped daemon contract):
 *   PARTS     gp/job_sN.<reqid>.<index>   raw UTF-8 slices ≤192KiB encoded,
 *                                         write-once, created by the client
 *                                         as TRACKED doc entries inside the
 *                                         "gp" folder (tracked ⇒ visible to
 *                                         daemon disk reads AND /zip)
 *   MANIFEST  job_sN.json                 published LAST:
 *                                         {"v":6,"id","size","sha12","parts":<int count>}
 *
 * Integrity: manifest size must equal summed part bytes; sha12 is over the
 * full reconstructed job's UTF-8 bytes and doubles as the response ack, so
 * all existing client latch/provenance logic works unchanged.
 */
import * as crypto from "node:crypto";
import * as zlib from "node:zlib";

/**
 * Gzip transport (GP_GZIP=1, requires daemon gunzip support):
 * JSON agent contexts compress ~4-8x, so a 3.5MB job becomes ~0.6MB of
 * base64 — fewer paced creations, smaller/faster sync + verify passes.
 * Parts carry base64 TEXT (pure ASCII → plain char slicing is byte-exact);
 * manifest.size/sha12 describe the ORIGINAL decoded UTF-8 job.
 */
export function gzipPlanParts(jobText: string): { parts: string[] } | null {
  try {
    const gz = zlib.gzipSync(Buffer.from(jobText, "utf8"), { level: 1 });
    const b64 = gz.toString("base64");
    const maxChars = Number(process.env.GP_PART_BYTES ?? 250 * 1024);
    const parts: string[] = [];
    for (let off = 0; off < b64.length; off += maxChars) {
      parts.push(b64.slice(off, off + maxChars));
    }
    return { parts };
  } catch {
    return null;
  }
}

/** Split text into chunks whose UTF-8 encoding is at most maxBytes, never
 * splitting a multi-byte sequence (back off to the previous char boundary).
 * Default 250KiB — safely under the daemon's 256KiB hard part cap. */
export function splitUtf8(
  text: string,
  maxBytes = Number(process.env.GP_PART_BYTES ?? 250 * 1024),
): string[] {
  const bytes = Buffer.from(text, "utf8");
  if (bytes.length <= maxBytes) return [text];
  const out: string[] = [];
  let start = 0;
  while (start < bytes.length) {
    let end = Math.min(start + maxBytes, bytes.length);
    if (end < bytes.length) {
      // UTF-8 continuation bytes are 0b10xxxxxx — never end mid-sequence
      while (end > start && (bytes[end] & 0xc0) === 0x80) end--;
    }
    out.push(bytes.subarray(start, end).toString("utf8"));
    start = end;
  }
  return out;
}

/**
 * Global creation limiter — doc entry CREATIONS are correlated with sandbox
 * allocation failures when bursted (empirical), so cap the whole process at
 * GP_CREATE_MAX (default 3) new-entry writes per rolling GP_CREATE_WINDOW
 * (default 450ms). Updates/clears are not limited.
 */
const CREATE_WINDOW = Number(process.env.GP_CREATE_WINDOW ?? 450);
const CREATE_MAX = Number(process.env.GP_CREATE_MAX ?? 3);
let creationStamps: number[] = [];
export async function creationSlot(): Promise<void> {
  for (;;) {
    const now = Date.now();
    creationStamps = creationStamps.filter((t) => now - t < CREATE_WINDOW);
    if (creationStamps.length < CREATE_MAX) {
      creationStamps.push(now);
      return;
    }
    const wait = creationStamps[0] + CREATE_WINDOW - now + 10;
    await new Promise((r) => setTimeout(r, wait));
  }
}

export interface PlannedJob {
  reqid: string;
  /** workspace-relative directory holding parts ("gp") */
  dir: string;
  /** base = doc-entry filename inside the folder; key = expected disk/zip path */
  parts: { base: string; key: string; text: string }[];
  manifestText: string;
  /** "gz+b64" when parts carry gzip+base64 of the job instead of raw UTF-8 */
  enc: "raw" | "gz+b64";
}

export interface PlannedV63Part {
  index: number;
  base: string;
  key: string;
  text: string;
}

export interface PlannedV63Job {
  version: 6.3;
  reqid: string;
  dir: string;
  parts: PlannedV63Part[];
  publishOrder: PlannedV63Part[];
  enc: "raw" | "gz+b64";
  size: number;
  sha12: string;
}

export function newReqid(): string {
  return crypto.randomBytes(5).toString("hex");
}

/** Plan a segmented job per daemon v6.2 contract. With opts.gzip (and when it
 * actually shrinks the payload), parts carry base64(gzip(job)) slices and the
 * manifest gains enc:"gz+b64"; size/sha12 always describe the ORIGINAL job. */
export function planJob(
  jobName: string,
  jobText: string,
  opts?: { gzip?: boolean; dir?: string },
): PlannedJob {
  const dir = opts?.dir ?? "gp";
  const reqid = newReqid();
  const size = Buffer.byteLength(jobText, "utf8");
  const sha12 = crypto.createHash("sha256").update(Buffer.from(jobText, "utf8")).digest("hex").slice(0, 12);

  let texts: string[] | null = null;
  let enc: "raw" | "gz+b64" = "raw";
  if (opts?.gzip) {
    const gz = gzipPlanParts(jobText);
    if (gz && gz.parts.length < splitUtf8(jobText).length) {
      texts = gz.parts;
      enc = "gz+b64";
    }
  }
  if (!texts) texts = splitUtf8(jobText);

  const parts = texts.map((text, i) => {
    const base = `${jobName}.${reqid}.${i}`;
    return { base, key: `${dir}/${base}`, text };
  });
  const manifestText = JSON.stringify({
    v: 6,
    id: reqid,
    size,
    sha12,
    parts: texts.length,
    ...(enc === "gz+b64" ? { enc } : {}),
  });
  return { reqid, dir, parts, manifestText, enc };
}

export function chooseSegmentProtocol(protocols: Iterable<string>): 6.2 | 6.3 {
  return new Set(protocols).has("segmented-v6.3-embedded-commit") ? 6.3 : 6.2;
}

/** Large requests use segmented transport by default. GP_SEGMENT=0 is the
 * explicit emergency compatibility opt-out. */
export function isSegmentTransportEnabled(value = process.env.GP_SEGMENT): boolean {
  return value !== "0";
}

function splitUtf8WithCaps(text: string, firstMaxBytes: number, nextMaxBytes: number): string[] {
  const bytes = Buffer.from(text, "utf8");
  if (!bytes.length) return [""];
  const out: string[] = [];
  let start = 0;
  let cap = firstMaxBytes;
  while (start < bytes.length) {
    let end = Math.min(start + cap, bytes.length);
    if (end < bytes.length) {
      while (end > start && (bytes[end] & 0xc0) === 0x80) end--;
    }
    if (end === start) throw new RangeError("segment byte limit is too small for one UTF-8 code point");
    out.push(bytes.subarray(start, end).toString("utf8"));
    start = end;
    cap = nextMaxBytes;
  }
  return out;
}

export function planV63Job(
  jobName: string,
  jobText: string,
  options: { gzip?: boolean; dir?: string; partBytes?: number; maxPartBytes?: number } = {},
): PlannedV63Job {
  const dir = options.dir ?? "gp";
  const maxPartBytes = Math.trunc(options.maxPartBytes ?? 256 * 1024);
  const requestedPartBytes = Math.trunc(options.partBytes ?? Number(process.env.GP_PART_BYTES ?? 250 * 1024));
  if (maxPartBytes <= 1200 || requestedPartBytes < 1) throw new RangeError("invalid v6.3 part byte limits");
  const subsequentCap = Math.min(requestedPartBytes, maxPartBytes);
  // The daemon permits up to 1024 header bytes. Reserve that full bound plus
  // magic/newline so part zero remains valid even as part-count digits grow.
  const firstPayloadCap = Math.min(requestedPartBytes, maxPartBytes - 1030);
  const original = Buffer.from(jobText, "utf8");
  const reqid = newReqid();
  const digest = crypto.createHash("sha256").update(original).digest("hex").slice(0, 12);

  const rawParts = splitUtf8WithCaps(jobText, firstPayloadCap, subsequentCap);
  let payloadParts = rawParts;
  let enc: "raw" | "gz+b64" = "raw";
  if (options.gzip) {
    const encoded = zlib.gzipSync(original, { level: 1 }).toString("base64");
    const gzipParts: string[] = [];
    gzipParts.push(encoded.slice(0, firstPayloadCap));
    for (let offset = firstPayloadCap; offset < encoded.length; offset += subsequentCap) {
      gzipParts.push(encoded.slice(offset, offset + subsequentCap));
    }
    if (gzipParts.length < rawParts.length) {
      payloadParts = gzipParts;
      enc = "gz+b64";
    }
  }

  const header = JSON.stringify({
    v: 6.3,
    id: reqid,
    parts: payloadParts.length,
    size: original.length,
    sha12: digest,
    ...(enc === "gz+b64" ? { enc } : {}),
  });
  if (Buffer.byteLength(header, "utf8") > 1024) throw new RangeError("v6.3 commit header exceeds daemon limit");

  const parts = payloadParts.map((payload, index): PlannedV63Part => {
    const base = `${jobName}.${reqid}.${index}`;
    const text = index === 0 ? `GP63 ${header}\n${payload}` : payload;
    if (Buffer.byteLength(text, "utf8") > maxPartBytes) throw new RangeError(`v6.3 part ${index} exceeds daemon limit`);
    return { index, base, key: `${dir}/${base}`, text };
  });
  return {
    version: 6.3,
    reqid,
    dir,
    parts,
    publishOrder: [...parts.slice(1), parts[0]],
    enc,
    size: original.length,
    sha12: digest,
  };
}
