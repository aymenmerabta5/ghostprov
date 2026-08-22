/**
 * Slot pool — concurrent relay execution over the sandbox daemon.
 *
 * Instead of serializing every request through the single shared render
 * pipeline (job.json + POST /render + response.json), each request gets its
 * own slot file pair (job_s<N>.json / resp_s<N>.json) processed by an
 * independent daemon worker thread inside the sandbox:
 *
 *   1. ensureReady()      — daemon alive? else deploy files + one revive render
 *   2. acquireSlot()      — FIFO wait for a free slot (pool = daemon-reported)
 *   3. write job_sN.json  — via persistent Yjs doc connection (real-time)
 *   4. poll resp_sN.json  — local doc view, no network; v2 tail protocol fed
 *                           into SseDeltaExtractor for true incremental SSE
 *   5. release slot       — final consumed or failed
 *
 * Stale-output protection: the daemon stamps every resp write with
 * `ack` = sha12(job text). A final object is only trusted AFTER a progress
 * frame carrying OUR ack was seen (the daemon always writes the accepted
 * frame before touching upstream).
 */
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import { fileURLToPath } from "node:url";
import { config } from "../config";
import { renderQueue } from "../utils/queue";
import { triggerRender, fetchZipFiles, pageFetch, bust } from "../prism/gateway";
import { deleteDocFile, readDocFile, writeDocFile, writeDocFileAppended, ensureDocFolder } from "../prism/ydoc";
import {
  chooseSegmentProtocol,
  planJob,
  planV63Job,
  creationSlot,
  type PlannedJob,
  type PlannedV63Job,
} from "./segments";
import { TerminalBackedSseExtractor, sanitizeFinal, type SseFrame } from "./sse";
import { matchesTerminalEnvelope, settleWithTerminalRescue } from "./terminal-rescue";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function sha12(text: string): string {
  return crypto.createHash("sha256").update(text).digest("hex").slice(0, 12);
}

export class SlotError extends Error {
  constructor(message: string, readonly kind: "sync" | "timeout" | "no-bridge" | "upstream" | "exhausted") {
    super(message);
  }
}

// ---------------------------------------------------------------------------
// Daemon deployment (.latexmkrc arms the launcher on every render — the
// Yjs doc re-materializes both files after any reboot, making this
// self-healing: first render of a session revives the daemon)
// ---------------------------------------------------------------------------

/**
 * .latexmkrc that launches BOTH the legacy runner and the slot daemon.
 * Launch is UNCONDITIONAL — the daemon self-guards via /tmp/gp_daemon.pid
 * (signal-0 liveness probe). Any pgrep-based guard self-matches the shell
 * running this very string (its argv contains the literal launch text), so
 * pattern-based guards can never work here.
 */
export const LATEXMKRC_SLOTS = [
  "$pdf_mode = 1;",
  "system('rm -f main.pdf main.aux main.fls main.fdb_latexmk; rm -f /workspace/response.json');",
  "$pdflatex = 'nohup python gp_daemon.py >> /tmp/gp_daemon.log 2>&1 & nohup python runner.py > /tmp/gp_bg.log 2>&1 & pdflatex -interaction=nonstopmode %O %S'",
].join("\n") + "\n";

export const DAEMON_FILE = "gp_daemon.py";
export const STATUS_FILE = "gp_status.json";

let cachedDaemonSrc: string | null = null;

/** Sandbox-side daemon source (single source of truth: sandbox/gp_daemon.py). */
function getDaemonSrc(): string {
  if (cachedDaemonSrc) return cachedDaemonSrc;
  const p = fileURLToPath(new URL("../../sandbox/gp_daemon.py", import.meta.url));
  cachedDaemonSrc = fs.readFileSync(p, "utf8");
  return cachedDaemonSrc;
}

/** Read a file's DISK content via /zip (authoritative; doc view can be stale). */
export async function readDiskFile(name: string): Promise<string | null> {
  const files = await fetchZipFiles([name]);
  const data = files.get(name);
  return data ? new TextDecoder().decode(data) : null;
}

interface DaemonStatus {
  pid: number;
  ts: number;
  slots: number;
  active: Record<string, string>;
  done: number;
  failed: number;
  /** set by v6.2.2+ daemons — surfaced on revive so stale-source incidents are visible */
  daemon_build?: string;
  job_protocols?: string[];
}

function parseStatus(raw: string | null): DaemonStatus | null {
  if (!raw) return null;
  try {
    const j = JSON.parse(raw);
    if (typeof j?.pid === "number" && typeof j?.ts === "number") return j as DaemonStatus;
  } catch {}
  return null;
}

let liveJobProtocols = new Set<string>();

function rememberDaemonCapabilities(status: DaemonStatus): void {
  liveJobProtocols = new Set(Array.isArray(status.job_protocols) ? status.job_protocols : []);
}

export function daemonJobProtocols(): ReadonlySet<string> {
  return liveJobProtocols;
}

/** Daemon considered live if heartbeat is fresher than this. */
function statusFresh(s: DaemonStatus | null): boolean {
  return !!s && Date.now() / 1000 - s.ts < 15;
}

let deployLock: Promise<number> | null = null;

/**
 * Lane provisioning: guarantee every job_sN/resp_sN pair is TRACKED (has a
 * Yjs doc entry). The /zip view only archives tracked files, and resp_sN.json
 * is written by the daemon directly on disk (atomic os.replace) — without a
 * doc entry it exists on disk but is INVISIBLE to this client forever, which
 * presents as "daemon never accepted the job". Lanes are provisioned by
 * deployAndRevive, but that only runs when the daemon is DOWN — growing
 * GP_SLOTS while the daemon lives skips it, blinding every new lane.
 *
 * Scan is local (Yjs map lookups, no network); only MISSING entries are
 * written, paced ~450ms apart (creation bursts correlate with sandbox
 * allocation failures). One-time cost when the pool grows; free afterwards.
 */
let provisionLock: Promise<void> | null = null;

async function provisionLanes(n: number): Promise<void> {
  const missing: string[] = [];
  for (let i = 1; i <= n; i++) {
    for (const name of [`job_s${i}.json`, `resp_s${i}.json`]) {
      try {
        if ((await readDocFile(name)) === null) missing.push(name);
      } catch { /* ydoc unavailable — request flow will surface it */ }
    }
  }
  if (!missing.length) return;
  console.log(`[slots] ${missing.length} lane files untracked — provisioning (paced)…`);
  for (const name of missing) {
    try {
      await writeDocFile(name, "");
    } catch (e: any) {
      console.warn(`[slots] provision ${name} failed: ${String(e?.message ?? e).slice(0, 140)}`);
    }
    await sleep(450);
  }
  console.log(`[slots] lane provisioning complete (${missing.length} files created)`);
}

function ensureProvisioned(n: number): Promise<void> {
  if (!provisionLock) {
    provisionLock = provisionLanes(n).finally(() => { provisionLock = null; });
  }
  return provisionLock;
}

/**
 * Ensure the daemon is deployed AND alive; returns its live slot count.
 * Serialized so N concurrent requests trigger at most one deploy/revive.
 *
 * NOTE: liveness is checked via /zip (authoritative DISK view). The daemon's
 * atomic os.replace() writes do NOT propagate disk->Yjs doc, so WebSocket
 * doc reads show stale empties for everything the daemon writes.
 */
export async function ensureReady(): Promise<number> {
  // fast path — no lock contention when healthy
  const quick = parseStatus(await readDiskFile(STATUS_FILE).catch(() => null));
  if (statusFresh(quick)) {
    rememberDaemonCapabilities(quick!);
    const n = Math.max(1, Math.min(config.slots, quick!.slots || config.slots));
    await ensureProvisioned(n);
    return n;
  }

  if (!deployLock) {
    deployLock = deployAndRevive().finally(() => { deployLock = null; });
  }
  const n = await deployLock;
  await ensureProvisioned(n);
  return Math.max(1, Math.min(config.slots, n));
}

async function deployAndRevive(): Promise<number> {
  console.log("[slots] daemon not alive — deploying/reviving…");
  // 1. push expected files into the doc.
  //    SINGLE-WRITER RULE for gp_daemon.py: the sandbox agent OWNS its
  //    content and ships upgrades directly on disk (the daemon self-restarts
  //    via its source-signature watchdog). We only BOOTSTRAP the file when
  //    the doc has none — never overwrite, or every revive would downgrade
  //    a newer live daemon to our bundled copy (observed as "stale daemon"
  //    during multi-agent runs).
  let haveDaemonSource = false;
  try {
    const docDaemon = await readDocFile(DAEMON_FILE);
    if (docDaemon === null || docDaemon.trim() === "") {
      console.log("[slots] bootstrapping gp_daemon.py (doc had no copy)…");
      await writeDocFile(DAEMON_FILE, getDaemonSrc());
      await sleep(500);
      haveDaemonSource = true;
    } else {
      haveDaemonSource = true;
      console.log("[slots] gp_daemon.py present in doc — leaving untouched (sandbox agent owns it)");
    }
    if ((await readDocFile(".latexmkrc")) !== LATEXMKRC_SLOTS) {
      console.log("[slots] writing .latexmkrc launcher to doc…");
      await writeDocFile(".latexmkrc", LATEXMKRC_SLOTS);
      await sleep(500);
    }
    // Pre-create ALL tracked entries the loop needs. Disk files without a doc
    // entry are invisible to the sync (zip only archives tracked files), so
    // resp_sN/gp_status MUST exist here before the daemon writes them.
    // CREATIONS ARE PACED (~500ms apart): a large burst of new doc entries is
    // correlated with sandbox allocation failures — grow the pool gently.
    const n = config.slots;
    for (let i = 1; i <= n; i++) {
      for (const name of [`job_s${i}.json`, `resp_s${i}.json`]) {
        if ((await readDocFile(name)) === null) {
          await writeDocFile(name, "");
          await sleep(500);
        }
      }
    }
    if ((await readDocFile(STATUS_FILE)) === null) {
      await writeDocFile(STATUS_FILE, "");
      await sleep(500);
    }
  } catch (e: any) {
    throw new SlotError(`ydoc deploy failed: ${String(e?.message ?? e)}`, e?.kind === "auth" ? "no-bridge" : "sync");
  }

  // 2. verify files reached DISK via authoritative zip view BEFORE rendering.
  //    gp_daemon.py: PRESENCE check only (content is owned by the sandbox
  //    agent — equality against our bundled copy would clobber upgrades).
  //    .latexmkrc: WE own it, enforce full equality with bounded rewrites.
  const syncDeadline = Date.now() + config.syncTimeoutMs + 30_000;
  let latexRewrites = 0;
  for (;;) {
    let ok = true;
    const daemonOnDisk = await readDiskFile(DAEMON_FILE).catch(() => null);
    if (!haveDaemonSource || daemonOnDisk === null || daemonOnDisk.trim() === "") {
      ok = false;
      if (Date.now() > syncDeadline) throw new SlotError("gp_daemon.py did not sync to sandbox disk in time", "sync");
      await sleep(800);
    }
    const latexOnDisk = await readDiskFile(".latexmkrc").catch(() => null);
    if (latexOnDisk !== LATEXMKRC_SLOTS) {
      ok = false;
      if (Date.now() > syncDeadline) throw new SlotError(".latexmkrc did not sync to sandbox disk in time", "sync");
      // disk still stale (or echo-reverted) — push the doc copy again
      if (latexRewrites < 6) {
        latexRewrites++;
        console.log(`[slots] .latexmkrc disk mismatch — rewriting doc copy (#${latexRewrites})…`);
        await writeDocFile(".latexmkrc", LATEXMKRC_SLOTS);
        await sleep(1200);
      } else {
        await sleep(800);
      }
    }
    if (ok) break;
  }

  // 3. revive render (renderQueue-serialized — shared backend editor).
  //    A crashed/stuck render WEDGES the backend queue: every later render
  //    silently no-ops (202, latexmk never runs). Cancel first to unstick.
  await renderQueue.add(async () => {
    await pageFetch(`/s/sandboxes/proxy/render-cancel?prism_cache_bust=${bust()}`, {
      method: "POST",
      json: {},
    }).catch(() => undefined);
    await triggerRender();
  });

  // 4. wait for a fresh heartbeat — via DISK view (/zip); doc view is stale
  //    for daemon-written files (atomic os.replace defeats disk->doc sync)
  const hbDeadline = Date.now() + 60_000;
  for (;;) {
    const s = parseStatus(await readDiskFile(STATUS_FILE).catch(() => null));
    if (statusFresh(s)) {
      rememberDaemonCapabilities(s!);
      console.log(
        `[slots] daemon alive pid=${s!.pid} build=${s!.daemon_build ?? "unknown"} ` +
        `slots=${s!.slots} done=${s!.done} failed=${s!.failed}` +
        (s!.job_protocols ? ` protocols=[${s!.job_protocols.join(",")}]` : ""),
      );
      return s!.slots || config.slots;
    }
    if (Date.now() > hbDeadline) {
      throw new SlotError("daemon did not report after revive render", "timeout");
    }
    await sleep(1000);
  }
}

// ---------------------------------------------------------------------------
// Slot pool (grows on demand up to the daemon's watched slot count)
// ---------------------------------------------------------------------------

interface SlotState {
  n: number;
  busy: boolean;
}

class SlotPool {
  private slots: SlotState[] = [];

  growTo(n: number) {
    for (let i = this.slots.length + 1; i <= n; i++) this.slots.push({ n: i, busy: false });
  }

  get size(): number {
    return this.slots.length;
  }

  get available(): number {
    return this.slots.filter((s) => !s.busy).length;
  }

  /** FIFO-ish acquisition with bounded wait. */
  async acquire(timeoutMs: number): Promise<SlotState> {
    const t0 = Date.now();
    for (;;) {
      const free = this.slots.find((s) => !s.busy);
      if (free) {
        free.busy = true;
        return free;
      }
      if (Date.now() - t0 > timeoutMs) {
        throw new SlotError(`slot pool exhausted (${this.size}) — all slots busy`, "exhausted");
      }
      await sleep(40);
    }
  }

  release(slot: SlotState) {
    slot.busy = false;
  }
}

const pool = new SlotPool();

/** Highest chunk-part count ever written per lane — cleanup clears up to this. */
const laneMaxParts = new Map<number, number>();

/** Doc-entry basenames created per lane (segment parts) — cleared after use. */
const laneArtifacts = new Map<number, Set<string>>();
function laneArtifactSet(n: number): Set<string> {
  let s = laneArtifacts.get(n);
  if (!s) { s = new Set(); laneArtifacts.set(n, s); }
  return s;
}

/** Cached uuid of the shared segment folder (avoid entry scans per request). */
let gpFolderId: string | null = null;

// ---------------------------------------------------------------------------
// Job execution
// ---------------------------------------------------------------------------

export interface RunHandle {
  events(): AsyncGenerator<SseFrame>;
  final: Promise<any>;
  stats: { bytes: number; firstByteMs: number | null; frames: number };
}

let reqCounter = 0;

/**
 * Execute one relay request through its own slot. Streaming frames arrive as
 * real relay SSE events; non-stream callers just await `final`.
 */
export async function runSlotJob(body: Record<string, unknown>): Promise<RunHandle> {
  const tag = `slot${++reqCounter}`;
  const jobText = JSON.stringify(body);
  const ack = sha12(jobText);
  console.log(`[slots ${tag}] start model=${body.model} job=${(jobText.length / 1024).toFixed(1)}KiB stream=${body.stream === true}`);

  const daemonSlots = await ensureReady();
  pool.growTo(daemonSlots);

  const slot = await pool.acquire(300_000);
  const jobName = `job_s${slot.n}.json`;
  const respName = `resp_s${slot.n}.json`;
  console.log(`[slots ${tag}] acquired slot ${slot.n} (${pool.available}/${pool.size} free)`);

  const frames: SseFrame[] = [];
  const stats = { bytes: 0, firstByteMs: null as number | null, frames: 0 };
  let notify: (() => void) | null = null;
  let closed = false;
  const extractor = new TerminalBackedSseExtractor();
  let activeReqid: string | null = null;

  // --- job write plan -------------------------------------------------------
  // Segmented jobs (GP_SEGMENT=1): payloads >512KiB are split into small
  // write-once entries under the shared "gp" folder. An advertised v6.3
  // daemon commits by publishing embedded-header part zero last, avoiding the
  // shared job_sN cursor; older daemons receive a v6.2 cursor manifest.
  // The daemon
  // assembles parts, verifies size+sha12, and responds via the UNCHANGED v5
  // windowed/terminal protocol. This removes the single-Yjs-update size
  // ceiling entirely (backend silently truncated monolithic updates at a
  // moving ~2.4-3.5MB limit — deterministic, unfixable by retry).
  const SEGMENT = process.env.GP_SEGMENT === "1";
  let segPlan: PlannedJob | null = null;
  let v63Plan: PlannedV63Job | null = null;
  if (SEGMENT && jobText.length > 512 * 1024) {
    if (chooseSegmentProtocol(liveJobProtocols) === 6.3) {
      v63Plan = planV63Job(jobName, jobText, { gzip: process.env.GP_GZIP !== "0" });
    } else {
      segPlan = planJob(jobName, jobText, { gzip: process.env.GP_GZIP !== "0" });
    }
    activeReqid = (v63Plan ?? segPlan)!.reqid;
    const activePlan = v63Plan ?? segPlan!;
    console.log(
      `[slots ${tag}] segmented job: ${activePlan.parts.length} parts ` +
      `(v${v63Plan ? "6.3" : "6.2"}${activePlan.enc === "gz+b64" ? "+gz" : ""}, id=${activePlan.reqid})`,
    );
  }

  // legacy chunked mode (superseded by segments; kept as fallback)
  const CHUNKING_ENABLED = process.env.GP_CHUNK_JOBS === "1";
  const CHUNK_SIZE = 512 * 1024;
  const parts: { name: string; text: string }[] = [];
  let indexText: string | null = null;
  if (!SEGMENT && CHUNKING_ENABLED && jobText.length > CHUNK_SIZE) {
    const k = Math.ceil(jobText.length / CHUNK_SIZE);
    for (let i = 0; i < k; i++) {
      parts.push({ name: `${jobName}.p${i}`, text: jobText.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE) });
    }
    indexText = JSON.stringify({ gp_chunked: true, parts: k, size: jobText.length });
    laneMaxParts.set(slot.n, Math.max(laneMaxParts.get(slot.n) ?? 0, k));
    console.log(`[slots ${tag}] chunked job: ${parts.length} parts × ≤${CHUNK_SIZE / 1024}KiB`);
  }

  const norm = (s: string) => s.replace(/^\uFEFF/, "").replace(/\r\n/g, "\n");

  // Deferred settlement + release forensics. finalPromise settles ONLY via
  // settleFinal/settleError below; the release path then guarantees a PROVEN
  // terminal (matching gp_ack + request id) before freeing the lane — it must
  // NEVER release merely because the incremental stream view went quiet.
  let settleFinal!: (v: any) => void;
  let settleError!: (e: any) => void;
  const finalPromise = new Promise<any>((res, rej) => { settleFinal = res; settleError = rej; });
  let settled = false;
  let sawTerminal = false;
  let requestPublished = false;
  let releaseCause = "pending";
  function fail(kind: SlotError["kind"], message: string): void {
    console.error(`[slots ${tag}] FAILED (${kind}): ${message.slice(0, 240)}`);
    if (!settled) { settled = true; releaseCause = `error:${kind}`; settleError(new SlotError(message, kind)); }
  }
  function succeed(value: any, reason: string): void {
    if (!settled) { settled = true; releaseCause = reason; settleFinal(value); }
  }

  /** Write one doc entry and verify it reached DISK (scaled deadline + diagnostics).
   * Content above GP_APPEND_JOBS threshold (default 512KB) goes out as small
   * sequential appends — monolithic multi-MB updates are silently truncated
   * by the backend (observed -712B deterministic loss at ~3.5MB). */
  async function writeAndVerify(name: string, text: string): Promise<void> {
    try {
      if (text.length > 512 * 1024 && process.env.GP_APPEND_JOBS !== "0") {
        await writeDocFileAppended(name, text);
      } else {
        await writeDocFile(name, text);
      }
    } catch (e: any) {
      console.error(`[slots ${tag}] WRITE FAILED ${name} (${text.length}B): ${String(e?.message ?? e).slice(0, 200)}`);
      throw new SlotError(`job write failed: ${name}: ${String(e?.message ?? e).slice(0, 120)}`, e?.kind === "auth" ? "no-bridge" : "sync");
    }
    // doc->disk latency scales with size; scale the window too (20s base,
    // +25ms/KB, cap 120s), and LOG progress so sync stalls self-explain.
    const deadline = Date.now() + Math.min(20_000 + Math.floor(text.length / 40), 120_000);
    let lastLog = Date.now();
    for (;;) {
      const disk = await readDiskFile(name).catch(() => null);
      if (disk && norm(disk) === norm(text)) return;
      if (Date.now() > deadline) {
        console.error(`[slots ${tag}] VERIFY FAILED ${name}: want=${text.length}B got=${disk ? disk.length + "B" : "absent"} (doc->disk sync did not converge)`);
        throw new SlotError(`${name} never reached sandbox disk`, "sync");
      }
      if (Date.now() - lastLog > 10_000) {
        lastLog = Date.now();
        console.log(`[slots ${tag}] waiting disk-sync ${name}: want=${text.length}B got=${disk ? disk.length + "B" : "absent"}`);
      }
      await sleep(1500);
    }
  }

  const runLoop = (async (): Promise<any> => {
    // v6.2 publisher: paced part writes → manifest LAST. NO client-side part
    // readback — the daemon validates per-part size, gzip, decoded size, and
    // full-job sha12; failures come back as bad_v6_* failed statuses which we
    // auto-retry once below with a fresh reqid. (Readback cost 1-3s/request.)
    let segRetried = false;
    async function segmentFolder(dir: string): Promise<string> {
      let folderId: string;
      if (gpFolderId) {
        folderId = gpFolderId;
      } else {
        try {
          folderId = await ensureDocFolder(dir);
          gpFolderId = folderId;
        } catch (e: any) {
          throw new SlotError(`ensureDocFolder failed: ${String(e?.message ?? e).slice(0, 140)}`, "sync");
        }
      }
      return folderId;
    }

    async function publishSegments(plan: PlannedJob): Promise<void> {
      const t0w = Date.now();
      const folderId = await segmentFolder(plan.dir);
      for (const p of plan.parts) {
        await creationSlot();
        try {
          await writeDocFile(p.base, p.text, { folderId });
        } catch (e: any) {
          console.error(`[slots ${tag}] PART WRITE FAILED ${p.key}: ${String(e?.message ?? e).slice(0, 180)}`);
          throw new SlotError(`part write failed: ${p.base}`, e?.kind === "auth" ? "no-bridge" : "sync");
        }
        laneArtifactSet(slot.n).add(p.base);
      }
      const tW = Date.now() - t0w;
      const tM0 = Date.now();
      await writeAndVerify(jobName, plan.manifestText);
      console.log(
        `[slots ${tag}] transport: parts=${plan.parts.length} enc=${plan.enc} ` +
        `write=${tW}ms manifest=${Date.now() - tM0}ms`,
      );
    }

    async function publishV63(plan: PlannedV63Job): Promise<void> {
      const t0w = Date.now();
      const folderId = await segmentFolder(plan.dir);
      for (const part of plan.publishOrder) {
        await creationSlot();
        try {
          await writeDocFile(part.base, part.text, { folderId });
        } catch (e: any) {
          console.error(`[slots ${tag}] V63 PART WRITE FAILED ${part.key}: ${String(e?.message ?? e).slice(0, 180)}`);
          throw new SlotError(`v6.3 part write failed: ${part.base}`, e?.kind === "auth" ? "no-bridge" : "sync");
        }
        laneArtifactSet(slot.n).add(part.base);
      }
      console.log(
        `[slots ${tag}] transport: v=6.3 parts=${plan.parts.length} enc=${plan.enc} ` +
        `write=${Date.now() - t0w}ms commit=${plan.parts[0].base}`,
      );
    }

    if (v63Plan) {
      await publishV63(v63Plan);
    } else if (segPlan) {
      const tGz0 = Date.now();
      // note: gzip ran inside planJob; re-measure is not possible post-hoc,
      // so we log planning+gzip as a single stage next to writes instead.
      void tGz0;
      await publishSegments(segPlan);
    } else {
      for (const p of parts) await writeAndVerify(p.name, p.text);
      await writeAndVerify(jobName, indexText ?? jobText);
    }
    requestPublished = true;

    const t0 = Date.now();
    let lastProgressAt = Date.now();
    let lastRaw: string | null = null;
    let latched = false;
    let interval = 60;
    // Unlatch watchdog state: the daemon acks within seconds of dispatch, so
    // silence past the deadline means THIS slot is wedged (zombie worker) or
    // the dispatch was swallowed by a daemon restart baseline. Rewrite to
    // force a fresh mtime -> rescan; fail LOUDLY long before stallTimeoutMs.
    let unlatchAttempts = 0;
    let unlatchDeadline = t0 + config.unlatchTimeoutMs;
    for (;;) {
      // stall detection ONLY — healthy streams may run indefinitely
      if (Date.now() - lastProgressAt > config.stallTimeoutMs) {
        console.error(`[slots ${tag}] STALL slot=${slot.n} latched=${latched}`);
        throw new SlotError(`stream stalled ${Math.round(config.stallTimeoutMs / 1000)}s with no data`, "timeout");
      }
      if (!latched && Date.now() > unlatchDeadline) {
        if (unlatchAttempts < 2) {
          unlatchAttempts++;
          unlatchDeadline = Date.now() + config.unlatchTimeoutMs;
          console.warn(`[slots ${tag}] no ack within ${config.unlatchTimeoutMs}ms — republishing commit (recovery #${unlatchAttempts})`);
          // Segmented: republish ONLY the manifest (parts are immutable and
          // already verified on disk; a fresh manifest mtime triggers rescan).
          if (v63Plan) {
            // Republish the commit part INTO THE gp FOLDER — without folderId
            // this created a root-level stray the daemon could never see.
            const fid = await segmentFolder(v63Plan.dir);
            await writeDocFile(v63Plan.parts[0].base, v63Plan.parts[0].text, { folderId: fid }).catch(() => undefined);
            continue;
          }
          const rewrite = segPlan ? segPlan.manifestText : (indexText ?? jobText);
          if (!segPlan && rewrite.length > 512 * 1024 && process.env.GP_APPEND_JOBS !== "0") {
            await writeDocFileAppended(jobName, rewrite).catch(() => undefined);
          } else {
            await writeDocFile(jobName, rewrite).catch(() => undefined);
          }
          continue;
        }
        console.error(`[slots ${tag}] UNLATCHED slot=${slot.n} — daemon never accepted the job after ${unlatchAttempts} recoveries`);
        throw new SlotError(`daemon never accepted job on ${jobName}`, "sync");
      }

      // Daemon output is invisible in the WS doc view (atomic os.replace
      // swaps the inode; the disk->doc watcher never sees it) — read via
      // /zip, the authoritative disk view.
      const files = await fetchZipFiles([respName]).catch(() => null);
      const data = files?.get(respName);
      const raw = data ? new TextDecoder().decode(data) : null;
      if (raw && raw.length > 2 && raw !== lastRaw) {
        lastRaw = raw; // the file changed since last poll → daemon is alive
        lastProgressAt = Date.now();

        let j: any = null;
        try { j = JSON.parse(raw); } catch { /* mid-write partial JSON */ }

        if (j?.streaming === true) {
          if (latched || j.ack === ack) {
            if (!latched) {
              latched = true;
              stats.firstByteMs = Date.now() - t0;
              console.log(`[slots ${tag}] ack latched at ${stats.firstByteMs}ms`);
            }
            const hadDeferredGap = extractor.deferredGap !== null;
            const freshFrames = extractor.feedProgress(raw);
            if (!hadDeferredGap && extractor.deferredGap) {
              console.warn(
                `[slots ${tag}] incremental SSE window gap ` +
                `(cursor=${extractor.deferredGap.cursor}, start=${extractor.deferredGap.windowStart}); ` +
                "deferring deltas until authoritative terminal replay",
              );
            }
            if (freshFrames.length) {
              for (const f of freshFrames) frames.push(f);
              stats.frames += freshFrames.length;
              stats.bytes += freshFrames.reduce((n, f) => n + f.data.length, 0);
              (notify as (() => void) | null)?.();
            }
          }
          // ack mismatch before latch → stale output from a previous run; ignore
        } else if (j?.gp_final !== undefined || j?.status === "failed" ||
                   j?.status === "completed" || j?.status === "incomplete") {
          // Terminal write. Authoritative when it carries OUR ack (daemon
          // always stamps gp_ack/ack) or after we latched this run.
          if (matchesTerminalEnvelope(j, ack, activeReqid)) {
            latched = true;
            // Daemon rejected our segments (size/gzip/sha validation). One
            // automatic republish with a fresh reqid before surfacing —
            // covers rare deterministic sync loss without burning a Codex turn.
            const code = typeof j.error?.code === "string" ? j.error.code : "";
            if (j.status === "failed" && (segPlan || v63Plan) && !segRetried && code.startsWith("bad_v6")) {
              segRetried = true;
              console.warn(`[slots ${tag}] daemon rejected segments (${code}) — republishing with fresh reqid`);
              if (v63Plan) {
                v63Plan = planV63Job(jobName, jobText, { gzip: process.env.GP_GZIP !== "0" });
              } else {
                segPlan = planJob(jobName, jobText, { gzip: process.env.GP_GZIP !== "0" });
              }
              activeReqid = (v63Plan ?? segPlan)!.reqid;
              latched = false;
              unlatchAttempts = 0;
              unlatchDeadline = Date.now() + config.unlatchTimeoutMs;
              lastProgressAt = Date.now();
              if (v63Plan) await publishV63(v63Plan);
              else await publishSegments(segPlan!);
              continue;
            }
            if (j.status === "failed") {
              console.error(`[slots ${tag}] UPSTREAM FAILED slot=${slot.n}: ${JSON.stringify(j.error ?? {}).slice(0, 300)}`);
              throw new SlotError(`upstream failed: ${JSON.stringify(j.error ?? {}).slice(0, 300)}`, "upstream");
            }
            const fin = j.gp_final ?? j;
            // CRITICAL: flush any SSE the polls missed between the last
            // progress snapshot and this terminal write (progress snapshots
            // are throttled; the terminal carries the authoritative full
            // transcript). The extractor's cursor makes this idempotent —
            // only unconsumed frames come back. Tool-call deltas live here,
            // so skipping this breaks agentic clients (Codex).
            if (typeof j.gp_sse === "string" && j.gp_sse.length > 0) {
              const missed = extractor.replayTerminal(
                j.gp_sse,
                Number.isFinite(j.gp_total) ? j.gp_total : undefined,
              );
              if (missed.length) {
                for (const f of missed) frames.push(f);
                stats.frames += missed.length;
                stats.bytes += missed.reduce((n, f) => n + f.data.length, 0);
                console.log(`[slots ${tag}] terminal flush: +${missed.length} missed frames`);
              }
              (notify as (() => void) | null)?.();
            }
            console.log(`[slots ${tag}] completed in ${Date.now() - t0}ms frames=${stats.frames} bytes=${stats.bytes}`);
            sawTerminal = true;
            return sanitizeFinal(fin);
          }
          // else: stale terminal from a previous run — ignore
        }
      }

      await sleep(interval);
      // local reads are cheap; back off gently when nothing changes
      interval = Math.min(Math.max(interval * 1.3, 60), 250);
    }
  })();

  // Consumed lanes are reset to the empty (idle) state so stale multi-MB
  // job/response blobs never linger in the doc or the /zip archive. Empty
  // files are already the protocol's idle state on both sides: the daemon
  // skips empty jobs, and startup recovery ignores empty pairs.
  async function clearLane(): Promise<void> {
    try { await writeDocFile(jobName, ""); } catch { /* best-effort */ }
    const pm = laneMaxParts.get(slot.n) ?? 0;
    for (let i = 0; i < pm; i++) {
      try { await writeDocFile(`${jobName}.p${i}`, ""); } catch {}
    }
    const artifacts = laneArtifactSet(slot.n);
    for (const base of artifacts) {
      try { await deleteDocFile(base); } catch {}
    }
    artifacts.clear();
    try { await writeDocFile(respName, ""); } catch {}
  }

  /**
   * AUTHORITATIVE terminal fetch — the sandbox agent contract: never release
   * a lane merely because the incremental stream view went quiet. Fetch the
   * response directly, require matching gp_ack AND request id, replay the
   * remaining gp_sse (including response.completed), and only then resolve.
   */
  async function rescueTerminal(): Promise<any | null> {
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      try {
        const files = await fetchZipFiles([respName]).catch(() => null);
        const data = files?.get(respName);
        const txt = data ? new TextDecoder().decode(data) : null;
        let j: any = null;
        if (txt && txt.length > 2) { try { j = JSON.parse(txt); } catch { /* partial */ } }
        if (j && (j.gp_final !== undefined || j.status === "failed")) {
          if (!matchesTerminalEnvelope(j, ack, activeReqid)) {
            console.warn(`[slots ${tag}] rescue aborted: response belongs to another request (gp_ack=${j.gp_ack ?? j.ack})`);
            return null;
          }
          if (j.status === "failed") {
            console.error(`[slots ${tag}] rescue: upstream failed ${JSON.stringify(j.error ?? {}).slice(0, 200)}`);
            return null;
          }
          if (typeof j.gp_sse === "string" && j.gp_sse.length > 0) {
            const missed = extractor.replayTerminal(
              j.gp_sse,
              Number.isFinite(j.gp_total) ? j.gp_total : undefined,
            );
            if (missed.length) {
              for (const f of missed) frames.push(f);
              stats.frames += missed.length;
              stats.bytes += missed.reduce((n, f) => n + f.data.length, 0);
              console.log(`[slots ${tag}] rescue replay: +${missed.length} missed frames`);
            }
            (notify as (() => void) | null)?.();
          }
          sawTerminal = true;
          return sanitizeFinal(j.gp_final ?? j);
        }
      } catch { /* transient zip/parse issues — keep polling */ }
      await sleep(500);
    }
    return null;
  }

  const settlement = settleWithTerminalRescue(runLoop, {
    hasTerminal: () => sawTerminal,
    rescue: () => requestPublished ? rescueTerminal() : Promise.resolve(null),
  });
  void settlement.then(
    result => succeed(result.value, result.reason),
    (e: any) => {
      const kind = typeof e?.kind === "string" ? e.kind : "internal";
      fail(kind, String(e?.message ?? e));
    },
  ).finally(async () => {
    closed = true;
    notify?.();
    // Clear before releasing so the next occupant never races the cleanup;
    // cap the wait — an uncleared file is cosmetic, a blocked lane is not.
    void Promise.race([clearLane(), sleep(5000)]).catch(() => undefined).then(() => {
      pool.release(slot);
      console.log(`[slots ${tag}] released slot ${slot.n} (reason=${releaseCause}, frames=${stats.frames})`);
    });
  });

  async function* eventStream(): AsyncGenerator<SseFrame> {
    let i = 0;
    for (;;) {
      while (i < frames.length) yield frames[i++];
      if (closed) return;
      await new Promise<void>((r) => { notify = r; });
      notify = null;
    }
  }

  return { events: eventStream, final: finalPromise, stats };
}
