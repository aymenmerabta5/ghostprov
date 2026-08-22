/**
 * Relay pipeline — the proven stable loop (PRISM_FINDINGS.md §12.9):
 *
 *   1. write job.json  (verbatim Responses-API body: input_image, input_audio,
 *      tools incl. image_generation, text.format structured outputs, reasoning
 *      {effort/mode}, previous_response_id, prompt_cache_*)
 *   2. ensure .latexmkrc arms runner.py in background + clears stale outputs
 *   3. POST /render    → latexmk kicks runner.py instantly (~300ms)
 *   4. poll GET /zip   → response.json carries a bounded TAIL of the SSE stream
 *      (constant per-poll cost — huge reasoning outputs never degrade TPS)
 *   5a. streaming callers receive real relay SSE frames as they appear
 *   5b. non-stream callers receive the final response object
 */
import { config } from "../config";
import { renderQueue } from "../utils/queue";
import { triggerRender, deleteFile, fetchZipFiles, readTrackedFile } from "../prism/gateway";
import { writeTrackedFile } from "../prism/workspace";
import { TerminalBackedSseExtractor, sanitizeFinal, type SseFrame } from "./sse";
// SAME launcher as the slot path — otherwise the fallback rewrites the old
// .latexmkrc over the daemon-armed one and the two paths fight forever.
import { LATEXMKRC_SLOTS as LATEXMKRC } from "./slots";

export class RelayError extends Error {
  constructor(
    message: string,
    readonly kind: "sync" | "render" | "timeout" | "no-bridge" | "upstream",
    readonly details?: unknown,
  ) {
    super(message);
  }
}

/** Wait until sandbox-side file content equals expected exactly (or timeout). */
async function verifySync(name: string, expected: string, timeoutMs: number): Promise<void> {
  const t0 = Date.now();
  for (;;) {
    const actual = await readTrackedFile(name).catch(() => null);
    if (actual === expected) return;
    if (Date.now() - t0 > timeoutMs) throw new RelayError(`sync timeout on ${name}`, "sync");
    await new Promise((r) => setTimeout(r, 400));
  }
}

/**
 * Cache of last VERIFIED file contents. Verification costs one /zip round-trip
 * (~200-400 ms); when content is unchanged from the previous verified write we
 * skip it entirely. Cache is invalidated on any relay failure (sandbox may have
 * rebooted and reset disk).
 */
const verifiedCache = new Map<string, string>();

async function ensureFileSync(name: string, content: string): Promise<void> {
  if (verifiedCache.get(name) === content) return; // fast path
  const current = await readTrackedFile(name).catch(() => null);
  if (current === content) {
    verifiedCache.set(name, content); // already correct on disk — just cache it
    return;
  }
  // Prefer the persistent Yjs doc connection (works for ANY tracked file,
  // no monaco heuristics); fall back to the browser editor write.
  try {
    const { writeDocFile } = await import("../prism/ydoc");
    await writeDocFile(name, content);
  } catch {
    await writeTrackedFile(name, content);
  }
  // scale timeout with payload size (agentic turns carry huge tool/history blobs)
  const timeout = Math.min(config.syncTimeoutMs + Math.floor(content.length / 100), 240_000);
  await verifySync(name, content, timeout);
  verifiedCache.set(name, content);
}

function invalidateCache(): void {
  verifiedCache.delete(".latexmkrc");
  verifiedCache.delete("job.json");
}

function isFinalObject(raw: string): { final: any } | null {
  try {
    const j = JSON.parse(raw);
    if (
      j && typeof j === "object" && j.streaming !== true &&
      (j.status === "completed" || j.status === "failed" || j.status === "incomplete")
    ) {
      return { final: j };
    }
  } catch {}
  return null;
}

export interface RunHandle {
  /** Async iterator of raw relay SSE frames (real OpenAI events). */
  events(): AsyncGenerator<SseFrame>;
  /** Promise resolving with the final Responses-API object. */
  final: Promise<any>;
  /** Diagnostics: stream byte counts and wall-clock milestones. */
  stats: { bytes: number; firstByteMs: number | null; frames: number };
}

/**
 * Execute one relay request.
 *
 * Dispatch order:
 *   1. Slot pool (concurrent) — daemon pipeline, one slot per request
 *   2. Legacy single-job loop — fallback when the daemon can't be brought up
 *
 * The write+render critical section of the LEGACY path is serialized by
 * renderQueue (the backend has one shared editor); polling afterwards is
 * concurrent-safe. The slot path bypasses the render queue entirely.
 */
export async function runRelay(body: Record<string, unknown>): Promise<RunHandle> {
  if (process.env.GP_DISABLE_SLOTS === "1") return runRelayLegacy(body);
  try {
    const { runSlotJob } = await import("./slots");
    return await runSlotJob(body);
  } catch (e: any) {
    const kind: string = e?.kind ?? "internal";
    // infrastructure failures degrade to the proven serial path; request-level
    // failures (upstream rejected the body) propagate untouched
    if (kind === "upstream") throw e;
    console.warn(`[relay] slot path unavailable (${kind}: ${String(e?.message ?? e).slice(0, 160)}) — falling back to legacy render pipeline`);
    return runRelayLegacy(body);
  }
}

/** Proven single-slot pipeline (PRISM_FINDINGS §12.9/§12.10) — fallback. */
let reqCounter = 0;

export async function runRelayLegacy(body: Record<string, unknown>): Promise<RunHandle> {
  const jobJson = JSON.stringify(body, null, 2);
  const tag = `req${++reqCounter}`;
  console.log(`[relay ${tag}] start model=${body.model} jobJson=${(jobJson.length / 1024).toFixed(1)}KiB stream=${body.stream === true} inputItems=${Array.isArray(body.input) ? body.input.length : "scalar"}`);
  if (jobJson.length > config.inlinePayloadWarnBytes) {
    console.warn(`[relay ${tag}] large job.json ${(jobJson.length / 1024) | 0}KiB — sync will be slow`);
  }

  const frames: SseFrame[] = [];
  const stats = { bytes: 0, firstByteMs: null as number | null, frames: 0 };
  let notify: (() => void) | null = null;
  let closed = false;

  const finalPromise = renderQueue.add(async () => {
    try {
      let t = Date.now();
      await ensureFileSync(".latexmkrc", LATEXMKRC);
      await ensureFileSync("job.json", jobJson);
      console.log(`[relay ${tag}] synced files in ${Date.now() - t}ms`);

      // clear stale output so progress polling can't see the previous run
      await deleteFile("response.json");
      await triggerRender();
      console.log(`[relay ${tag}] render kicked`);
      t = Date.now();
    } catch (e: any) {
      invalidateCache(); // sandbox may have rebooted — force re-verify next time
      console.error(`[relay ${tag}] setup failed: ${String(e?.message ?? e).slice(0, 200)}`);
      throw e;
    }

    const extractor = new TerminalBackedSseExtractor();
    const t0 = Date.now();
    let lastProgressAt = Date.now();
    let lastTotal = -1;
    let interval = 120;
    let pollErrors = 0;
    let lastLogAt = Date.now();
    for (;;) {
      // Stall detection ONLY — a healthy stream runs indefinitely, however long.
      if (Date.now() - lastProgressAt > config.stallTimeoutMs) {
        invalidateCache();
        console.error(`[relay ${tag}] STALL: no stream growth for ${Math.round(config.stallTimeoutMs / 1000)}s (lastTotal=${lastTotal}, frames=${frames.length})`);
        throw new RelayError(
          `stream stalled ${Math.round(config.stallTimeoutMs / 1000)}s with no data`,
          "timeout",
        );
      }
      // poll first, sleep after — first byte lands with zero artificial delay
      let raw: string | null = null;
      try {
        const files = await fetchZipFiles(["response.json"]);
        const data = files.get("response.json");
        if (data) raw = new TextDecoder().decode(data);
        pollErrors = 0;
      } catch (e: any) {
        pollErrors++;
        if (pollErrors <= 3 || pollErrors % 20 === 0) {
          console.warn(`[relay ${tag}] zip poll error #${pollErrors}: ${String(e?.message ?? e).slice(0, 160)}`);
        }
        await new Promise((r) => setTimeout(r, interval));
        continue;
      }
      if (raw === null) {
        // response.json not in zip yet — runner hasn't started writing
        const now = Date.now();
        if (now - lastLogAt > 5000) {
          console.log(`[relay ${tag}] waiting for runner output… ${Math.round((now - t0) / 1000)}s`);
          lastLogAt = now;
        }
        await new Promise((r) => setTimeout(r, interval));
        continue;
      }

      const hadDeferredGap = extractor.deferredGap !== null;
      const fresh = extractor.feedProgress(raw);
      if (!hadDeferredGap && extractor.deferredGap) {
        console.warn(
          `[relay ${tag}] incremental SSE window gap ` +
          `(cursor=${extractor.deferredGap.cursor}, start=${extractor.deferredGap.windowStart}); ` +
          "deferring deltas until authoritative terminal replay",
        );
      }
      if (fresh.length) {
        for (const f of fresh) frames.push(f);
        stats.frames += fresh.length;
        stats.bytes += fresh.reduce((n, f) => n + f.data.length, 0);
        if (stats.firstByteMs === null) {
          stats.firstByteMs = Date.now() - t0;
          console.log(`[relay ${tag}] first frame at ${stats.firstByteMs}ms`);
        }
        interval = 120; // actively streaming — max TPS
        lastProgressAt = Date.now();
        lastLogAt = Date.now();
        notify?.();
      } else {
        // no complete frames yet — but is the stream still growing? (silent reasoning)
        try {
          const j = JSON.parse(raw);
          const total = typeof j.total === "number" ? j.total : typeof j.sse === "string" ? j.sse.length : -1;
          if (total > lastTotal) {
            lastTotal = total;
            lastProgressAt = Date.now();
          }
        } catch {}
        const now = Date.now();
        if (now - lastLogAt > 10000) {
          console.log(`[relay ${tag}] streaming… frames=${frames.length} bytes=${stats.bytes} (${Math.round((now - t0) / 1000)}s)`);
          lastLogAt = now;
        }
        interval = Math.min(Math.max(interval * 1.5, 150), config.pollIntervalMs);
      }

      const fin = isFinalObject(raw);
      if (fin) {
        if (fin.final.status === "failed") {
          invalidateCache();
          console.error(`[relay ${tag}] UPSTREAM FAILED: ${JSON.stringify(fin.final.error ?? {}).slice(0, 400)}`);
          throw new RelayError(
            `upstream failed: ${JSON.stringify(fin.final.error ?? {}).slice(0, 300)}`,
            "upstream",
            fin.final.error,
          );
        }
        if (typeof fin.final.gp_sse === "string" && fin.final.gp_sse.length > 0) {
          const missed = extractor.replayTerminal(
            fin.final.gp_sse,
            Number.isFinite(fin.final.gp_total) ? fin.final.gp_total : undefined,
          );
          if (missed.length) {
            for (const frame of missed) frames.push(frame);
            stats.frames += missed.length;
            stats.bytes += missed.reduce((total, frame) => total + frame.data.length, 0);
            if (stats.firstByteMs === null) stats.firstByteMs = Date.now() - t0;
            notify?.();
          }
        }
        console.log(`[relay ${tag}] completed in ${Date.now() - t0}ms frames=${frames.length} bytes=${stats.bytes}`);
        // runner.py merges gp_sse/gp_ack/gp_total into the final object —
        // strip them (private protocol, never exposed to clients)
        return sanitizeFinal(fin.final);
      }
      await new Promise((r) => setTimeout(r, interval));
    }
  }) as Promise<any>;

  async function* eventStream(): AsyncGenerator<SseFrame> {
    let i = 0;
    for (;;) {
      while (i < frames.length) yield frames[i++];
      if (closed) return;
      await new Promise<void>((r) => { notify = r; });
      notify = null;
    }
  }

  void finalPromise
    .catch(() => undefined)
    .finally(() => { closed = true; notify?.(); });

  return { events: eventStream, final: finalPromise, stats };
}
