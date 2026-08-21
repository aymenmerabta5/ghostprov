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
import { SseDeltaExtractor, type SseFrame } from "./sse";

const LATEXMKRC = [
  "$pdf_mode = 1;",
  "system('rm -f main.pdf main.aux main.fls main.fdb_latexmk; rm -f /workspace/response.json');",
  "$pdflatex = 'nohup python runner.py > /tmp/gp_bg.log 2>&1 & pdflatex -interaction=nonstopmode %O %S'",
].join("\n") + "\n";

export class RelayError extends Error {
  constructor(message: string, readonly kind: "sync" | "render" | "timeout" | "no-bridge" | "upstream") {
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
  await writeTrackedFile(name, content);
  // scale timeout with payload size (images/audio inflate job.json)
  const timeout = Math.min(config.syncTimeoutMs + Math.floor(content.length / 200), 120_000);
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
    if (j && typeof j === "object" && j.streaming !== true && (j.status === "completed" || j.status === "failed")) {
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
 * Execute one relay request. The write+render critical section is serialized
 * by renderQueue (the backend has one shared editor); polling afterwards is
 * concurrent-safe.
 *
 * Poll cadence adapts: 150 ms while frames are actively arriving (max TPS),
 * backing off to config.pollIntervalMs when idle (reasoning pauses).
 */
export async function runRelay(body: Record<string, unknown>): Promise<RunHandle> {
  const jobJson = JSON.stringify(body, null, 2);
  if (jobJson.length > config.inlinePayloadWarnBytes) {
    console.warn(`[relay] large job.json ${(jobJson.length / 1024) | 0}KiB — sync will be slow`);
  }

  const frames: SseFrame[] = [];
  const stats = { bytes: 0, firstByteMs: null as number | null, frames: 0 };
  let notify: (() => void) | null = null;
  let closed = false;

  const finalPromise = renderQueue.add(async () => {
    try {
      await ensureFileSync(".latexmkrc", LATEXMKRC);
      await ensureFileSync("job.json", jobJson);

      // clear stale output so progress polling can't see the previous run
      await deleteFile("response.json");
      await triggerRender();
    } catch (e: any) {
      invalidateCache(); // sandbox may have rebooted — force re-verify next time
      throw e;
    }

    const extractor = new SseDeltaExtractor();
    const t0 = Date.now();
    let lastProgressAt = Date.now();
    let lastTotal = -1;
    let interval = 120;
    for (;;) {
      // Stall detection ONLY — a healthy stream runs indefinitely, however long.
      if (Date.now() - lastProgressAt > config.stallTimeoutMs) {
        invalidateCache();
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
      } catch {
        await new Promise((r) => setTimeout(r, interval));
        continue;
      }

      if (raw !== null) {
        const fresh = extractor.feed(raw);
        if (fresh.length) {
          for (const f of fresh) frames.push(f);
          stats.frames += fresh.length;
          stats.bytes += fresh.reduce((n, f) => n + f.data.length, 0);
          if (stats.firstByteMs === null) stats.firstByteMs = Date.now() - t0;
          interval = 120; // actively streaming — max TPS
          lastProgressAt = Date.now();
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
          interval = Math.min(Math.max(interval * 1.5, 150), config.pollIntervalMs);
        }

        const fin = isFinalObject(raw);
        if (fin) {
          if (fin.final.status === "failed") {
            invalidateCache();
            throw new RelayError(`upstream failed: ${JSON.stringify(fin.final.error ?? {}).slice(0, 300)}`, "upstream");
          }
          return fin.final;
        }
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
