import * as Y from "yjs";
import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import type { SupervisorAwarenessProviderLike } from "./provider";
import {
  SUPERVISOR_RETIREMENT_KEY, SUPERVISOR_RETIREMENT_MAP, parseSupervisorRetirementReceipt,
} from "./retirement-receipt";

/** A remote-only provider bound by the caller to the exact credential/document. */
export interface SupervisorRetirementReadbackOptions {
  readonly createProvider: (doc: Y.Doc) => SupervisorAwarenessProviderLike;
  readonly expectedRaw: string;
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
  readonly clock?: Clock;
}

export async function proveSupervisorRetirementReadback(options: SupervisorRetirementReadbackOptions): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 15_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 15_000) {
    throw new RangeError("Supervisor retirement readback timeout must be from 1 through 15000");
  }
  if (!parseSupervisorRetirementReceipt(options.expectedRaw)) {
    throw new TypeError("Invalid supervisor retirement readback value");
  }
  if (options.signal?.aborted) throw options.signal.reason;
  const clock = options.clock ?? SYSTEM_CLOCK;
  // Never seed this document with the writer's state. A writer-side value or
  // sync event cannot acknowledge remote persistence.
  const doc = new Y.Doc({ guid: `ghostprov:retirement-readback:${crypto.randomUUID()}` });
  let provider: SupervisorAwarenessProviderLike | null = null;
  try {
    provider = options.createProvider(doc);
    const reader = provider;
    const receipts = doc.getMap<unknown>(SUPERVISOR_RETIREMENT_MAP);
    await new Promise<void>((resolve, reject) => {
      let synced = false;
      let settled = false;
      const timer = new AbortController();
      const finish = (error?: unknown) => {
        if (settled) return;
        settled = true;
        timer.abort();
        receipts.unobserve(check);
        reader.off("sync", onSync);
        options.signal?.removeEventListener("abort", onAbort);
        if (error !== undefined) reject(error); else resolve();
      };
      const check = () => {
        if (!synced) return;
        if (receipts.size === 1 && receipts.get(SUPERVISOR_RETIREMENT_KEY) === options.expectedRaw) finish();
      };
      const onSync = (value: boolean) => { synced = value; check(); };
      const onAbort = () => finish(options.signal?.reason ?? new DOMException("Supervisor retirement readback aborted", "AbortError"));
      receipts.observe(check);
      reader.on("sync", onSync);
      options.signal?.addEventListener("abort", onAbort, { once: true });
      void clock.sleep(timeoutMs, timer.signal).then(
        () => finish(new Error("Supervisor retirement readback deadline expired")),
        error => { if (!timer.signal.aborted) finish(error); },
      );
      if (options.signal?.aborted) onAbort();
      else { try { reader.connect(); } catch (error) { finish(error); } }
    });
  } finally {
    try { provider?.disconnect(); }
    finally { try { provider?.destroy(); } finally { doc.destroy(); } }
  }
}
