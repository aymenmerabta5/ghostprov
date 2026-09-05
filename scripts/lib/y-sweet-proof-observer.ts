import { WebsocketProvider } from "y-websocket";

/** Process-local diagnostics for a guarded verifier's exact dedicated projects. */
export function observeYSweetProofConnections(projects: readonly string[]) {
  const workers = new Map(projects.map((project, index) => [project, index + 1]));
  if (workers.size !== projects.length || (workers.size !== 1 && workers.size !== 8)) {
    throw new Error("Y-Sweet proof observer requires one or eight distinct dedicated projects");
  }
  const records = new Map<WebsocketProvider, {
    worker: number; role: "supervisor" | "project";
    createdAt: number; connectCalls: number; dials: number; opens: number; syncs: number; closes: number; errors: number;
    firstSyncMs: number | null; closeCode: number | null; errorCode: string | null;
    status: (value: unknown) => void; sync: (value: unknown) => void;
    close: (value: unknown) => void; error: (value: unknown) => void;
  }>();
  let truncated = 0;
  const original = WebsocketProvider.prototype.connect;
  const wrapped = function(this: WebsocketProvider) {
    const worker = workers.get(this.roomname);
    if (worker !== undefined && !records.has(this)) {
      if (records.size >= projects.length * 16) truncated++;
      else {
        const entry = {
          // Workspace and relay providers can share the same document guid.
          worker, role: this.doc.guid.endsWith(":ghostprov:supervisor:host") ? "supervisor" as const : "project" as const,
          createdAt: performance.now(), connectCalls: 0, dials: 0, opens: 0, syncs: 0, closes: 0, errors: 0,
          firstSyncMs: null as number | null, closeCode: null as number | null, errorCode: null as string | null,
          status: (value: unknown) => {
            if (value && typeof value === "object" && "status" in value && value.status === "connected") entry.opens++;
            if (value && typeof value === "object" && "status" in value && value.status === "connecting") entry.dials++;
          },
          sync: (value: unknown) => {
            if (value === true) { entry.syncs++; entry.firstSyncMs ??= Math.round(performance.now() - entry.createdAt); }
          },
          close: (value: unknown) => {
            entry.closes++;
            const code = value && typeof value === "object" && "code" in value ? value.code : null;
            entry.closeCode = typeof code === "number" && Number.isSafeInteger(code) && code >= 0 && code <= 65_535 ? code : null;
          },
          error: (value: unknown) => {
            entry.errors++;
            const nested = value && typeof value === "object" && "error" in value ? value.error : value;
            const code = nested && typeof nested === "object" && "code" in nested ? nested.code : null;
            entry.errorCode = typeof code === "string" && /^[A-Za-z][A-Za-z0-9_]{0,47}$/.test(code) ? code : null;
          },
        };
        records.set(this, entry);
        this.on("status", entry.status);
        this.on("sync", entry.sync);
        this.on("connection-close", entry.close);
        this.on("connection-error", entry.error);
      }
    }
    const entry = records.get(this);
    if (entry) entry.connectCalls++;
    return original.call(this);
  };
  WebsocketProvider.prototype.connect = wrapped;
  return {
    snapshot: () => ({ truncated, connections: [...records].map(([provider, entry], index) => ({
      connection: index + 1, worker: entry.worker, role: entry.role, connectCalls: entry.connectCalls,
      dials: entry.dials, opens: entry.opens, syncs: entry.syncs, closes: entry.closes, errors: entry.errors,
      firstSyncMs: entry.firstSyncMs, closeCode: entry.closeCode, errorCode: entry.errorCode,
      socketOpen: provider.wsconnected, synced: provider.synced,
    })) }),
    restore: () => {
      if (WebsocketProvider.prototype.connect === wrapped) WebsocketProvider.prototype.connect = original;
      for (const [provider, entry] of records) {
        provider.off("status", entry.status); provider.off("sync", entry.sync);
        provider.off("connection-close", entry.close); provider.off("connection-error", entry.error);
      }
      records.clear();
    },
  };
}
