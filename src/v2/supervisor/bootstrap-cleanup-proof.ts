import { MAX_MANIFEST_WORKERS } from "../../fleet/manifest";
import { projectId, type ProjectId } from "../../sandbox/types";
import type { SupervisorBootstrapJournal } from "./bootstrap-journal";

export type SupervisorBootstrapJournalFactory = (projectId: ProjectId) => SupervisorBootstrapJournal;

/** Deliberately excludes journal paths, private records, and underlying errors. */
export class BootstrapCleanupProofError extends Error {
  readonly name = "BootstrapCleanupProofError";
  constructor() { super("Supervisor bootstrap cleanup was incomplete"); }
}

function withinSignal(signal: AbortSignal, operation: () => Promise<void>): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (failed: boolean) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      if (failed) reject(new BootstrapCleanupProofError());
      else resolve();
    };
    const abort = () => finish(true);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) { abort(); return; }
    // Observe late rejection without releasing another owner's lease or
    // starting another project after the original shutdown deadline.
    void Promise.resolve().then(operation).then(() => finish(false), () => finish(true));
  });
}

/** Audit every authoritative manifest project after fleet writers are fenced. */
export async function assertBootstrapCleanupComplete(options: {
  readonly projectIds: readonly ProjectId[];
  readonly journalFactory: SupervisorBootstrapJournalFactory;
  readonly signal: AbortSignal;
}): Promise<void> {
  try {
    options.signal.throwIfAborted();
    if (!Array.isArray(options.projectIds) || options.projectIds.length > MAX_MANIFEST_WORKERS) {
      throw new BootstrapCleanupProofError();
    }
    const projects = options.projectIds.map(value => {
      if (typeof value !== "string" || value.length !== 36 || value !== value.toLowerCase()) {
        throw new BootstrapCleanupProofError();
      }
      return projectId(value);
    });
    if (new Set(projects).size !== projects.length) throw new BootstrapCleanupProofError();
    for (const project of projects) {
      await withinSignal(options.signal, async () => {
        options.signal.throwIfAborted();
        await options.journalFactory(project).withLease(options.signal, async entry => {
          options.signal.throwIfAborted();
          const record = await entry.load();
          options.signal.throwIfAborted();
          // Even cleanup_proven still requires local artifact/journal cleanup
          // before an unattended shutdown may claim restart readiness.
          if (record !== null) throw new BootstrapCleanupProofError();
        });
        options.signal.throwIfAborted();
      });
    }
    options.signal.throwIfAborted();
  } catch {
    throw new BootstrapCleanupProofError();
  }
}
