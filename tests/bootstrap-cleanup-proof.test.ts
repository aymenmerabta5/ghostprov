import { describe, expect, it } from "bun:test";
import { MAX_MANIFEST_WORKERS } from "../src/fleet/manifest";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { assertBootstrapCleanupComplete, BootstrapCleanupProofError,
  type SupervisorBootstrapJournalFactory } from "../src/v2/supervisor/bootstrap-cleanup-proof";
import type { SupervisorBootstrapAttempt, SupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";

const id = (index: number) => projectId(`00000000-0000-4000-8000-${String(index).padStart(12, "0")}`);
const projects = { ready: id(1), queued: id(2), held: id(3), unmanaged: id(4) };
const allProjects = Object.values(projects);

function pending(phase: SupervisorBootstrapAttempt["phase"]): SupervisorBootstrapAttempt {
  const binding = { projectId: projects.held, sandboxSessionId: "offline-session", leaderGeneration: 1,
    buildHash: "a".repeat(64), schemaHash: "b".repeat(64), nonce: "offline-nonce",
    protocolRevision: "r2" as const, schemaVersion: 2 as const };
  return { version: 1, phase, sessionId: id(90), binding, connectTimeoutMs: 1,
    sealed: { version: 1, binding, salt: "offline", iv: "offline", ciphertext: "private-offline-fixture" } };
}

function journal(load: () => Promise<SupervisorBootstrapAttempt | null>, closed = () => {}): SupervisorBootstrapJournal {
  return { async withLease(_signal, run) {
    try { return await run({ load,
      async save() { throw new Error("Audit must never save a journal"); },
      async clear() { throw new Error("Audit must never clear a journal"); } }); }
    finally { closed(); }
  } };
}

describe("manifest-wide bootstrap cleanup proof", () => {
  it("accepts an empty authoritative fleet and sequentially checks every project with no records", async () => {
    let active = 0, maximum = 0;
    const visited: ProjectId[] = [];
    const factory: SupervisorBootstrapJournalFactory = project => journal(async () => {
      active++; maximum = Math.max(maximum, active); visited.push(project); return null;
    }, () => { active--; });
    await assertBootstrapCleanupComplete({ projectIds: [], journalFactory: factory, signal: new AbortController().signal });
    const maximumFleet = Array.from({ length: MAX_MANIFEST_WORKERS }, (_, index) => id(index + 1));
    await assertBootstrapCleanupComplete({ projectIds: maximumFleet, journalFactory: factory, signal: new AbortController().signal });
    expect(visited).toEqual(maximumFleet);
    expect(maximum).toBe(1);
    expect(active).toBe(0);
  });

  it.each(["queued", "held", "unmanaged"] as const)("rejects a retained journal for a %s project even without a live owner", async role => {
    const visited: ProjectId[] = [];
    const factory: SupervisorBootstrapJournalFactory = project => journal(async () => {
      visited.push(project); return project === projects[role] ? pending("submitted") : null;
    });
    await expect(assertBootstrapCleanupComplete({ projectIds: allProjects, journalFactory: factory,
      signal: new AbortController().signal })).rejects.toBeInstanceOf(BootstrapCleanupProofError);
    expect(visited).toContain(projects[role]);
  });

  it.each(["prepared", "submitted", "cleanup_proven"] as const)("rejects retained phase %s without modifying it", async phase => {
    const record = pending(phase);
    await expect(assertBootstrapCleanupComplete({ projectIds: [projects.held],
      journalFactory: () => journal(async () => record), signal: new AbortController().signal })).rejects.toThrow("incomplete");
    expect(record.phase).toBe(phase);
  });

  it.each(["factory", "lease", "corrupt", "unreadable"] as const)("fails closed without private details on %s failure", async stage => {
    const secretMarker = "private-file-and-journal-material";
    const factory: SupervisorBootstrapJournalFactory = () => {
      if (stage === "factory") throw new Error(secretMarker);
      if (stage === "lease") return { async withLease() { throw new Error(secretMarker); } };
      return journal(async () => { throw new Error(secretMarker); });
    };
    const result = await assertBootstrapCleanupComplete({ projectIds: [projects.held], journalFactory: factory,
      signal: new AbortController().signal }).catch(error => error);
    expect(result).toBeInstanceOf(BootstrapCleanupProofError);
    expect(result.message).toBe("Supervisor bootstrap cleanup was incomplete");
    expect(result.cause).toBeUndefined();
  });

  it("does no work after pre-cancellation and bounds a noncooperative read without proceeding", async () => {
    const cancellation = new AbortController(); cancellation.abort(new Error("private cancellation"));
    let created = 0;
    const factory: SupervisorBootstrapJournalFactory = () => { created++; return journal(async () => null); };
    await expect(assertBootstrapCleanupComplete({ projectIds: allProjects, journalFactory: factory,
      signal: cancellation.signal })).rejects.toThrow("incomplete");
    expect(created).toBe(0);

    const waiting = Promise.withResolvers<SupervisorBootstrapAttempt | null>();
    const deadline = new AbortController();
    let released = 0;
    const started = assertBootstrapCleanupComplete({ projectIds: allProjects,
      journalFactory: () => { created++; return journal(() => waiting.promise, () => { released++; }); },
      signal: deadline.signal }).catch(error => error);
    await Promise.resolve();
    deadline.abort();
    expect(await started).toBeInstanceOf(BootstrapCleanupProofError);
    expect(created).toBe(1); expect(released).toBe(0);
    waiting.resolve(null);
    for (let turn = 0; turn < 10; turn++) await Promise.resolve();
    expect(created).toBe(1); expect(released).toBe(1);
  });

  it("rejects cancellation during final lease release instead of claiming completion", async () => {
    const deadline = new AbortController();
    await expect(assertBootstrapCleanupComplete({ projectIds: [projects.ready],
      journalFactory: () => journal(async () => null, () => deadline.abort()),
      signal: deadline.signal })).rejects.toThrow("incomplete");
  });

  it.each([
    { projectIds: Array.from({ length: MAX_MANIFEST_WORKERS + 1 }, (_, index) => id(index + 1)) },
    { projectIds: [projects.ready, projects.ready] },
    { projectIds: ["private-invalid-project" as ProjectId] },
    { projectIds: [`${projects.ready}\n` as ProjectId] },
  ])("rejects invalid or unbounded project lists before accessing journals", async ({ projectIds }) => {
    let called = false;
    await expect(assertBootstrapCleanupComplete({ projectIds,
      journalFactory: () => { called = true; return journal(async () => null); },
      signal: new AbortController().signal })).rejects.toThrow("incomplete");
    expect(called).toBe(false);
  });

  it("snapshots authoritative IDs before awaits so caller mutation cannot expand the audit", async () => {
    const mutable = [projects.ready, projects.queued];
    const visited: ProjectId[] = [];
    await assertBootstrapCleanupComplete({ projectIds: mutable, signal: new AbortController().signal,
      journalFactory: project => journal(async () => {
        visited.push(project); mutable[1] = projects.unmanaged; return null;
      }) });
    expect(visited).toEqual([projects.ready, projects.queued]);
  });
});
