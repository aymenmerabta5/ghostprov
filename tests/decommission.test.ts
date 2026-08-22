import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  FleetDecommissionService,
  FleetDecommissionError,
} from "../src/fleet/decommission";
import { FleetManifestStore } from "../src/fleet/manifest";
import { projectId, type ProjectId } from "../src/sandbox/types";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const B = projectId("69288e8b-c183-45f3-9131-d64b05b98928");
const C = projectId("11111111-1111-4111-8111-111111111111");
const FINGERPRINT = "a".repeat(64);
const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

class FakeProjects {
  readonly deleted: ProjectId[] = [];
  failOn: ProjectId | null = null;

  async deleteManaged(id: ProjectId): Promise<void> {
    if (id === this.failOn) throw new Error("remote delete failed");
    this.deleted.push(id);
  }
}

async function harness() {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-decommission-"));
  roots.push(root);
  const manifest = new FleetManifestStore({
    file: path.join(root, "fleet.json"),
    identityFingerprint: FINGERPRINT,
  });
  await manifest.initialize("authenticated");
  await manifest.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1 });
  await manifest.markCreated(A, 2);
  await manifest.reserveWorker({ index: 2, projectId: B, title: "GhostProv Worker 2", nowMs: 3 });
  await manifest.markCreated(B, 4);
  await manifest.reserveWorker({
    index: 3,
    projectId: C,
    title: "User project",
    nowMs: 5,
    managed: false,
  });
  await manifest.markCreated(C, 6);
  const projects = new FakeProjects();
  return {
    manifest,
    projects,
    service: new FleetDecommissionService({ manifest, projects }),
  };
}

describe("FleetDecommissionService", () => {
  it("requires the exact digest and deletes only managed requested IDs", async () => {
    const value = await harness();
    const preview = await value.service.preview([C, A, A]);

    expect(preview).toMatchObject({
      identityFingerprint: FINGERPRINT,
      projects: [A],
    });
    expect(preview.confirmationDigest).toMatch(/^[a-f0-9]{64}$/);
    await expect(value.service.execute(preview, "wrong")).rejects.toThrow(
      "confirmation digest",
    );
    expect(value.projects.deleted).toEqual([]);

    await value.service.execute(preview, preview.confirmationDigest);

    expect(value.projects.deleted).toEqual([A]);
    expect((await value.manifest.load()).workers.map(worker => worker.projectId)).toEqual([B, C]);
  });

  it("refuses a stale preview if managed targets changed", async () => {
    const value = await harness();
    const preview = await value.service.preview([A, B]);
    await value.manifest.removeManaged(B);

    await expect(value.service.execute(preview, preview.confirmationDigest)).rejects.toBeInstanceOf(
      FleetDecommissionError,
    );
    expect(value.projects.deleted).toEqual([]);
  });

  it("removes manifest records only after each successful remote deletion", async () => {
    const value = await harness();
    const preview = await value.service.preview([A, B]);
    value.projects.failOn = A;

    await expect(value.service.execute(preview, preview.confirmationDigest)).rejects.toThrow(
      "remote delete failed",
    );

    expect(value.projects.deleted).toEqual([B]);
    expect((await value.manifest.load()).workers.map(worker => worker.projectId)).toEqual([A, C]);
  });
});
