import { describe, expect, it } from "bun:test";
import { projectId } from "../src/sandbox/types";
import {
  createBundledV2SidecarRevisionSelector,
  V2SidecarRevisionSelector,
  type RevisionAddressedAssets,
} from "../src/v2/sidecar/revisions";
import {
  loadV2SidecarAssets,
  V2_R1_RELEASE,
  V2_R2_RELEASE,
  V2_R3_RELEASE,
  type V2SidecarAssets,
  type V2SidecarRevision,
} from "../src/v2/sidecar/assets";

const PROJECT_1 = projectId("11111111-1111-4111-8111-111111111111");
const PROJECT_2 = projectId("22222222-2222-4222-8222-222222222222");
const PROJECT_3 = projectId("33333333-3333-4333-8333-333333333333");
const UNKNOWN_PROJECT = projectId("99999999-9999-4999-8999-999999999999");

type TestRevision = "r1" | "r2";
type TestAssets = RevisionAddressedAssets<TestRevision> & {
  readonly marker: string;
};

function assets(revision: TestRevision, marker: string): TestAssets {
  return Object.freeze({
    revision,
    buildHash: revision === "r1" ? "a".repeat(64) : "c".repeat(64),
    schemaHash: revision === "r1" ? "b".repeat(64) : "d".repeat(64),
    marker,
  });
}

describe("V2SidecarRevisionSelector", () => {
  it("selects exact per-project overrides and caches one frozen asset load per revision", async () => {
    const calls = { r1: 0, r2: 0 };
    const r1 = assets("r1", "stable");
    const r2 = assets("r2", "canary");
    const selector = new V2SidecarRevisionSelector<TestRevision, TestAssets>({
      projectIds: [PROJECT_1, PROJECT_2, PROJECT_3],
      defaultRevision: "r1",
      projectOverrides: new Map([[PROJECT_2, "r2"]]),
      loaders: new Map([
        ["r1", async () => { calls.r1++; return r1; }],
        ["r2", async () => { calls.r2++; return r2; }],
      ]),
    });

    expect(selector.revisionForProject(PROJECT_1)).toBe("r1");
    expect(selector.revisionForProject(PROJECT_2)).toBe("r2");
    expect(selector.revisionForProject(PROJECT_3)).toBe("r1");
    expect(selector.selectedRevisions()).toEqual(["r1", "r2"]);
    expect(() => selector.loadedForProject(PROJECT_1)).toThrow(/not preloaded/i);

    await selector.preload();
    expect(selector.loadedForProject(PROJECT_1)).toEqual({
      projectId: PROJECT_1,
      revision: "r1",
      assets: r1,
    });
    expect(selector.loadedForProject(PROJECT_2)).toEqual({
      projectId: PROJECT_2,
      revision: "r2",
      assets: r2,
    });
    await Promise.all([
      selector.resolveProject(PROJECT_1),
      selector.resolveProject(PROJECT_2),
      selector.resolveProject(PROJECT_3),
      selector.loadRevision("r2"),
    ]);
    expect(calls).toEqual({ r1: 1, r2: 1 });
  });

  it("rejects unknown projects, unknown override revisions, and duplicate project registration", () => {
    const loaders = new Map<TestRevision, () => Promise<TestAssets>>([
      ["r1", async () => assets("r1", "stable")],
    ]);
    expect(() => new V2SidecarRevisionSelector({
      projectIds: [PROJECT_1],
      defaultRevision: "r1" as TestRevision,
      projectOverrides: { [UNKNOWN_PROJECT]: "r1" as TestRevision },
      loaders,
    })).toThrow(/unknown project/i);
    expect(() => new V2SidecarRevisionSelector({
      projectIds: [PROJECT_1],
      defaultRevision: "r1" as TestRevision,
      projectOverrides: { [PROJECT_1]: "r2" as TestRevision },
      loaders,
    })).toThrow(/no immutable.*r2/i);
    expect(() => new V2SidecarRevisionSelector({
      projectIds: [PROJECT_1, PROJECT_1],
      defaultRevision: "r1" as TestRevision,
      loaders,
    })).toThrow(/duplicate.*project/i);

    const selector = new V2SidecarRevisionSelector({
      projectIds: [PROJECT_1],
      defaultRevision: "r1" as TestRevision,
      loaders,
    });
    expect(() => selector.revisionForProject(UNKNOWN_PROJECT)).toThrow(/unknown project/i);
  });

  it("caches selected-load failure and never falls back to the default revision", async () => {
    const calls = { r1: 0, r2: 0 };
    const selector = new V2SidecarRevisionSelector<TestRevision, TestAssets>({
      projectIds: [PROJECT_2],
      defaultRevision: "r1",
      projectOverrides: { [PROJECT_2]: "r2" },
      loaders: new Map([
        ["r1", async () => { calls.r1++; return assets("r1", "stable"); }],
        ["r2", async () => {
          calls.r2++;
          throw new Error("R2 artifact unavailable");
        }],
      ]),
    });

    await expect(selector.resolveProject(PROJECT_2)).rejects.toThrow("R2 artifact unavailable");
    await expect(selector.resolveProject(PROJECT_2)).rejects.toThrow("R2 artifact unavailable");
    expect(calls).toEqual({ r1: 0, r2: 1 });
  });

  it("fails closed when a loader returns another revision or mutable assets", async () => {
    const mismatch = new V2SidecarRevisionSelector<TestRevision, TestAssets>({
      projectIds: [PROJECT_1],
      defaultRevision: "r1",
      loaders: new Map([
        ["r1", async () => assets("r2", "wrong")],
      ]),
    });
    await expect(mismatch.preload()).rejects.toThrow(/revision mismatch/i);

    const mutable = new V2SidecarRevisionSelector<TestRevision, TestAssets>({
      projectIds: [PROJECT_1],
      defaultRevision: "r1",
      loaders: new Map([
        ["r1", async () => ({
          revision: "r1",
          buildHash: "a".repeat(64),
          schemaHash: "b".repeat(64),
          marker: "mutable",
        })],
      ]),
    });
    await expect(mutable.preload()).rejects.toThrow(/not frozen/i);
  });

  it("provides a production R3 default backed by the pinned bundled release", async () => {
    const selector = createBundledV2SidecarRevisionSelector({
      projectIds: [PROJECT_1, PROJECT_2],
    });

    await selector.preload();
    expect(selector.defaultRevision).toBe("r3");
    expect(selector.loadedForProject(PROJECT_1).assets).toMatchObject({
      revision: "r3",
      protocolRevision: "r2",
      buildHash: V2_R3_RELEASE.buildHash,
    });
    expect(selector.loadedForProject(PROJECT_2).assets).toBe(
      selector.loadedForProject(PROJECT_1).assets,
    );
  });

  it("retains explicit pinned R1/R2 rollback and supports a mixed exact-project fleet", async () => {
    const selector = createBundledV2SidecarRevisionSelector({
      projectIds: [PROJECT_1, PROJECT_2, PROJECT_3],
      projectOverrides: { [PROJECT_2]: "r2", [PROJECT_3]: "r1" },
    });
    await selector.preload();
    expect(selector.loadedForProject(PROJECT_1)).toMatchObject({
      revision: "r3",
      assets: { protocolRevision: "r2", buildHash: V2_R3_RELEASE.buildHash },
    });
    expect(selector.loadedForProject(PROJECT_2)).toMatchObject({
      revision: "r2",
      assets: { buildHash: V2_R2_RELEASE.buildHash },
    });
    expect(selector.loadedForProject(PROJECT_3)).toMatchObject({
      revision: "r1",
      assets: { buildHash: V2_R1_RELEASE.buildHash },
    });
  });

  it("drills explicit R2 to fresh-session R1 rollback on the same existing project", async () => {
    const loaded = { r1: 0, r2: 0 };
    const acquired: Array<{ projectId: string; sessionId: string }> = [];
    let createdProjects = 0;
    const loaders = new Map<V2SidecarRevision, () => Promise<V2SidecarAssets>>([
      ["r1", async () => { loaded.r1++; return loadV2SidecarAssets({ revision: "r1" }); }],
      ["r2", async () => { loaded.r2++; return loadV2SidecarAssets({ revision: "r2" }); }],
    ]);
    const provisioning = {
      async acquireFreshSession(selectedProject: typeof PROJECT_1) {
        const session = {
          projectId: selectedProject,
          sessionId: `fresh-session-${acquired.length + 1}`,
        };
        acquired.push(session);
        return session;
      },
      createProject() { createdProjects++; },
    };
    const boot = async (revision: V2SidecarRevision) => {
      const selector = new V2SidecarRevisionSelector({
        projectIds: [PROJECT_1],
        defaultRevision: revision,
        loaders,
      });
      await selector.preload();
      const release = selector.loadedForProject(PROJECT_1);
      const sandbox = await provisioning.acquireFreshSession(PROJECT_1);
      return { release, sandbox };
    };

    const before = await boot("r2");
    // A full config restart builds a new selector and acquires a fresh sandbox
    // session; it never mutates the running sidecar or creates another project.
    const rollback = await boot("r1");
    expect(before.release).toMatchObject({
      projectId: PROJECT_1,
      revision: "r2",
      assets: { buildHash: V2_R2_RELEASE.buildHash },
    });
    expect(rollback.release).toMatchObject({
      projectId: PROJECT_1,
      revision: "r1",
      assets: { buildHash: V2_R1_RELEASE.buildHash },
    });
    expect(rollback.sandbox.projectId).toBe(before.sandbox.projectId);
    expect(rollback.sandbox.sessionId).not.toBe(before.sandbox.sessionId);
    expect(acquired.map(value => value.projectId)).toEqual([PROJECT_1, PROJECT_1]);
    expect(loaded).toEqual({ r1: 1, r2: 1 });
    expect(createdProjects).toBe(0);
  });
});
