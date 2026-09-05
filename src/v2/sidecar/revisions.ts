import type { ProjectId } from "../../sandbox/types";
import {
  DEFAULT_V2_SIDECAR_REVISION,
  loadV2SidecarAssets,
  V2_R1,
  V2_R2,
  V2_R3,
  V2_R4,
  V2_R5,
  V2_R6,
  V2_R7,
  V2_R8,
  V2_R9,
  V2_R10,
  type V2SidecarAssets,
  type V2SidecarRevision,
} from "./assets";

const SHA256 = /^[a-f0-9]{64}$/;

export interface RevisionAddressedAssets<Revision extends string = string> {
  readonly revision: Revision;
  readonly buildHash: string;
  readonly schemaHash: string;
}

export type V2RevisionAssetLoader<
  Revision extends string,
  Assets extends RevisionAddressedAssets<Revision>,
> = () => Promise<Assets>;

export type V2ProjectRevisionOverrides<Revision extends string> =
  | ReadonlyMap<ProjectId, Revision>
  | Readonly<Record<string, Revision>>;

export interface V2SidecarRevisionSelectorOptions<
  Revision extends string,
  Assets extends RevisionAddressedAssets<Revision>,
> {
  /** Exact projects known at startup; dynamic mode also accepts later managed IDs. */
  readonly projectIds: readonly ProjectId[];
  readonly defaultRevision: Revision;
  /** Exact project ID matches only; dynamic projects never inherit an override. */
  readonly projectOverrides?: V2ProjectRevisionOverrides<Revision>;
  /** Managed projects created after startup use the immutable default release. */
  readonly allowUnlistedProjects?: boolean;
  /** Every selectable revision must have one explicit immutable loader. */
  readonly loaders: ReadonlyMap<Revision, V2RevisionAssetLoader<Revision, Assets>>;
}

export interface V2ProjectRevisionSelection<
  Revision extends string,
  Assets extends RevisionAddressedAssets<Revision>,
> {
  readonly projectId: ProjectId;
  readonly revision: Revision;
  readonly assets: Assets;
}

function overrideEntries<Revision extends string>(
  overrides: V2ProjectRevisionOverrides<Revision> | undefined,
): readonly (readonly [string, Revision])[] {
  if (!overrides) return [];
  if (overrides instanceof Map) return [...overrides.entries()];
  return Object.entries(overrides);
}

/**
 * Process-local, fail-closed project -> immutable V2 release selector.
 *
 * It deliberately knows nothing about environment/config syntax. Callers
 * parse operator policy before construction, preload the selected releases,
 * and then synchronously hand one exact revision/assets pair to each runtime.
 */
export class V2SidecarRevisionSelector<
  Revision extends string,
  Assets extends RevisionAddressedAssets<Revision>,
> {
  readonly #projectIds: readonly ProjectId[];
  readonly #projects: ReadonlySet<ProjectId>;
  readonly #defaultRevision: Revision;
  readonly #allowUnlistedProjects: boolean;
  readonly #overrides: ReadonlyMap<ProjectId, Revision>;
  readonly #loaders: ReadonlyMap<Revision, V2RevisionAssetLoader<Revision, Assets>>;
  readonly #loading = new Map<Revision, Promise<Assets>>();
  readonly #loaded = new Map<Revision, Assets>();

  constructor(options: V2SidecarRevisionSelectorOptions<Revision, Assets>) {
    const projectIds = [...options.projectIds];
    const projects = new Set<ProjectId>();
    for (const projectId of projectIds) {
      if (projects.has(projectId)) {
        throw new Error(`Duplicate V2 revision project: ${projectId}`);
      }
      projects.add(projectId);
    }
    const loaders = new Map(options.loaders);
    if (!loaders.has(options.defaultRevision)) {
      throw new Error(`No immutable V2 assets registered for default revision ${options.defaultRevision}`);
    }
    const overrides = new Map<ProjectId, Revision>();
    for (const [rawProjectId, revision] of overrideEntries(options.projectOverrides)) {
      const projectId = rawProjectId as ProjectId;
      if (!projects.has(projectId) && !options.allowUnlistedProjects) {
        throw new Error(`V2 revision override names an unknown project: ${rawProjectId}`);
      }
      if (overrides.has(projectId)) {
        throw new Error(`Duplicate V2 revision override: ${rawProjectId}`);
      }
      if (!loaders.has(revision)) {
        throw new Error(`No immutable V2 assets registered for revision ${revision}`);
      }
      overrides.set(projectId, revision);
    }
    this.#projectIds = Object.freeze(projectIds);
    this.#projects = projects;
    this.#defaultRevision = options.defaultRevision;
    this.#allowUnlistedProjects = options.allowUnlistedProjects === true;
    this.#overrides = overrides;
    this.#loaders = loaders;
  }

  get defaultRevision(): Revision {
    return this.#defaultRevision;
  }

  revisionForProject(projectId: ProjectId): Revision {
    if (!this.#projects.has(projectId) && !this.#allowUnlistedProjects) {
      throw new Error(`Cannot select a V2 revision for unknown project: ${projectId}`);
    }
    return this.#overrides.get(projectId) ?? this.#defaultRevision;
  }

  selectedRevisions(): readonly Revision[] {
    const revisions = new Set<Revision>([this.#defaultRevision]);
    for (const projectId of this.#projectIds) revisions.add(this.revisionForProject(projectId));
    for (const revision of this.#overrides.values()) revisions.add(revision);
    return Object.freeze([...revisions]);
  }

  loadRevision(revision: Revision): Promise<Assets> {
    const existing = this.#loading.get(revision);
    if (existing) return existing;
    const loader = this.#loaders.get(revision);
    if (!loader) {
      return Promise.reject(new Error(`No immutable V2 assets registered for revision ${revision}`));
    }
    const loading = (async () => {
      const assets = await loader();
      if (!assets || typeof assets !== "object") {
        throw new Error(`Immutable V2 asset loader ${revision} returned no assets`);
      }
      if (assets.revision !== revision) {
        throw new Error(
          `Immutable V2 asset revision mismatch: selected ${revision}, loaded ${String(assets.revision)}`,
        );
      }
      if (!SHA256.test(assets.buildHash) || !SHA256.test(assets.schemaHash)) {
        throw new Error(`Immutable V2 asset identity is invalid for revision ${revision}`);
      }
      if (!Object.isFrozen(assets)) {
        throw new Error(`Immutable V2 assets for revision ${revision} are not frozen`);
      }
      this.#loaded.set(revision, assets);
      return assets;
    })();
    // Cache both success and failure. A missing/corrupt release cannot become a
    // different in-band choice; an operator must restart with corrected policy.
    this.#loading.set(revision, loading);
    return loading;
  }

  async preload(): Promise<void> {
    await Promise.all(this.selectedRevisions().map(revision => this.loadRevision(revision)));
  }

  async resolveProject(
    projectId: ProjectId,
  ): Promise<V2ProjectRevisionSelection<Revision, Assets>> {
    const revision = this.revisionForProject(projectId);
    return Object.freeze({
      projectId,
      revision,
      assets: await this.loadRevision(revision),
    });
  }

  loadedForProject(
    projectId: ProjectId,
  ): V2ProjectRevisionSelection<Revision, Assets> {
    const revision = this.revisionForProject(projectId);
    const assets = this.#loaded.get(revision);
    if (!assets) {
      throw new Error(`Immutable V2 assets for revision ${revision} were not preloaded`);
    }
    return Object.freeze({ projectId, revision, assets });
  }
}

export interface BundledV2SidecarRevisionSelectorOptions {
  readonly projectIds: readonly ProjectId[];
  readonly defaultRevision?: V2SidecarRevision;
  readonly projectOverrides?: V2ProjectRevisionOverrides<V2SidecarRevision>;
  readonly allowUnlistedProjects?: boolean;
}

/** Production registry with sandbox-relay R9 default and explicit prior-release rollback. */
export function createBundledV2SidecarRevisionSelector(
  options: BundledV2SidecarRevisionSelectorOptions,
): V2SidecarRevisionSelector<V2SidecarRevision, V2SidecarAssets> {
  const loaders = new Map<V2SidecarRevision, () => Promise<V2SidecarAssets>>([
    [V2_R1, () => loadV2SidecarAssets({ revision: V2_R1 })],
    [V2_R2, () => loadV2SidecarAssets({ revision: V2_R2 })],
    [V2_R3, () => loadV2SidecarAssets({ revision: V2_R3 })],
    [V2_R4, () => loadV2SidecarAssets({ revision: V2_R4 })],
    [V2_R5, () => loadV2SidecarAssets({ revision: V2_R5 })],
    [V2_R6, () => loadV2SidecarAssets({ revision: V2_R6 })],
    [V2_R7, () => loadV2SidecarAssets({ revision: V2_R7 })],
    [V2_R8, () => loadV2SidecarAssets({ revision: V2_R8 })],
    [V2_R9, () => loadV2SidecarAssets({ revision: V2_R9 })],
    [V2_R10, () => loadV2SidecarAssets({ revision: V2_R10 })],
  ]);
  return new V2SidecarRevisionSelector({
    projectIds: options.projectIds,
    defaultRevision: options.defaultRevision ?? DEFAULT_V2_SIDECAR_REVISION,
    projectOverrides: options.projectOverrides,
    allowUnlistedProjects: options.allowUnlistedProjects,
    loaders,
  });
}
