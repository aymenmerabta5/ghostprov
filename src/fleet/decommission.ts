import { timingSafeEqual } from "node:crypto";
import { Buffer } from "node:buffer";
import type { PrismProjectsClient } from "../prism/projects-client";
import type { ProjectId } from "../sandbox/types";
import { canonicalJson, sha256Hex } from "../v2/protocol/shared";
import type { FleetManifestStore } from "./manifest";

export interface DecommissionPreview {
  identityFingerprint: string;
  projects: readonly ProjectId[];
  confirmationDigest: string;
}

export class FleetDecommissionError extends Error {
  readonly name = "FleetDecommissionError";

  constructor(
    message: string,
    readonly kind: "confirmation" | "stale" | "aborted",
  ) {
    super(message);
  }
}

export interface DecommissionProjectsClient {
  deleteManaged(id: ProjectId, signal?: AbortSignal): Promise<void>;
}

function sameProjects(
  left: readonly ProjectId[],
  right: readonly ProjectId[],
): boolean {
  return left.length === right.length && left.every((id, index) => id === right[index]);
}

function constantTimeDigestEqual(left: string, right: string): boolean {
  if (!/^[a-f0-9]{64}$/.test(left) || !/^[a-f0-9]{64}$/.test(right)) return false;
  return timingSafeEqual(Buffer.from(left, "hex"), Buffer.from(right, "hex"));
}

async function digest(
  identityFingerprint: string,
  projects: readonly ProjectId[],
): Promise<string> {
  return sha256Hex(canonicalJson({
    operation: "ghostprov-v2-decommission",
    identityFingerprint,
    projects,
  }));
}

export class FleetDecommissionService {
  readonly #manifest: FleetManifestStore;
  readonly #projects: DecommissionProjectsClient;

  constructor(options: {
    manifest: FleetManifestStore;
    projects: DecommissionProjectsClient | PrismProjectsClient;
  }) {
    this.#manifest = options.manifest;
    this.#projects = options.projects;
  }

  async preview(requested: readonly ProjectId[]): Promise<DecommissionPreview> {
    const manifest = await this.#manifest.load();
    const requestedIds = new Set(requested);
    const projects = manifest.workers
      .filter(worker => worker.managed && requestedIds.has(worker.projectId))
      .map(worker => worker.projectId)
      .sort((left, right) => left.localeCompare(right));
    return Object.freeze({
      identityFingerprint: manifest.identityFingerprint,
      projects: Object.freeze(projects),
      confirmationDigest: await digest(manifest.identityFingerprint, projects),
    });
  }

  async execute(
    preview: DecommissionPreview,
    suppliedDigest: string,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!constantTimeDigestEqual(preview.confirmationDigest, suppliedDigest)) {
      throw new FleetDecommissionError(
        "Supplied confirmation digest does not match the preview",
        "confirmation",
      );
    }
    if (signal?.aborted) {
      throw new FleetDecommissionError("Fleet decommission was aborted", "aborted");
    }
    const current = await this.preview(preview.projects);
    if (
      current.identityFingerprint !== preview.identityFingerprint ||
      !sameProjects(current.projects, preview.projects) ||
      !constantTimeDigestEqual(current.confirmationDigest, preview.confirmationDigest)
    ) {
      throw new FleetDecommissionError(
        "Decommission preview is stale or its managed targets changed",
        "stale",
      );
    }

    for (const projectId of preview.projects) {
      if (signal?.aborted) {
        throw new FleetDecommissionError("Fleet decommission was aborted", "aborted");
      }
      const manifest = await this.#manifest.load();
      const worker = manifest.workers.find(value => value.projectId === projectId);
      if (!worker?.managed) {
        throw new FleetDecommissionError(
          `Managed decommission target changed: ${projectId}`,
          "stale",
        );
      }
      await this.#projects.deleteManaged(projectId, signal);
      await this.#manifest.removeManaged(projectId);
    }
  }
}
