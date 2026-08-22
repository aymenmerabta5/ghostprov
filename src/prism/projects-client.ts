import type { AccountRequestClient } from "./provisioning-client";
import { projectId, type ProjectId } from "../sandbox/types";

export interface PrismProjectSummary {
  projectId: ProjectId;
  title: string;
  owner: string | null;
  deleted: boolean;
  createdAt: string | null;
}

export type PrismProjectErrorKind = "http" | "protocol";

export class PrismProjectError extends Error {
  readonly name = "PrismProjectError";

  constructor(
    message: string,
    readonly kind: PrismProjectErrorKind,
    readonly status?: number,
  ) {
    super(message);
  }
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new PrismProjectError(`${label} must be an object`, "protocol");
  }
  return value as Record<string, unknown>;
}

function parseProject(value: unknown): PrismProjectSummary {
  const raw = object(value, "Prism project");
  if (typeof raw.uuid !== "string") {
    throw new PrismProjectError("Prism project UUID is missing", "protocol");
  }
  if (typeof raw.title !== "string" || raw.title.length === 0) {
    throw new PrismProjectError("Prism project title is missing", "protocol");
  }
  if (typeof raw.deleted !== "boolean") {
    throw new PrismProjectError("Prism project deleted flag is invalid", "protocol");
  }
  if (raw.owner != null && typeof raw.owner !== "string") {
    throw new PrismProjectError("Prism project owner is invalid", "protocol");
  }
  if (raw.created_at != null && typeof raw.created_at !== "string") {
    throw new PrismProjectError("Prism project creation time is invalid", "protocol");
  }
  return {
    projectId: projectId(raw.uuid),
    title: raw.title,
    owner: raw.owner ?? null,
    deleted: raw.deleted,
    createdAt: raw.created_at ?? null,
  };
}

async function responseJson(
  response: Response,
  operation: string,
): Promise<Record<string, unknown>> {
  if (!response.ok) {
    throw new PrismProjectError(
      `Prism project ${operation} failed (${response.status})`,
      "http",
      response.status,
    );
  }
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new PrismProjectError(
      `Prism project ${operation} returned invalid JSON`,
      "protocol",
      response.status,
    );
  }
  return object(body, `Prism project ${operation} response`);
}

export class PrismProjectsClient {
  readonly #account: AccountRequestClient;

  constructor(account: AccountRequestClient) {
    this.#account = account;
  }

  async list(signal?: AbortSignal): Promise<readonly PrismProjectSummary[]> {
    const response = await this.#account.request(
      "/api/projects?section=your_projects",
      { method: "GET", signal, headers: { accept: "application/json" } },
    );
    const body = await responseJson(response, "list");
    if (!Array.isArray(body.projects)) {
      throw new PrismProjectError("Prism project list is missing", "protocol");
    }
    return Object.freeze(body.projects.map(parseProject));
  }

  async get(
    id: ProjectId,
    signal?: AbortSignal,
  ): Promise<PrismProjectSummary> {
    const response = await this.#account.request(
      `/api/projects/${encodeURIComponent(id)}`,
      { method: "GET", signal, headers: { accept: "application/json" } },
    );
    const body = await responseJson(response, "metadata read");
    return parseProject(body.project);
  }

  async create(
    input: { projectId: ProjectId; title: string },
    signal?: AbortSignal,
  ): Promise<PrismProjectSummary> {
    if (!input.title.trim()) {
      throw new PrismProjectError("Prism project title must not be empty", "protocol");
    }
    const response = await this.#account.request("/api/projects", {
      method: "POST",
      signal,
      headers: {
        accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        project_uuid: input.projectId,
        title: input.title,
        file_uuids: [],
      }),
    });
    return parseProject(await responseJson(response, "creation"));
  }

  async deleteManaged(
    id: ProjectId,
    signal?: AbortSignal,
  ): Promise<void> {
    const response = await this.#account.request(
      `/api/projects/${encodeURIComponent(id)}`,
      { method: "DELETE", signal, headers: { accept: "application/json" } },
    );
    if (!response.ok) {
      throw new PrismProjectError(
        `Managed Prism project deletion failed (${response.status})`,
        "http",
        response.status,
      );
    }
  }
}
