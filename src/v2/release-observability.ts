import { V2_SELECTABLE_RELEASE_IDENTITIES } from "./sidecar/assets";

const SHA256 = /^[a-f0-9]{64}$/;
const RELEASE_REVISIONS = new Set(["r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9"]);
const PROTOCOL_REVISIONS = new Set(["r1", "r2"]);
const MAX_RELEASE_SELECTIONS = 256;
const EXACT_RELEASE_IDENTITIES = new Set(
  V2_SELECTABLE_RELEASE_IDENTITIES.map(release => (
    `${release.revision}:${release.protocolRevision}:${release.buildHash}:${release.schemaHash}`
  )),
);

export interface SafeV2ReleaseIdentity {
  releaseRevision: "r1" | "r2" | "r3" | "r4" | "r5" | "r6" | "r7" | "r8" | "r9";
  protocolRevision: "r1" | "r2";
  buildHash: string;
  schemaHash: string;
}

export interface SafeV2ReleaseSelection extends SafeV2ReleaseIdentity {
  runtimes: number;
}

export interface SafeV2ReleaseSummary {
  knownRuntimes: number;
  unknownRuntimes: number;
  selections: readonly SafeV2ReleaseSelection[];
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

export function safeV2ReleaseIdentity(value: unknown): SafeV2ReleaseIdentity | null {
  const raw = record(value);
  if (
    !raw ||
    typeof raw.releaseRevision !== "string" ||
    !RELEASE_REVISIONS.has(raw.releaseRevision) ||
    typeof raw.protocolRevision !== "string" ||
    !PROTOCOL_REVISIONS.has(raw.protocolRevision) ||
    (raw.releaseRevision === "r1"
      ? raw.protocolRevision !== "r1"
      : raw.protocolRevision !== "r2") ||
    typeof raw.buildHash !== "string" ||
    !SHA256.test(raw.buildHash) ||
    typeof raw.schemaHash !== "string" ||
    !SHA256.test(raw.schemaHash) ||
    !EXACT_RELEASE_IDENTITIES.has(
      `${raw.releaseRevision}:${raw.protocolRevision}:${raw.buildHash}:${raw.schemaHash}`,
    )
  ) return null;
  return Object.freeze({
    releaseRevision: raw.releaseRevision as "r1" | "r2" | "r3" | "r4" | "r5" | "r6" | "r7" | "r8" | "r9",
    protocolRevision: raw.protocolRevision as "r1" | "r2",
    buildHash: raw.buildHash,
    schemaHash: raw.schemaHash,
  });
}

/** Deterministic bounded aggregate; unknown/fake runtimes remain explicit. */
export function summarizeV2Releases(runtimes: readonly unknown[]): SafeV2ReleaseSummary {
  const selected = new Map<string, SafeV2ReleaseSelection>();
  let unknownRuntimes = 0;
  for (const runtime of runtimes) {
    const identity = safeV2ReleaseIdentity(runtime);
    if (!identity) {
      unknownRuntimes++;
      continue;
    }
    const key = `${identity.releaseRevision}:${identity.protocolRevision}:${identity.buildHash}:${identity.schemaHash}`;
    const current = selected.get(key);
    if (current) current.runtimes++;
    else if (selected.size < MAX_RELEASE_SELECTIONS) {
      selected.set(key, { ...identity, runtimes: 1 });
    } else {
      unknownRuntimes++;
    }
  }
  const selections = [...selected.values()].sort((left, right) => (
    left.releaseRevision.localeCompare(right.releaseRevision) ||
    left.protocolRevision.localeCompare(right.protocolRevision) ||
    left.buildHash.localeCompare(right.buildHash) ||
    left.schemaHash.localeCompare(right.schemaHash)
  ));
  const knownRuntimes = selections.reduce((sum, value) => sum + value.runtimes, 0);
  return Object.freeze({
    knownRuntimes,
    unknownRuntimes,
    selections: Object.freeze(selections.map(value => Object.freeze({ ...value }))),
  });
}

export function parseSafeV2ReleaseSummary(value: unknown): SafeV2ReleaseSummary | null {
  const raw = record(value);
  if (
    !raw ||
    !Number.isSafeInteger(raw.knownRuntimes) || Number(raw.knownRuntimes) < 0 ||
    !Number.isSafeInteger(raw.unknownRuntimes) || Number(raw.unknownRuntimes) < 0 ||
    !Array.isArray(raw.selections) || raw.selections.length > MAX_RELEASE_SELECTIONS
  ) return null;
  const expanded: unknown[] = [];
  for (const selection of raw.selections) {
    const item = safeV2ReleaseIdentity(selection);
    const count = record(selection)?.runtimes;
    if (!item || !Number.isSafeInteger(count) || Number(count) < 1) return null;
    if (expanded.length + Number(count) > 65_536) return null;
    for (let index = 0; index < Number(count); index++) expanded.push(item);
  }
  const summary = summarizeV2Releases(expanded);
  if (
    summary.knownRuntimes !== raw.knownRuntimes ||
    Number(raw.unknownRuntimes) > 65_536
  ) return null;
  return Object.freeze({
    ...summary,
    unknownRuntimes: Number(raw.unknownRuntimes),
  });
}
