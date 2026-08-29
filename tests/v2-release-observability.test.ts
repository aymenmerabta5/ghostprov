import { describe, expect, it } from "bun:test";
import {
  safeV2ReleaseIdentity,
  summarizeV2Releases,
} from "../src/v2/release-observability";
import {
  V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY,
  V2_R4_RELEASE,
  V2_R8_RELEASE,
} from "../src/v2/sidecar/assets";

describe("V2 release observability identity", () => {
  it("accepts only the exact registered release tuple", () => {
    const exact = {
      releaseRevision: V2_R8_RELEASE.revision,
      protocolRevision: V2_R8_RELEASE.protocolRevision,
      buildHash: V2_R8_RELEASE.buildHash,
      schemaHash: V2_R8_RELEASE.schemaHash,
    };
    const forged = {
      ...exact,
      buildHash: "0".repeat(64),
      schemaHash: "f".repeat(64),
    };
    const crossPaired = { ...exact, buildHash: V2_R4_RELEASE.buildHash };
    const retirementOnly = {
      releaseRevision: V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY.revision,
      protocolRevision: V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY.protocolRevision,
      buildHash: V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY.buildHash,
      schemaHash: V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY.schemaHash,
    };

    expect(safeV2ReleaseIdentity(exact)).toEqual(exact);
    expect(safeV2ReleaseIdentity(forged)).toBeNull();
    expect(safeV2ReleaseIdentity(crossPaired)).toBeNull();
    expect(safeV2ReleaseIdentity(retirementOnly)).toBeNull();
    expect(summarizeV2Releases([forged])).toEqual({
      knownRuntimes: 0,
      unknownRuntimes: 1,
      selections: [],
    });
  });
});
