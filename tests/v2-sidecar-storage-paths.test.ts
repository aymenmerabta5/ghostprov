import { describe, expect, it } from "bun:test";
import {
  FROZEN_R1_JOURNAL_ROOT,
  SIDECAR_OWNERSHIP_ROOT,
  sidecarJournalRoot,
} from "../sandbox/v2/storage-paths";

describe("V2 sidecar durable storage paths", () => {
  it("keeps one cross-revision ownership fence and disjoint immutable journals", () => {
    expect(SIDECAR_OWNERSHIP_ROOT).toBe("/tmp/ghostprov-v2");
    expect(FROZEN_R1_JOURNAL_ROOT).toBe("/tmp/ghostprov-v2");
    expect(sidecarJournalRoot("r1")).toBe(FROZEN_R1_JOURNAL_ROOT);
    expect(sidecarJournalRoot("r2")).toBe("/tmp/ghostprov-v2/journals/r2");
    expect(sidecarJournalRoot("r1")).not.toBe(sidecarJournalRoot("r2"));
    expect(() => sidecarJournalRoot("r3" as any)).toThrow(/unsupported/i);
  });
});
