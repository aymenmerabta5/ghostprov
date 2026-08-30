import { describe, expect, it } from "bun:test";
import {
  V2_RESEARCH_MODEL_REVISION,
  V2_RESEARCH_SCENARIOS,
  compareV2ResearchSnapshots,
  main,
  type V2ResearchScenarioResult,
  type V2ResearchSnapshot,
} from "../scripts/v2_performance_research";

const SCENARIO = {
  id: "mixed-test",
  description: "test",
  rttMs: 400,
  concurrency: 2,
  requestBytes: [128 * 1024, 512 * 1024],
  timeoutMs: 30_000,
} as const;

function result(
  overrides: Partial<V2ResearchScenarioResult> = {},
): V2ResearchScenarioResult {
  return {
    scenario: SCENARIO,
    repetitions: 2,
    commitP50Ms: 1_000,
    commitP95Ms: 2_000,
    commitMaxMs: 2_000,
    virtualElapsedMs: 2_500,
    wallCommitP95Ms: 20,
    wallTotalP95Ms: 25,
    hostPeakWindowBytes: 900_000,
    attemptedWireBytes: 1_000_000,
    usefulRequestBytes: 640 * 1024,
    retransmissions: 0,
    upstreamMaxActive: 2,
    exactRequests: true,
    exactTerminals: true,
    classes: {
      tiny: { requests: 1, commitP95Ms: 1_000, commitMaxMs: 1_000, ackRoundsP95: 2 },
      medium: { requests: 1, commitP95Ms: 2_000, commitMaxMs: 2_000, ackRoundsP95: 2 },
    },
    ...overrides,
  };
}

function snapshot(value: V2ResearchScenarioResult): V2ResearchSnapshot {
  return {
    schemaVersion: 1,
    modelRevision: V2_RESEARCH_MODEL_REVISION,
    suite: "ghostprov-v2-performance-research",
    commit: "a".repeat(40),
    transportProfile: "bulk-v1",
    results: [value],
  };
}

describe("V2 continuous performance research", () => {
  it("covers short, medium, long, large, mixed, high-RTT, and forty-agent workloads", () => {
    expect(new Set(V2_RESEARCH_SCENARIOS.map(scenario => scenario.id)).size)
      .toBe(V2_RESEARCH_SCENARIOS.length);
    expect(Math.max(...V2_RESEARCH_SCENARIOS.map(scenario => scenario.concurrency))).toBe(40);
    expect(V2_RESEARCH_SCENARIOS.some(scenario => scenario.rttMs === 1_000)).toBe(true);
    expect(V2_RESEARCH_SCENARIOS.some(scenario => Array.isArray(scenario.requestBytes))).toBe(true);
    const sizes = V2_RESEARCH_SCENARIOS.flatMap(scenario => (
      typeof scenario.requestBytes === "number"
        ? [scenario.requestBytes]
        : [...scenario.requestBytes]
    ));
    expect(Math.min(...sizes)).toBeLessThanOrEqual(128 * 1024);
    expect(Math.max(...sizes)).toBeGreaterThanOrEqual(13 * 1024 ** 2);
  });

  it("keeps an equivalent candidate stable and accepts measurable improvements", () => {
    expect(compareV2ResearchSnapshots(snapshot(result()), snapshot(result())))
      .toMatchObject({ decision: "stable", improvements: [], regressions: [] });
    expect(compareV2ResearchSnapshots(snapshot(result()), snapshot(result({
      commitP95Ms: 1_800,
      virtualElapsedMs: 1_800,
    })))).toMatchObject({
      decision: "keep",
      improvements: expect.arrayContaining([
        "mixed-test: commit p95 improved",
        "mixed-test: virtual elapsed time improved",
      ]),
      regressions: [],
    });
    expect(compareV2ResearchSnapshots(snapshot(result()), snapshot(result({
      virtualElapsedMs: 2_900,
    }))).decision).toBe("stable");
    expect(compareV2ResearchSnapshots(snapshot(result()), snapshot(result({
      virtualElapsedMs: 3_000,
    }))).decision).toBe("reject");
  });

  it("rejects tiny latency, integrity, concurrency, retransmission, window, and wire regressions", () => {
    for (const candidate of [
      result({
        classes: {
          ...result().classes,
          tiny: { requests: 1, commitP95Ms: 1_010, commitMaxMs: 1_010, ackRoundsP95: 2 },
        },
      }),
      result({ exactRequests: false }),
      result({ exactTerminals: false }),
      result({ upstreamMaxActive: 1 }),
      result({ retransmissions: 1 }),
      result({ hostPeakWindowBytes: 1024 ** 2 + 1 }),
      result({ attemptedWireBytes: 1_020_000 }),
    ]) {
      const comparison = compareV2ResearchSnapshots(snapshot(result()), snapshot(candidate));
      expect(comparison.decision).toBe("reject");
      expect(comparison.regressions.length).toBeGreaterThan(0);
    }
  });

  it("rejects missing, added, or silently changed scenarios and invalid CLI input", async () => {
    const baseline = snapshot(result());
    expect(() => compareV2ResearchSnapshots(
      { ...baseline, modelRevision: "legacy-wall-clock" },
      baseline,
    )).toThrow("Incompatible V2 research model revision");
    expect(compareV2ResearchSnapshots(baseline, { ...baseline, results: [] }).decision)
      .toBe("reject");
    expect(compareV2ResearchSnapshots({ ...baseline, results: [] }, baseline).decision)
      .toBe("reject");
    expect(compareV2ResearchSnapshots(baseline, snapshot(result({
      scenario: { ...SCENARIO, rttMs: 50 },
    }))).decision).toBe("reject");
    await expect(main(["--unknown"])).rejects.toThrow("Unknown argument");
    await expect(main(["--record", "baseline.json", "--require-improvement"]))
      .rejects.toThrow("valid only with --compare");
  });
});
