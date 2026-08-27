import { access, mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import {
  runV2PerformanceHarness,
  type V2PerformanceHarnessReport,
} from "../tests/support/v2-performance-harness";

const KiB = 1024;
const MiB = KiB ** 2;
const DEFAULT_BASELINE = path.resolve("benchmarks/v2-performance-baseline.json");

export type ResearchRequestClass = "tiny" | "medium" | "large";

export interface V2ResearchScenario {
  id: string;
  description: string;
  rttMs: number;
  concurrency: number;
  requestBytes: number | readonly number[];
  timeoutMs: number;
}

export interface V2ResearchClassMetrics {
  requests: number;
  commitP95Ms: number;
  commitMaxMs: number;
  ackRoundsP95: number;
}

export interface V2ResearchScenarioResult {
  scenario: V2ResearchScenario;
  repetitions: number;
  commitP50Ms: number;
  commitP95Ms: number;
  commitMaxMs: number;
  virtualElapsedMs: number;
  wallCommitP95Ms: number;
  wallTotalP95Ms: number;
  hostPeakWindowBytes: number;
  attemptedWireBytes: number;
  usefulRequestBytes: number;
  retransmissions: number;
  upstreamMaxActive: number;
  exactRequests: boolean;
  exactTerminals: boolean;
  classes: Partial<Record<ResearchRequestClass, V2ResearchClassMetrics>>;
}

export interface V2ResearchSnapshot {
  schemaVersion: 1;
  suite: "ghostprov-v2-performance-research";
  commit: string;
  transportProfile: "bulk-v1";
  results: readonly V2ResearchScenarioResult[];
}

export interface V2ResearchComparison {
  decision: "keep" | "reject" | "stable";
  improvements: readonly string[];
  regressions: readonly string[];
  scenarios: readonly {
    id: string;
    baselineCommitP95Ms: number;
    candidateCommitP95Ms: number;
    commitImprovementPercent: number;
    baselineVirtualElapsedMs: number;
    candidateVirtualElapsedMs: number;
    virtualImprovementPercent: number;
    wireImprovementPercent: number;
    tinyP95DeltaMs: number | null;
  }[];
}

const alternating = (left: number, right: number, count: number): readonly number[] =>
  Object.freeze(Array.from({ length: count }, (_, index) => index % 2 === 0 ? left : right));

export const V2_RESEARCH_SCENARIOS: readonly V2ResearchScenario[] = Object.freeze([
  Object.freeze({
    id: "tiny-128k-c40-rtt50",
    description: "Forty short-context agents on a low-latency link",
    rttMs: 50,
    concurrency: 40,
    requestBytes: 128 * KiB,
    timeoutMs: 30_000,
  }),
  Object.freeze({
    id: "tiny-128k-c24-rtt400",
    description: "Twenty-four short-context agents on the observed high-RTT class",
    rttMs: 400,
    concurrency: 24,
    requestBytes: 128 * KiB,
    timeoutMs: 30_000,
  }),
  Object.freeze({
    id: "medium-512k-c1-rtt400",
    description: "One isolated medium-context request",
    rttMs: 400,
    concurrency: 1,
    requestBytes: 512 * KiB,
    timeoutMs: 30_000,
  }),
  Object.freeze({
    id: "medium-512k-c40-rtt400",
    description: "Forty concurrent medium-context agents",
    rttMs: 400,
    concurrency: 40,
    requestBytes: 512 * KiB,
    timeoutMs: 120_000,
  }),
  Object.freeze({
    id: "mixed-128k-512k-c24-rtt400",
    description: "Production-deadline short/medium context mix",
    rttMs: 400,
    concurrency: 24,
    requestBytes: alternating(128 * KiB, 512 * KiB, 24),
    timeoutMs: 30_000,
  }),
  Object.freeze({
    id: "long-2m-c16-rtt400",
    description: "Sixteen concurrent long-context agents",
    rttMs: 400,
    concurrency: 16,
    requestBytes: 2 * MiB,
    timeoutMs: 60_000,
  }),
  Object.freeze({
    id: "large-13m-c1-rtt400",
    description: "Observed thirteen-MiB request class",
    rttMs: 400,
    concurrency: 1,
    requestBytes: 13 * MiB,
    timeoutMs: 30_000,
  }),
  Object.freeze({
    id: "medium-1m-c4-rtt1000",
    description: "Four medium-context agents on a one-second RTT",
    rttMs: 1_000,
    concurrency: 4,
    requestBytes: MiB,
    timeoutMs: 60_000,
  }),
]);

function percentile(values: readonly number[], quantile: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(sorted.length * quantile) - 1)]!;
}

function median(values: readonly number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[middle]!
    : (sorted[middle - 1]! + sorted[middle]!) / 2;
}

function requestClass(bytes: number): ResearchRequestClass {
  if (bytes <= 256 * KiB) return "tiny";
  if (bytes <= 2 * MiB) return "medium";
  return "large";
}

function requestSizes(scenario: V2ResearchScenario): readonly number[] {
  return typeof scenario.requestBytes === "number"
    ? Array.from({ length: scenario.concurrency }, () => scenario.requestBytes as number)
    : scenario.requestBytes;
}

function classMetrics(
  reports: readonly V2PerformanceHarnessReport[],
  category: ResearchRequestClass,
): V2ResearchClassMetrics | undefined {
  const perReport = reports.map(report => report.requests.filter(
    request => requestClass(request.requestBytes) === category,
  ));
  if (perReport[0]?.length === 0) return undefined;
  return {
    requests: perReport[0]!.length,
    commitP95Ms: median(perReport.map(requests => percentile(
      requests.map(request => request.modeledCommitMs),
      0.95,
    ))),
    commitMaxMs: median(perReport.map(requests => Math.max(
      ...requests.map(request => request.modeledCommitMs),
    ))),
    ackRoundsP95: median(perReport.map(requests => percentile(
      requests.map(request => request.ackRoundsAtCommit),
      0.95,
    ))),
  };
}

export function summarizeV2ResearchScenario(
  scenario: V2ResearchScenario,
  reports: readonly V2PerformanceHarnessReport[],
): V2ResearchScenarioResult {
  if (reports.length < 1) throw new RangeError("research scenario requires at least one report");
  for (const report of reports) {
    if (report.concurrency !== scenario.concurrency || report.transportProfile !== "bulk-v1") {
      throw new Error(`Research report does not match scenario ${scenario.id}`);
    }
  }
  const classes: Partial<Record<ResearchRequestClass, V2ResearchClassMetrics>> = {};
  for (const category of ["tiny", "medium", "large"] as const) {
    const metrics = classMetrics(reports, category);
    if (metrics) classes[category] = metrics;
  }
  return {
    scenario,
    repetitions: reports.length,
    commitP50Ms: median(reports.map(report => report.timings.modeledLink.commitMs.p50)),
    commitP95Ms: median(reports.map(report => report.timings.modeledLink.commitMs.p95)),
    commitMaxMs: median(reports.map(report => report.timings.modeledLink.commitMs.max)),
    virtualElapsedMs: median(reports.map(report => report.virtualElapsedMs)),
    wallCommitP95Ms: median(reports.map(report => report.timings.measuredWall.commitMs.p95)),
    wallTotalP95Ms: median(reports.map(report => report.timings.measuredWall.totalMs.p95)),
    hostPeakWindowBytes: Math.max(...reports.map(report => report.transport.peaks.hostWindowBytes)),
    attemptedWireBytes: median(reports.map(report => report.wire.attemptedBytes)),
    usefulRequestBytes: median(reports.map(report => report.wire.usefulRequestBytes)),
    retransmissions: Math.max(...reports.map(report => report.transport.retransmissions)),
    upstreamMaxActive: Math.min(...reports.map(report => report.upstream.maxActive)),
    exactRequests: reports.every(report => report.requests.every(request => request.exactRequest)),
    exactTerminals: reports.every(report => report.requests.every(request => request.exactTerminal)),
    classes,
  };
}

function regressionLimit(baseline: number, ratio: number, absoluteMs: number): number {
  return baseline * ratio + absoluteMs;
}

export function compareV2ResearchSnapshots(
  baseline: V2ResearchSnapshot,
  candidate: V2ResearchSnapshot,
): V2ResearchComparison {
  if (baseline.schemaVersion !== 1 || candidate.schemaVersion !== 1) {
    throw new Error("Unsupported V2 research snapshot schema");
  }
  const candidateById = new Map(candidate.results.map(result => [result.scenario.id, result]));
  const improvements: string[] = [];
  const regressions: string[] = [];
  const scenarios: V2ResearchComparison["scenarios"][number][] = [];
  for (const previous of baseline.results) {
    const current = candidateById.get(previous.scenario.id);
    if (!current) {
      regressions.push(`${previous.scenario.id}: scenario is missing`);
      continue;
    }
    if (JSON.stringify(current.scenario) !== JSON.stringify(previous.scenario)) {
      regressions.push(`${previous.scenario.id}: scenario definition changed`);
      continue;
    }
    scenarios.push({
      id: previous.scenario.id,
      baselineCommitP95Ms: previous.commitP95Ms,
      candidateCommitP95Ms: current.commitP95Ms,
      commitImprovementPercent: (1 - current.commitP95Ms / previous.commitP95Ms) * 100,
      baselineVirtualElapsedMs: previous.virtualElapsedMs,
      candidateVirtualElapsedMs: current.virtualElapsedMs,
      virtualImprovementPercent:
        (1 - current.virtualElapsedMs / previous.virtualElapsedMs) * 100,
      wireImprovementPercent:
        (1 - current.attemptedWireBytes / previous.attemptedWireBytes) * 100,
      tinyP95DeltaMs: previous.classes.tiny && current.classes.tiny
        ? current.classes.tiny.commitP95Ms - previous.classes.tiny.commitP95Ms
        : null,
    });
    if (!current.exactRequests || !current.exactTerminals) {
      regressions.push(`${previous.scenario.id}: exact request or terminal proof failed`);
    }
    if (current.upstreamMaxActive !== current.scenario.concurrency) {
      regressions.push(`${previous.scenario.id}: upstream concurrency fell to ${current.upstreamMaxActive}`);
    }
    if (current.retransmissions > previous.retransmissions) {
      regressions.push(`${previous.scenario.id}: retransmissions increased`);
    }
    if (current.hostPeakWindowBytes > 1024 * KiB) {
      regressions.push(`${previous.scenario.id}: host window exceeded 1 MiB`);
    }
    if (current.usefulRequestBytes !== previous.usefulRequestBytes) {
      regressions.push(`${previous.scenario.id}: useful request bytes changed`);
    }
    const tinyPrevious = previous.classes.tiny;
    const tinyCurrent = current.classes.tiny;
    if (tinyPrevious && (!tinyCurrent || tinyCurrent.requests !== tinyPrevious.requests)) {
      regressions.push(`${previous.scenario.id}: tiny request coverage changed`);
    } else if (
      tinyPrevious && tinyCurrent &&
      tinyCurrent.commitP95Ms > regressionLimit(tinyPrevious.commitP95Ms, 1.001, 0.05)
    ) {
      regressions.push(`${previous.scenario.id}: tiny p95 regressed`);
    }
    if (current.commitP95Ms > regressionLimit(previous.commitP95Ms, 1.02, 1)) {
      regressions.push(`${previous.scenario.id}: overall commit p95 regressed`);
    }
    // Completion scheduling can shift by one whole simulated network turn even
    // when commit timing and wire work are byte-identical. Keep commit p95
    // strict and permit exactly one scenario RTT for this secondary total.
    if (
      current.virtualElapsedMs > regressionLimit(
        previous.virtualElapsedMs,
        1.02,
        previous.scenario.rttMs + 1,
      )
    ) {
      regressions.push(`${previous.scenario.id}: virtual elapsed time regressed`);
    }
    if (current.attemptedWireBytes > previous.attemptedWireBytes * 1.01 + 1024) {
      regressions.push(`${previous.scenario.id}: attempted wire bytes regressed`);
    }
    if (current.commitP95Ms < previous.commitP95Ms * 0.98 - 1) {
      improvements.push(`${previous.scenario.id}: commit p95 improved`);
    }
    if (
      current.virtualElapsedMs <
        previous.virtualElapsedMs * 0.98 - previous.scenario.rttMs - 1
    ) {
      improvements.push(`${previous.scenario.id}: virtual elapsed time improved`);
    }
  }
  for (const current of candidate.results) {
    if (!baseline.results.some(previous => previous.scenario.id === current.scenario.id)) {
      regressions.push(`${current.scenario.id}: candidate added an unbaselined scenario`);
    }
  }
  return Object.freeze({
    decision: regressions.length > 0
      ? "reject"
      : improvements.length > 0
        ? "keep"
        : "stable",
    improvements: Object.freeze(improvements),
    regressions: Object.freeze(regressions),
    scenarios: Object.freeze(scenarios),
  });
}

async function currentCommit(): Promise<string> {
  const result = Bun.spawnSync(["git", "rev-parse", "HEAD"], {
    cwd: process.cwd(),
    stdout: "pipe",
    stderr: "pipe",
  });
  if (result.exitCode !== 0) throw new Error("Unable to resolve current Git commit");
  return result.stdout.toString().trim();
}

export async function runV2ResearchSnapshot(options: {
  repetitions?: number;
  scenarios?: readonly V2ResearchScenario[];
} = {}): Promise<V2ResearchSnapshot> {
  const repetitions = options.repetitions ?? 3;
  if (!Number.isSafeInteger(repetitions) || repetitions < 1 || repetitions > 10) {
    throw new RangeError("research repetitions must be 1 through 10");
  }
  const scenarios = options.scenarios ?? V2_RESEARCH_SCENARIOS;
  const results: V2ResearchScenarioResult[] = [];
  for (const scenario of scenarios) {
    const sizes = requestSizes(scenario);
    const aggregateBytes = sizes.reduce((total, bytes) => total + bytes, 0);
    const reports: V2PerformanceHarnessReport[] = [];
    for (let repetition = 0; repetition < repetitions; repetition++) {
      process.stderr.write(
        `[v2-research] ${scenario.id} ${repetition + 1}/${repetitions}\n`,
      );
      reports.push(await runV2PerformanceHarness({
        protocolRevision: "r2",
        transportProfile: "bulk-v1",
        corpus: "incompressible",
        rttMs: scenario.rttMs,
        requestBytes: typeof scenario.requestBytes === "number"
          ? scenario.requestBytes
          : Math.max(...scenario.requestBytes),
        ...(typeof scenario.requestBytes === "number"
          ? {}
          : { requestBytesByRequest: scenario.requestBytes }),
        concurrency: scenario.concurrency,
        maxAggregateRequestBytes: aggregateBytes,
        timeoutMs: scenario.timeoutMs,
      }));
    }
    results.push(summarizeV2ResearchScenario(scenario, reports));
  }
  return Object.freeze({
    schemaVersion: 1,
    suite: "ghostprov-v2-performance-research",
    commit: await currentCommit(),
    transportProfile: "bulk-v1",
    results: Object.freeze(results),
  });
}

interface CliOptions {
  mode: "compare" | "record";
  baselinePath: string;
  replace: boolean;
  requireImprovement: boolean;
  repetitions: number;
  scenarioIds: readonly string[];
}

function cliOptions(args: readonly string[]): CliOptions {
  let mode: CliOptions["mode"] = "compare";
  let baselinePath = DEFAULT_BASELINE;
  let replace = false;
  let requireImprovement = false;
  let repetitions = 3;
  const scenarioIds: string[] = [];
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (arg === "--record" || arg === "--compare") {
      mode = arg === "--record" ? "record" : "compare";
      const value = args[++index];
      if (!value) throw new Error(`${arg} requires a path`);
      baselinePath = path.resolve(value);
    } else if (arg === "--replace") {
      replace = true;
    } else if (arg === "--require-improvement") {
      requireImprovement = true;
    } else if (arg === "--repetitions") {
      const value = Number(args[++index]);
      if (!Number.isSafeInteger(value) || value < 1 || value > 10) {
        throw new Error("--repetitions must be 1 through 10");
      }
      repetitions = value;
    } else if (arg === "--scenario") {
      const value = args[++index];
      if (!value) throw new Error("--scenario requires an ID");
      scenarioIds.push(value);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (mode === "record" && requireImprovement) {
    throw new Error("--require-improvement is valid only with --compare");
  }
  return { mode, baselinePath, replace, requireImprovement, repetitions, scenarioIds };
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

function selectedScenarios(ids: readonly string[]): readonly V2ResearchScenario[] {
  if (ids.length === 0) return V2_RESEARCH_SCENARIOS;
  const unique = new Set(ids);
  const scenarios = V2_RESEARCH_SCENARIOS.filter(scenario => unique.has(scenario.id));
  if (scenarios.length !== unique.size) {
    const known = new Set(scenarios.map(scenario => scenario.id));
    throw new Error(`Unknown scenario: ${[...unique].find(id => !known.has(id))}`);
  }
  return scenarios;
}

export async function main(args: readonly string[] = process.argv.slice(2)): Promise<void> {
  const options = cliOptions(args);
  const scenarios = selectedScenarios(options.scenarioIds);
  const snapshot = await runV2ResearchSnapshot({
    repetitions: options.repetitions,
    scenarios,
  });
  if (options.mode === "record") {
    if (!options.replace && await fileExists(options.baselinePath)) {
      throw new Error("Baseline exists; pass --replace only after a verified keep decision");
    }
    await mkdir(path.dirname(options.baselinePath), { recursive: true });
    await writeFile(options.baselinePath, `${JSON.stringify(snapshot, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({
      mode: "record",
      baselinePath: options.baselinePath,
      commit: snapshot.commit,
      scenarios: snapshot.results.length,
    }, null, 2));
    return;
  }
  const baseline = JSON.parse(await readFile(options.baselinePath, "utf8")) as V2ResearchSnapshot;
  const selectedIds = new Set(scenarios.map(scenario => scenario.id));
  const selectedBaseline: V2ResearchSnapshot = options.scenarioIds.length === 0
    ? baseline
    : {
        ...baseline,
        results: baseline.results.filter(result => selectedIds.has(result.scenario.id)),
      };
  const comparison = compareV2ResearchSnapshots(selectedBaseline, snapshot);
  console.log(JSON.stringify({
    mode: "compare",
    baselinePath: options.baselinePath,
    baselineCommit: baseline.commit,
    candidateCommit: snapshot.commit,
    ...comparison,
  }, null, 2));
  if (comparison.decision === "reject") process.exitCode = 1;
  if (options.requireImprovement && comparison.decision !== "keep") process.exitCode = 2;
}

if (import.meta.main) {
  await main();
}
