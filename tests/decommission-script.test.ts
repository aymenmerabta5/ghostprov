import { describe, expect, it } from "bun:test";
import {
  parseDecommissionArguments,
} from "../scripts/decommission_v2_fleet";
import { projectId } from "../src/sandbox/types";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const DIGEST = "a".repeat(64);

describe("V2 decommission CLI guards", () => {
  it("defaults to preview-only for explicit repeated project UUIDs", () => {
    expect(parseDecommissionArguments([
      "--project", A,
      "--project", A,
    ], {})).toEqual({
      projects: [A],
      confirm: null,
      execute: false,
    });
  });

  it("requires the destructive opt-in and exact digest shape for execution", () => {
    expect(() => parseDecommissionArguments([
      "--project", A,
      "--confirm", DIGEST,
    ], {})).toThrow("GP_ALLOW_PROJECT_DELETE=1");
    expect(() => parseDecommissionArguments([
      "--project", A,
      "--confirm", "bad",
    ], { GP_ALLOW_PROJECT_DELETE: "1" })).toThrow("64-character");
    expect(parseDecommissionArguments([
      "--project", A,
      "--confirm", DIGEST,
    ], { GP_ALLOW_PROJECT_DELETE: "1" })).toMatchObject({
      execute: true,
      confirm: DIGEST,
    });
  });

  it.each(["all", "*", "GhostProv Worker 1", "../fleet"])(
    "rejects ambiguous target %s",
    target => {
      expect(() => parseDecommissionArguments([
        "--project", target,
      ], {})).toThrow();
    },
  );

  it("rejects missing targets and unknown flags", () => {
    expect(() => parseDecommissionArguments([], {})).toThrow("--project");
    expect(() => parseDecommissionArguments(["--all"], {})).toThrow("Unknown argument");
  });
});
