import { describe, expect, it } from "bun:test";
import {
  requiredLiveRuntimes,
  resolvePersistedContinuationLiveOptions,
} from "../scripts/verify_persisted_continuation";

const PROJECTS = [
  "00000000-0000-4000-8000-000000000001",
  "00000000-0000-4000-8000-000000000002",
];

function env(overrides: Record<string, string | undefined> = {}) {
  return {
    GP_PERSISTED_CONTINUATION_LIVE: "1",
    GP_CONTINUATION_TEST_CONFIRM: "dedicated-test-projects",
    GP_CONTINUATION_TEST_PROJECT_IDS: PROJECTS.join(","),
    GP_CONTINUATION_TEST_COOKIE_FILE: "D:/secrets/test-prism-cookies.json",
    ...overrides,
  };
}

describe("persisted continuation live verifier guards", () => {
  it("requires explicit paid-live and dedicated-project guards", () => {
    expect(() => resolvePersistedContinuationLiveOptions(env({
      GP_PERSISTED_CONTINUATION_LIVE: undefined,
    }))).toThrow("GP_PERSISTED_CONTINUATION_LIVE=1");
    expect(() => resolvePersistedContinuationLiveOptions(env({
      GP_CONTINUATION_TEST_CONFIRM: "production",
    }))).toThrow("dedicated-test-projects");
    expect(() => resolvePersistedContinuationLiveOptions(env({
      GP_AUTO_CREATE_PROJECTS: "1",
    }))).toThrow("never creates projects");
  });

  it("accepts only 2-8 unique dedicated UUIDs and a credential source", () => {
    for (const value of [
      PROJECTS[0]!,
      `${PROJECTS[0]},${PROJECTS[0]}`,
      `${PROJECTS[0]},${PROJECTS[0]!.toUpperCase()}`,
      `${PROJECTS[0]},not-a-uuid`,
      Array.from({ length: 9 }, (_, index) =>
        `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`).join(","),
    ]) {
      expect(() => resolvePersistedContinuationLiveOptions(env({
        GP_CONTINUATION_TEST_PROJECT_IDS: value,
      }))).toThrow("2-8 unique dedicated UUIDs");
    }
    expect(() => resolvePersistedContinuationLiveOptions(env({
      GP_CONTINUATION_TEST_COOKIE_FILE: undefined,
    }))).toThrow("GP_CONTINUATION_TEST_COOKIE_FILE");
  });

  it("returns an immutable dedicated-ID list and selected model", () => {
    const resolved = resolvePersistedContinuationLiveOptions(env({
      GP_LIVE_MODEL: "gpt-5.6-terra",
      PORT: "18787",
    }));
    expect(resolved).toEqual({
      projectIds: PROJECTS,
      model: "gpt-5.6-terra",
    });
    expect(Object.isFrozen(resolved.projectIds)).toBe(true);
  });

  it("bounds readiness while tolerating at most one unavailable project", () => {
    expect(requiredLiveRuntimes(2)).toBe(2);
    expect(requiredLiveRuntimes(3)).toBe(2);
    expect(requiredLiveRuntimes(8)).toBe(7);
    for (const value of [1, 9, 2.5]) {
      expect(() => requiredLiveRuntimes(value)).toThrow(
        "project count must be between 2 and 8",
      );
    }
  });
});
