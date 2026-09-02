import { describe, expect, it } from "bun:test";
import { Buffer } from "node:buffer";
import {
  buildSandboxSearchLauncher,
  buildSandboxSearchPython,
  createStandaloneSearchRequest,
  deleteExactCreatedProject,
  nativeSandboxSearchUpstreamRequest,
  parseStandaloneSearchProbeMarkers,
  resolveStandaloneSearchProbeOptions,
  type StandaloneSearchProbeMetadata,
} from "../scripts/probe_standalone_search";
import { PrismProjectError } from "../src/prism/projects-client";
import { projectId, type ProjectId } from "../src/sandbox/types";

const COOKIE_FILE = "C:\\private\\prism-cookie.json";
const PROJECT = projectId("11111111-1111-4111-8111-111111111111");

function guardedEnv(overrides: Record<string, string | undefined> = {}) {
  return {
    GP_LIVE_STANDALONE_SEARCH: "1",
    GP_LIVE_CREATE_TEST_PROJECTS: "1",
    GP_LIVE_SANDBOX_PROBE: "1",
    GP_ALLOW_PROJECT_DELETE: "1",
    GP_LIVE_PROJECT_COUNT: "1",
    PRISM_COOKIE_FILE: COOKIE_FILE,
    ...overrides,
  };
}

function marker(metadata: Record<string, unknown>): string {
  const encoded = Buffer.from(JSON.stringify(metadata), "utf8").toString("base64");
  const chunks = encoded.match(/.{1,40}/g) ?? [];
  return [
    ...chunks.map((chunk, index) => `GPSEARCHPART:${index + 1}:${chunks.length}:${chunk}`),
    `GPSEARCHDONE:${chunks.length}`,
  ].join("\n");
}

describe("standalone search live-probe guards", () => {
  it("requires independent create, execution, and deletion opt-ins", () => {
    for (const name of [
      "GP_LIVE_STANDALONE_SEARCH",
      "GP_LIVE_CREATE_TEST_PROJECTS",
      "GP_LIVE_SANDBOX_PROBE",
      "GP_ALLOW_PROJECT_DELETE",
    ]) {
      expect(() => resolveStandaloneSearchProbeOptions(guardedEnv({ [name]: undefined })))
        .toThrow(name);
    }
  });

  it("hard-fences the probe to one project and a credential file", () => {
    expect(() => resolveStandaloneSearchProbeOptions(guardedEnv({
      GP_LIVE_PROJECT_COUNT: "2",
    }))).toThrow("exactly 1");
    expect(() => resolveStandaloneSearchProbeOptions(guardedEnv({
      PRISM_COOKIE_FILE: undefined,
    }))).toThrow("PRISM_COOKIE_FILE");
    expect(resolveStandaloneSearchProbeOptions(guardedEnv())).toMatchObject({
      prismOrigin: "https://prism.openai.com",
      timeoutMs: 120_000,
    });
  });

  it("rejects origins carrying credentials, paths, queries, or fragments", () => {
    for (const origin of [
      "https://user:pass@prism.openai.com",
      "https://prism.openai.com/path",
      "https://prism.openai.com?x=1",
      "https://prism.openai.com#fragment",
    ]) {
      expect(() => resolveStandaloneSearchProbeOptions(guardedEnv({ PRISM_ORIGIN: origin })))
        .toThrow("credential-free");
    }
  });
});

describe("standalone search request", () => {
  it("builds one tiny fixed benign standalone command and native Responses adapter body", () => {
    const request = createStandaloneSearchRequest("11111111-1111-4111-8111-111111111111");
    expect(request).toEqual({
      id: "11111111-1111-4111-8111-111111111111",
      model: "gpt-5.6-sol",
      input: [{
        type: "message",
        role: "user",
        content: [{
          type: "input_text",
          text: "Find the official OpenAI API documentation page.",
        }],
      }],
      commands: {
        search_query: [{
          q: "OpenAI API documentation",
          domains: ["developers.openai.com"],
        }],
        response_length: "short",
      },
      settings: {
        search_context_size: "low",
        allowed_callers: ["direct"],
        external_web_access: true,
      },
      max_output_tokens: 10_000,
    });
    expect(request).not.toHaveProperty("stream");
    expect(request).not.toHaveProperty("store");
    const upstream = nativeSandboxSearchUpstreamRequest(request);
    expect(upstream).not.toHaveProperty("id");
    expect(upstream).not.toHaveProperty("commands");
    expect(upstream).not.toHaveProperty("settings");
    expect(upstream).toMatchObject({
      model: "gpt-5.6-sol",
      stream: true,
      store: true,
      tool_choice: "required",
      tools: [{ type: "web_search", external_web_access: true }],
    });
  });

  it("embeds only the fixed request and reads credentials inside the sandbox", () => {
    const request = createStandaloneSearchRequest("probe-id");
    const python = buildSandboxSearchPython(request);
    const launcher = buildSandboxSearchLauncher(python);
    expect(python).toContain('os.environ.get("OPENAI_BASE_URL"');
    expect(python).toContain('os.environ.get("OPENAI_API_KEY"');
    expect(python).toContain('base.rstrip("/") + "/responses"');
    expect(python).toContain("class NoRedirect");
    expect(python).toContain("with opener.open(request");
    expect(python).not.toContain("urllib.request.urlopen");
    expect(python).not.toContain("18787");
    expect(launcher).not.toContain("|| python");
    expect(launcher).not.toContain("deleteManaged");
    expect(launcher).not.toMatch(/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i);
  });
});

describe("standalone search metadata markers", () => {
  const metadata = {
    version: 1,
    ok: true,
    outcome: "ok",
    status: 200,
    responseBytes: 321,
    jsonObject: true,
    outputChars: 42,
    resultsCount: 1,
    encryptedOutputPresent: true,
    unexpectedFieldCount: 0,
    errorType: null,
    errorCode: null,
    errorParam: null,
    errorClassification: null,
    webSearchSeen: true,
    terminal: "response.completed",
  } satisfies StandaloneSearchProbeMetadata;

  it("reassembles small indexed markers and tolerates identical repeated render output", () => {
    const encoded = marker(metadata);
    expect(parseStandaloneSearchProbeMarkers(`${encoded}\n${encoded}`)).toEqual(metadata);
  });

  it("rejects incomplete, conflicting, and oversized metadata", () => {
    expect(parseStandaloneSearchProbeMarkers("GPSEARCHDONE:1")).toBeNull();
    expect(parseStandaloneSearchProbeMarkers(
      "GPSEARCHPART:1:1:e30=\nGPSEARCHPART:1:1:e31=\nGPSEARCHDONE:1",
    )).toBeNull();
    expect(parseStandaloneSearchProbeMarkers("x".repeat(512 * 1024 + 1))).toBeNull();
  });

  it("drops response content and returns only allowlisted metadata", () => {
    expect(parseStandaloneSearchProbeMarkers(marker({
      ...metadata,
      output: "raw response must not pass",
    }))).toEqual(metadata);
  });

  it("retains only bounded error discriminators and a fixed classification", () => {
    const errorMetadata: StandaloneSearchProbeMetadata = {
      ...metadata,
      ok: false,
      outcome: "http_error",
      status: 400,
      jsonObject: false,
      outputChars: null,
      resultsCount: null,
      encryptedOutputPresent: null,
      unexpectedFieldCount: null,
      errorType: "invalid_request_error",
      errorCode: "model_not_found",
      errorParam: "model",
      errorClassification: "model_unsupported",
      webSearchSeen: false,
      terminal: null,
    };
    expect(parseStandaloneSearchProbeMarkers(marker({
      ...errorMetadata,
      message: "raw provider message must be dropped",
      body: "raw provider body must be dropped",
    }))).toEqual(errorMetadata);
  });
});

describe("exact disposable-project cleanup", () => {
  it("deletes exactly the freshly generated ID once", async () => {
    const deleted: ProjectId[] = [];
    const result = await deleteExactCreatedProject({
      async deleteManaged(id) { deleted.push(id); },
    }, PROJECT, true);
    expect(result).toBe("deleted");
    expect(deleted).toEqual([PROJECT]);
  });

  it("does nothing before a creation attempt and treats 404 as already absent", async () => {
    let calls = 0;
    const projects = {
      async deleteManaged() {
        calls++;
        throw new PrismProjectError("absent", "http", 404);
      },
    };
    expect(await deleteExactCreatedProject(projects, PROJECT, false)).toBe("not_needed");
    expect(calls).toBe(0);
    expect(await deleteExactCreatedProject(projects, PROJECT, true)).toBe("absent");
    expect(calls).toBe(1);
  });
});
