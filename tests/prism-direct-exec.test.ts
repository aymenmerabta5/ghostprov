import { describe, expect, it } from "bun:test";
import {
  DIRECT_SUPERVISOR_EXEC_RESPONSE_BYTES,
  directSupervisorExecBody,
  parseDirectSupervisorExecStarted,
} from "../src/prism/direct-exec";
import { SandboxGateway } from "../src/sandbox/gateway";
import { parseProvisionedSandbox } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";

const SESSION = "11111111-1111-4111-8111-111111111111";
const OTHER = "22222222-2222-4222-8222-222222222222";
const ack = (sessionId = SESSION) => ({ status: "started", session_id: sessionId,
  codex_workspace: `/codex_workspace/${sessionId}`, job_id: "job-1", runner_kind: "subprocess",
  direct_mode_active: true, effective_model: "gpt-5.6-sol", runner_pid: 123 });

describe("Prism direct supervisor execution protocol", () => {
  it("uses the requested prompt and the shell executable without response lineage", () => {
    expect(directSupervisorExecBody({ sessionId: SESSION })).toEqual({
      prompt: "Hiii", model: "gpt-5.6-sol", codex_path: "/bin/sh", session_id: SESSION,
      timeout_s: 60, image_generation: false,
    });
    expect(parseDirectSupervisorExecStarted(ack(), SESSION)).toEqual({
      sessionId: SESSION, workspaceRoot: `/codex_workspace/${SESSION}`, jobId: "job-1",
    });
  });

  it("rejects malformed execution identities before constructing a request", () => {
    for (const sessionId of ["", "short", `${SESSION}\n`, `${SESSION}/suffix`, `prefix${SESSION}`, SESSION.toUpperCase().replace("8111", "A111")]) {
      expect(() => directSupervisorExecBody({ sessionId })).toThrow();
    }
  });

  it("fails closed on mismatched sessions, workspaces, execution modes, and job identities", () => {
    for (const value of [null, [], "started", {},
      { ...ack(), session_id: OTHER }, { ...ack(), codex_workspace: `/codex_workspace/${OTHER}` },
      { ...ack(), codex_workspace: `/codex_workspace/${SESSION}\n` },
      { ...ack(), codex_workspace: `/codex_workspace/${SESSION}/extra` },
      { ...ack(), status: "completed" }, { ...ack(), runner_kind: "agent" },
      { ...ack(), direct_mode_active: false }, { ...ack(), effective_model: "other" },
      { ...ack(), job_id: "" }, { ...ack(), job_id: "job\n" }, { ...ack(), job_id: "x".repeat(257) },
    ]) expect(() => parseDirectSupervisorExecStarted(value, SESSION)).toThrow();
  });
});

function gatewayFixture(handler: (init: RequestInit) => Response | Promise<Response>, timeoutMs = 1_000) {
  const calls: Array<{ path: string; init: RequestInit }> = [];
  const gateway = new SandboxGateway({ projectId: projectId(SESSION),
    sandbox: parseProvisionedSandbox({ url: "https://offline.invalid/sandbox/", token: "private-sandbox-token" }),
    requestTimeoutMs: timeoutMs,
    account: { async request(path, init = {}) { calls.push({ path, init }); return handler(init); } },
  });
  return { gateway, calls };
}

describe("SandboxGateway direct supervisor submission", () => {
  it("sends one scoped request and keeps an immutable request identity while awaiting its result", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const f = gatewayFixture(async () => { entered.resolve(); await release.promise; return Response.json(ack()); });
    const request = { sessionId: SESSION };
    const operation = f.gateway.startSupervisorExec(request);
    await entered.promise;
    request.sessionId = OTHER;
    release.resolve();
    await expect(operation).resolves.toMatchObject({ sessionId: SESSION });
    expect(f.calls).toHaveLength(1);
    expect(new URL(f.calls[0]!.path).pathname).toBe("/sandbox/codex/codex_exec_async");
    expect(new Headers(f.calls[0]!.init.headers).get("x-crixet-sandbox-token")).toBe("private-sandbox-token");
    expect(JSON.parse(String(f.calls[0]!.init.body))).toMatchObject({ prompt: "Hiii", session_id: SESSION, codex_path: "/bin/sh" });
  });

  it("does not replay rejected, malformed, ambiguous, or timed-out submissions", async () => {
    for (const handler of [
      () => Response.json({ error: "private-provider-token" }, { status: 429 }),
      () => new Response("private-provider-token", { status: 200 }),
      () => Response.json(ack(OTHER)),
      () => Promise.reject(new Error("network disconnected")),
      () => new Promise<Response>(() => undefined),
    ]) {
      const f = gatewayFixture(handler, 15);
      const error = await f.gateway.startSupervisorExec({ sessionId: SESSION }).catch(error => error);
      expect(error).toBeInstanceOf(Error);
      expect(error.message).not.toContain("private-provider-token");
      expect(f.calls).toHaveLength(1);
      expect(f.calls.some(call => call.path.includes("/api/llm/"))).toBe(false);
    }
  });

  it("caps acknowledgement bytes and sends nothing for a cancelled or invalid request", async () => {
    const f = gatewayFixture(() => Response.json({ ...ack(), extra: "x".repeat(DIRECT_SUPERVISOR_EXEC_RESPONSE_BYTES) }));
    await expect(f.gateway.startSupervisorExec({ sessionId: SESSION })).rejects.toMatchObject({ status: 502 });
    expect(f.calls).toHaveLength(1);
    const stopped = gatewayFixture(() => { throw new Error("must not call"); });
    const abort = new AbortController(); abort.abort();
    await expect(stopped.gateway.startSupervisorExec({ sessionId: SESSION }, abort.signal)).rejects.toMatchObject({ name: "AbortError" });
    await expect(stopped.gateway.startSupervisorExec({ sessionId: `${SESSION}\n` })).rejects.toThrow();
    expect(stopped.calls).toHaveLength(0);
  });
});
