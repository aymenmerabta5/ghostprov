import { main, CapacityProofFailure, type CapacityProofHooks } from "./live_capacity_reliability";
import { canonicalJson, sha256Hex, type AckCursor } from "../src/v2/protocol/shared";
import { parseFrames } from "../src/relay/sse";

type Context = Parameters<NonNullable<CapacityProofHooks["afterWorkload"]>>[0];
interface Fault {
  readonly kind: "incoming_data" | "return_ack";
  readonly lane: "data" | "control";
  readonly baseline: AckCursor;
  phase: "armed" | "blocking" | "reconnected";
  started: number | null;
  reconnected: number | null;
  affected: number;
  reconnects: number;
  pendingAgeAtReconnect: number | null;
}
const record = (value: unknown): Record<string, unknown> | null =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
function fail(code: string): never { throw new CapacityProofFailure(code); }

/** Explicitly injected packet faults; no request body or execution identity is changed. */
export function createCapacityFaultHooks(): CapacityProofHooks {
  let fault: Fault | null = null;
  let current: Context | null = null;
  let lastControlAck: AckCursor = { epoch: 1, sequence: -1 };
  const activate = () => {
    if (fault?.phase !== "armed" || !current) return;
    const snapshot = current.system.registry.snapshots()[0];
    const host = snapshot?.diagnostics?.host;
    if (!host || host.windowFrames !== 0 || host.schedulerFrames !== 0) return;
    fault.phase = "blocking"; fault.started = performance.now();
    current.record({ event: "capacity.fault.started", kind: fault.kind, hostWindowFrames: 0 });
  };

  return {
    onChannel(channel, context) {
      const subscribe = channel.subscribe.bind(channel), publish = channel.publish.bind(channel);
      const reconnect = channel.reconnect.bind(channel);
      channel.subscribe = listener => subscribe(update => {
        activate();
        if (fault?.phase === "blocking" && fault.kind === "incoming_data" && context.lane === "data") {
          fault.affected++; return;
        }
        listener(update);
      });
      channel.publish = envelope => {
        activate();
        let outgoing = envelope;
        if (context.lane === "control") {
          if (fault?.phase === "blocking" && fault.kind === "return_ack" &&
            envelope.ack.epoch === fault.baseline.epoch && envelope.ack.sequence > fault.baseline.sequence) {
            // Deliberately retain an older valid cursor. Never fabricate an ACK
            // for unprocessed data; fresh pings and credits still cross the lane.
            outgoing = { ...envelope, ack: fault.baseline }; fault.affected++;
          }
          lastControlAck = { ...outgoing.ack };
        }
        return publish(outgoing);
      };
      channel.reconnect = async (lane, signal) => {
        if (fault?.phase === "blocking" && context.lane === fault.lane && lane === fault.lane) {
          const host = current?.system.registry.snapshots()[0]?.diagnostics?.host;
          fault.reconnects++;
          fault.pendingAgeAtReconnect = host?.sidecarPendingAgeMs ?? null;
          fault.reconnected = performance.now(); fault.phase = "reconnected";
          current?.record({ event: "capacity.fault.reconnect", kind: fault.kind,
            blockedMs: fault.started === null ? null : fault.reconnected - fault.started,
            affected: fault.affected, pendingAgeMs: fault.pendingAgeAtReconnect });
        }
        return reconnect(lane, signal);
      };
    },
    async afterWorkload(context) {
      current = context;
      const outcomes: Record<string, unknown>[] = [];
      try {
        for (const slot of [0, 1] as const) {
          const initial = context.system.registry.snapshots()[0];
          if (!initial || initial.active !== 0 || initial.quarantined !== 0 || (initial.ambiguousQuarantined ?? 0) !== 0) fail("fault_runtime_not_idle");
          const selected: Fault = { kind: slot === 0 ? "incoming_data" : "return_ack", lane: slot === 0 ? "data" : "control",
            baseline: { ...lastControlAck }, phase: "armed", started: null, reconnected: null,
            affected: 0, reconnects: 0, pendingAgeAtReconnect: null };
          const marker = `CAPACITY_FAULT_${slot}_COMPLETE`;
          const body = { model: "gpt-6-astra", stream: true, store: true, service_tier: "fast",
            reasoning: { effort: "xhigh", summary: "auto" }, max_output_tokens: 8192,
            input: `Write approximately 1200 words explaining how reliable queues preserve ordering and avoid duplicate execution. End with exactly ${marker}.` };
          const digest = await sha256Hex(canonicalJson(body));
          context.beginRequest(digest, slot);
          const deadline = AbortSignal.timeout(210_000);
          const signal = AbortSignal.any([context.signal, deadline]);
          const started = performance.now();
          let bytes = 0, events = 0, terminalId: string | null = null, terminalCount = 0;
          let armed = false;
          const response = await fetch(`${context.baseUrl}/v1/responses`, { method: "POST", signal,
            headers: { authorization: `Bearer ${context.apiKey}`, "content-type": "application/json" }, body: JSON.stringify(body) });
          if (!response.ok || !response.body) { await response.body?.cancel(); fail("fault_public_request_failed"); }
          const reader = response.body.getReader(), decoder = new TextDecoder("utf-8", { fatal: true });
          let buffer = "";
          try {
            for (;;) {
              const next = await reader.read(); if (next.done) break;
              bytes += next.value.byteLength;
              if (bytes > 8 * 1024 * 1024) fail("fault_response_byte_bound");
              buffer += decoder.decode(next.value, { stream: true });
              if (buffer.length > 2 * 1024 * 1024) fail("fault_response_frame_bound");
              const parsed = parseFrames(buffer, 0); buffer = buffer.slice(parsed.nextOffset);
              for (const frame of parsed.frames) {
                if (++events > 32768) fail("fault_response_event_bound");
                if (frame.data === "[DONE]") continue;
                const event = record(JSON.parse(frame.data));
                if (!event || typeof event.type !== "string") fail("fault_response_invalid");
                if (!armed && event.type.startsWith("response.")) {
                  // Wait until public commitment, then affect only the response
                  // transport. The original execution can never be resubmitted.
                  armed = true; fault = selected; activate();
                }
                if (["error", "response.failed", "response.incomplete"].includes(event.type)) fail("fault_response_failed");
                if (event.type !== "response.completed") continue;
                if (++terminalCount !== 1) fail("fault_duplicate_terminal");
                const final = record(event.response);
                if (!final || final.status !== "completed" || typeof final.id !== "string" || !final.id || final.id.length > 256 || !Array.isArray(final.output)) fail("fault_invalid_terminal");
                let tail = "", chars = 0;
                for (const item of final.output) {
                  const parts = record(item)?.content; if (!Array.isArray(parts)) continue;
                  for (const part of parts) {
                    const text = record(part); if (text?.type !== "output_text" || typeof text.text !== "string") continue;
                    chars += text.text.length; if (chars > 128 * 1024) fail("fault_output_bound");
                    tail = (tail + text.text).slice(-128);
                  }
                }
                if (!tail.trimEnd().endsWith(marker)) fail("fault_terminal_marker_mismatch");
                terminalId = final.id;
              }
            }
            if (decoder.decode().length || buffer.trim()) fail("fault_incomplete_sse");
          } finally { fault = null; await reader.cancel().catch(() => {}); reader.releaseLock(); }
          if (!terminalId || terminalCount !== 1 || selected.affected === 0 || selected.reconnects !== 1 ||
            selected.started === null || selected.reconnected === null ||
            selected.pendingAgeAtReconnect === null || selected.pendingAgeAtReconnect < 50_000) fail("fault_recovery_not_proved");
          context.completeRequest(digest, terminalId);
          const until = performance.now() + 20_000;
          for (;;) {
            const latest = context.system.registry.snapshots()[0];
            if (!latest || latest.incarnation !== initial.incarnation || latest.quarantined !== 0 || (latest.ambiguousQuarantined ?? 0) !== 0) fail("fault_changed_runtime_or_quarantined");
            if (latest.active === 0 && latest.available > 0) break;
            if (signal.aborted || performance.now() >= until) fail("fault_recovery_not_idle");
            await Bun.sleep(100);
          }
          const outcome = { kind: selected.kind, completed: true, bytes, events, totalMs: performance.now() - started,
            blockedMs: selected.reconnected - selected.started, affected: selected.affected,
            reconnects: selected.reconnects, pendingAgeMs: selected.pendingAgeAtReconnect, sameIncarnation: true };
          outcomes.push(outcome); context.record({ event: "capacity.fault.completed", ...outcome });
        }
        return { completed: outcomes.length, outcomes };
      } finally { fault = null; current = null; }
    },
  };
}

if (import.meta.main) await main(process.env, createCapacityFaultHooks());
