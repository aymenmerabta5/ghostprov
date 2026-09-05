import { diagnosticLog, type DiagnosticLog } from "../../../sandbox/v2/diagnostic-log.ts";
import { SidecarDiagnosticsObserver as BaseObserver } from "./sidecar-observer";
import type { V2TransportDiagnostics } from "./types";

/** Local timing collection is independent of optional wire diagnostic fields. */
export class SidecarDiagnosticsObserver extends BaseObserver {
  readonly #publicEnabled: boolean;
  readonly #now: () => number;
  readonly #log: DiagnosticLog;
  #lastSample = -Infinity;

  constructor(options: ConstructorParameters<typeof BaseObserver>[0] & { log?: DiagnosticLog }) {
    super({ ...options, enabled: true });
    this.#publicEnabled = options.enabled;
    this.#now = options.now ?? (() => performance.now());
    this.#log = options.log ?? diagnosticLog("sidecar");
  }

  override get enabled(): boolean { return this.#publicEnabled; }

  #phase(id: string, phase: string, fields: Record<string, unknown> = {}): void {
    this.#log.record("request_phase", { request: this.#log.ordinal(id), phase, ...fields });
  }

  override requestOpened(id: string): void { super.requestOpened(id); this.#phase(id, "requestOpened"); }
  override commitReceived(id: string): void { super.commitReceived(id); this.#phase(id, "commitReceived"); }
  override assemblyCompleted(id: string): void { super.assemblyCompleted(id); this.#phase(id, "assemblyCompleted"); }
  override claimCompleted(id: string, ms: number): void { super.claimCompleted(id, ms); this.#phase(id, "claimCompleted", { duration_ms: ms }); }
  override admissionCompleted(id: string, ms: number): void { super.admissionCompleted(id, ms); this.#phase(id, "admissionCompleted", { duration_ms: ms }); }
  override prepared(id: string): void { super.prepared(id); this.#phase(id, "prepared"); }
  override fetchStarted(id: string, syncMs = 0): void { super.fetchStarted(id, syncMs); this.#phase(id, "fetchStarted", { duration_ms: syncMs }); }

  override upstreamEvent(id: string, index: number) {
    const result = super.upstreamEvent(id, index);
    if (result) this.#log.record("request_metrics", { request: this.#log.ordinal(id), phase: "upstreamEvent", ...result });
    return this.#publicEnabled ? result : undefined;
  }

  override requestAck(id: string) {
    const result = super.requestAck(id);
    if (result) this.#log.record("request_metrics", { request: this.#log.ordinal(id), phase: "requestAck", ...result }, true);
    return this.#publicEnabled ? result : undefined;
  }

  override requestTerminal(id: string) {
    const result = super.requestTerminal(id);
    if (result) this.#log.record("request_metrics", { request: this.#log.ordinal(id), phase: "requestTerminal", ...result }, true);
    return this.#publicEnabled ? result : undefined;
  }

  override transport(value: Partial<V2TransportDiagnostics>): void {
    super.transport(value);
    const now = this.#now();
    if (now - this.#lastSample < 1000) return;
    this.#lastSample = now;
    const envelope = super.envelope();
    this.#log.record("resource_sample", { ...(envelope.resource ?? {}), malformedCount: envelope.malformedCount });
    this.#log.record("transport_sample", { ...envelope.transport });
  }

  override removeRequest(id: string): void {
    super.removeRequest(id);
    this.#phase(id, "removeRequest");
    this.#log.forget(id);
  }
}
