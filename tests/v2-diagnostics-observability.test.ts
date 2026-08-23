import { expect, it } from "bun:test";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { observabilityEvent } from "../src/observability/events";

it("accepts only explicit safe V2 diagnostic fields", () => {
  const event=observabilityEvent("transport.committed",{trace_id:"t",project_id:"p",transport:"v2",queue_wait_ms:1,commit_ack_ms:2,retry_count:0,host_prepare_ms:3,host_publish_to_ack_ms:4,sidecar_claim_sync_ms:1,transport_commit_residual_ms:3});
  expect(()=>assertSafeObservabilityEvent(event)).not.toThrow();
  expect(JSON.stringify(event)).not.toMatch(/prompt|output|cookie|token|secret/i);
});
