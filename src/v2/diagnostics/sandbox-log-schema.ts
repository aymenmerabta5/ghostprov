export const SANDBOX_LOG_SEGMENT_BYTES = 128 * 1024;
export const SANDBOX_LOG_RESERVED_BYTES = 8 * 1024;
export const SANDBOX_LOG_SEGMENT_RECORDS = 1024;
const PHASES = new Set([
  'config','environment','ownership','bootstrap','storage','engine_start','awareness_connect','runtime','shutdown',
  'credential_wait','credential_observed','credential_accepted','credential_rejected_binding','credential_rejected_key',
  'credential_rejected_ciphertext','credential_rejected_payload','credential_rejected_unknown','ready',
  'validate','claim','stage','spawn','identity','hello_wait','hello_ready','hello_timeout',
  'requestOpened','commitReceived','assemblyCompleted','claimCompleted','admissionCompleted','prepared','fetchStarted',
  'upstreamEvent','requestAck','requestTerminal','transport','removeRequest','credential','launch','stop','rotate',
  'before_fetch','fetch','headers','parse','deliver','terminal',
]);
export const SANDBOX_LOG_EVENTS = new Set(['process_start','phase','supervisor_phase','launch_phase','request_phase','request_metrics',
  'upstream_start','upstream_headers','upstream_event','event_delivery','upstream_chunk','upstream_terminal',
  'upstream_error','transport_sample','resource_sample','fatal','logger_stats']);
const ERRORS = new Set(['Error','TypeError','RangeError','NotCapable','AbortError','TimeoutError','SupervisorLifecycleError',
  'SupervisorCommandError','SidecarLifecycleError','SidecarBootstrapCryptoError','SidecarOwnershipError',
  'UpstreamExecutionError','V2ExecutionFenceError','SidecarJournalError','ConnectionRefused','BadResource']);
const CODES = new Set(['fatal','other','timeout','closed','connect','disconnected','auth','transport','before_fetch',
  'invalid_body','missing_body','content_type','prelude','event_too_large','missing_terminal','binding','key','ciphertext',
  'payload','identity_invalid','spawn_failed','hello_timeout','not_ready','owner','ownership_lost','instance_mismatch',
  'stream','http','sse','rate_limit','forced_drain','journal_error','write','open']);
const SSE = new Set(['response.created','response.in_progress','response.output_item.added','response.output_item.done',
  'response.content_part.added','response.content_part.done','response.output_text.delta','response.output_text.done',
  'response.reasoning_summary_part.added','response.reasoning_summary_part.done','response.reasoning_summary_text.delta',
  'response.reasoning_summary_text.done','response.function_call_arguments.delta','response.function_call_arguments.done',
  'response.custom_tool_call_input.delta','response.custom_tool_call_input.done','response.refusal.delta',
  'response.refusal.done','response.completed','response.incomplete','response.failed','error']);
const NUMBERS = new Set(['request','duration_ms','since_start_ms','event_chars','chunk_bytes','gap_ms','event_index',
  'status','bytes','events','input_tokens','output_tokens','cached_tokens','active','queued','pid','child_pid',
  'claimSyncMs','admissionWaitMs','firstFrameToCommitMs','commitToAssemblyMs','commitProcessingMs',
  'preparedToFetchStartMs','fetchStartSyncMs','upstreamFirstEventMs','schedulerWaitMs','upstreamTotalMs',
  'schedulerPeakFrames','schedulerPeakBytes','ackProgressAgePeakMs','windowFrames','windowBytes','schedulerFrames',
  'schedulerBytes','schedulerStreams','oldestQueuedFrameMs','ackProgressAgeMs','retransmissionAttempt',
  'peakRetransmissionAttempt','retransmissionsTotal','publicationsTotal','publishedBytesTotal','admittedUncommitted',
  'roundTripEwmaMs','pacedPublicationsTotal','urgentPublicationsTotal','logicalEventsTotal','responseBatchFramesTotal',
  'batchedEventsTotal','dataPublicationsTotal','controlPublicationsTotal','dataPublishedBytesTotal',
  'controlPublishedBytesTotal','flightCompletionsTotal','adaptiveCapacity','capacityIncreasesTotal','capacityReductionsTotal',
  'unadmittedTimeoutsTotal','rssBytes','heapUsedBytes','heapTotalBytes','externalBytes','arrayBuffersBytes','cpuPercent',
  'records','dropped','write_ms','format_ms','malformedCount','sampledAtMs','eventLoopP50Ms','eventLoopP95Ms',
  'eventLoopMaxMs','gcSamples','gcDurationP95Ms','gcDurationMaxMs','roundTripVariationMs',
  'acknowledgedGoodputBytesPerSecond','uniqueAcknowledgedBytesTotal','retransmittedBytesTotal','stallEventsTotal',
  'first_event_ms','first_text_ms','first_tool_ms','tool_ready_ms','fetch_to_terminal_ms','event_gap_max_ms','event_delivery_max_ms','event_delivery_total_ms','chunks','sampled_events','processRssBytes','arrayBufferBytes','systemTotalBytes','systemFreeBytes','processCpuPercent','normalizedLoad1',
  'eventLoopSamples','record_seq','rotations','retained_bytes']);
const NULLABLE = new Set(['first_event_ms','first_text_ms','first_tool_ms','tool_ready_ms','fetch_to_terminal_ms','arrayBufferBytes','systemTotalBytes','systemFreeBytes','processCpuPercent','normalizedLoad1',
  'roundTripEwmaMs','roundTripVariationMs','acknowledgedGoodputBytesPerSecond']);

export function sanitizeDiagnosticFields(fields: Record<string, unknown>): Record<string, string | number | boolean | null> {
  const safe: Record<string, string | number | boolean | null> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (NUMBERS.has(key) && typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER) safe[key] = value;
    else if (NULLABLE.has(key) && value === null) safe[key] = null;
    else if (key === 'phase' && typeof value === 'string') safe[key] = PHASES.has(value) ? value : 'other';
    else if (key === 'event_type' && typeof value === 'string') safe[key] = SSE.has(value) ? value : 'other';
    else if (key === 'error_name' && typeof value === 'string') safe[key] = ERRORS.has(value) ? value : 'other';
    else if (key === 'error_code' && typeof value === 'string') safe[key] = CODES.has(value) ? value : 'other';
    else if (key === 'permission' && typeof value === 'string' && ['read','write','env','net','sys','run','ffi'].includes(value)) safe[key] = value;
    else if (['first_event','first_text','first_tool','tool_ready','ok'].includes(key) && typeof value === 'boolean') safe[key] = value;
  }
  return safe;
}

export function parseDiagnosticLine(line: string): Record<string, unknown> {
  if (line.length > 2048) throw new Error('Diagnostic line exceeds its bound');
  const value = JSON.parse(line);
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid diagnostic record');
  const { schema, version, role, ts, mono_ms, event, ...fields } = value;
  if (schema !== 'ghostprov-sandbox-diagnostic' || version !== 1 || !['sidecar','supervisor'].includes(role) ||
    typeof ts !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(ts) ||
    typeof mono_ms !== 'number' || !Number.isFinite(mono_ms) || mono_ms < 0 || !SANDBOX_LOG_EVENTS.has(event)) throw new Error('Invalid diagnostic metadata');
  const safe = sanitizeDiagnosticFields(fields);
  if (Object.keys(fields).length !== Object.keys(safe).length || Object.keys(fields).some(key => fields[key] !== safe[key])) throw new Error('Unsafe diagnostic fields');
  return value;
}


export const SANDBOX_LOG_FILES = Object.freeze(["supervisor.jsonl", "supervisor.1.jsonl", "sidecar.jsonl", "sidecar.1.jsonl", "sidecar.previous.jsonl", "sidecar.previous.1.jsonl"] as const);
export type SandboxLogFile = (typeof SANDBOX_LOG_FILES)[number];
export function isSandboxLogFile(value: string): value is SandboxLogFile { return (SANDBOX_LOG_FILES as readonly string[]).includes(value); }
