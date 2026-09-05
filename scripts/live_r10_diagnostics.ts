import { copyFile, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import * as path from 'node:path';
import { PrismSessionManager } from '../src/prism/session-manager';
import { PrismProjectsClient } from '../src/prism/projects-client';
import { projectId } from '../src/sandbox/types';
import { FleetManifestStore } from '../src/fleet/manifest';
import { createBrowserlessSystem, type BrowserlessSystem } from '../src/server/system';
import { V2SandboxSupervisor } from '../src/v2/supervisor/controller';
import type { V2BootstrapGateway } from '../src/v2/bootstrap/controller';
import { loadV2SidecarAssets, V2_R10_RELEASE } from '../src/v2/sidecar/assets';
import { ObservabilityLogger } from '../src/observability/logger';
import { RotatingJsonlSink } from '../src/observability/rotating-jsonl';
import { resolveObservabilityConfig } from '../src/observability/config';
import { parseFrames } from '../src/relay/sse';
import { SANDBOX_LOG_FILES, parseDiagnosticLine } from '../src/v2/diagnostics/sandbox-log-schema';
import { checkR10Sidecar } from './build_v2_sidecar_r10';

if (process.env.GP_LIVE_R10_DIAGNOSTICS !== '1' || process.env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== '1' || process.env.PORT !== '18888') {
  throw new Error('Explicit one-sandbox diagnostic guards and port 18888 are required');
}
await checkR10Sidecar();
const sourceCookie = process.env.PRISM_COOKIE_FILE;
if (!sourceCookie) throw new Error('Diagnostic cookie source is missing');
const model = process.env.GP_LIVE_MODEL ?? 'gpt-6-astra';
if (!/^[A-Za-z0-9._-]{1,128}$/.test(model)) throw new Error('Invalid diagnostic model');
const recoveryCycles = Number(process.env.GP_LIVE_DIAGNOSTIC_RECOVERY_CYCLES ?? '0');
if (!Number.isSafeInteger(recoveryCycles) || recoveryCycles < 0 || recoveryCycles > 3) throw new Error('Diagnostic recovery cycles must be 0..3');
const failureCapture = process.env.GP_LIVE_R10_FAILURE_CAPTURE === '1' || recoveryCycles === 0;
if (process.env.GP_LIVE_R10_FAILURE_CAPTURE !== undefined && !['0', '1'].includes(process.env.GP_LIVE_R10_FAILURE_CAPTURE)) throw new Error('Failure capture guard must be 0 or 1');
const diagnosticAssets = await loadV2SidecarAssets({ revision: 'r10' });
const root = path.resolve('.ghostprov/live-r10'); await mkdir(root, { recursive: true });
const directory = await mkdtemp(path.join(root, 'one-'));
const cookieFile = path.join(directory, 'candidate-cookies.json');
const id = projectId(crypto.randomUUID()); const title = `GhostProv R10 logging diagnosis ${path.basename(directory)}`;
const key = `diagnostic-${crypto.randomUUID()}`;
const logConfig = resolveObservabilityConfig({ GP_LOG_LEVEL: 'debug', GP_LOG_DIR: path.join(directory, 'host'),
  GP_LOG_ROTATE_BYTES: String(1024 * 1024), GP_LOG_RETAIN_FILES: '2', GP_LOG_QUEUE_MAX_EVENTS: '2000',
  GP_LOG_QUEUE_MAX_BYTES: String(1024 * 1024), GP_LOG_FLUSH_MS: '100', GP_METRICS_INTERVAL_MS: '1000' });
const logger = new ObservabilityLogger({ config: logConfig, sinks: [new RotatingJsonlSink({ directory: logConfig.directory,
  prefix: 'diagnostic-host', rotateBytes: logConfig.rotateBytes, retainFiles: logConfig.retainFiles })] });
let session: PrismSessionManager | null = null;
let projects: PrismProjectsClient | null = null;
let system: BrowserlessSystem | null = null;
let supervisor: V2SandboxSupervisor | null = null;
let gateway: V2BootstrapGateway | null = null;
let latestNonce: string | null = null;
let latestPid: number | null = null;
let launches = 0;
let faultEnabled = false;
let created = false; let deleted = false;
let directCalls = 0; let llmCalls = 0;
let phase = 'create'; let failure: unknown = null;
const calls: Array<Record<string, unknown>> = [];
const downloaded = new Map<string, string>();
let fatalReadBack = false;
const recoveries: Array<{ cycle: number; durationMs: number; sameSupervisor: boolean; freshChild: boolean }> = [];
const progress = (next: string) => { phase = next; console.log(JSON.stringify({ event: 'diagnostic.phase', phase })); };
const bounded = <T>(promise: Promise<T>, ms: number): Promise<T> => new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error('Diagnostic operation timed out')), ms);
  promise.then(value => { clearTimeout(timer); resolve(value); }, error => { clearTimeout(timer); reject(error); });
});
const safeError = (error: unknown) => {
  const safe = (value: unknown) => typeof value === 'string' && /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(value) ? value : null;
  const record = error && typeof error === 'object' ? error as Record<string, unknown> : {};
  const prefix = process.cwd() + path.sep;
  const locations = typeof record.stack === 'string' ? record.stack.slice(0, 8192).split('\n').slice(1, 9).flatMap(line => {
    const at = line.indexOf(prefix); if (at < 0) return [];
    const match = /^((?:src|sandbox|scripts)[\\/][A-Za-z0-9._/\\-]+\.ts):(\d+)/.exec(line.slice(at + prefix.length));
    return match ? [{ file: match[1]!.replaceAll('\\','/'), line: Number(match[2]) }] : [];
  }).slice(0, 4) : [];
  return { name: safe(record.name), kind: safe(record.kind), code: safe(record.code), status: typeof record.status === 'number' ? record.status : null, locations };
};
async function readLogs(): Promise<Array<Record<string, unknown>>> {
  if (!supervisor || !system) return [];
  const all: Array<Record<string, unknown>> = [];
  for (const file of SANDBOX_LOG_FILES) {
    const response = await fetch(`http://127.0.0.1:18888/v1/diagnostics/sandboxes/${id}/logs/${file}`, {
      headers: { authorization: `Bearer ${key}` }, signal: AbortSignal.timeout(12000),
    });
    if (response.status === 404) { await response.body?.cancel(); continue; }
    if (response.status !== 200) { await response.body?.cancel(); throw new Error('Authenticated sandbox log download failed'); }
    const content = await response.text();
    if (Buffer.byteLength(content) > 128 * 1024) throw new Error('Downloaded sandbox log exceeds its bound');
    const records = content.split('\n').filter(Boolean).map(parseDiagnosticLine);
    if (records.length > 1024) throw new Error('Downloaded sandbox log record bound exceeded');
    downloaded.set(file, content); all.push(...records);
  }
  return all;
}
async function saveLogCheckpoint(label: string): Promise<void> {
  if (!/^(profile|recovery_[1-3])$/.test(label)) throw new Error('Invalid diagnostic checkpoint');
  const checkpoint = path.join(directory, label);
  await mkdir(checkpoint, { recursive: true });
  for (const [name, content] of downloaded) await writeFile(path.join(checkpoint, name), content);
}
async function waitForIdle(): Promise<void> {
  const deadline = performance.now() + 10000;
  while (true) {
    const snapshot = system?.registry.get(id)?.snapshot();
    if (snapshot && snapshot.active === 0 && snapshot.quarantined === 0 && snapshot.queueDepth === 0 &&
      snapshot.diagnostics?.host?.windowFrames === 0 && snapshot.diagnostics?.host?.schedulerFrames === 0) return;
    if (performance.now() >= deadline) throw new Error('Diagnostic probe did not reach an idle boundary');
    await Bun.sleep(100);
  }
}
async function request(caseName: string, route: string, body: Record<string, unknown>): Promise<Record<string, any> | null> {
  const start = performance.now(); let previous = start; let firstEvent: number | null = null; let firstText: number | null = null; let firstTool: number | null = null; let toolReady: number | null = null;
  let bytes = 0; let count = 0; let maxGap = 0; let final: Record<string, any> | null = null;
  let expectedText = false; let tail = ''; let messageStopped = false;
  const timeline: Array<Record<string, string | number>> = [];
  const knownEvents = new Set(['response.created','response.in_progress','response.output_item.added','response.output_item.done',
    'response.content_part.added','response.content_part.done','response.output_text.delta','response.output_text.done',
    'response.reasoning_summary_part.added','response.reasoning_summary_part.done','response.reasoning_summary_text.delta','response.reasoning_summary_text.done',
    'response.function_call_arguments.delta','response.function_call_arguments.done','response.completed','response.incomplete','response.failed',
    'message_start','message_delta','message_stop','content_block_start','content_block_delta','content_block_stop','ping','error']);
  const response = await fetch(`http://127.0.0.1:18888${route}`, { method: 'POST', signal: AbortSignal.timeout(180000),
    headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json', 'anthropic-version': '2023-06-01' }, body: JSON.stringify(body) });
  if (!response.ok || !response.body) throw new Error('Diagnostic client request failed');
  const headersMs = performance.now() - start;
  const reader = response.body.getReader(); const decoder = new TextDecoder(); let buffer = '';
  try {
    for (;;) {
      const value = await reader.read(); if (value.done) break;
      bytes += value.value.byteLength;
      if (bytes > 2 * 1024 * 1024 || buffer.length + value.value.byteLength > 256 * 1024) throw new Error('Diagnostic SSE body bound');
      buffer += decoder.decode(value.value, { stream: true });
      const parsed = parseFrames(buffer, 0); buffer = buffer.slice(parsed.nextOffset);
      for (const frame of parsed.frames) {
        if (++count > 4096) throw new Error('Diagnostic SSE event bound');
        const now = performance.now(); if (count > 1) maxGap = Math.max(maxGap, now - previous); previous = now;
        firstEvent ??= now - start;
        if (frame.data === '[DONE]') continue;
        const data = JSON.parse(frame.data);
        const text = frame.event === 'response.output_text.delta' && typeof data.delta === 'string' ? data.delta
          : frame.event === 'content_block_delta' && data.delta?.type === 'text_delta' && typeof data.delta.text === 'string' ? data.delta.text : '';
        if (text.length) { firstText ??= now - start; expectedText ||= (tail + text).includes('Hiii'); tail = (tail + text).slice(-3); }
        const argumentChars = frame.event === 'response.function_call_arguments.delta' && typeof data.delta === 'string' ? data.delta.length
          : frame.event === 'content_block_delta' && data.delta?.type === 'input_json_delta' && typeof data.delta.partial_json === 'string' ? data.delta.partial_json.length : 0;
        if (frame.event === 'response.output_item.added' && ['function_call','custom_tool_call'].includes(data.item?.type)) firstTool ??= now - start;
        timeline.push({ event_type: knownEvents.has(frame.event) ? frame.event : 'other', at_ms: now - start, delta_chars: text.length, argument_chars: argumentChars });
        if (frame.event === 'response.function_call_arguments.done') toolReady ??= now - start;
        if (frame.event === 'response.completed') final = data.response;
        if (frame.event === 'message_stop') messageStopped = true;
        if (frame.event === 'error' || frame.event === 'response.failed' || frame.event === 'response.incomplete') throw new Error('Diagnostic stream returned a failure');
      }
    }
  } finally { await reader.cancel().catch(() => undefined); reader.releaseLock(); }
  const metric = { case: caseName, headers_ms: headersMs, first_event_ms: firstEvent, first_text_ms: firstText, first_tool_ms: firstTool, tool_ready_ms: toolReady,
    total_ms: performance.now() - start, events: count, bytes, max_event_gap_ms: maxGap, expected_text: caseName === 'tool' ? null : expectedText };
  calls.push({ ...metric, timeline }); console.log(JSON.stringify({ event: 'diagnostic.request', ...metric }));
  if (route === '/v1/responses' && !final) throw new Error('Diagnostic Responses request has no completed terminal');
  if (route === '/v1/messages' && !messageStopped) throw new Error('Diagnostic Messages stream has no terminal');
  if (caseName !== 'tool' && !expectedText) throw new Error('Diagnostic response omitted the expected marker');
  return final;
}
const native = (input: unknown) => ({ model, stream: true, store: true, max_output_tokens: 256, reasoning: { effort: 'low' }, input });
try {
  await copyFile(sourceCookie, cookieFile);
  session = await PrismSessionManager.create({ origin: process.env.PRISM_ORIGIN ?? 'https://prism.openai.com', mode: 'authenticated', allowAnonymous: false, cookieFile });
  const identity = await session.resolve(AbortSignal.timeout(60000));
  const accountRequest = identity.account.request.bind(identity.account);
  identity.account.request = async (url, init) => {
    const pathname = new URL(url, process.env.PRISM_ORIGIN ?? 'https://prism.openai.com').pathname;
    if (pathname.startsWith('/api/llm/')) { llmCalls++; throw new Error('Diagnostic bootstrap must not call an LLM'); }
    if (pathname.endsWith('/codex/codex_exec_async')) directCalls++;
    return accountRequest(url, init);
  };
  projects = new PrismProjectsClient(identity.account);
  await writeFile(path.join(directory, 'owned-project.json'), JSON.stringify({ id, title })); created = true;
  await projects.create({ projectId: id, title }, AbortSignal.timeout(60000));
  progress('startup');
  system = await createBrowserlessSystem({ ...process.env, PORT: '18888', GP_CANDIDATE: '1', GHOSTPROV_API_KEY: key,
    PRISM_COOKIE_FILE: cookieFile, PRISM_COOKIE: undefined, GP_PRISM_SESSION: 'authenticated', GP_ALLOW_ANONYMOUS: '0',
    PRISM_PROJECT_IDS: id, PRISM_DOC_ID: '', GP_TARGET_SANDBOXES: '1', GP_MAX_PROJECTS: '1', GP_AUTO_CREATE_PROJECTS: '0',
    GP_PROJECT_REUSE: 'managed', GP_GUEST_MAX_PROJECTS: '1', GP_FLEET_MANIFEST_FILE: path.join(directory, 'fleet.json'),
    GP_PROVISION_CONCURRENCY: '1', GP_COLD_START_CONCURRENCY: '1', GP_V2_SIDECAR_REVISION: 'r10', GP_V2_SIDECAR_PROJECT_REVISIONS: '',
    GP_V2_BOOTSTRAP_LAUNCH_MODE: 'relay', GP_V2_SUPERVISOR_BOOTSTRAP_MODE: 'direct', GP_V2_DIAGNOSTICS: '1', GP_V2_DIAGNOSTICS_INTERVAL_MS: '1000',
  }, { sessionManager: session, projects, logger,
    manifest: new FleetManifestStore({ file: path.join(directory, 'fleet.json'), identityFingerprint: identity.fingerprint }),
    sandboxSupervisorFactory: input => {
      if (input.binding.projectId !== id || supervisor) throw new Error('Diagnostic run cannot replace its original supervisor');
      gateway = input.gateway;
      const value = new V2SandboxSupervisor(input); supervisor = value;
      const launch = value.launch.bind(value);
      value.launch = async (request, signal) => {
        let submitted = request;
        if (faultEnabled) {
          const config = JSON.parse(request.staticConfig);
          submitted = { ...request, staticConfig: JSON.stringify({ ...config, diagnostics: { ...config.diagnostics, intervalMs: 0 } }) };
        }
        try {
          const result = await launch(submitted, signal);
          latestNonce = request.sidecarBinding.nonce; latestPid = result.process.pid; launches++; return result;
        } catch (error) {
          if (faultEnabled) {
            const records = await readLogs();
            fatalReadBack = records.some(value => value.role === 'sidecar' && value.event === 'fatal' && value.phase === 'config');
          }
          throw error;
        }
      };
      return value;
    },
  });
  await bounded(system.start(), 150000);
  const initial = await readLogs();
  if (!initial.some(value => value.role === 'sidecar' && value.event === 'phase' && value.phase === 'runtime')) throw new Error('Sandbox startup log is unavailable');
  console.log(JSON.stringify({ event: 'diagnostic.log_readback', files: downloaded.size, records: initial.length }));
  progress('profile');
  const rootResponse = await request('text', '/v1/responses', native('Reply with exactly Hiii.'));
  const tools = [{ type: 'function', name: 'diagnostic_echo', description: 'Return the diagnostic marker.',
    parameters: { type: 'object', properties: { value: { type: 'string' } }, required: ['value'], additionalProperties: false }, strict: true }];
  const tool = await request('tool', '/v1/responses', { ...native('Call diagnostic_echo once with value Hiii.'), tools,
    tool_choice: { type: 'function', name: 'diagnostic_echo' } });
  const call = tool?.output?.find((value: Record<string, unknown>) => value.type === 'function_call');
  if (!call || typeof call.call_id !== 'string' || typeof tool?.id !== 'string') throw new Error('Diagnostic tool identity is missing');
  if (call.name !== 'diagnostic_echo' || JSON.parse(call.arguments).value !== 'Hiii') throw new Error('Diagnostic tool payload does not match its fixture');
  await request('tool_followup', '/v1/responses', { ...native([{ type: 'function_call_output', call_id: call.call_id, output: 'Hiii' }]), previous_response_id: tool.id, tools });
  await request('anthropic_text', '/v1/messages', { model, stream: true, max_tokens: 256, messages: [{ role: 'user', content: 'Reply with exactly Hiii.' }] });
  const data = Array.from({ length: 4096 }, (_, i) => `value_${i.toString(36).padStart(4,'0')}`).join(' ');
  await request('medium_input', '/v1/responses', native(`Ignore the following reference data and reply with exactly Hiii.\n${data}`));
  await Promise.all(Array.from({ length: 4 }, (_, i) => request(`concurrent_${i + 1}`, '/v1/responses', native('Reply with exactly Hiii.'))));
  const measured = await readLogs();
  if (!measured.some(value => value.event === 'upstream_event' && value.tool_ready === true) ||
    !measured.some(value => value.event === 'upstream_terminal') || !measured.some(value => value.event === 'request_metrics')) throw new Error('Sandbox processing metrics are incomplete');
  if (measured.some(value => value.event === 'logger_stats' && typeof value.dropped === 'number' && value.dropped > 0)) throw new Error('Diagnostic profile dropped samples');
  await saveLogCheckpoint('profile');
  if (!supervisor || !latestNonce) throw new Error('Missing exact diagnostic child identity');
  const selected: V2SandboxSupervisor = supervisor;
  if (recoveryCycles > 0) {
    for (let cycle = 1; cycle <= recoveryCycles; cycle++) {
      await waitForIdle();
      progress(`normal_recovery_${cycle}`);
      const originalInstance = selected.instanceId;
      const originalNonce = latestNonce;
      const originalPid = latestPid;
      if (launches !== cycle || !originalPid || !originalNonce) throw new Error('Unexpected diagnostic child generation');
      const started = performance.now();
      const stopped = await selected.stop({ launchNonces: [originalNonce], reason: 'diagnostic_normal_recovery' }, AbortSignal.timeout(45000));
      if (stopped !== originalNonce) throw new Error('Diagnostic stop lacked exact child proof');
      const deadline = performance.now() + 180000;
      let nextRead = performance.now();
      while (true) {
        const snapshot = system.registry.get(id)?.snapshot();
        if (supervisor !== selected || selected.instanceId !== originalInstance || launches > cycle + 1) throw new Error('Normal recovery replaced the supervisor or launched extra children');
        if (launches === cycle + 1 && latestNonce !== originalNonce &&
          snapshot?.state === 'ready' && snapshot.capacity > 0 && snapshot.available > 0 && snapshot.transportConnected === true &&
          snapshot.quarantined === 0 && (snapshot.ambiguousQuarantined ?? 0) === 0) break;
        if (performance.now() >= deadline) throw new Error('Normal child recovery did not restore usable capacity');
        if (performance.now() >= nextRead) {
          await readLogs(); await saveLogCheckpoint(`recovery_${cycle}`);
          nextRead = performance.now() + 5000;
        }
        await Bun.sleep(250);
      }
      const proof = { cycle, durationMs: performance.now() - started, sameSupervisor: selected.instanceId === originalInstance,
        freshChild: latestPid !== originalPid };
      if (!proof.sameSupervisor || !proof.freshChild || directCalls !== 1 || llmCalls !== 0) throw new Error('Normal recovery identity proof failed');
      recoveries.push(proof);
      console.log(JSON.stringify({ event: 'diagnostic.recovery', ...proof }));
      progress(`requests_after_recovery_${cycle}`);
      if (typeof rootResponse?.id !== 'string') throw new Error('Missing diagnostic provider lineage');
      await request(`recovery_${cycle}_continuation`, '/v1/responses', { ...native('Reply with exactly Hiii.'), previous_response_id: rootResponse.id });
      await request(`recovery_${cycle}_anthropic`, '/v1/messages', { model, stream: true, max_tokens: 256, messages: [{ role: 'user', content: 'Reply with exactly Hiii.' }] });
      await readLogs(); await saveLogCheckpoint(`recovery_${cycle}`);
    }
  }
  if (failureCapture) {
    progress('controlled_failure');
    await waitForIdle();
    faultEnabled = true;
    await selected.stop({ launchNonces: [latestNonce], reason: 'diagnostic_failure_probe' }, AbortSignal.timeout(15000));
    const deadline = performance.now() + 70000;
    while (performance.now() < deadline) {
      if (fatalReadBack) break;
      await Bun.sleep(1000);
      const entries = await readLogs();
      if (fatalReadBack || entries.some(value => value.role === 'sidecar' && value.event === 'fatal' && value.phase === 'config')) { fatalReadBack = true; break; }
    }
    if (!fatalReadBack) throw new Error('Controlled sandbox failure was not readable');
  }
  progress('complete');
} catch (error) { failure = error; console.log(JSON.stringify({ event: 'diagnostic.failure', phase, ...safeError(error) })); }
finally {
  await readLogs().catch(() => undefined);
  await system?.close().catch(error => { failure ??= error; });
  await session?.close().catch(error => { failure ??= error; });
  await logger.close();
  let cleanup: PrismSessionManager | null = null;
  try {
    if (created) {
      cleanup = await PrismSessionManager.create({ origin: process.env.PRISM_ORIGIN ?? 'https://prism.openai.com', mode: 'authenticated', allowAnonymous: false, cookieFile });
      const identity = await cleanup.resolve(AbortSignal.timeout(60000));
      const client = new PrismProjectsClient(identity.account);
      for (let attempt = 0; attempt < 3 && !deleted; attempt++) {
        try {
          const before = await client.get(id, AbortSignal.timeout(30000));
          if (before.title !== title) throw new Error('Diagnostic cleanup ownership changed');
          if (!before.deleted) await client.deleteManaged(id, AbortSignal.timeout(60000));
          deleted = (await client.get(id, AbortSignal.timeout(30000))).deleted;
        } catch (error) {
          if (error && typeof error === 'object' && 'status' in error && (error.status === 404 || error.status === 410)) deleted = true;
          else if (attempt === 2) failure ??= error;
        }
      }
    }
  } catch (error) { failure ??= error; }
  await cleanup?.close();
  for (const file of [cookieFile, `${cookieFile}.identity.json`, `${cookieFile}.lock`]) {
    if (!path.resolve(file).startsWith(directory + path.sep)) throw new Error('Diagnostic cleanup escaped its directory');
    await rm(file, { force: true });
  }
  for (const [name, content] of downloaded) await writeFile(path.join(directory, name), content);
  const report = { live: true, revision: 'r10', success: failure === null && deleted && recoveries.length === recoveryCycles && (!failureCapture || fatalReadBack), phase,
    buildHash: diagnosticAssets.buildHash, bundleSha256: V2_R10_RELEASE.bundleSha256, model, candidatePort: 18888,
    requests: calls, directBootstrapCalls: directCalls, llmBootstrapCalls: llmCalls, logFiles: downloaded.size,
    failureCapture, fatalReadBack, recoveryCycles, recoveries, projectCreated: created, projectDeleted: deleted, logger: logger.snapshot(), failure: failure ? safeError(failure) : null };
  await writeFile(path.join(directory, 'report.json'), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ event: 'diagnostic.result', ...report, directory }));
  if (!report.success) process.exitCode = 1;
}
