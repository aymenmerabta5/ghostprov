import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import * as path from "node:path";
import WebSocket from "ws";
import { PrismSessionManager } from "../src/prism/session-manager";
import { PrismProjectError, PrismProjectsClient } from "../src/prism/projects-client";
import { FleetManifestStore } from "../src/fleet/manifest";
import { createBrowserlessSystem, type BrowserlessSystem } from "../src/server/system";
import { projectId, type ProjectId } from "../src/sandbox/types";
import type { RunHandle, RunDiagnostics } from "../src/relay/run-handle";
import { parseFrames, type SseFrame } from "../src/relay/sse";
import type { LoggerLike } from "../src/observability/logger";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";

const KNOWN_EVENTS = new Set([
  "response.created", "response.in_progress", "response.completed", "response.failed", "response.incomplete",
  "response.output_item.added", "response.output_item.done", "response.content_part.added", "response.content_part.done",
  "response.output_text.delta", "response.output_text.done", "response.function_call_arguments.delta", "response.function_call_arguments.done",
  "response.reasoning_summary_part.added", "response.reasoning_summary_part.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
  "response.reasoning_text.delta", "response.reasoning_text.done", "message_start", "message_delta", "message_stop",
  "content_block_start", "content_block_delta", "content_block_stop", "error", "ping",
]);
export function traceUsageCounts(value: unknown): Record<string, number> {
  const result: Record<string, number> = {};
  if (!value || typeof value !== "object" || Array.isArray(value)) return result;
  const raw = value as Record<string, unknown>;
  for (const key of ["input_tokens", "output_tokens", "total_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"]) {
    if (typeof raw[key] === "number" && Number.isSafeInteger(raw[key]) && (raw[key] as number) >= 0) result[key] = raw[key] as number;
  }
  for (const [container, key] of [["input_tokens_details", "cached_tokens"], ["input_tokens_details", "cache_write_tokens"],
    ["output_tokens_details", "reasoning_tokens"], ["output_tokens_details", "thinking_tokens"]] as const) {
    const nested = raw[container];
    if (nested && typeof nested === "object" && !Array.isArray(nested)) {
      const count = (nested as Record<string, unknown>)[key];
      if (typeof count === "number" && Number.isSafeInteger(count) && count >= 0) result[key] = count;
    }
  }
  return result;
}
const MAX_EVENTS = 4096;
const MAX_TRACES = 32;
const MAX_STREAM_BYTES = 2 * 1024 * 1024;
type Lane = "responses" | "anthropic" | "websocket";
type Point = { atMs: number; event: string; sequence: number | null; textChars: number; argumentChars: number };
type Trace = {
  label: string; lane: Lane; start: number; routerAtMs?: number; runtimeAtMs?: number; handleAtMs?: number;
  sourceFinalAtMs?: number; headersAtMs?: number; doneAtMs?: number; injectedLineage?: boolean;
  relay: Point[]; client: Point[]; diagnostics?: RunDiagnostics; usage?: Record<string, unknown>;
  toolExecutionMs?: number; error?: string;
};
const emit = (event: string, value: Record<string, unknown> = {}) => console.log(JSON.stringify({ event, ...value }));
const elapsed = (trace: Trace) => Math.round((performance.now() - trace.start) * 1000) / 1000;
function point(trace: Trace, stage: "relay" | "client", frame: SseFrame, value: any): void {
  if (trace[stage].length >= MAX_EVENTS) throw new Error("Trace event bound exceeded");
  const text = frame.event === "response.output_text.delta" ? value.delta
    : value.delta?.type === "text_delta" ? value.delta.text : null;
  const args = frame.event === "response.function_call_arguments.delta" ? value.delta
    : value.delta?.type === "input_json_delta" ? value.delta.partial_json : null;
  let event = KNOWN_EVENTS.has(frame.event) ? frame.event : "unrecognized_event";
  if (frame.event === "response.output_item.added" && value.item?.type === "function_call") event += ":function_call";
  if (frame.event === "content_block_start" && value.content_block?.type === "tool_use") event += ":tool_use";
  trace[stage].push({ atMs: elapsed(trace), event,
    sequence: Number.isSafeInteger(value.sequence_number) ? value.sequence_number : null,
    textChars: typeof text === "string" ? text.length : 0, argumentChars: typeof args === "string" ? args.length : 0 });
}
function tracedHandle(handle: RunHandle, trace: Trace): RunHandle {
  trace.handleAtMs = elapsed(trace);
  void handle.final.then(() => { trace.sourceFinalAtMs = elapsed(trace); }, () => { trace.error = "source_final"; });
  return {
    async *events() {
      for await (const frame of handle.events()) {
        if (frame.data.length > MAX_STREAM_BYTES) throw new Error("Trace frame bound exceeded");
        point(trace, "relay", frame, JSON.parse(frame.data));
        yield frame;
      }
    },
    get final() { return handle.final; },
    get rateLimit() { return handle.rateLimit; },
    get upstreamPrelude() { return handle.upstreamPrelude; },
    stats: handle.stats,
    discardUnconsumedEvents: () => handle.discardUnconsumedEvents?.(),
  };
}
function summary(trace: Trace) {
  const first = (list: Point[], predicate: (value: Point) => boolean) => list.find(predicate)?.atMs ?? null;
  const last = (list: Point[], predicate: (value: Point) => boolean) => {
    for (let index = list.length - 1; index >= 0; index--) if (predicate(list[index]!)) return list[index]!.atMs;
    return null;
  };
  const nativeSource = new Map(trace.relay.filter(p => p.sequence !== null).map(p => [p.sequence, p.atMs]));
  const forwarding = trace.lane !== "anthropic" ? trace.client.flatMap(p => {
    const source = nativeSource.get(p.sequence); return source === undefined ? [] : [Math.max(0, p.atMs - source)];
  }).sort((a,b)=>a-b) : [];
  const gaps = trace.client.filter(p=>p.textChars>0).map(p=>p.atMs);
  const deltaGaps = gaps.slice(1).map((time,index)=>time-gaps[index]!).sort((a,b)=>a-b);
  const visiblePauses = deltaGaps.filter(gap => gap > 1);
  const percentile = (values:number[], fraction:number) => values.length ? Math.round(values[Math.ceil(values.length*fraction)-1]! *1000)/1000 : null;
  return { label:trace.label,lane:trace.lane,routerAtMs:trace.routerAtMs,runtimeAtMs:trace.runtimeAtMs,
    handleAtMs:trace.handleAtMs,headersAtMs:trace.headersAtMs,doneAtMs:trace.doneAtMs,
    sourceFinalAtMs:trace.sourceFinalAtMs,injectedLineage:trace.injectedLineage,
    sourceFirstTextMs:first(trace.relay,p=>p.textChars>0),clientFirstTextMs:first(trace.client,p=>p.textChars>0),
    sourceFirstToolMs:first(trace.relay,p=>p.event.endsWith(":function_call")||p.argumentChars>0),
    sourceFirstArgumentsMs:first(trace.relay,p=>p.argumentChars>0),sourceLastArgumentsMs:last(trace.relay,p=>p.argumentChars>0),
    sourceArgumentsDoneMs:first(trace.relay,p=>p.event==="response.function_call_arguments.done"),
    clientFirstToolMs:first(trace.client,p=>p.event.endsWith(":function_call")||p.event.endsWith(":tool_use")||p.argumentChars>0),
    clientFirstArgumentsMs:first(trace.client,p=>p.argumentChars>0),clientLastArgumentsMs:last(trace.client,p=>p.argumentChars>0),
    clientTerminalMs:last(trace.client,p=>p.event==="response.completed"||p.event==="message_stop"),
    relayEvents:trace.relay.length,clientEvents:trace.client.length,textDeltaEvents:trace.client.filter(p=>p.textChars>0).length,
    textChars:trace.client.reduce((n,p)=>n+p.textChars,0),argumentChars:trace.client.reduce((n,p)=>n+p.argumentChars,0),
    forwardingP50Ms:percentile(forwarding,0.5),forwardingP95Ms:percentile(forwarding,0.95),
    textDeltaGapP95Ms:percentile(deltaGaps,0.95),textDeltaGapMaxMs:deltaGaps.at(-1)??null,
    deliveryBursts:gaps.length ? visiblePauses.length + 1 : 0,
    visiblePauseP50Ms:percentile(visiblePauses,0.5),visiblePauseP95Ms:percentile(visiblePauses,0.95),
    diagnostics:trace.diagnostics,usage:trace.usage,toolExecutionMs:trace.toolExecutionMs,error:trace.error };
}

export interface ToolLatencyOptions {
  readonly cookieFile: string;
  readonly origin: string;
  readonly batching: boolean;
  readonly receiptCoalescing: boolean;
}

export function resolveToolLatencyOptions(env: Record<string, string | undefined>): ToolLatencyOptions {
  if (env.GP_LIVE_TOOL_LATENCY_TRACE !== "1" || env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new Error("Tool latency tracing requires both explicit live and dedicated-project mutation guards");
  }
  if (Number(env.PORT ?? 18888) !== 18888) throw new Error("Tool latency tracing is restricted to port 18888");
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new Error("Tool latency tracing requires PRISM_COOKIE_FILE");
  const flag = (key: string): boolean => {
    const value = env[key] ?? "1";
    if (value !== "0" && value !== "1") throw new Error(`${key} must be 0 or 1`);
    return value === "1";
  };
  return Object.freeze({ cookieFile: path.resolve(cookieFile),
    origin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""),
    batching: flag("GP_TRACE_BATCHING"), receiptCoalescing: flag("GP_V2_TERMINAL_RECEIPT_COALESCING") });
}

/** Cleanup also covers an ambiguous create; only the exact dedicated project is eligible. */
export async function deleteOwnedToolTraceProject(
  projects: Pick<PrismProjectsClient, "get" | "deleteManaged">,
  id: ProjectId,
  title: string,
): Promise<void> {
  const read = async () => {
    try { return await projects.get(id, AbortSignal.timeout(20_000)); }
    catch (error) {
      if (error instanceof PrismProjectError && error.kind === "http" && (error.status === 404 || error.status === 410)) return null;
      throw error;
    }
  };
  const current = await read();
  if (current === null) return;
  if (current.projectId !== id || current.title !== title) throw new Error("Cleanup identity mismatch");
  if (current.deleted) return;
  await projects.deleteManaged(id, AbortSignal.timeout(30_000));
  const remaining = await read();
  if (remaining !== null && (remaining.projectId !== id || remaining.title !== title || !remaining.deleted)) {
    throw new Error("Project deletion was not confirmed");
  }
}

export async function main(env: Record<string, string | undefined> = process.env): Promise<void> {
  const options = resolveToolLatencyOptions(env);
  const batching = options.batching;
  const receiptCoalescing = options.receiptCoalescing;
  const root = path.resolve(".ghostprov","tool-latency"); await mkdir(root,{recursive:true});
  const directory = await mkdtemp(path.join(root,batching?"batch-on-":"batch-off-"));
  const cookieFile=path.join(directory,"cookies.json");
  const fixtureFile=path.join(directory,"fixture.txt");
  const origin=options.origin;
  const id=projectId(crypto.randomUUID());const title=`GhostProv Tool Trace ${id.slice(0,8)}`;const key=`trace-${crypto.randomUUID()}`;
  let session:PrismSessionManager|null=null;let system:BrowserlessSystem|null=null;let createAttempted=false;let failure:unknown=null;
  let clientSocket: WebSocket | null = null;
  let phase="create";let directStarts=0;let llmAttempts=0;let invalidLogs=0;
  const traces:Trace[]=[];const currentTools=new Map<string,Trace>();let currentText:Trace|null=null;
  const handles=new Map<Trace,RunHandle>();
  const report:Record<string,unknown>={batching,receiptCoalescing,model:"gpt-5.6-sol",startedAt:new Date().toISOString(),candidatePort:18888};
  const logger:LoggerLike={emit(event){try{assertSafeObservabilityEvent(event);}catch{invalidLogs++;}
    if(event.event==="request.failed")emit("trace_request_failed",{kind:event.error_kind,status:event.status});},
    snapshot:()=>({queuedEvents:0,queuedBytes:0,droppedSamples:0,droppedProgress:0,droppedTerminals:0,sinkErrors:invalidLogs}),async close(){}};
  const traceFor = (body:Record<string,unknown>) => {
    const tools=Array.isArray(body.tools)?body.tools:[];
    const tool=tools.find(x=>x&&typeof x==="object"&&typeof x.name==="string");
    return (tool?currentTools.get(tool.name):null)??currentText;
  };
  async function websocketResult(trace:Trace, body:Record<string,unknown>):Promise<any>{
    if(!clientSocket || clientSocket.readyState === WebSocket.CLOSED){
      clientSocket=new WebSocket("ws://127.0.0.1:18888/v1/responses",{headers:{authorization:`Bearer ${key}`}});
      clientSocket.on("error",()=>{});
      const socket=clientSocket;
      await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>{socket.terminate();reject(new Error("Trace WebSocket connection timeout"));},10000);
        const cleanup=()=>{clearTimeout(timer);socket.off("open",open);socket.off("error",error);};
        const open=()=>{cleanup();resolve();};const error=()=>{cleanup();reject(new Error("Trace WebSocket connection failed"));};
        socket.once("open",open);socket.once("error",error);
      });
    }
    trace.headersAtMs=elapsed(trace);const socket=clientSocket;let bytes=0;
    return new Promise((resolve,reject)=>{
      const cleanup=()=>{clearTimeout(timer);socket.off("message",message);socket.off("error",error);socket.off("close",closed);};
      const fail=(error:Error)=>{cleanup();reject(error);};
      const error=()=>fail(new Error("Trace WebSocket failed"));const closed=()=>fail(new Error("Trace WebSocket closed before terminal"));
      const message=(raw:WebSocket.RawData)=>{try{
        const data=raw.toString();bytes+=Buffer.byteLength(data);if(bytes>MAX_STREAM_BYTES)throw new Error("Trace WebSocket byte bound exceeded");
        const value=JSON.parse(data);point(trace,"client",{event:value.type,data},value);
        if(value.type==="error"||value.type==="response.failed"||value.type==="response.incomplete")throw new Error("Trace WebSocket error response");
        if(value.type==="response.completed"){trace.usage=traceUsageCounts(value.response?.usage);cleanup();resolve(value.response);}
      }catch(cause){fail(cause instanceof Error?cause:new Error("Trace WebSocket parse failed"));}};
      const timer=setTimeout(()=>{socket.terminate();fail(new Error("Trace WebSocket response timed out"));},120000);
      socket.on("message",message);socket.once("error",error);socket.once("close",closed);
      socket.send(JSON.stringify({...body,stream:true,type:"response.create"}));
    });
  }
  function finishTrace(trace:Trace,final:any,toolName?:string){
    trace.doneAtMs=elapsed(trace);const handle=handles.get(trace);
    if(handle?.stats.diagnostics)trace.diagnostics=structuredClone(handle.stats.diagnostics);
    handles.delete(trace);if(toolName)currentTools.delete(toolName);else currentText=null;
    if(!final)throw new Error("Trace missing terminal");return final;
  }
  async function request(lane:Lane,label:string,body:Record<string,unknown>,toolName?:string):Promise<any>{
    if(traces.length>=MAX_TRACES)throw new Error("Trace count bound exceeded");
    const trace:Trace={label,lane,start:performance.now(),relay:[],client:[]};traces.push(trace);
    if(toolName)currentTools.set(toolName,trace);else currentText=trace;
    if(lane==="websocket")return finishTrace(trace,await websocketResult(trace,body),toolName);
    const response=await fetch(`http://127.0.0.1:18888${lane!=="anthropic"?"/v1/responses":"/v1/messages"}`,{
      method:"POST",headers:{authorization:`Bearer ${key}`,"content-type":"application/json","anthropic-version":"2023-06-01"},
      body:JSON.stringify({...body,stream:true}),signal:AbortSignal.timeout(120000)});
    trace.headersAtMs=elapsed(trace);
    if(!response.ok||!response.body){await response.body?.cancel();throw new Error(`Trace HTTP ${response.status}`);}
    const reader=response.body.getReader();const decoder=new TextDecoder();let buffer="";let bytes=0;let final:any=null;
    const blocks:any[]=[];const argumentsByIndex=new Map<number,string>();let anthropic:any=null;let terminal=false;
    try{
      for(;;){const part=await reader.read();if(part.done)break;bytes+=part.value.byteLength;if(bytes>MAX_STREAM_BYTES)throw new Error("Trace stream bound exceeded");
        buffer+=decoder.decode(part.value,{stream:true});const parsed=parseFrames(buffer,0);buffer=buffer.slice(parsed.nextOffset);
        for(const frame of parsed.frames){if(frame.data==="[DONE]")continue;const value=JSON.parse(frame.data);point(trace,"client",frame,value);
          if(frame.event==="error"||frame.event==="response.failed")throw new Error("Trace upstream error");
          if(frame.event==="response.completed"){final=value.response;trace.usage=traceUsageCounts(final.usage);terminal=true;}
          if(frame.event==="message_start")anthropic={...value.message,content:blocks};
          if(frame.event==="content_block_start"){
            if(!Number.isSafeInteger(value.index)||value.index<0||value.index>=32)throw new Error("Trace content bound exceeded");
            blocks[value.index]={...value.content_block};if(value.content_block.type==="tool_use")argumentsByIndex.set(value.index,"");
          }
          if(frame.event==="content_block_delta"){
            const block=blocks[value.index];if(!block)throw new Error("Trace block missing");
            if(value.delta.type==="text_delta")block.text=(block.text??"")+value.delta.text;
            if(value.delta.type==="thinking_delta")block.thinking=(block.thinking??"")+value.delta.thinking;
            if(value.delta.type==="signature_delta")block.signature=(block.signature??"")+value.delta.signature;
            if(value.delta.type==="input_json_delta")argumentsByIndex.set(value.index,(argumentsByIndex.get(value.index)??"")+value.delta.partial_json);
          }
          if(frame.event==="content_block_stop"&&blocks[value.index]?.type==="tool_use")blocks[value.index].input=JSON.parse(argumentsByIndex.get(value.index)||"{}");
          if(frame.event==="message_delta"&&anthropic){anthropic.stop_reason=value.delta.stop_reason;anthropic.usage={...anthropic.usage,...value.usage};}
          if(frame.event==="message_stop"){final=anthropic;trace.usage=traceUsageCounts(final.usage);terminal=true;}
        }
      }
    }finally{await reader.cancel().catch(()=>{});reader.releaseLock();}
    if(!terminal||!final)throw new Error("Trace missing terminal");
    return finishTrace(trace,final,toolName);
  }
  const native=(input:unknown,extra:Record<string,unknown>={})=>({model:"gpt-5.6-sol",store:true,reasoning:{effort:"low",summary:"auto"},max_output_tokens:2048,input,...extra});
  const anthropic=(messages:unknown[],extra:Record<string,unknown>={})=>({model:"gpt-5.6-sol",max_tokens:2048,output_config:{effort:"low"},messages,...extra});

  try{
    await copyFile(options.cookieFile,cookieFile);
    await writeFile(fixtureFile,"Fixture line: bounded tool result for the latency trace.\n".repeat(650));
    session=await PrismSessionManager.create({origin,mode:"authenticated",allowAnonymous:false,cookieFile});
    const identity=await session.resolve(AbortSignal.timeout(30000));const originalAccount=identity.account.request.bind(identity.account);
    identity.account.request=async(url,init)=>{const p=new URL(url,origin).pathname;if(p.startsWith("/api/llm/")){llmAttempts++;throw new Error("LLM bootstrap forbidden");}
      if(p.endsWith("/codex/codex_exec_async")){directStarts++;const body=JSON.parse(String(init?.body));if(body.prompt!=="Hiii"||body.codex_path!=="/bin/sh")throw new Error("Wrong direct request");}
      return originalAccount(url,init);};
    const projects=new PrismProjectsClient(identity.account);
    await writeFile(path.join(directory,"owned-project.json"),JSON.stringify({id,title}));
    createAttempted=true;
    await projects.create({projectId:id,title},AbortSignal.timeout(30000));phase="startup";
    system=await createBrowserlessSystem({...env,PORT:"18888",GP_CANDIDATE:"1",GHOSTPROV_API_KEY:key,
      PRISM_COOKIE_FILE:cookieFile,PRISM_COOKIE:undefined,GP_PRISM_SESSION:"authenticated",GP_ALLOW_ANONYMOUS:"0",
      PRISM_PROJECT_IDS:id,PRISM_DOC_ID:"",GP_TARGET_SANDBOXES:"1",GP_MAX_PROJECTS:"1",GP_AUTO_CREATE_PROJECTS:"0",GP_PROJECT_REUSE:"managed",
      GP_FLEET_MANIFEST_FILE:path.join(directory,"fleet.json"),GP_PROVISION_CONCURRENCY:"1",GP_COLD_START_CONCURRENCY:"1",
      GP_V2_SIDECAR_REVISION:"r9",GP_V2_SIDECAR_PROJECT_REVISIONS:"",GP_V2_BOOTSTRAP_LAUNCH_MODE:"relay",GP_V2_SUPERVISOR_BOOTSTRAP_MODE:"direct",
      GP_V2_BATCHING:batching?"1":"0",GP_V2_TERMINAL_RECEIPT_COALESCING:receiptCoalescing?"1":"0"},{sessionManager:session,projects,logger,
        manifest:new FleetManifestStore({file:path.join(directory,"fleet.json"),identityFingerprint:identity.fingerprint})},AbortSignal.timeout(180000));
    await system.start();emit("trace_candidate_ready",{batching});
    const routerStart=system.router.startRelay.bind(system.router);
    system.router.startRelay=async(body,options)=>{const trace=traceFor(body);if(trace)trace.routerAtMs=elapsed(trace);return routerStart(body,options);};
    for(const runtime of system.registry.readyRuntimes()){
      const originalRun=runtime.run.bind(runtime);
      runtime.run=async(body,signal,options)=>{const trace=traceFor(body);if(trace){trace.runtimeAtMs=elapsed(trace);trace.injectedLineage=typeof body.previous_response_id==="string";}
        const handle=await originalRun(body,signal,options);if(!trace)return handle;handles.set(trace,handle);return tracedHandle(handle,trace);};
    }
    phase="text";
    for(const lane of ["responses","anthropic","websocket"] as const){
      const prompt="Explain in about 250 words how a hash table resolves collisions. Use plain paragraphs.";
      await request(lane,`${lane}_text`,lane!=="anthropic"?native(prompt):anthropic([{role:"user",content:prompt}]));
    }
    phase="tools";
    for(let sample=0;sample<2;sample++){for(const lane of ["responses","anthropic","websocket"] as const){for(const size of [0,1024]){
      const toolName=`latency_probe_${lane}_${size}_${sample}`;const marker=`TRACE_${lane}_${size}_${sample}`;const payload="x".repeat(size);
      const schema={type:"object",properties:{marker:{type:"string",enum:[marker]},payload:{type:"string",enum:[payload]}},required:["marker","payload"],additionalProperties:false};
      const tool=lane!=="anthropic"?{type:"function",name:toolName,description:"Read the bounded latency test fixture.",parameters:schema,strict:true}
        :{name:toolName,description:"Read the bounded latency test fixture.",input_schema:schema};
      const prompt=`Call ${toolName} exactly once with marker ${marker} and payload equal to the following quoted string. After the tool result, reply with the marker. Payload: ${JSON.stringify(payload)}`;
      const initialBody=lane!=="anthropic"?native(prompt,{tools:[tool],tool_choice:"auto",parallel_tool_calls:false})
        :anthropic([{role:"user",content:prompt}],{tools:[tool],tool_choice:{type:"auto",disable_parallel_tool_use:true}});
      const first=await request(lane,`${lane}_tool_${size}_r${sample}`,initialBody,toolName);
      const calls=lane!=="anthropic"?first.output.filter((v:any)=>v.type==="function_call"):first.content.filter((v:any)=>v.type==="tool_use");
      if(calls.length!==1||calls[0].name!==toolName)throw new Error("Expected one exact tool call");
      const args=lane!=="anthropic"?JSON.parse(calls[0].arguments):calls[0].input;
      if(args.marker!==marker||args.payload!==payload)throw new Error("Tool arguments differ from the requested fixture");
      const toolStart=performance.now();const fixture=await Bun.file(fixtureFile).slice(0,size?32768:128).text();
      const result=JSON.stringify({marker,fixture});const toolMs=Math.round((performance.now()-toolStart)*1000)/1000;
      traces.at(-1)!.toolExecutionMs=toolMs;
      const follow=lane!=="anthropic"?native([{type:"function_call_output",call_id:calls[0].call_id,output:result},
        {role:"user",content:`Reply exactly ${marker}.`}],{tools:[tool],tool_choice:"auto",parallel_tool_calls:false,previous_response_id:first.id})
        :anthropic([{role:"user",content:prompt},{role:"assistant",content:first.content},{role:"user",content:[
          {type:"tool_result",tool_use_id:calls[0].id,content:result},{type:"text",text:`Reply exactly ${marker}.`}]}],
          {tools:[tool],tool_choice:{type:"auto",disable_parallel_tool_use:true}});
      const final=await request(lane,`${lane}_resume_${size}_r${sample}`,follow,toolName);
      const parts=lane!=="anthropic"?final.output.flatMap((v:any)=>v.content??[]):final.content;
      if(!parts.some((v:any)=>typeof v.text==="string"&&v.text.includes(marker)))throw new Error("Tool result was not used by the continuation");
      emit("trace_pair_completed",{lane,size,sample});
    }}}
    if(directStarts!==1||llmAttempts!==0||invalidLogs!==0)throw new Error("Trace execution or logging invariant failed");
    phase="complete";
  }catch(error){failure=error;emit("trace_failure",{phase,errorName:(error as Error)?.name});}
  finally{
    if(clientSocket){const socket=clientSocket;await new Promise<void>(resolve=>{const timer=setTimeout(()=>{socket.terminate();resolve();},2000);socket.once("close",()=>{clearTimeout(timer);resolve();});socket.close();});}
    await system?.close().catch(error=>{failure??=error;});await session?.close().catch(error=>{failure??=error;});
    if(createAttempted){let cleanup:PrismSessionManager|null=null;try{
      cleanup=await PrismSessionManager.create({origin,mode:"authenticated",allowAnonymous:false,cookieFile});const identity=await cleanup.resolve(AbortSignal.timeout(30000));
      await deleteOwnedToolTraceProject(new PrismProjectsClient(identity.account),id,title);
      report.deleted=true;
    }catch(error){failure??=error;report.deleted=false;}finally{await cleanup?.close().catch(()=>{});}}
    for(const file of [cookieFile,`${cookieFile}.identity.json`]) {
      const relative=path.relative(directory,file);
      if(!relative || relative.startsWith("..") || path.isAbsolute(relative))throw new Error("Trace cleanup path escaped its directory");
      await rm(file,{force:true}).catch(error=>{failure??=error;});
    }
    handles.clear();currentTools.clear();currentText=null;
  }
  report.phase=phase;report.success=failure===null;report.directStarts=directStarts;report.llmAttempts=llmAttempts;report.invalidLogs=invalidLogs;
  report.summaries=traces.map(summary);report.traces=traces.map(({start,diagnostics,usage,...rest})=>({...rest,requestOffsetMs:start-(traces[0]?.start??start)}));
  await writeFile(path.join(directory,"report.json"),JSON.stringify(report,null,2));
  emit("trace_report",{success:report.success,phase,batching,receiptCoalescing,requests:traces.length,directStarts,llmAttempts,deleted:report.deleted,reportFile:path.join(directory,"report.json")});
  if(failure)process.exitCode=1;
}

if (import.meta.main) await main();
