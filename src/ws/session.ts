import type { WebSocket } from "ws";
import type { RunHandle } from "../relay/run-handle";
import { config } from "../config";
import {
  PreviousResponseNotFoundError,
  ResponseGraph,
  type ResponseCreateEvent,
} from "./response-graph";

export type WebSocketRelayStarter = (
  body: Record<string, unknown>,
  options: { signal: AbortSignal },
) => Promise<RunHandle>;

export interface ResponsesWebSocketSessionOptions {
  startRelay: WebSocketRelayStarter;
  maxActive?: number;
  maxNamedStreams?: number;
  maxBufferedBytes?: number;
  graph?: ResponseGraph;
}

interface SemaphoreWaiter {
  resolve: (release: () => void) => void;
  reject: (error: unknown) => void;
  signal: AbortSignal;
  onAbort: () => void;
}

class AsyncSemaphore {
  private active = 0;
  private readonly waiters: SemaphoreWaiter[] = [];

  constructor(private readonly capacity: number) {}

  acquire(signal: AbortSignal): Promise<() => void> {
    if (signal.aborted) return Promise.reject(new DOMException("WebSocket request aborted", "AbortError"));
    return new Promise((resolve, reject) => {
      const waiter = {
        resolve,
        reject,
        signal,
        onAbort: () => {
          const index = this.waiters.indexOf(waiter);
          if (index === -1) return;
          this.waiters.splice(index, 1);
          reject(new DOMException("WebSocket request aborted", "AbortError"));
        },
      } satisfies SemaphoreWaiter;
      signal.addEventListener("abort", waiter.onAbort, { once: true });
      this.waiters.push(waiter);
      this.drain();
    });
  }

  private drain(): void {
    while (this.active < this.capacity && this.waiters.length) {
      const waiter = this.waiters.shift()!;
      waiter.signal.removeEventListener("abort", waiter.onAbort);
      if (waiter.signal.aborted) {
        waiter.reject(new DOMException("WebSocket request aborted", "AbortError"));
        continue;
      }
      this.active++;
      let released = false;
      waiter.resolve(() => {
        if (released) return;
        released = true;
        this.active--;
        this.drain();
      });
    }
  }
}

function errorEvent(
  code: string,
  message: string,
  options: { streamId?: string; param?: string; status?: number } = {},
): Record<string, unknown> {
  return {
    type: "error",
    status: options.status ?? 400,
    ...(options.streamId ? { stream_id: options.streamId } : {}),
    error: {
      type: "invalid_request_error",
      code,
      message,
      param: options.param ?? null,
    },
  };
}

function isNamedStreamId(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_.-]{1,256}$/.test(value);
}

function parseEventData(data: string): Record<string, unknown> {
  const parsed = JSON.parse(data);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("upstream event data is not a JSON object");
  }
  return parsed as Record<string, unknown>;
}

export class ResponsesWebSocketSession {
  private readonly startRelay: WebSocketRelayStarter;
  private readonly maxNamedStreams: number;
  private readonly maxBufferedBytes: number;
  private readonly graph: ResponseGraph;
  private readonly semaphore: AsyncSemaphore;
  private readonly namedStreams = new Set<string>();
  private readonly laneTails = new Map<string, Promise<void>>();
  private readonly controllers = new Set<AbortController>();
  private sendTail = Promise.resolve();
  private closed = false;

  constructor(private readonly socket: WebSocket, options: ResponsesWebSocketSessionOptions) {
    this.startRelay = options.startRelay;
    this.maxNamedStreams = options.maxNamedStreams ?? config.wsNamedStreams;
    this.maxBufferedBytes = options.maxBufferedBytes ?? 1024 * 1024;
    this.graph = options.graph ?? new ResponseGraph();
    this.semaphore = new AsyncSemaphore(options.maxActive ?? config.wsActiveResponses);
  }

  receive(raw: string): void {
    if (this.closed) return;
    let event: ResponseCreateEvent;
    try {
      event = JSON.parse(raw);
    } catch {
      this.send(errorEvent("invalid_json", "Client event must be valid JSON"));
      return;
    }
    const validation = this.validate(event);
    if (validation) {
      this.send(validation);
      return;
    }

    const lane = event.stream_id ?? "";
    if (event.stream_id) this.namedStreams.add(event.stream_id);
    const previous = this.laneTails.get(lane) ?? Promise.resolve();
    const task = previous.catch(() => undefined).then(() => this.execute(event));
    this.laneTails.set(lane, task);
    void task.finally(() => {
      if (this.laneTails.get(lane) === task) this.laneTails.delete(lane);
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const controller of this.controllers) controller.abort();
    this.controllers.clear();
    this.graph.clear();
    this.laneTails.clear();
  }

  private validate(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return errorEvent("invalid_request", "Client event must be a JSON object");
    }
    const event = value as ResponseCreateEvent;
    const streamId = typeof event.stream_id === "string" ? event.stream_id : undefined;
    if (event.type !== "response.create") {
      return errorEvent("invalid_event_type", "Only response.create events are supported", { streamId, param: "type" });
    }
    if (event.stream_id !== undefined && !isNamedStreamId(event.stream_id)) {
      return errorEvent("invalid_stream_id", "stream_id must be 1-256 letters, numbers, underscores, hyphens, or periods", {
        streamId,
        param: "stream_id",
      });
    }
    if (event.background != null) {
      return errorEvent("unsupported_parameter", "background is not supported in WebSocket mode", { streamId, param: "background" });
    }
    if (event.stream !== undefined) {
      return errorEvent(
        "unsupported_parameter",
        "stream is implicit and must not be sent in WebSocket mode",
        { streamId, param: "stream" },
      );
    }
    if (streamId && !this.namedStreams.has(streamId) && this.namedStreams.size >= this.maxNamedStreams) {
      return errorEvent("websocket_stream_limit_reached", "This WebSocket connection has reached its stream limit.", {
        streamId,
        param: "stream_id",
      });
    }
    return null;
  }

  private async execute(event: ResponseCreateEvent): Promise<void> {
    const controller = new AbortController();
    this.controllers.add(controller);
    let release: (() => void) | undefined;
    try {
      release = await this.semaphore.acquire(controller.signal);
      if (event.generate === false) {
        this.completeWarmup(event);
        return;
      }
      const resolved = this.graph.resolve(event);
      const handle = await this.startRelay(resolved.relayBody, { signal: controller.signal });
      let sawTerminal = false;
      for await (const frame of handle.events()) {
        const outbound = this.graph.sanitizeOutbound(parseEventData(frame.data), resolved);
        if (event.stream_id) (outbound as Record<string, unknown>).stream_id = event.stream_id;
        const type = (outbound as Record<string, unknown>).type;
        if (type === "response.completed" || type === "response.failed" || type === "response.incomplete") {
          sawTerminal = true;
        }
        this.send(outbound);
      }
      const final = await handle.final;
      this.graph.record(resolved, final);
      if (!sawTerminal) {
        this.send(this.graph.sanitizeOutbound({
          type: "response.completed",
          response: final,
          ...(event.stream_id ? { stream_id: event.stream_id } : {}),
        }, resolved));
      }
    } catch (error: any) {
      if (this.closed || error?.name === "AbortError") return;
      const streamId = event.stream_id;
      if (error instanceof PreviousResponseNotFoundError) {
        this.send(errorEvent(error.code, error.message, { streamId, param: "previous_response_id" }));
      } else {
        this.send(errorEvent(error?.code ?? "websocket_request_failed", String(error?.message ?? error), {
          streamId,
          status: 500,
        }));
      }
    } finally {
      release?.();
      this.controllers.delete(controller);
    }
  }

  private completeWarmup(event: ResponseCreateEvent): void {
    const id = `resp_gp_warmup_${crypto.randomUUID().replaceAll("-", "")}`;
    const resolved = this.graph.resolve({ ...event, generate: true });
    const final = {
      id,
      object: "response",
      created_at: Math.floor(Date.now() / 1000),
      status: "completed",
      model: event.model ?? null,
      output: [],
      store: event.store ?? true,
    };
    this.graph.record(resolved, final);
    this.send(this.graph.sanitizeOutbound({
      type: "response.completed",
      response: final,
      ...(event.stream_id ? { stream_id: event.stream_id } : {}),
    }, resolved));
  }

  private send(value: unknown): void {
    this.sendTail = this.sendTail.then(async () => {
      if (this.closed || this.socket.readyState !== this.socket.OPEN) return;
      while (this.socket.bufferedAmount > this.maxBufferedBytes) {
        await new Promise(resolve => setTimeout(resolve, 5));
        if (this.closed || this.socket.readyState !== this.socket.OPEN) return;
      }
      await new Promise<void>((resolve, reject) => {
        this.socket.send(JSON.stringify(value), error => error ? reject(error) : resolve());
      });
    }).catch(() => undefined);
  }
}
