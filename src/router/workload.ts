export type WorkloadClass =
  | "text-small"
  | "text-large"
  | "image-generation"
  | "image-edit";

export interface WorkloadDescriptor {
  class: WorkloadClass;
  requestBytes: number;
  reservedBytes: number;
  cpuPermits: 0 | 1;
  stream: boolean;
  serializedBody: string;
  model?: string;
}

export interface WorkloadOptions {
  encodedBytes?: number;
  serializedBody?: string;
}

const MiB = 1024 ** 2;

function containsInputImage(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(containsInputImage);
  const record = value as Record<string, unknown>;
  if (record.type === "input_image") return true;
  return Object.values(record).some(containsInputImage);
}

export function classifyWorkload(
  body: Record<string, unknown>,
  options: WorkloadOptions = {},
): WorkloadDescriptor {
  const serializedBody = options.serializedBody ?? JSON.stringify(body);
  const requestBytes = options.encodedBytes ?? Buffer.byteLength(serializedBody, "utf8");
  const tools = Array.isArray(body.tools) ? body.tools : [];
  const imageTool = tools.find(tool => tool && typeof tool === "object" && (tool as any).type === "image_generation") as Record<string, unknown> | undefined;
  const stream = body.stream === true;
  let workloadClass: WorkloadClass;
  let reservedBytes: number;
  let cpuPermits: 0 | 1;

  if (imageTool && (imageTool.action === "edit" || containsInputImage(body.input))) {
    workloadClass = "image-edit";
    reservedBytes = Math.max(64 * MiB, requestBytes * 3 + 32 * MiB);
    cpuPermits = 1;
  } else if (imageTool) {
    workloadClass = "image-generation";
    reservedBytes = Math.max(24 * MiB, requestBytes * 3 + 16 * MiB);
    cpuPermits = requestBytes > 512 * 1024 ? 1 : 0;
  } else if (requestBytes > 512 * 1024) {
    workloadClass = "text-large";
    reservedBytes = requestBytes * 6 + (stream ? 4 * MiB : 2 * MiB);
    cpuPermits = 1;
  } else {
    workloadClass = "text-small";
    reservedBytes = Math.max(8 * MiB, requestBytes * 4 + (stream ? 2 * MiB : MiB));
    cpuPermits = 0;
  }

  return {
    class: workloadClass,
    requestBytes,
    reservedBytes: Math.ceil(reservedBytes),
    cpuPermits,
    stream,
    serializedBody,
    model: typeof body.model === "string" ? body.model : undefined,
  };
}
