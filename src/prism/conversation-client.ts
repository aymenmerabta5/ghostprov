import type { ProjectId } from "../sandbox/types";
import type { AccountRequestClient } from "./provisioning-client";

const CREATE_PROJECT_CONVERSATION_ACTION = "60f6ef46a6584bd2320328016a144c15d9401a47e1";
const MAX_ACTION_RESPONSE_BYTES = 256 * 1024;
const CONVERSATION_ID = /^cdx[0-9]+_[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export class PrismConversationError extends Error {
  readonly name = "PrismConversationError";

  constructor(
    message: string,
    readonly kind: "transport" | "action" | "protocol",
    readonly status: number | null = null,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

async function boundedText(response: Response): Promise<string> {
  const reader = response.body?.getReader();
  if (!reader) return "";
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_ACTION_RESPONSE_BYTES) {
        await reader.cancel().catch(() => undefined);
        throw new PrismConversationError("Conversation action response exceeds its bound", "protocol", response.status);
      }
      chunks.push(value.slice());
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}

export async function createProjectConversation(
  account: AccountRequestClient,
  projectId: ProjectId,
  signal?: AbortSignal,
): Promise<string> {
  let response: Response;
  try {
    response = await account.request("/", {
      method: "POST",
      signal,
      headers: {
        accept: "text/x-component",
        "content-type": "text/plain;charset=UTF-8",
        "next-action": CREATE_PROJECT_CONVERSATION_ACTION,
      },
      body: JSON.stringify([projectId]),
    });
  } catch (cause) {
    if (signal?.aborted) throw signal.reason;
    throw new PrismConversationError("Unable to create a Prism project conversation", "transport", null, { cause });
  }
  const body = await boundedText(response);
  if (!response.ok) {
    throw new PrismConversationError("Prism conversation action was rejected", "action", response.status);
  }
  if (!response.headers.get("content-type")?.toLowerCase().startsWith("text/x-component")) {
    throw new PrismConversationError("Prism conversation action returned an unexpected content type", "protocol", response.status);
  }
  const matches = [...body.matchAll(/(?:^|\n)[0-9]+:"(cdx[0-9]+_[0-9a-f-]{36})"(?=\r?\n|$)/gi)]
    .map(match => match[1]!)
    .filter((value, index, all) => all.indexOf(value) === index);
  if (matches.length !== 1 || !CONVERSATION_ID.test(matches[0]!)) {
    throw new PrismConversationError("Prism conversation action returned no unique conversation", "protocol", response.status);
  }
  return matches[0]!;
}
