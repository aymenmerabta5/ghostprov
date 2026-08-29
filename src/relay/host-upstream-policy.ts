import type { NormalizedUpstreamResponsesRequest } from "./upstream-policy";

/**
 * Host-side upstream boundary. In addition to the frozen R8 value invariant,
 * require both policy fields to be enumerable so canonical JSON cannot omit
 * accessor- or descriptor-created true values.
 */
export function normalizeHostUpstreamResponsesRequest<
  const Request extends Readonly<Record<string, unknown>>,
>(body: Request): NormalizedUpstreamResponsesRequest<Request> {
  const stream = Object.getOwnPropertyDescriptor(body, "stream");
  const store = Object.getOwnPropertyDescriptor(body, "store");
  if (
    stream?.enumerable === true &&
    "value" in stream &&
    stream.value === true &&
    store?.enumerable === true &&
    "value" in store &&
    store.value === true
  ) {
    return body as unknown as NormalizedUpstreamResponsesRequest<Request>;
  }
  return {
    ...body,
    stream: true,
    store: true,
  } as NormalizedUpstreamResponsesRequest<Request>;
}
