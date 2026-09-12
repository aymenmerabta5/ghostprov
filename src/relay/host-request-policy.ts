import { normalizeHostUpstreamResponsesRequest } from "./host-upstream-policy";
import { applyUpstreamModelPolicy } from "./model-policy";

/** Apply host routing policy before continuation identity and canonical bytes. */
export function prepareHostUpstreamResponsesRequest<
  const Request extends Readonly<Record<string, unknown>>,
>(body: Request) {
  return normalizeHostUpstreamResponsesRequest(applyUpstreamModelPolicy(body));
}
