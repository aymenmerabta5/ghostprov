export type NormalizedUpstreamResponsesRequest<
  Request extends Readonly<Record<string, unknown>>,
> = Omit<Request, "stream" | "store"> & {
  readonly stream: true;
  readonly store: true;
};

/**
 * Applies the invariant used at every upstream Responses boundary without
 * changing client-facing stream semantics or any continuation metadata.
 */
export function normalizeUpstreamResponsesRequest<
  const Request extends Readonly<Record<string, unknown>>,
>(body: Request): NormalizedUpstreamResponsesRequest<Request> {
  if (
    Object.hasOwn(body, "stream") &&
    body.stream === true &&
    Object.hasOwn(body, "store") &&
    body.store === true
  ) {
    return body as unknown as NormalizedUpstreamResponsesRequest<Request>;
  }
  return {
    ...body,
    stream: true,
    store: true,
  } as NormalizedUpstreamResponsesRequest<Request>;
}
