import type { MultiSandboxRouter } from "../router/router";
import type {
  StandaloneSearchHandler,
  StandaloneSearchResponse,
} from "../http/standalone-search";
import {
  buildStandaloneSearchResponsesRequest,
  standaloneSearchResponseFromFinal,
} from "./responses-adapter";
import {
  releaseAtomicBytePermit,
  transferAtomicBytePermit,
} from "../v2/memory/permit-carrier";
import { StandaloneSearchSessionIndex } from "./session-index";

/**
 * Bind the public unary Codex contract to the shared exact-once fleet. Only
 * content-free provider lineage is retained; no public SSE surface is exposed.
 */
export function createRouterStandaloneSearchHandler(
  router: MultiSandboxRouter,
  sessions = new StandaloneSearchSessionIndex(),
): StandaloneSearchHandler & { clear(): void } {
  const handler: StandaloneSearchHandler = async (body, context) => {
    const execute = async (previousResponseId: string | null) => {
      const incomingLease = context.transferIncomingLease();
      const responsesBody = buildStandaloneSearchResponsesRequest(body, previousResponseId);
      const handle = await router.startRelay(responsesBody, {
        clientId: context.clientId,
        source: "internal",
        signal: context.signal,
        incomingLease,
        authorizationScope: context.authorizationScope ?? undefined,
      });

      // V2 gives the verified terminal its own memory reference. Final-only
      // callers explicitly discard the event copy so a never-read SSE queue
      // cannot retain capacity.
      let drained = Promise.resolve();
      if (handle.discardUnconsumedEvents) handle.discardUnconsumedEvents();
      else {
        drained = (async () => {
          for await (const frame of handle.events()) releaseAtomicBytePermit(frame);
        })();
      }

      let final: Record<string, unknown> | null = null;
      try {
        // Observe and own the permit-bearing final before awaiting the optional
        // prelude. If a malformed handle rejects its prelude after settling a
        // final, the finally block must still release that final allocation.
        final = await handle.final as Record<string, unknown>;
        const prelude = await (
          handle.upstreamPrelude ?? Promise.resolve({ status: 200 })
        );
        await drained;
        const responseId = typeof final.id === "string" ? final.id : "";
        const response = standaloneSearchResponseFromFinal(final) as StandaloneSearchResponse;
        transferAtomicBytePermit(final, response, "http-alpha-search-json");
        final = null;
        return {
          responseId,
          value: {
            kind: "success" as const,
            status: prelude.status,
            body: response,
          },
        };
      } finally {
        releaseAtomicBytePermit(final);
      }
    };

    if (context.authorizationScope && body.id) {
      return sessions.run(context.authorizationScope, body.id, execute);
    }
    return (await execute(null)).value;
  };
  return Object.assign(handler, { clear: () => sessions.clear() });
}
