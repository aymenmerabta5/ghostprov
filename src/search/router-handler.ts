import type { MultiSandboxRouter } from "../router/router";
import type {
  StandaloneSearchHandler,
  StandaloneSearchHandlerResult,
  StandaloneSearchResponse,
} from "../http/standalone-search";
import { validateStandaloneSearchSuccess } from "../http/standalone-search";
import {
  buildStandaloneSearchResponsesRequest,
  StandaloneSearchAdapterError,
  standaloneSearchResponseFromFinal,
} from "./responses-adapter";
import {
  atomicBytePermitOf,
  releaseAtomicBytePermit,
  transferAtomicBytePermit,
} from "../v2/memory/permit-carrier";
import {
  StandaloneSearchSessionIndex,
  type StandaloneSearchSessionOutcome,
} from "./session-index";
import { ResponseProvenanceObsoleteError } from "../router/provenance";
import { incomingLeaseTimingView } from "../http/request-lease";
import { v2RetainedEventPermitBytes } from "../v2/memory/event-memory-sizing";
import { definitivePreviousResponseNotFound } from "../relay/continuation-error";

function literalHttpReference(value: string): boolean {
  if (!/^https?:\/\//i.test(value)) return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function canExecuteWithoutSearchLineage(
  commands: Parameters<StandaloneSearchHandler>[0]["commands"],
): boolean {
  if (!commands) return true;
  if (commands.click !== undefined) return false;
  return [commands.open, commands.find, commands.screenshot].every(items => (
    items === undefined || items.every(item => literalHttpReference(item.ref_id))
  ));
}

function expiredReferenceFailure(): StandaloneSearchHandlerResult {
  return {
    kind: "failure",
    status: 400,
    body: {
      error: {
        message: "Standalone search reference expired; run a fresh search query before using this reference.",
        type: "invalid_request_error",
        param: "commands",
        code: "search_reference_expired",
      },
    },
  };
}

/**
 * Bind the public unary Codex contract to the shared exact-once fleet. Only
 * content-free provider lineage is retained; no public SSE surface is exposed.
 */
export function createRouterStandaloneSearchHandler(
  router: MultiSandboxRouter,
  sessions = new StandaloneSearchSessionIndex(),
): StandaloneSearchHandler & { clear(): void; beginShutdown(): void } {
  const handler: StandaloneSearchHandler = async (body, context) => {
    let incomingLeaseTransferred = false;
    let incomingLease = context.incomingLease;
    let incomingLeaseOwned = false;
    const loadIncomingLease = () => {
      if (!incomingLeaseTransferred) {
        incomingLeaseTransferred = true;
        incomingLease = context.transferIncomingLease();
        incomingLeaseOwned = incomingLease !== undefined;
      }
      return incomingLease;
    };
    const handoffIncomingLease = () => {
      const lease = loadIncomingLease();
      incomingLeaseOwned = false;
      incomingLease = undefined;
      return lease;
    };
    const releaseIncomingLease = () => {
      if (!incomingLeaseOwned) return;
      incomingLeaseOwned = false;
      incomingLease?.release();
      incomingLease = undefined;
    };
    const execute = async (
      previousResponseId: string | null,
      recoverObsolete?: (expectedResponseId: string) => boolean,
    ): Promise<StandaloneSearchSessionOutcome<StandaloneSearchHandlerResult>> => {
      const responsesBody = buildStandaloneSearchResponsesRequest(body, previousResponseId);
      const retainsLeaseForRecovery = previousResponseId !== null && recoverObsolete !== undefined;
      const attemptIncomingLease = retainsLeaseForRecovery
        ? incomingLeaseTimingView(loadIncomingLease())
        : handoffIncomingLease();
      let handle;
      try {
        handle = await router.startRelay(responsesBody, {
          clientId: context.clientId,
          source: "internal",
          signal: context.signal,
          incomingLease: attemptIncomingLease,
          authorizationScope: context.authorizationScope ?? undefined,
        });
        // Admission is complete, but a cached attempt retains the original
        // body allocation until its terminal so a safe lineage fallback never
        // runs with unaccounted request memory.
        attemptIncomingLease?.completeAdmission?.();
        incomingLease?.completeAdmission?.();
      } catch (error) {
        if (
          previousResponseId !== null &&
          error instanceof ResponseProvenanceObsoleteError &&
          recoverObsolete?.(previousResponseId)
        ) {
          // startRelay rejected before returning a handle, so no provider work
          // can have begun. The retry deliberately receives no recovery hook.
          if (!canExecuteWithoutSearchLineage(body.commands)) {
            releaseIncomingLease();
            return { responseId: "", value: expiredReferenceFailure() };
          }
          return execute(null);
        }
        releaseIncomingLease();
        throw error;
      }
      if (!retainsLeaseForRecovery) releaseIncomingLease();

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
        const validated = await validateStandaloneSearchSuccess(
          prelude.status,
          response,
          context.signal,
        );
        const finalPermit = atomicBytePermitOf(final);
        if (finalPermit) {
          if (finalPermit.bytes < v2RetainedEventPermitBytes(validated.encodedBytes)) {
            throw new StandaloneSearchAdapterError(
              "Standalone search projection exceeds its retained memory permit",
            );
          }
        }
        transferAtomicBytePermit(final, validated.body, "http-alpha-search-json");
        final = null;
        return {
          responseId,
          value: {
            kind: "success" as const,
            status: validated.status,
            body: validated.body,
          },
        };
      } catch (error) {
        if (
          previousResponseId !== null &&
          final === null &&
          handle.stats.frames === 0 &&
          definitivePreviousResponseNotFound(error) &&
          recoverObsolete?.(previousResponseId)
        ) {
          // A definitive initial HTTP rejection has no relay event and cannot
          // have produced a child response. Retry only the current rootable
          // search request; opaque refs have no safe reconstruction.
          if (!canExecuteWithoutSearchLineage(body.commands)) {
            releaseIncomingLease();
            return { responseId: "", value: expiredReferenceFailure() };
          }
          return execute(null);
        }
        throw error;
      } finally {
        releaseAtomicBytePermit(final);
      }
    };

    try {
      if (context.authorizationScope && body.id) {
        return await sessions.run(
          context.authorizationScope,
          body.id,
          (previousResponseId, compareDelete) =>
            execute(previousResponseId, compareDelete),
        );
      }
      return (await execute(null)).value;
    } finally {
      releaseIncomingLease();
    }
  };
  return Object.assign(handler, {
    clear: () => sessions.clear(),
    beginShutdown: () => sessions.beginShutdown(),
  });
}
