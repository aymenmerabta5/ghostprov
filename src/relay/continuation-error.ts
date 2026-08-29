import { providerErrorEnvelopeFrom } from "../v2/provider-error";
import { V2CommittedError } from "../v2/transport/run-handle";

/**
 * The only provider rejection that proves an inferred stored parent did not
 * execute work. Callers must additionally prove no relay/public event escaped.
 */
export function definitivePreviousResponseNotFound(error: unknown): boolean {
  if (!(error instanceof V2CommittedError) || error.ambiguous !== false) return false;
  const provider = providerErrorEnvelopeFrom(error);
  return provider !== null && provider.origin === "http" &&
    (provider.status === 400 || provider.status === 404) &&
    provider.error.code === "previous_response_not_found" &&
    (provider.error.param === null || provider.error.param === "previous_response_id");
}
