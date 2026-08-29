function usesImageGeneration(body: Readonly<Record<string, unknown>>): boolean {
  if (typeof body.model === "string" && body.model.startsWith("gpt-image-")) return true;
  if (!Array.isArray(body.tools)) return false;
  return body.tools.some(tool => (
    tool !== null &&
    typeof tool === "object" &&
    !Array.isArray(tool) &&
    (tool as Record<string, unknown>).type === "image_generation"
  ));
}

/**
 * Applies the product-wide paid fast-tier policy without sending unsupported
 * service-tier fields to image generation. It is idempotent and copy-on-write
 * so both router and direct-runtime verification paths can enforce it safely.
 */
export function applyUpstreamServiceTierPolicy<
  const Request extends Readonly<Record<string, unknown>>,
>(body: Request): Request | Omit<Request, "service_tier"> |
  (Omit<Request, "service_tier"> & { readonly service_tier: "fast" }) {
  if (usesImageGeneration(body)) {
    if (!("service_tier" in body)) return body;
    const { service_tier: _unsupportedImageTier, ...withoutImageTier } = body;
    return withoutImageTier as Omit<Request, "service_tier">;
  }
  if (Object.hasOwn(body, "service_tier") && body.service_tier === "fast") return body;
  return {
    ...body,
    service_tier: "fast",
  } as Omit<Request, "service_tier"> & { readonly service_tier: "fast" };
}
