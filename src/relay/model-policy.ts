export const FALLBACK_UPSTREAM_MODEL = "gpt-5.6-terra";

/** Bound the model metadata retained after the original body is released. */
export function canFallbackFromModel(model: unknown): model is string {
  return typeof model === "string" && model.length > 0 && model.length <= 128 &&
    model !== FALLBACK_UPSTREAM_MODEL;
}

/** Resolve the supported upstream target for a client-facing model alias. */
export function resolveUpstreamModelAlias(model: string): string {
  return model === "gpt-5.6-luna" ? FALLBACK_UPSTREAM_MODEL : model;
}

/** Preserve caller ownership and every field except the exact model alias. */
export function applyUpstreamModelPolicy<
  const Request extends Readonly<Record<string, unknown>>,
>(body: Request): Request | (Omit<Request, "model"> & { readonly model: "gpt-5.6-terra" }) {
  if (body.model !== "gpt-5.6-luna") return body;
  return { ...body, model: FALLBACK_UPSTREAM_MODEL };
}
