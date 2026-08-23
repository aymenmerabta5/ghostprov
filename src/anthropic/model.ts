export type AnthropicModelOptions = {
  opus: string;
  sonnet: string;
  haiku: string;
  fallback: string;
  passthrough: ReadonlySet<string>;
};

export function resolveAnthropicModel(requested: string, options: AnthropicModelOptions): string {
  if (options.passthrough.has(requested)) return requested;

  const value = requested.toLowerCase();
  if (value.includes("opus")) return options.opus;
  if (value.includes("sonnet")) return options.sonnet;
  if (value.includes("haiku")) return options.haiku;
  return options.fallback;
}
