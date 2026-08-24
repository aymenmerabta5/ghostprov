export type AnthropicModelOptions = {
  opus: string;
  sonnet: string;
  haiku: string;
  fallback: string;
  passthrough: ReadonlySet<string>;
};

const DEFAULT_MODELS = Object.freeze({
  opus: "gpt-5.6-sol",
  sonnet: "gpt-5.6-terra",
  haiku: "gpt-5.6-luna",
  fallback: "gpt-5.6-sol",
});

const NATIVE_MODELS = Object.freeze([
  "gpt-5.6-sol",
  "gpt-5.6-terra",
  "gpt-5.6-luna",
  "gpt-5.5",
]);

type AnthropicModelEnvironment = Record<string, string | undefined>;

function configured(value: string | undefined, fallback: string): string {
  return value?.trim() || fallback;
}

export function anthropicModelsFromEnv(
  environment: AnthropicModelEnvironment = process.env,
): AnthropicModelOptions {
  const opus = configured(environment.GP_ANTHROPIC_OPUS_MODEL, DEFAULT_MODELS.opus);
  const sonnet = configured(environment.GP_ANTHROPIC_SONNET_MODEL, DEFAULT_MODELS.sonnet);
  const haiku = configured(environment.GP_ANTHROPIC_HAIKU_MODEL, DEFAULT_MODELS.haiku);
  const fallback = configured(
    environment.GP_ANTHROPIC_DEFAULT_MODEL,
    DEFAULT_MODELS.fallback,
  );
  return {
    opus,
    sonnet,
    haiku,
    fallback,
    passthrough: new Set([...NATIVE_MODELS, opus, sonnet, haiku, fallback]),
  };
}

export function resolveAnthropicModel(requested: string, options: AnthropicModelOptions): string {
  if (options.passthrough.has(requested)) return requested;

  const value = requested.toLowerCase();
  if (value.includes("opus")) return options.opus;
  if (value.includes("sonnet")) return options.sonnet;
  if (value.includes("haiku")) return options.haiku;
  return options.fallback;
}
