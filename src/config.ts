function integer(v: string | undefined, d: number): number {
  const n = Number(v);
  return Number.isInteger(n) && n > 0 ? n : d;
}

export const CONFIG_DEFAULTS = {
  wsActiveResponses: 16,
  wsNamedStreams: 32,
} as const;

export const config = {
  wsActiveResponses: integer(process.env.GP_WS_ACTIVE_RESPONSES, CONFIG_DEFAULTS.wsActiveResponses),
  wsNamedStreams: integer(process.env.GP_WS_NAMED_STREAMS, CONFIG_DEFAULTS.wsNamedStreams),
} as const;
