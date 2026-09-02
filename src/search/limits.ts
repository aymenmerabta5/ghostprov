/**
 * Codex can attach substantial completed-turn context to a search call. Keep
 * the request below the shared 64 MiB parser ceiling while allowing the same
 * multi-megabyte real-world histories accepted by the Responses path.
 */
export const STANDALONE_SEARCH_MAX_REQUEST_BYTES = 16 * 1024 * 1024;
export const STANDALONE_SEARCH_MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
