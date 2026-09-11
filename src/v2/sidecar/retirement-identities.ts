/**
 * Archived immutable R10 identities only. This module may be bundled into a
 * successor: it must never import or embed that successor's own build hash.
 */
export const V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "ce9f48171863bdaa0adcd1a6344ec175502dd1ef9d1b9fae9e4a512a1474c32a",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "a808969a1e89db0ec9636915a667613246c17fe5829b877aab0e8f192adc9776",
});

/** First retirement-publication candidate, retained after live validation. */
export const V2_R10_C1091742_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "c1091742ecd1605a22cd5cc68745df56411570adc9317962c261c73cb7222099",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "a10809f86d2da82073d08775a453952f0b233d7ac89767647853eaafd9402642",
});

export const ARCHIVED_R10_RELEASE_IDENTITIES = Object.freeze([
  V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY,
  V2_R10_C1091742_RETIREMENT_IDENTITY,
]);
