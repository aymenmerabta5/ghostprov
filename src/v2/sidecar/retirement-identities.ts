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

/** Released diagnostics/liveness build before progress-aware response admission. */
export const V2_R10_9D2AA8B7_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "9d2aa8b7aecefcad429dff4a1f7fb0707b95dec9abe0beac4fb6d77ad70e5b7b",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "21c83c16d210b3208a1cb097df2363f4690fac705ae6d1ccc71e143805854121",
});

/** Progress-aware admission build before cleanup and ACK-publication repairs. */
export const V2_R10_16847357_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "16847357a8ee0c930d4d15e433e3004216f0e8645fdcb94df76a39a4c251f01a",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "1ad4b7d3101cdc6686496b885bfeb593ddc0b7c1886a2fbca2633a1b37129131",
});

/** Deployed ACK/cleanup build before local publication and receive recovery. */
export const V2_R10_FE8BBF0D_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "fe8bbf0d0b897f227db7b7516871ec2c4f70ae82fa219d85fd316608d176171f",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "f05a5a0d052adc3273821e8a48542361fe579e22f2520939501780c14a594ef7",
});

/** Verified receive-recovery candidate before the diagnostic size fallback. */
export const V2_R10_BC95E5CC_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "bc95e5cc2f396e2f5a50e4391df081c8c5d36a91ef1cd065c72a4770ea4489c3",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "f564dab7d707332f8d51c7da066293b82b0d5fbdc3b7b503131b641bcf844fec",
});

/** Receive-recovery build retained before native CONTROL mirror repair. */
export const V2_R10_9879DDF9_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "9879ddf958329183173b0bf027430e1b75e3cade814fd3dd10688e287e20f1ec",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "36637c8dca035f0e1d99d13920df6902a35ce7a2b54004c2dff623b0916707c4",
});

/** Verified CONTROL-repair candidate before refreshed socket endpoint selection. */
export const V2_R10_A0A96FC5_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "a0a96fc516227d22babc5c8eb91e73f851e36b28265d757429ca51cbeed5f96b",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "b60affedddc1a232ac096f0f2cb0edf18509cd6a186838f7fc4cd51256506594",
});

/** Endpoint-refresh release before prepared retention and provider-error repairs. */
export const V2_R10_395D4FC1_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "395d4fc17f7c2fe24b14722e35f30a94de4064e43081e36f8433956abac81215",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "62cca7d042e435fe95a7de43a0ba8d24c5c25b83f951f94c05f327f4771d5ec0",
});

/** Prepared-retention candidate superseded while restoring frozen R8/R9 builds. */
export const V2_R10_9D383779_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "9d383779641474994275f797c50fd9f16d22910b55aa383cac47dc72b2856928",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "4637d1d320cbea8fed305610e124510d6ce9040531b25b4662526a0d7f0426cf",
});

/** R10 provider-repair candidate before the final normalization exception guard. */
export const V2_R10_F1982EF3_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "f1982ef38a39b398566d89f904b0fd00ec048b0c921bee9d1e2b43cdd2e82a8a",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "639da28d5a4a017dfaee73e4afced904c82cd4bb426113037174c50168dee087",
});

/** Prepared/error-repair release before the 60-second bounded admission gate. */
export const V2_R10_8B9E7096_RETIREMENT_IDENTITY = Object.freeze({
  revision: "r10" as const,
  protocolRevision: "r2" as const,
  transportProfile: "bulk-v1" as const,
  buildHash: "8b9e7096f216496944d67c91e2f1b884c5e0c27bf44e47b6a3215f56e3946745",
  schemaHash: "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f",
  bundleSha256: "e0ab30f88d608a74450ae3f3b7942ca76d308d7f79297ee77a70cb17fb0f53c5",
});

export const ARCHIVED_R10_RELEASE_IDENTITIES = Object.freeze([
  V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY,
  V2_R10_C1091742_RETIREMENT_IDENTITY,
  V2_R10_9D2AA8B7_RETIREMENT_IDENTITY,
  V2_R10_16847357_RETIREMENT_IDENTITY,
  V2_R10_FE8BBF0D_RETIREMENT_IDENTITY,
  V2_R10_BC95E5CC_RETIREMENT_IDENTITY,
  V2_R10_9879DDF9_RETIREMENT_IDENTITY,
  V2_R10_A0A96FC5_RETIREMENT_IDENTITY,
  V2_R10_395D4FC1_RETIREMENT_IDENTITY,
  V2_R10_9D383779_RETIREMENT_IDENTITY,
  V2_R10_F1982EF3_RETIREMENT_IDENTITY,
  V2_R10_8B9E7096_RETIREMENT_IDENTITY,
]);
