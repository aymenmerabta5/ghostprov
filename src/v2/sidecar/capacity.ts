/** Maximum canonical transcript reserved for every prepared/active request. */
export const V2_TERMINAL_SPOOL_RESERVATION_BYTES = 64 * 1024 * 1024;

/** Proven aggregate terminal-spool quota for one sandbox sidecar. */
export const V2_TERMINAL_SPOOL_TOTAL_BYTES = 512 * 1024 * 1024;

/** Every prepared request owns one full worst-case spool reservation. */
export const V2_TERMINAL_SPOOL_CAPACITY = Math.floor(
  V2_TERMINAL_SPOOL_TOTAL_BYTES / V2_TERMINAL_SPOOL_RESERVATION_BYTES,
);
