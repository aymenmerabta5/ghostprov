/**
 * Synchronous backing for a bounded in-process byte budget.
 *
 * Owners must grow the reservation before retaining bytes and shrink it only
 * after releasing those bytes. The hard maximum remains immutable even when
 * the currently reserved amount is zero.
 */
export interface ElasticByteReservation {
  readonly maximumBytes: number;
  readonly reservedBytes: number;
  tryResize(bytes: number): boolean;
  close(): void;
}
