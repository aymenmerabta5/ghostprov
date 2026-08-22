export class TerminalNotProvenError extends Error {
  readonly kind = "sync";

  constructor() {
    super("relay loop settled without a proven terminal response");
    this.name = "TerminalNotProvenError";
  }
}

export function canReleaseLane(options: {
  requestPublished: boolean;
  terminalProven: boolean;
}): boolean {
  return !options.requestPublished || options.terminalProven;
}

export function matchesTerminalEnvelope(
  envelope: any,
  expectedAck: string,
  expectedRequestId: string | null,
): boolean {
  if (!envelope || (envelope.gp_ack ?? envelope.ack) !== expectedAck) return false;
  if (expectedRequestId === null) return true;
  return (envelope.gp_transport?.id ?? envelope.transport?.id) === expectedRequestId;
}

function errorKind(error: any): string {
  return typeof error?.kind === "string" ? error.kind : "internal";
}

export async function settleWithTerminalRescue<T>(
  run: Promise<T>,
  options: {
    hasTerminal: () => boolean;
    rescue: () => Promise<T | null>;
  },
): Promise<{ value: T; reason: string }> {
  let value: T;
  try {
    value = await run;
  } catch (error) {
    if (options.hasTerminal()) throw error;
    const recovered = await options.rescue();
    if (recovered !== null) {
      return { value: recovered, reason: `recovered-after-error:${errorKind(error)}` };
    }
    throw error;
  }
  if (options.hasTerminal()) return { value, reason: "terminal" };
  const recovered = await options.rescue();
  if (recovered !== null) return { value: recovered, reason: "recovered-after-unproven-close" };
  throw new TerminalNotProvenError();
}
