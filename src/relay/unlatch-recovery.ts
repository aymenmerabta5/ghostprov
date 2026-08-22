export interface UnlatchRecoveryOptions {
  attempt: number;
  maxAttempts: number;
  ensureDaemonReady: () => Promise<void>;
  republish: () => Promise<void>;
}

export interface UnlatchRecoveryResult {
  recovered: boolean;
  nextAttempt: number;
}

/**
 * A sandbox can reprovision after a job is published. Revalidate daemon
 * liveness before asking its immutable commit scanner to observe the job
 * again; otherwise repeated commit writes cannot help a daemon that is down.
 */
export async function recoverUnlatchedPublication(
  options: UnlatchRecoveryOptions,
): Promise<UnlatchRecoveryResult> {
  if (options.attempt >= options.maxAttempts) {
    return { recovered: false, nextAttempt: options.attempt };
  }
  await options.ensureDaemonReady();
  await options.republish();
  return { recovered: true, nextAttempt: options.attempt + 1 };
}
