/** A verifier must not mistake the controller's unopened shutdown for a signed acknowledgement. */
export async function proveSignedShutdown(
  supervisor: { readonly instanceId: string; shutdown(reason: string, signal: AbortSignal): Promise<void> },
  authenticatedInstanceId: string | null,
  reason: string,
  signal: AbortSignal,
): Promise<boolean> {
  if (authenticatedInstanceId === null || authenticatedInstanceId === "pending" || supervisor.instanceId !== authenticatedInstanceId) return false;
  await supervisor.shutdown(reason, signal);
  return true;
}
