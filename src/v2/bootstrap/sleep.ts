export async function abortableSleep(
  ms: number,
  signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted) throw new DOMException("Bootstrap aborted", "AbortError");
  await new Promise<void>((resolve, reject) => {
    const done = () => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    };
    const timer = setTimeout(done, ms);
    const onAbort = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      reject(new DOMException("Bootstrap aborted", "AbortError"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
