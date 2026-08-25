import { FileLease } from "../../src/core/file-lease";

const file = process.argv[2];
if (!file) throw new Error("file lease worker requires a lock path");

const lease = new FileLease({ file, staleAfterMs: 60_000 });
await lease.acquire({ timeoutMs: 5_000 });
console.log("READY");

// Deliberately do not release: the process test terminates this worker to prove
// that a valid dead-owner record is reclaimed without waiting for stale age.
setInterval(() => undefined, 60_000);
