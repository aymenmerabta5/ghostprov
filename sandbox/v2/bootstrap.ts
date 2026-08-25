import {
  createBootstrapKeyPair,
  openBootstrapCredential,
  type BootstrapBinding,
  type BootstrapCredentialPayload,
  type SealedBootstrapCredential,
} from "../../src/v2/bootstrap/crypto.ts";
import {
  base64UrlEncode,
  canonicalJson,
  utf8Bytes,
} from "../../src/v2/protocol/shared.ts";
import { abortableSleep } from "../../src/v2/bootstrap/sleep.ts";

export interface PhaseZeroBootstrapResult {
  binding: BootstrapBinding;
  credential: BootstrapCredentialPayload;
  instanceId: string;
}

export interface PhaseZeroBootstrapOptions {
  binding: BootstrapBinding;
  instanceId?: string;
  encryptedPath: string;
  helloPath?: string;
  /** Must finish the owner-held PID promotion before the hello becomes visible. */
  beforeHelloPublished?: () => Promise<void>;
  timeoutMs?: number;
  signal?: AbortSignal;
}

const SAFE_LAUNCH_NONCE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

/**
 * Resolves the hello path for one exact R2 fork. The launcher passes the
 * authenticated launch nonce as a distinct argv field, while Deno.pid binds
 * the file to that fork so an overlapping loser cannot consume another
 * candidate's ownership hello.
 */
export function resolveSidecarBootstrapHelloPath(options: {
  launchNonce: string;
  pid: number;
  args: readonly string[];
  root?: string;
}): { helloPath: string; processStart: string } {
  if (!SAFE_LAUNCH_NONCE.test(options.launchNonce)) {
    throw new TypeError("Bootstrap launch nonce is invalid");
  }
  if (!Number.isSafeInteger(options.pid) || options.pid < 2) {
    throw new TypeError("Bootstrap process PID is invalid");
  }
  const expectedArgument = `--ghostprov-launch-nonce=${options.launchNonce}`;
  const processStart = options.args[1]?.match(/^--ghostprov-process-start=([0-9]+)$/)?.[1];
  if (options.args.length !== 2 || options.args[0] !== expectedArgument || !processStart) {
    throw new TypeError("Bootstrap launch argument does not match the binding nonce");
  }
  const root = (options.root ?? "/tmp/ghostprov-v2").replace(/\/+$/, "");
  if (!root) throw new TypeError("Bootstrap runtime root is invalid");
  return {
    helloPath: `${root}/bootstrap-hello.${options.launchNonce}.${options.pid}`,
    processStart,
  };
}

async function atomicWrite(path: string, value: string): Promise<void> {
  const slash = path.lastIndexOf("/");
  const directory = slash >= 0 ? path.slice(0, slash) : ".";
  await Deno.mkdir(directory, { recursive: true, mode: 0o700 });
  const temporary = `${path}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
  await Deno.writeTextFile(temporary, value, {
    createNew: true,
    mode: 0o600,
  });
  const file = await Deno.open(temporary, { write: true });
  try {
    await file.syncData();
  } finally {
    file.close();
  }
  await Deno.rename(temporary, path);
  await Deno.chmod(path, 0o600);
}

export async function phaseZeroBootstrap(
  options: PhaseZeroBootstrapOptions,
): Promise<PhaseZeroBootstrapResult> {
  const helloPath = options.helloPath ?? "/tmp/ghostprov-v2/bootstrap-hello";
  const timeoutMs = options.timeoutMs ?? 30_000;
  const instanceId = options.instanceId ?? crypto.randomUUID();
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(instanceId)) {
    throw new TypeError("Bootstrap instanceId is invalid");
  }
  const keys = await createBootstrapKeyPair(options.binding);
  const hello = {
    version: 1,
    binding: options.binding,
    instanceId,
    publicJwk: keys.publicJwk,
  };
  const line = `GPV2_BOOTSTRAP ${base64UrlEncode(utf8Bytes(canonicalJson(hello)))}\n`;
  try {
    // A visible hello is the commit proof that this exact child both owns the
    // sidecar lease and completed canonical PID promotion. Promotion failure
    // therefore cannot leave a consumable historical hello behind.
    await options.beforeHelloPublished?.();
    await atomicWrite(helloPath, line);
    const deadline = Date.now() + timeoutMs;
    let lastCiphertext = "";
    while (Date.now() < deadline) {
      if (options.signal?.aborted) {
        throw new DOMException("Bootstrap aborted", "AbortError");
      }
      let raw = "";
      try {
        raw = await Deno.readTextFile(options.encryptedPath);
      } catch (error) {
        if (!(error instanceof Deno.errors.NotFound)) throw error;
      }
      if (raw && raw !== lastCiphertext) {
        lastCiphertext = raw;
        try {
          const sealed = JSON.parse(raw) as SealedBootstrapCredential;
          const credential = await openBootstrapCredential({
            sealed,
            sidecarPrivateKey: keys.privateKey,
            expectedBinding: options.binding,
          });
          return { binding: options.binding, credential, instanceId };
        } catch {
          // A stale/partial encrypted entry is ignored until the host publishes
          // the ciphertext bound to this launch nonce and generation.
        }
      }
      await abortableSleep(50, options.signal);
    }
    throw new Error("Encrypted bootstrap credential timed out");
  } finally {
    await Deno.remove(helloPath).catch(() => undefined);
  }
}
