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
  encryptedPath: string;
  helloPath?: string;
  timeoutMs?: number;
  signal?: AbortSignal;
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
  const instanceId = crypto.randomUUID();
  const keys = await createBootstrapKeyPair(options.binding);
  const hello = {
    version: 1,
    binding: options.binding,
    instanceId,
    publicJwk: keys.publicJwk,
  };
  const line = `GPV2_BOOTSTRAP ${base64UrlEncode(utf8Bytes(canonicalJson(hello)))}\n`;
  await atomicWrite(helloPath, line);

  const deadline = Date.now() + timeoutMs;
  let lastCiphertext = "";
  try {
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
