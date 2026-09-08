// Narrow SSHSIG Ed25519 signer for a disposable Git bootstrap.
// Wire format: openssh-portable/PROTOCOL.sshsig. No imports or network access.
const MAX_MESSAGE = 65536;
const encode = new TextEncoder();
const decode = new TextDecoder("utf-8", { fatal: true });
const text = value => encode.encode(value);
const magic = text("SSHSIG");
const algorithm = text("ssh-ed25519");
const namespace = text("git");
const empty = new Uint8Array();
function check(value) { if (!value) throw new Error("invalid"); }
function join(...parts) {
  const result = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) { result.set(part, offset); offset += part.length; }
  return result;
}
function uint32(value) { const bytes = new Uint8Array(4); new DataView(bytes.buffer).setUint32(0, value); return bytes; }
function string(value) { return join(uint32(value.length), value); }
function equal(left, right) { return left.length === right.length && left.every((byte, index) => byte === right[index]); }
function base64(bytes) { return btoa(String.fromCharCode(...bytes)); }
function unbase64(value) {
  check(value.length <= 4096 && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value));
  const bytes = Uint8Array.from(atob(value), item => item.charCodeAt(0));
  check(base64(bytes) === value);
  return bytes;
}
function base64url(bytes) { return base64(bytes).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, ""); }
function unbase64url(value) {
  check(typeof value === "string" && /^[A-Za-z0-9_-]{43}$/.test(value));
  const result = unbase64(value.replaceAll("-", "+").replaceAll("_", "/") + "=");
  check(result.length === 32 && base64url(result) === value);
  return result;
}
async function readStream(stream, limit) {
  const bytes = new Uint8Array(limit + 1);
  let used = 0;
  while (used <= limit) {
    const count = await stream.read(bytes.subarray(used));
    if (count === null) return bytes.slice(0, used);
    check(count > 0);
    used += count;
  }
  throw new Error("bounded");
}
async function readFile(path, limit) {
  const file = await Deno.open(path, { read: true });
  try {
    const before = await file.stat();
    check(before.isFile && before.size <= limit);
    const bytes = await readStream(file, limit);
    check(bytes.length === before.size && (await file.stat()).size === before.size);
    return bytes;
  } finally { file.close(); }
}
async function writeAll(destination, bytes) {
  let offset = 0;
  while (offset < bytes.length) { const count = await destination.write(bytes.subarray(offset)); check(count > 0); offset += count; }
}
async function newFile(path, bytes) {
  const file = await Deno.open(path, { write: true, createNew: true, mode: 0o600 });
  try { await writeAll(file, bytes); await file.sync(); } finally { file.close(); }
}
function reader(bytes) {
  let offset = 0;
  return {
    take(length) { check(Number.isSafeInteger(length) && length >= 0 && offset + length <= bytes.length); const out = bytes.slice(offset, offset + length); offset += length; return out; },
    number() { return new DataView(this.take(4).buffer).getUint32(0); },
    string(limit) { const length = this.number(); check(length <= limit); return this.take(length); },
    done() { check(offset === bytes.length); },
  };
}
async function readFrame(limits) {
  const source = reader(await readStream(Deno.stdin, limits.reduce((sum, limit) => sum + limit + 4, 0)));
  const parts = limits.map(limit => source.string(limit));
  source.done();
  return parts;
}
function publicBlob(raw) { check(raw.length === 32); return join(string(algorithm), string(raw)); }
function publicKey(blob) {
  const source = reader(blob);
  check(equal(source.string(32), algorithm));
  const raw = source.string(32);
  check(raw.length === 32);
  source.done();
  return raw;
}
function parseArmor(bytes) {
  const armor = decode.decode(bytes);
  const match = /^-----BEGIN SSH SIGNATURE-----\n([A-Za-z0-9+/=\n]+)\n-----END SSH SIGNATURE-----(?:\n)?$/.exec(armor);
  check(match && match[1].split("\n").every(line => line.length > 0 && line.length <= 76));
  const source = reader(unbase64(match[1].replaceAll("\n", "")));
  check(equal(source.take(6), magic) && source.number() === 1);
  const blob = source.string(128);
  const raw = publicKey(blob);
  const domain = source.string(64);
  check(equal(domain, namespace));
  const reserved = source.string(256);
  const hash = decode.decode(source.string(16));
  check(hash === "sha512" || hash === "sha256");
  const signature = reader(source.string(128));
  check(equal(signature.string(32), algorithm));
  const signed = signature.string(64);
  check(signed.length === 64);
  signature.done();
  source.done();
  return { blob, raw, domain, reserved, hash, signed };
}
async function payload(message, domain, reserved, hash) {
  const digest = new Uint8Array(await crypto.subtle.digest(hash === "sha512" ? "SHA-512" : "SHA-256", message));
  return join(magic, string(domain), string(reserved), string(text(hash)), string(digest));
}
async function generate(path) {
  const pair = await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
  const privateJwk = await crypto.subtle.exportKey("jwk", pair.privateKey);
  const raw = new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey));
  check(equal(unbase64url(privateJwk.x), raw));
  unbase64url(privateJwk.d);
  await newFile(path, text(JSON.stringify({ version: 1, d: privateJwk.d, x: privateJwk.x })));
  await newFile(path + ".pub", text("ssh-ed25519 " + base64(publicBlob(raw)) + " ghostprov-disposable\n"));
}
async function signMessage(privateBytes, message) {
  const serialized = decode.decode(privateBytes);
  const privateJwk = JSON.parse(serialized);
  check(privateJwk && privateJwk.version === 1 && Object.keys(privateJwk).sort().join() === "d,version,x" &&
    JSON.stringify({ version: 1, d: privateJwk.d, x: privateJwk.x }) === serialized);
  const raw = unbase64url(privateJwk.x);
  unbase64url(privateJwk.d);
  const key = await crypto.subtle.importKey("jwk", { kty: "OKP", crv: "Ed25519", x: privateJwk.x, d: privateJwk.d }, { name: "Ed25519" }, false, ["sign"]);
  const input = await payload(message, namespace, empty, "sha512");
  const signed = new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, key, input));
  const verificationKey = await crypto.subtle.importKey("raw", raw, { name: "Ed25519" }, false, ["verify"]);
  check(await crypto.subtle.verify({ name: "Ed25519" }, verificationKey, signed, input));
  const blob = join(magic, uint32(1), string(publicBlob(raw)), string(namespace), string(empty), string(text("sha512")),
    string(join(string(algorithm), string(signed))));
  const wrapped = base64(blob).match(/.{1,76}/g).join("\n");
  await writeAll(Deno.stdout, text("-----BEGIN SSH SIGNATURE-----\n" + wrapped + "\n-----END SSH SIGNATURE-----\n"));
}
async function verifyMessage(allowedBytes, signatureBytes, principal, message) {
  check(principal === "ghostprov-disposable" || principal === "sandbox@example.com");
  const allowed = decode.decode(allowedBytes);
  const match = /^(ghostprov-disposable|sandbox@example\.com) ssh-ed25519 ([A-Za-z0-9+/=]{68})(?: ghostprov-disposable)?\n$/.exec(allowed);
  check(match && match[1] === principal);
  const signature = parseArmor(signatureBytes);
  check(equal(unbase64(match[2]), signature.blob));
  const input = await payload(message, signature.domain, signature.reserved, signature.hash);
  const key = await crypto.subtle.importKey("raw", signature.raw, { name: "Ed25519" }, false, ["verify"]);
  check(await crypto.subtle.verify({ name: "Ed25519" }, key, signature.signed, input));
  await writeAll(Deno.stdout, text("verified\n"));
}
try {
  const [operation, ...args] = Deno.args;
  check(args.every(value => typeof value === "string" && value.length > 0 && value.length <= 1024 && !/[\r\n\0]/.test(value)));
  if (operation === "generate" && args.length === 1) await generate(args[0]);
  else if (operation === "sign" && args.length === 1) {
    await signMessage(await readFile(args[0], 512), await readStream(Deno.stdin, MAX_MESSAGE));
  } else if (operation === "verify" && args.length === 3) {
    await verifyMessage(await readFile(args[0], 512), await readFile(args[1], 2048), args[2], await readStream(Deno.stdin, MAX_MESSAGE));
  } else if (operation === "sign-stdin" && args.length === 0) {
    const [privateBytes, message] = await readFrame([512, MAX_MESSAGE]);
    await signMessage(privateBytes, message);
  } else if (operation === "verify-stdin" && args.length === 1) {
    const [allowed, signature, message] = await readFrame([512, 2048, MAX_MESSAGE]);
    await verifyMessage(allowed, signature, args[0], message);
  }
  else throw new Error("arguments");
} catch {
  console.error("sshsig:rejected");
  Deno.exit(1);
}
