"""Cross-verify the import-free Deno signer with installed OpenSSH, offline."""
import base64
import hashlib
import json
import pathlib
import shutil
import subprocess
import tempfile
import sys

if len(sys.argv) != 5:
    raise SystemExit("Expected signer, runtime, report and source hash")
SOURCE = pathlib.Path(sys.argv[1]).resolve()
RUNTIME = sys.argv[2]
DESTINATION = pathlib.Path(sys.argv[3]).resolve()
CANONICAL_SOURCE = SOURCE.read_text().replace("\r\n", "\n").encode()
assert hashlib.sha256(CANONICAL_SOURCE).hexdigest() == sys.argv[4]
assert RUNTIME in ("/tmp/ghostprov-deno/deno", "/usr/local/bin/deno")
assert DESTINATION.name == "sshsig.json" and DESTINATION.parent.name.startswith("run-")
assert DESTINATION.parent.parent.name == "git-ssh-signing-offline"
ROOT = pathlib.Path(tempfile.mkdtemp(prefix="ghostprov-sshsig-", dir="/tmp"))
ENV = {"PATH": "/usr/bin:/bin", "LANG": "C", "HOME": "/nonexistent"}
CASES = []


def run(args, data=None):
    result = subprocess.run(args, input=data, env=ENV, capture_output=True, timeout=3)
    assert len(result.stdout) + len(result.stderr) <= 8192
    return result


def deno(*args, data=None):
    return run([RUNTIME, "run", "--no-config", "--no-remote", "--no-npm", "--no-lock", "--no-code-cache",
                "--allow-read", "--allow-write", str(SOURCE), *map(str, args)], data)


def framed(*parts):
    return b"".join(len(part).to_bytes(4, "big") + part for part in parts)


def deno_stdin(*args, data):
    # The live bridge gives data through stdin; the runtime has no file-data,
    # subprocess, environment, or network permissions for signing/verification.
    return run([RUNTIME, "run", "--no-config", "--no-remote", "--no-npm", "--no-lock", "--no-code-cache",
                "--no-prompt", str(SOURCE), *args], data)


def check(name, value):
    CASES.append({"id": name, "passed": bool(value)})


def armor(blob):
    text = base64.b64encode(blob).decode()
    return ("-----BEGIN SSH SIGNATURE-----\n" + "\n".join(text[i:i+76] for i in range(0, len(text), 76))
            + "\n-----END SSH SIGNATURE-----\n").encode()


try:
    key = ROOT / "key"
    generated = deno("generate", key)
    check("generate", generated.returncode == 0 and generated.stdout == b""
          and key.stat().st_mode & 0o777 == 0o600)
    allowed = ROOT / "allowed"
    allowed.write_bytes(b"ghostprov-disposable " + key.with_suffix(".pub").read_bytes())
    signature = ROOT / "signature"
    for name, message in (("empty", b""), ("text", b"INIT disposable Git commit\n"),
                          ("binary", bytes(range(256))), ("max", b"x" * 65536)):
        signed = deno("sign", key, data=message)
        signature.write_bytes(signed.stdout)
        external = run(["/usr/bin/ssh-keygen", "-Y", "verify", "-I", "ghostprov-disposable", "-n", "git",
                        "-f", str(allowed), "-s", str(signature)], message)
        internal = deno("verify", allowed, signature, "ghostprov-disposable", data=message)
        check("openssh_verifies_" + name, signed.returncode == 0 and external.returncode == 0)
        check("deno_verifies_" + name, internal.returncode == 0 and internal.stdout == b"verified\n")
    check("oversized_input", deno("sign", key, data=b"x" * 65537).returncode != 0)

    message = b"fixed offline cross verification\n"
    baseline = deno("sign", key, data=message).stdout
    signature.write_bytes(baseline)
    check("wrong_message", deno("verify", allowed, signature, "ghostprov-disposable", data=message + b"x").returncode != 0)
    check("wrong_principal", deno("verify", allowed, signature, "sandbox@example.com", data=message).returncode != 0)
    blob = base64.b64decode(b"".join(baseline.splitlines()[1:-1]))
    bad_blobs = {"version": blob[:6] + (2).to_bytes(4, "big") + blob[10:], "trailing": blob + b"x",
                 "truncated": blob[:-1], "namespace": blob.replace(b"git", b"bad", 1),
                 "hash": blob.replace(b"sha512", b"sha999", 1), "public_algorithm": blob.replace(b"ssh-ed25519", b"ssh-ed25518", 1),
                 "signature": blob[:-1] + bytes([blob[-1] ^ 1]), "length": blob[:10] + b"\xff\xff\xff\xff" + blob[14:]}
    for name, value in bad_blobs.items():
        signature.write_bytes(armor(value))
        check("reject_" + name, deno("verify", allowed, signature, "ghostprov-disposable", data=message).returncode != 0)
    for name, value in (("armor_suffix", baseline + b"extra"), ("armor_prefix", b"extra" + baseline),
                        ("oversized_signature", baseline * 8)):
        signature.write_bytes(value)
        check("reject_" + name, deno("verify", allowed, signature, "ghostprov-disposable", data=message).returncode != 0)

    other = ROOT / "other"
    assert deno("generate", other).returncode == 0
    wrong_allowed = ROOT / "wrong-allowed"
    wrong_allowed.write_bytes(b"ghostprov-disposable " + other.with_suffix(".pub").read_bytes())
    signature.write_bytes(baseline)
    check("wrong_key", deno("verify", wrong_allowed, signature, "ghostprov-disposable", data=message).returncode != 0)
    original = key.read_text()
    private = json.loads(original)
    for name, value in (("duplicate_private", original[:-1] + ',"version":1}'),
                        ("extra_private", original[:-1] + ',"extra":true}'),
                        ("mismatched_private", json.dumps({**private, "x": json.loads(other.read_text())["x"]}, separators=(",", ":")))):
        key.write_text(value)
        check("reject_" + name, deno("sign", key, data=message).returncode != 0)
    key.write_text(original)

    for name, message in (("empty", b""), ("binary", bytes(range(256))), ("max", b"x" * 65536)):
        signed = deno_stdin("sign-stdin", data=framed(key.read_bytes(), message))
        signature.write_bytes(signed.stdout)
        external = run(["/usr/bin/ssh-keygen", "-Y", "verify", "-I", "ghostprov-disposable", "-n", "git",
                        "-f", str(allowed), "-s", str(signature)], message)
        check("stdin_openssh_verifies_" + name, signed.returncode == 0 and external.returncode == 0)
        internal = deno_stdin("verify-stdin", "ghostprov-disposable", data=framed(allowed.read_bytes(), signed.stdout, message))
        check("stdin_deno_verifies_" + name, internal.returncode == 0 and internal.stdout == b"verified\n")
    for name, data in (("truncated", framed(key.read_bytes(), b"x")[:-1]),
                       ("trailing", framed(key.read_bytes(), b"x") + b"x"),
                       ("length", b"\xff\xff\xff\xff"),
                       ("large_key", framed(b"x" * 513, b"")),
                       ("large_message", framed(key.read_bytes(), b"x" * 65537))):
        check("stdin_reject_" + name, deno_stdin("sign-stdin", data=data).returncode != 0)
    message = b"framed negative verification\n"
    signed = deno_stdin("sign-stdin", data=framed(key.read_bytes(), message))
    assert signed.returncode == 0
    check("stdin_reject_wrong_message", deno_stdin("verify-stdin", "ghostprov-disposable",
        data=framed(allowed.read_bytes(), signed.stdout, message + b"x")).returncode != 0)
    check("stdin_reject_wrong_key", deno_stdin("verify-stdin", "ghostprov-disposable",
        data=framed(wrong_allowed.read_bytes(), signed.stdout, message)).returncode != 0)
    check("stdin_reject_signature_bound", deno_stdin("verify-stdin", "ghostprov-disposable",
        data=framed(allowed.read_bytes(), b"x" * 2049, b"")).returncode != 0)

    openssh = ROOT / "openssh"
    assert run(["/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "ghostprov-disposable", "-f", str(openssh)]).returncode == 0
    allowed.write_bytes(b"ghostprov-disposable " + openssh.with_suffix(".pub").read_bytes())
    for hash_name in ("sha512", "sha256"):
        external = run(["/usr/bin/ssh-keygen", "-Y", "sign", "-n", "git", "-O", "hashalg=" + hash_name,
                        "-f", str(openssh)], message)
        signature.write_bytes(external.stdout)
        internal = deno("verify", allowed, signature, "ghostprov-disposable", data=message)
        check("deno_verifies_openssh_" + hash_name, external.returncode == 0 and internal.returncode == 0 and internal.stdout == b"verified\n")
    report = {"live": False, "signerSha256": hashlib.sha256(CANONICAL_SOURCE).hexdigest(),
              "runtime": run([RUNTIME, "--version"]).stdout.decode().splitlines()[0],
              "passed": sum(item["passed"] for item in CASES), "failed": sum(not item["passed"] for item in CASES), "cases": CASES}
    assert report["runtime"].startswith("deno 2.9.5 ")
    DESTINATION.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    raise SystemExit(1 if report["failed"] else 0)
finally:
    assert ROOT.parent == pathlib.Path("/tmp") and ROOT.name.startswith("ghostprov-sshsig-")
    shutil.rmtree(ROOT)
