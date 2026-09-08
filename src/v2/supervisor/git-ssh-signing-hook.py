#!/usr/bin/python3
"""Experimental Git SSH bootstrap. No credentials, networking, or PID signals.

Git calls --prepare through defaultKeyCommand, then calls this file as its
signer. A launch receipt is submission evidence only; R10 signed readiness is
still mandatory. Any interrupted handoff requires retiring the test sandbox.
"""
import fcntl
import hashlib
import json
import os
import pathlib
import re
import select
import stat
import subprocess
import sys
import time

MAX_FILE = 64 * 1024
SAFE_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C", "HOME": "/nonexistent"}
OUTCOMES = {"submitted", "cancelled", "expired", "foreign_lock", "foreign_config",
            "restore_identity", "missing_original", "bounded_file", "identity_mismatch",
            "invalid_arguments", "duplicate", "signing_failed", "local_failure", "interrupted", "ambiguous"}
PHASE = "entry"


class Rejected(Exception):
    pass


def reject(reason):
    raise Rejected(reason)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def strict_json(value):
    def unique(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                reject("identity_mismatch")
            result[key] = item
        return result
    return json.loads(value, object_pairs_hook=unique)


def regular(path, limit):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                or metadata.st_nlink != 1 or metadata.st_size > limit):
            reject("bounded_file")
        value = os.read(descriptor, limit + 1)
        if len(value) > limit or len(value) != metadata.st_size:
            reject("bounded_file")
        if identity(os.fstat(descriptor)) != identity(metadata):
            reject("identity_mismatch")
        return value, metadata
    finally:
        os.close(descriptor)


def checked_directory(path):
    if not path.is_absolute() or str(path) != os.path.normpath(str(path)):
        reject("identity_mismatch")
    for item in (*reversed(path.parents), path):
        if not stat.S_ISDIR(item.lstat().st_mode):
            reject("identity_mismatch")


def exclusive(path, content):
    if len(content) > MAX_FILE:
        reject("bounded_file")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        if os.write(descriptor, content) != len(content):
            reject("local_failure")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def json_once(path, value):
    exclusive(path, json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def failure_details(error, plan):
    # Content-free diagnostics only. Never serialize the exception message,
    # filename, command, environment, stdout, stderr, or signing material.
    allowed = {"Rejected", "FileNotFoundError", "PermissionError", "AttributeError", "OSError",
               "TimeoutExpired", "BlockingIOError", "FileExistsError", "ValueError", "TypeError"}
    kind = type(error).__name__
    number = getattr(error, "errno", None)
    line = None
    trace = error.__traceback__
    while trace is not None:
        if trace.tb_frame.f_code.co_filename == __file__:
            line = trace.tb_lineno
        trace = trace.tb_next
    return {"version": 1, "sessionId": plan["sessionId"], "phase": PHASE,
            "exception": kind if kind in allowed else "Unknown",
            "errno": number if type(number) is int and 0 < number < 4096 else None,
            "line": line}


def identity(metadata):
    return (metadata.st_dev, metadata.st_ino, metadata.st_mode, metadata.st_uid,
            metadata.st_size, metadata.st_mtime_ns, metadata.st_nlink)


def unlink_owned(path, expected):
    value, metadata = regular(path, MAX_FILE)
    if list(identity(metadata)) != expected["identity"] or digest(value) != expected["sha256"]:
        reject("identity_mismatch")
    if identity(path.lstat()) != identity(metadata):
        reject("identity_mismatch")
    path.unlink()


def fingerprint(path, limit=8192):
    value, metadata = regular(path, limit)
    return {"identity": list(identity(metadata)), "sha256": digest(value)}


def load_plan():
    script = pathlib.Path(os.path.abspath(__file__))
    directory = script.parent
    checked_directory(directory)
    plan = strict_json(regular(directory / "plan.json", 8192)[0])
    required = {"version", "sessionId", "workspace", "gitDirectory", "gitConfig", "configLock",
                "configSha256", "hookSha256", "launchSha256", "gitTempDirectory", "expiresAtUnixMs", "denoPath", "signerSha256"}
    if (not isinstance(plan, dict) or set(plan) != required
            or type(plan["version"]) is not int or plan["version"] != 1):
        reject("identity_mismatch")
    if not isinstance(plan["sessionId"], str) or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", plan["sessionId"]):
        reject("identity_mismatch")
    for field in ("configSha256", "hookSha256", "launchSha256", "signerSha256"):
        if not isinstance(plan[field], str) or not re.fullmatch(r"[0-9a-f]{64}", plan[field]):
            reject("identity_mismatch")
    if type(plan["expiresAtUnixMs"]) is not int or plan["expiresAtUnixMs"] < 1:
        reject("identity_mismatch")
    for field in ("workspace", "gitDirectory", "gitConfig", "configLock", "gitTempDirectory"):
        if (not isinstance(plan[field], str) or len(plan[field]) > 1024
                or not re.fullmatch(r"/(?:[A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+", plan[field])
                or os.path.normpath(plan[field]) != plan[field]):
            reject("identity_mismatch")
    workspace = pathlib.Path(plan["workspace"])
    if (workspace.name != plan["sessionId"] or workspace.parent.name != "codex_workspace"
            or directory != workspace / ".ghostprov-v2" / "ssh-bootstrap"
            or script.name != "hook.py" or plan["configLock"] != plan["gitConfig"] + ".lock"
            or pathlib.Path.cwd() != pathlib.Path(plan["gitDirectory"])):
        reject("identity_mismatch")
    for location in (workspace, pathlib.Path(plan["gitDirectory"]), pathlib.Path(plan["gitConfig"]).parent,
                     pathlib.Path(plan["gitTempDirectory"])):
        checked_directory(location)
    if digest(regular(script, MAX_FILE)[0]) != plan["hookSha256"]:
        reject("identity_mismatch")
    if plan["denoPath"] not in ("/usr/local/bin/deno", "/tmp/ghostprov-deno/deno") or (plan["gitDirectory"] == "/workspace" and plan["denoPath"] != "/usr/local/bin/deno"):
        reject("identity_mismatch")
    return plan, directory


def git_identity(pid):
    if os.readlink(f"/proc/{pid}/exe") != "/usr/bin/git":
        reject("identity_mismatch")
    fields = pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return {"pid": pid, "startTicks": fields[19]}


def parent_identity():
    return git_identity(os.getppid())


def restore(plan, directory, result):
    config = pathlib.Path(plan["gitConfig"])
    lock = pathlib.Path(plan["configLock"])
    checked_directory(lock)
    owner = (plan["sessionId"] + "\n").encode()
    lock_identity = identity(lock.lstat())
    if sorted(os.listdir(lock)) != ["owner"] or regular(lock / "owner", 128)[0] != owner:
        reject("foreign_lock")
    current, current_metadata = regular(config, 8192)
    if digest(current) != plan["configSha256"]:
        reject("foreign_config")
    backup = directory / "original.gitconfig"
    absent = directory / "config-was-absent"
    if os.path.lexists(backup):
        _, before = regular(backup, 8192)
        if identity(config.lstat()) != identity(current_metadata):
            reject("foreign_config")
        os.replace(backup, config)
        if identity(config.lstat()) != identity(before):
            reject("restore_identity")
    elif os.path.lexists(absent):
        checked_directory(absent)
        if os.listdir(absent) or identity(config.lstat()) != identity(current_metadata):
            reject("missing_original")
        config.unlink()
    else:
        reject("missing_original")
    result["configRestored"] = True
    if identity(lock.lstat()) != lock_identity or regular(lock / "owner", 128)[0] != owner:
        reject("foreign_lock")
    (lock / "owner").unlink()
    lock.rmdir()
    result["lockReleased"] = True


def available(plan, directory):
    if os.path.lexists(directory / "cancelled"):
        reject("cancelled")
    if int(time.time() * 1000) >= plan["expiresAtUnixMs"]:
        reject("expired")


def clean_keys(directory, owned):
    failed = False
    for name, expected in owned.items():
        path = directory / name
        if os.path.lexists(path):
            try:
                unlink_owned(path, expected)
            except Exception:
                failed = True
    if failed:
        reject("identity_mismatch")


def keys_absent(directory):
    return all(not os.path.lexists(directory / name) for name in ("ephemeral-key", "ephemeral-key.pub"))


def load_prepared(plan, directory):
    prepared = strict_json(regular(directory / "prepared.json", 4096)[0])
    if (not isinstance(prepared, dict) or set(prepared) != {"version", "sessionId", "parent", "publicKey",
            "configRestored", "lockReleased", "keys", "expiresAtUnixMs"}
            or type(prepared["version"]) is not int or prepared["version"] != 1
            or prepared["sessionId"] != plan["sessionId"] or prepared["configRestored"] is not True
            or prepared["lockReleased"] is not True or type(prepared["expiresAtUnixMs"]) is not int
            or not 0 < prepared["expiresAtUnixMs"] <= plan["expiresAtUnixMs"]
            or not isinstance(prepared["publicKey"], str)
            or not re.fullmatch(r"ssh-ed25519 [A-Za-z0-9+/=]{68} ghostprov-disposable", prepared["publicKey"])):
        reject("identity_mismatch")
    parent = prepared["parent"]
    if (not isinstance(parent, dict) or set(parent) != {"pid", "startTicks"}
            or type(parent["pid"]) is not int or parent["pid"] <= 1
            or not isinstance(parent["startTicks"], str) or not re.fullmatch(r"[0-9]{1,24}", parent["startTicks"])):
        reject("identity_mismatch")
    keys = prepared["keys"]
    if not isinstance(keys, dict) or set(keys) != {"ephemeral-key", "ephemeral-key.pub"}:
        reject("identity_mismatch")
    for item in keys.values():
        if (not isinstance(item, dict) or set(item) != {"identity", "sha256"}
                or not isinstance(item["identity"], list) or len(item["identity"]) != 7
                or any(type(number) is not int or number < 0 for number in item["identity"])
                or not isinstance(item["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])):
            reject("identity_mismatch")
    return prepared


def sealed_memory(name, content):
    descriptor = os.memfd_create(name, os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        os.fchmod(descriptor, 0o600)
        if os.write(descriptor, content) != len(content):
            reject("local_failure")
        os.lseek(descriptor, 0, os.SEEK_SET)
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS,
                    fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL)
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def cleanup_prepared(plan, directory, ready_descriptor):
    """At most five seconds, keys only: never launches, signals, or retries."""
    prepared = load_prepared(plan, directory)
    gate = os.open(directory / "gate.lock", os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(gate)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.getuid():
            reject("identity_mismatch")
        deadline = time.monotonic() + min(4.5, max(0, (prepared["expiresAtUnixMs"] - int(time.time() * 1000)) / 1000))
        os.write(ready_descriptor, b"1")
        os.close(ready_descriptor)
        reason = "expired"
        while time.monotonic() < deadline:
            if os.path.lexists(directory / "result.json"):
                break
            try:
                if git_identity(prepared["parent"]["pid"]) != prepared["parent"]:
                    reason = "interrupted"
                    break
            except Exception:
                reason = "interrupted"
                break
            time.sleep(0.02)
        # A signer already holding the gate owns its terminal cleanup. The
        # monitor never removes files under an active attempt or claims launch.
        lock_deadline = time.monotonic() + 0.25
        while True:
            try:
                fcntl.flock(gate, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= lock_deadline:
                    return 1
                time.sleep(0.01)
        cleaned = False
        try:
            clean_keys(directory, prepared["keys"])
            cleaned = keys_absent(directory)
        except Exception:
            reason = "identity_mismatch"
        # Even a rejected signer can have written a terminal before it could
        # recover key ownership. This monitor retained the prepare-time key
        # identities and may clean those exact files, but never revises receipts.
        if os.path.lexists(directory / "result.json") or os.path.lexists(directory / "launch.claim"):
            return 0 if cleaned else 1
        json_once(directory / "result.json", {"version": 1, "sessionId": plan["sessionId"],
                  "outcome": reason, "configRestored": True, "lockReleased": True,
                  "signingComplete": False, "launchSubmitted": False, "keysRemoved": cleaned})
        return 0
    finally:
        os.close(gate)


"""Deno SSHSIG bridge. Sealed code, bounded stdin, no procfs data access."""


def inherited_bytes(path, descriptors, limit):
    match = re.fullmatch(r"/proc/self/fd/([0-9]{1,6})", path)
    if match is None or int(match[1]) not in descriptors:
        reject("invalid_arguments")
    descriptor = int(match[1])
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or before.st_size > limit:
        reject("bounded_file")
    content = os.pread(descriptor, limit + 1, 0)
    if len(content) != before.st_size or identity(os.fstat(descriptor)) != identity(before):
        reject("identity_mismatch")
    return content


def stdin_frame(*parts):
    return b"".join(len(part).to_bytes(4, "big") + part for part in parts)


def webcrypto_command(argv, **kwargs):
    plan, directory = load_plan()
    if set(kwargs) - {"input", "pass_fds"}:
        reject("invalid_arguments")
    descriptors = kwargs.get("pass_fds", ())
    if not isinstance(descriptors, tuple) or len(descriptors) > 2 or any(
            type(descriptor) is not int or not 3 <= descriptor <= 999999 for descriptor in descriptors):
        reject("invalid_arguments")
    message = kwargs.get("input")
    if message is not None and (not isinstance(message, bytes) or len(message) > MAX_FILE):
        reject("bounded_file")
    permissions = []
    framed = None
    if argv[:10] == ["/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "ghostprov-disposable", "-f", str(directory / "ephemeral-key")]:
        if len(argv) != 10 or kwargs:
            reject("invalid_arguments")
        arguments = ["generate", argv[9]]
        permissions = ["--allow-write=" + argv[9] + "," + argv[9] + ".pub"]
    elif len(argv) == 7 and argv[:6] == ["/usr/bin/ssh-keygen", "-Y", "sign", "-n", "git", "-f"]:
        if message is None:
            reject("invalid_arguments")
        arguments = ["sign-stdin"]
        framed = stdin_frame(inherited_bytes(argv[6], descriptors, 512), message)
    elif (len(argv) == 11 and argv[:3] == ["/usr/bin/ssh-keygen", "-Y", "verify"]
          and argv[3] == "-I" and argv[5:8] == ["-n", "git", "-f"] and argv[9] == "-s"):
        if message is None:
            reject("invalid_arguments")
        arguments = ["verify-stdin", argv[4]]
        framed = stdin_frame(inherited_bytes(argv[8], descriptors, 512),
                             inherited_bytes(argv[10], descriptors, 2048), message)
    else:
        reject("invalid_arguments")
    source, _ = regular(directory / "sshsig.mjs", MAX_FILE)
    if digest(source) != plan["signerSha256"]:
        reject("identity_mismatch")
    descriptor = sealed_memory("ghostprov-sshsig-code", source)
    try:
        completed = subprocess.run([plan["denoPath"], "run", "--no-config", "--no-remote", "--no-npm", "--no-lock",
            "--no-code-cache", "--no-prompt", "--ext=js", *permissions, f"/proc/self/fd/{descriptor}", *arguments],
            input=framed, env=SAFE_ENV, capture_output=True, timeout=2, pass_fds=(descriptor,))
    finally:
        os.close(descriptor)
    if len(completed.stdout) + len(completed.stderr) > 8192 or completed.returncode != 0:
        reject("signing_failed")
    if arguments[0] == "verify-stdin" and completed.stdout != b"verified\n":
        reject("signing_failed")
    return completed.stdout


def command(argv, **kwargs):
    if argv[0] == "/usr/bin/ssh-keygen":
        return webcrypto_command(argv, **kwargs)
    completed = subprocess.run(argv, env=SAFE_ENV, capture_output=True, timeout=2, **kwargs)
    if len(completed.stdout) + len(completed.stderr) > 8192 or completed.returncode != 0:
        reject("signing_failed")
    return completed.stdout


def prepare(plan, directory, result, owned_keys):
    global PHASE
    PHASE = "prepare_identity"
    parent = parent_identity()
    exclusive(directory / "prepare.claim", (plan["sessionId"] + "\n").encode())
    PHASE = "restore_config"
    restore(plan, directory, result)
    available(plan, directory)
    PHASE = "enable_hook"
    descriptor = os.open(directory / "hook.py", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if digest(os.read(descriptor, MAX_FILE + 1)) != plan["hookSha256"]:
            reject("identity_mismatch")
        os.fchmod(descriptor, 0o700)
    finally:
        os.close(descriptor)
    key = directory / "ephemeral-key"
    if os.path.lexists(key) or os.path.lexists(directory / "ephemeral-key.pub"):
        reject("identity_mismatch")
    PHASE = "generate_key"
    try:
        command(["/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "ghostprov-disposable", "-f", str(key)])
    finally:
        for name in ("ephemeral-key", "ephemeral-key.pub"):
            if os.path.lexists(directory / name):
                owned_keys[name] = fingerprint(directory / name)
    public = regular(directory / "ephemeral-key.pub", 256)[0].strip()
    if not re.fullmatch(rb"ssh-ed25519 [A-Za-z0-9+/=]{68} ghostprov-disposable", public):
        reject("signing_failed")
    available(plan, directory)
    json_once(directory / "prepared.json", {"version": 1, "sessionId": plan["sessionId"],
              "parent": parent, "publicKey": public.decode(), "configRestored": True, "lockReleased": True,
              "keys": owned_keys, "expiresAtUnixMs": min(plan["expiresAtUnixMs"], int(time.time() * 1000) + 4000)})
    PHASE = "start_key_monitor"
    read_descriptor, write_descriptor = os.pipe2(os.O_CLOEXEC)
    try:
        subprocess.Popen(["/usr/bin/python3", str(directory / "hook.py"), "--cleanup-prepared", str(write_descriptor)],
                         env=SAFE_ENV, cwd=plan["gitDirectory"], stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True,
                         pass_fds=(write_descriptor,), start_new_session=True)
        os.close(write_descriptor)
        write_descriptor = -1
        if not select.select([read_descriptor], [], [], 0.75)[0] or os.read(read_descriptor, 1) != b"1":
            reject("local_failure")
    finally:
        os.close(read_descriptor)
        if write_descriptor >= 0:
            os.close(write_descriptor)
    PHASE = "handoff_key"
    sys.stdout.buffer.write(b"key::" + public + b"\n")
    sys.stdout.buffer.flush()


def signing_path(value, prefix, plan):
    path = pathlib.Path(value)
    if (not path.is_absolute() or path.parent != pathlib.Path(plan["gitTempDirectory"])
            or not re.fullmatch(re.escape(prefix) + r"[A-Za-z0-9]{6}", path.name)):
        reject("invalid_arguments")
    return path


def sign(plan, directory, result, args, owned_keys, owned_temporary):
    global PHASE
    PHASE = "sign_identity"
    prepared = load_prepared(plan, directory)
    if prepared["parent"] != parent_identity():
        reject("identity_mismatch")
    owned_keys.update(prepared["keys"])
    result["configRestored"] = True
    result["lockReleased"] = True
    if len(args) != 8 or args[:5] != ["-Y", "sign", "-n", "git", "-f"] or args[6] != "-U":
        reject("invalid_arguments")
    available(plan, directory)
    if int(time.time() * 1000) >= prepared["expiresAtUnixMs"]:
        reject("expired")
    PHASE = "read_git_inputs"
    public_path = signing_path(args[5], ".git_signing_key_tmp", plan)
    buffer_path = signing_path(args[7], ".git_signing_buffer_tmp", plan)
    public_bytes, public_metadata = regular(public_path, 256)
    public = public_bytes.strip()
    if public != prepared["publicKey"].encode():
        reject("identity_mismatch")
    # Git 2.43 leaves its literal-public-key temporary on some terminal paths.
    # Remove only the exact temporary file presented by the verified Git parent.
    unlink_owned(public_path, {"identity": list(identity(public_metadata)), "sha256": digest(public_bytes)})
    buffer, buffer_metadata = regular(buffer_path, MAX_FILE)
    private = regular(directory / "ephemeral-key", 8192)[0]
    PHASE = "seal_key"
    key_descriptor = sealed_memory("ghostprov-ephemeral-signing", private)
    try:
        clean_keys(directory, owned_keys)
        result["keysRemoved"] = keys_absent(directory)
        PHASE = "sign_commit"
        signature = command(["/usr/bin/ssh-keygen", "-Y", "sign", "-n", "git", "-f", f"/proc/self/fd/{key_descriptor}"],
                            input=buffer, pass_fds=(key_descriptor,))
    finally:
        os.close(key_descriptor)
    if len(signature) > 2048 or not signature.startswith(b"-----BEGIN SSH SIGNATURE-----\n"):
        reject("signing_failed")
    PHASE = "verify_signature"
    signature_descriptor = sealed_memory("ghostprov-ssh-signature", signature)
    allowed_descriptor = sealed_memory("ghostprov-ssh-public", b"ghostprov-disposable " + public + b"\n")
    try:
        command(["/usr/bin/ssh-keygen", "-Y", "verify", "-I", "ghostprov-disposable", "-n", "git",
                 "-f", f"/proc/self/fd/{allowed_descriptor}", "-s", f"/proc/self/fd/{signature_descriptor}"],
                input=buffer, pass_fds=(allowed_descriptor, signature_descriptor))
    finally:
        os.close(signature_descriptor)
        os.close(allowed_descriptor)
    result["signingComplete"] = True
    available(plan, directory)
    PHASE = "seal_launcher"
    launch, _ = regular(pathlib.Path(plan["workspace"]) / "exec", MAX_FILE)
    if digest(launch) != plan["launchSha256"]:
        reject("identity_mismatch")
    launch_descriptor = sealed_memory("ghostprov-supervisor-launch", launch)
    try:
        if identity(buffer_path.lstat()) != identity(buffer_metadata):
            reject("identity_mismatch")
        if parent_identity() != prepared["parent"]:
            reject("identity_mismatch")
        available(plan, directory)
        if int(time.time() * 1000) >= prepared["expiresAtUnixMs"]:
            reject("expired")
        signature_path = pathlib.Path(str(buffer_path) + ".sig")
        exclusive(signature_path, signature)
        owned_temporary[signature_path] = fingerprint(signature_path)
        PHASE = "claim_launch"
        exclusive(directory / "launch.claim", (plan["sessionId"] + "\n").encode())
        # Claim is durable before Popen. Every failure after this point remains
        # non-replayable; no process is signalled and no alternate method runs.
        PHASE = "submit_launch"
        subprocess.Popen(["/usr/bin/setsid", "/bin/sh", f"/proc/self/fd/{launch_descriptor}"],
                         cwd=plan["workspace"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, close_fds=True, pass_fds=(launch_descriptor,))
    finally:
        os.close(launch_descriptor)
    result["launchSubmitted"] = True
    result["outcome"] = "submitted"


def main():
    global PHASE
    os.umask(0o077)
    plan, directory = load_plan()
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--cleanup-prepared" and re.fullmatch(r"[0-9]{1,6}", args[1]):
        PHASE = "key_monitor"
        try:
            return cleanup_prepared(plan, directory, int(args[1]))
        except Exception as error:
            try:
                json_once(directory / "failure.json", failure_details(error, plan))
            except Exception:
                pass
            return 1
    if args != ["--prepare"] and (not args or args[0] != "-Y"):
        reject("invalid_arguments")
    gate = os.open(directory / "gate.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    result = {"version": 1, "sessionId": plan["sessionId"], "outcome": "local_failure",
              "configRestored": False, "lockReleased": False, "signingComplete": False,
              "launchSubmitted": False, "keysRemoved": False}
    owns_attempt = False
    owned_keys = {}
    owned_temporary = {}
    try:
        metadata = os.fstat(gate)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.getuid():
            reject("identity_mismatch")
        try:
            fcntl.flock(gate, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            reject("duplicate")
        if os.path.lexists(directory / "result.json") or os.path.lexists(directory / "launch.claim"):
            reject("duplicate")
        if args == ["--prepare"] and os.path.lexists(directory / "prepare.claim"):
            reject("duplicate")
        owns_attempt = True
        if args == ["--prepare"]:
            prepare(plan, directory, result, owned_keys)
            return 0
        sign(plan, directory, result, args, owned_keys, owned_temporary)
        PHASE = "write_receipt"
        json_once(directory / "result.json", result)
        return 0
    except Exception as error:
        reason = str(error) if isinstance(error, Rejected) else "local_failure"
        if owns_attempt:
            details = failure_details(error, plan)
            result["outcome"] = reason if reason in OUTCOMES else "local_failure"
            if os.path.lexists(directory / "launch.claim"):
                result["outcome"] = "ambiguous"
            # Never overwrite a durable submission receipt or another attempt.
            try:
                clean_keys(directory, owned_keys)
            except Exception:
                pass
            result["keysRemoved"] = keys_absent(directory)
            for path, expected in owned_temporary.items():
                try:
                    unlink_owned(path, expected)
                except Exception:
                    pass
            try:
                json_once(directory / "result.json", result)
            except Exception:
                pass
            try:
                json_once(directory / "failure.json", details)
            except Exception:
                pass
        sys.stderr.write("git-ssh-bootstrap:" + (reason if reason in OUTCOMES else "local_failure") + "\n")
        return 1
    finally:
        os.close(gate)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        sys.stderr.write("git-ssh-bootstrap:rejected\n")
        sys.exit(1)
