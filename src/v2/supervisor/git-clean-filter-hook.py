#!/usr/bin/python3
"""Research-only Git clean filter. One normal-UID launch, no network or signals.

Only the nonce-named probe document is accepted. Its bytes pass through exactly.
A receipt proves submission; authenticated R10 readiness must be proved separately.
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

MAX_FILE = 65536
MAX_INPUT = 8192
OUTCOMES = {"submitted", "cancelled", "expired", "foreign_lock", "foreign_config",
            "restore_identity", "missing_original", "bounded_file", "identity_mismatch",
            "invalid_arguments", "input_mismatch", "io_timeout", "local_failure", "ambiguous"}


class Rejected(Exception):
    pass


def reject(reason):
    raise Rejected(reason)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_size,
            info.st_mtime_ns, info.st_nlink)


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
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or before.st_nlink != 1 or before.st_size > limit):
            reject("bounded_file")
        value = os.read(fd, limit + 1)
        if len(value) != before.st_size or len(value) > limit:
            reject("bounded_file")
        if identity(os.fstat(fd)) != identity(before):
            reject("identity_mismatch")
        return value, before
    finally:
        os.close(fd)


def checked_directory(path):
    if not path.is_absolute() or str(path) != os.path.normpath(str(path)):
        reject("identity_mismatch")
    for item in (*reversed(path.parents), path):
        if not stat.S_ISDIR(item.lstat().st_mode):
            reject("identity_mismatch")


def exclusive(path, content):
    if len(content) > MAX_FILE:
        reject("bounded_file")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        if os.write(fd, content) != len(content):
            reject("local_failure")
        os.fsync(fd)
    finally:
        os.close(fd)


def json_once(path, value):
    exclusive(path, json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def load_plan():
    if os.getuid() == 0 or os.getuid() != os.geteuid():
        reject("identity_mismatch")
    script = pathlib.Path(os.path.abspath(__file__))
    directory = script.parent
    checked_directory(directory)
    plan = strict_json(regular(directory / "plan.json", 8192)[0])
    required = {"version", "sessionId", "workspace", "gitDirectory", "gitConfig", "configLock",
                "configSha256", "hookSha256", "launchSha256", "inputSha256", "probeName", "expiresAtUnixMs"}
    if (not isinstance(plan, dict) or set(plan) != required or type(plan["version"]) is not int
            or plan["version"] != 1):
        reject("identity_mismatch")
    if not isinstance(plan["sessionId"], str) or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", plan["sessionId"]):
        reject("identity_mismatch")
    for field in ("configSha256", "hookSha256", "launchSha256", "inputSha256"):
        if not isinstance(plan[field], str) or not re.fullmatch(r"[0-9a-f]{64}", plan[field]):
            reject("identity_mismatch")
    if type(plan["expiresAtUnixMs"]) is not int or plan["expiresAtUnixMs"] < 1:
        reject("identity_mismatch")
    for field in ("workspace", "gitDirectory", "gitConfig", "configLock"):
        value = plan[field]
        if (not isinstance(value, str) or len(value) > 1024
                or not re.fullmatch(r"/(?:[A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+", value)
                or os.path.normpath(value) != value):
            reject("identity_mismatch")
    workspace = pathlib.Path(plan["workspace"])
    if (workspace.name != plan["sessionId"] or workspace.parent.name != "codex_workspace"
            or directory != workspace / ".ghostprov-v2" / "clean-bootstrap" or script.name != "hook.py"
            or plan["configLock"] != plan["gitConfig"] + ".lock"
            or pathlib.Path.cwd() != pathlib.Path(plan["gitDirectory"])
            or plan["probeName"] != "ghostprov-fifth-" + plan["sessionId"] + ".tex"):
        reject("identity_mismatch")
    for location in (workspace, pathlib.Path(plan["gitDirectory"]), pathlib.Path(plan["gitConfig"]).parent):
        checked_directory(location)
    if digest(regular(script, MAX_FILE)[0]) != plan["hookSha256"]:
        reject("identity_mismatch")
    return plan, directory


def read_input():
    data = bytearray()
    deadline = time.monotonic() + 1.5
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([0], [], [], remaining)[0]:
            reject("io_timeout")
        part = os.read(0, MAX_INPUT + 1 - len(data))
        if not part:
            return bytes(data)
        data.extend(part)
        if len(data) > MAX_INPUT:
            reject("bounded_file")


def write_output(data):
    deadline = time.monotonic() + 1
    previous = os.get_blocking(1)
    os.set_blocking(1, False)
    offset = 0
    try:
        while offset < len(data):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([], [1], [], remaining)[1]:
                reject("io_timeout")
            try:
                offset += os.write(1, data[offset:])
            except BlockingIOError:
                continue
    finally:
        os.set_blocking(1, previous)


def restore(plan, directory, result):
    config = pathlib.Path(plan["gitConfig"])
    lock = pathlib.Path(plan["configLock"])
    checked_directory(lock)
    lock_identity = identity(lock.lstat())
    owner = (plan["sessionId"] + "\n").encode()
    if sorted(os.listdir(lock)) != ["owner"] or regular(lock / "owner", 128)[0] != owner:
        reject("foreign_lock")
    current, current_info = regular(config, 8192)
    if digest(current) != plan["configSha256"]:
        reject("foreign_config")
    backup = directory / "original.gitconfig"
    absent = directory / "config-was-absent"
    if os.path.lexists(backup):
        _, before = regular(backup, 8192)
        if os.path.lexists(absent) or identity(config.lstat()) != identity(current_info):
            reject("foreign_config")
        os.replace(backup, config)
        if identity(config.lstat()) != identity(before):
            reject("restore_identity")
    elif os.path.lexists(absent):
        checked_directory(absent)
        if os.listdir(absent) or identity(config.lstat()) != identity(current_info):
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


def sealed_memory(content):
    fd = os.memfd_create("ghostprov-clean-supervisor-launch", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        if os.write(fd, content) != len(content):
            reject("local_failure")
        seals = fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, seals)
        if fcntl.fcntl(fd, fcntl.F_GET_SEALS) != seals:
            reject("identity_mismatch")
        os.lseek(fd, 0, os.SEEK_SET)
        return fd
    except Exception:
        os.close(fd)
        raise


def main():
    os.umask(0o077)
    plan, directory = load_plan()
    if sys.argv[1:] != [plan["probeName"]]:
        reject("invalid_arguments")
    payload = read_input()
    if digest(payload) != plan["inputSha256"]:
        reject("input_mismatch")
    gate = os.open(directory / "gate.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    result = {"version": 1, "sessionId": plan["sessionId"], "outcome": "local_failure",
              "configRestored": False, "lockReleased": False, "inputVerified": True,
              "payloadWritten": False, "launchSubmitted": False}
    owns_attempt = False
    try:
        info = os.fstat(gate)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_size != 0:
            reject("identity_mismatch")
        try:
            fcntl.flock(gate, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            write_output(payload)
            return 0
        if os.path.lexists(directory / "result.json") or os.path.lexists(directory / "launch.claim"):
            write_output(payload)
            return 0
        owns_attempt = True
        restore(plan, directory, result)
        available(plan, directory)
        launcher, _ = regular(pathlib.Path(plan["workspace"]) / "exec", MAX_FILE)
        if digest(launcher) != plan["launchSha256"] or not launcher.startswith(b"#!/bin/sh\n"):
            reject("identity_mismatch")
        fd = sealed_memory(launcher)
        try:
            write_output(payload)
            result["payloadWritten"] = True
            available(plan, directory)
            exclusive(directory / "launch.claim", (plan["sessionId"] + "\n").encode())
            # Durable claim precedes the only launch. A lost response never
            # authorizes a second process, another method, or a PID signal.
            subprocess.Popen(["/usr/bin/setsid", "/bin/sh", f"/proc/self/fd/{fd}"],
                             cwd=plan["workspace"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, close_fds=True, pass_fds=(fd,))
        finally:
            os.close(fd)
        result["launchSubmitted"] = True
        result["outcome"] = "submitted"
        json_once(directory / "result.json", result)
        return 0
    except Exception as error:
        if owns_attempt:
            reason = str(error) if isinstance(error, Rejected) else "local_failure"
            result["outcome"] = reason if reason in OUTCOMES else "local_failure"
            if os.path.lexists(directory / "launch.claim"):
                result["outcome"] = "ambiguous"
            try:
                json_once(directory / "result.json", result)
            except Exception:
                pass
        return 1
    finally:
        os.close(gate)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        # No exception text, content, environment, paths, or credentials leak.
        sys.exit(1)
