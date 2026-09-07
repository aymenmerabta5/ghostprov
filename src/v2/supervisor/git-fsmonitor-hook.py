"""Experimental one-shot Git bootstrap. Runs only in the owned sandbox."""
import fcntl
import hashlib
import json
import os
import pathlib
import stat
import subprocess
import sys
import time


def read_regular(filename, limit):
    descriptor = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
            raise ValueError("bounded_file")
        value = os.read(descriptor, limit + 1)
        if len(value) > limit:
            raise ValueError("bounded_file")
        return value
    finally:
        os.close(descriptor)


def exclusive_write(filename, content):
    descriptor = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        if os.write(descriptor, content) != len(content):
            raise OSError("short_write")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main():
    directory = pathlib.Path(__file__).resolve().parent
    plan = json.loads(read_regular(directory / "plan.json", 8192))
    workspace = pathlib.Path(plan["workspace"])
    config = pathlib.Path(plan["gitConfig"])
    config_lock = pathlib.Path(plan["configLock"])
    backup = directory / "original.gitconfig"
    absent = directory / "config-was-absent"
    owner = (plan["sessionId"] + "\n").encode()
    result_file = directory / "result.json"
    claim_file = directory / "launch.claim"
    result = {"version": 1, "sessionId": plan["sessionId"], "outcome": "failed",
              "configRestored": False, "lockReleased": False, "launchSubmitted": False}
    gate = os.open(directory / "gate.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(gate, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if result_file.exists() or claim_file.exists():
            return
        try:
            if not stat.S_ISDIR(config_lock.lstat().st_mode):
                raise ValueError("foreign_lock")
            if read_regular(config_lock / "owner", 128) != owner or os.listdir(config_lock) != ["owner"]:
                raise ValueError("foreign_lock")
            current = read_regular(config, 8192)
            if hashlib.sha256(current).hexdigest() != plan["configSha256"]:
                # Leave foreign configuration and the original backup intact.
                raise ValueError("foreign_config")
            if os.path.lexists(backup):
                before = backup.lstat()
                os.replace(backup, config)
                after = config.lstat()
                if (before.st_dev, before.st_ino, before.st_mode, before.st_size, before.st_mtime_ns) != (
                        after.st_dev, after.st_ino, after.st_mode, after.st_size, after.st_mtime_ns):
                    raise ValueError("restore_identity")
            elif absent.is_dir():
                config.unlink()
            else:
                raise ValueError("missing_original")
            result["configRestored"] = True
            if read_regular(config_lock / "owner", 128) != owner:
                raise ValueError("foreign_lock")
            (config_lock / "owner").unlink()
            config_lock.rmdir()
            result["lockReleased"] = True
            if (directory / "cancelled").exists():
                result["outcome"] = "cancelled"
            elif time.time() > plan["expiresAtSeconds"]:
                result["outcome"] = "expired"
            else:
                exclusive_write(claim_file, owner)
                # The claim stays on all failures. An uncertain Popen outcome
                # never authorizes another launch, and no PID is signalled.
                subprocess.Popen(["/usr/bin/setsid", "/bin/sh", "./exec"], cwd=workspace,
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, close_fds=True)
                result["launchSubmitted"] = True
                result["outcome"] = "submitted"
        except Exception as error:
            allowed = {"foreign_lock", "foreign_config", "restore_identity", "missing_original", "bounded_file"}
            reason = str(error)
            result["outcome"] = reason if reason in allowed else "local_failure"
        try:
            exclusive_write(result_file, json.dumps(result, separators=(",", ":")).encode())
        except FileExistsError:
            pass
    finally:
        os.close(gate)


try:
    main()
finally:
    # Git protocol acknowledgement contains no process or credential details.
    sys.stdout.buffer.write(b"1\0")
