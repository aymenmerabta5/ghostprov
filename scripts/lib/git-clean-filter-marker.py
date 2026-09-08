"""Finite, content-free observer for the clean-filter probe's one detached launch."""
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


def bounded_file(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ValueError()
        data = os.read(fd, limit + 1)
        if len(data) != info.st_size or len(data) > limit:
            raise ValueError()
        return data
    finally:
        os.close(fd)


def git_blob(plan):
    child = subprocess.Popen(["/usr/bin/git", "-C", plan["gitDirectory"], "cat-file", "blob", "HEAD:" + plan["probeName"]],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    data = bytearray()
    deadline = time.monotonic() + 0.75
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([child.stdout], [], [], remaining)[0]:
                return None
            chunk = os.read(child.stdout.fileno(), 8193 - len(data))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > 8192:
                return None
        code = child.wait(timeout=max(0.001, deadline - time.monotonic()))
        return bytes(data) if code == 0 else None
    finally:
        # Only this retained, short-lived Git helper can be terminated here.
        if child.poll() is None:
            child.kill()
        child.wait(timeout=0.5)
        child.stdout.close()


if len(sys.argv) != 2 or not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", sys.argv[1]):
    raise SystemExit(1)
directory = pathlib.Path.cwd() / ".ghostprov-v2" / "clean-bootstrap"
plan_raw = bounded_file(directory / "plan.json", 8192)
plan = json.loads(plan_raw)
result = {"version": 1, "nonce": sys.argv[1], "outcome": "failed", "uid": os.getuid(), "euid": os.geteuid(),
          "configInactive": False, "configLockReleased": False, "claimVerified": False,
          "gitSnapshotVerified": False, "worktreeBytesPreserved": False}
try:
    config = pathlib.Path(plan["gitConfig"])
    result["configInactive"] = (not os.path.lexists(config) or
                                hashlib.sha256(bounded_file(config, 8192)).hexdigest() != plan["configSha256"])
    result["configLockReleased"] = not os.path.lexists(plan["configLock"])
    result["claimVerified"] = bounded_file(directory / "launch.claim", 128) == (plan["sessionId"] + "\n").encode()
    working = bounded_file(pathlib.Path(plan["gitDirectory"]) / plan["probeName"], 8192)
    result["worktreeBytesPreserved"] = len(working) <= 8192 and hashlib.sha256(working).hexdigest() == plan["inputSha256"]
    deadline = time.monotonic() + 8
    for _ in range(24):
        if time.monotonic() >= deadline:
            break
        # cat-file reads the committed blob directly, without applying filters.
        snapshot = git_blob(plan)
        if snapshot is not None and hashlib.sha256(snapshot).hexdigest() == plan["inputSha256"]:
            result["gitSnapshotVerified"] = True
            break
        time.sleep(0.1)
    if os.getuid() != 0 and os.geteuid() == os.getuid() and all(result[name] for name in (
            "configInactive", "configLockReleased", "claimVerified", "gitSnapshotVerified", "worktreeBytesPreserved")):
        result["outcome"] = "verified"
except Exception:
    pass
payload = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
fd = os.open(directory / "marker.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
try:
    if os.write(fd, payload) != len(payload):
        raise SystemExit(1)
    os.fsync(fd)
finally:
    os.close(fd)
raise SystemExit(0 if result["outcome"] == "verified" else 1)
