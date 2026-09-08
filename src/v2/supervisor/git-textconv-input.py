#!/usr/bin/python3
"""Bounded Git textconv input adapter for the pinned R10 content executor."""
import hashlib
import os
import pathlib
import re
import stat
import sys

CORE_SHA256 = "cbaf273695287f5a34f5cac9fb9e359be0d2a4e8ffbb671cd145dd40aec5b555"
MAX_SOURCE = 65536
MAX_INPUT = 8192


def identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid,
            info.st_size, info.st_mtime_ns, info.st_nlink)


def regular(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or before.st_nlink != 1 or before.st_size > limit):
            raise ValueError()
        data = os.read(fd, limit + 1)
        if len(data) != before.st_size or len(data) > limit or identity(os.fstat(fd)) != identity(before):
            raise ValueError()
        return data
    finally:
        os.close(fd)


def checked_directory(path):
    if not path.is_absolute() or str(path) != os.path.normpath(str(path)):
        raise ValueError()
    for item in (*reversed(path.parents), path):
        if not stat.S_ISDIR(item.lstat().st_mode):
            raise ValueError()


def main():
    os.umask(0o077)
    if os.getuid() == 0 or os.geteuid() != os.getuid() or len(sys.argv) != 3:
        raise ValueError()
    expected = sys.argv[1]
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise ValueError()
    script = pathlib.Path(os.path.abspath(__file__))
    directory = script.parent
    checked_directory(directory)
    if script.name != "textconv-input.py" or hashlib.sha256(regular(script, MAX_SOURCE)).hexdigest() != expected:
        raise ValueError()
    core_path = directory / "hook.py"
    source = regular(core_path, MAX_SOURCE)
    if hashlib.sha256(source).hexdigest() != CORE_SHA256:
        raise ValueError()
    # Execute only the already verified, immutable core source. Its main guard
    # stays inactive until the exact Git blob has been checked and sealed.
    core = {"__file__": str(core_path), "__name__": "ghostprov_textconv_core"}
    exec(compile(source, str(core_path), "exec"), core)
    plan, core_directory = core["load_plan"]()
    if core_directory != directory or len(sys.argv[2]) > 1024:
        raise ValueError()
    supplied = pathlib.Path(sys.argv[2])
    if not supplied.is_absolute() or str(supplied) != os.path.normpath(str(supplied)) or supplied.name != plan["probeName"]:
        raise ValueError()
    working = pathlib.Path(plan["gitDirectory"]) / plan["probeName"]
    if supplied != working and not re.fullmatch(r"git-blob-[A-Za-z0-9]{6}", supplied.parent.name):
        raise ValueError()
    checked_directory(supplied.parent)
    payload = regular(supplied, MAX_INPUT)
    if hashlib.sha256(payload).hexdigest() != plan["inputSha256"]:
        raise ValueError()
    fd = core["sealed_memory"](payload)
    try:
        os.dup2(fd, 0, inheritable=False)
        sys.argv = [str(core_path), plan["probeName"]]
        return core["main"]()
    finally:
        os.close(fd)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        # No filename, input, environment, or exception details reach Git logs.
        sys.exit(1)
