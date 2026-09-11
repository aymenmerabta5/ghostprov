"""One diagnostic snapshot. Never launches or signals processes or alters lifecycle files."""
import fcntl
import errno
import hashlib
import json
import os
import re
import stat
import time

AUTHORITY = json.loads(AUTHORITY_BYTES)
CHECKS = {name: False for name in ("workspace", "static", "artifacts", "retirement", "owners", "processes", "unchanged")}
HELD = []
ABSENT = []
INSPECTED = 0
RETIREMENT_HASH = None
PROCESS_CONFLICTS = []
PROCESS_CONFLICT_COUNT = 0
SCANS_COMPLETED = 0
FILE_FAILURE = None
STARTED = time.monotonic()
MAX_PROCESSES = 8192


class Blocked(Exception):
    pass


def require(condition, code):
    if not condition:
        raise Blocked(code)


def file_failure(role, reason, component, code="authority_file"):
    global FILE_FAILURE
    if FILE_FAILURE is None:
        FILE_FAILURE = {"role": role, "reason": reason, "component": component}
    raise Blocked(code)


def access_reason(error):
    return {errno.ENOENT: "absent", errno.ELOOP: "symlink", errno.ENOTDIR: "not_directory",
            errno.EACCES: "permission", errno.EPERM: "permission"}.get(error.errno, "io")


def open_path(path, flags):
    require(path.startswith("/") and "//" not in path and all(p not in (".", "..") for p in path.split("/")), "authority_file")
    parts = path.split("/")[1:]
    parent = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for index, part in enumerate(parts[:-1]):
            try:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
            except OSError as error:
                if error.errno == errno.ENOTDIR:
                    try:
                        if stat.S_ISLNK(os.stat(part, dir_fd=parent, follow_symlinks=False).st_mode):
                            error = OSError(errno.ELOOP, "")
                    except OSError:
                        pass
                error.preflight_component = "workspace" if parts[0] == "codex_workspace" and index == 1 else "ancestor"
                raise error
            os.close(parent)
            parent = child
        try:
            return os.open(parts[-1], flags | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent)
        except OSError as error:
            error.preflight_component = "leaf"
            raise
    finally:
        os.close(parent)


def identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode, info.st_uid)


def read_regular(path, maximum, keep=False, owner=False, optional=False, proc=False, role="unknown"):
    try:
        fd = open_path(path, os.O_RDONLY)
    except OSError as error:
        if optional and error.errno == errno.ENOENT:
            if keep:
                ABSENT.append((path, role))
            return None
        file_failure(role, access_reason(error), getattr(error, "preflight_component", "ancestor"))
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            file_failure(role, "nonregular", "opened")
        if before.st_size > maximum:
            file_failure(role, "size_bound", "opened")
        if not proc:
            if before.st_uid != os.getuid():
                file_failure(role, "uid_mismatch", "opened")
            if before.st_mode & 0o022:
                file_failure(role, "mode_writable", "opened")
        if owner:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                file_failure(role, "locked", "opened", "owner_locked")
        data = bytearray()
        while len(data) <= maximum:
            piece = os.read(fd, min(65536, maximum + 1 - len(data)))
            if not piece:
                break
            data.extend(piece)
        if len(data) > maximum:
            file_failure(role, "size_bound", "content")
        if not proc and len(data) != before.st_size:
            file_failure(role, "size_mismatch", "content")
        if identity(os.fstat(fd)) != identity(before):
            file_failure(role, "changed", "opened", "authority_changed")
        if keep:
            HELD.append((fd, path, identity(before), hashlib.sha256(data).hexdigest(), role))
            fd = None
        return bytes(data)
    except OSError as error:
        file_failure(role, access_reason(error), "opened")
    finally:
        if fd is not None:
            os.close(fd)


def authority_file(root, expected, role):
    data = read_regular(root + "/" + expected["path"], expected["bytes"], keep=True, role=role)
    if len(data) != expected["bytes"]:
        file_failure(role, "size_mismatch", "content")
    if hashlib.sha256(data).hexdigest() != expected["sha256"]:
        file_failure(role, "hash_mismatch", "content")


def owner_file(path, pid=None, instance=None, role="unknown"):
    data = read_regular(path, 4096, keep=True, owner=True, optional=True, role=role)
    if data is None:
        return None
    try:
        value = json.loads(data)
    except (ValueError, UnicodeError):
        file_failure(role, "format_invalid", "content", "owner_record")
    if not (type(value) is dict and sorted(value) == ["acquiredAtMs", "instanceId", "nonce", "pid", "version"]):
        file_failure(role, "format_invalid", "content", "owner_record")
    if not (value["version"] == 1 and type(value["pid"]) is int and value["pid"] >= 2 and
            type(value["acquiredAtMs"]) in (float, int) and 0 <= value["acquiredAtMs"] < 2**53 and
            all(type(value[k]) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value[k]) for k in ("instanceId", "nonce"))):
        file_failure(role, "format_invalid", "content", "owner_record")
    if not ((pid is None or value["pid"] == pid) and (instance is None or value["instanceId"] == instance)):
        file_failure(role, "identity_mismatch", "content", "owner_record")
    return value["pid"]


def proc_identity(pid):
    raw = read_regular("/proc/%d/stat" % pid, 4096, optional=True, proc=True, role="process")
    if raw is None:
        return None
    text = raw.decode("utf-8", "strict")
    end = text.rfind(") ")
    require(end > 1 and text.startswith(str(pid) + " ("), "process_scan")
    tail = text[end + 2:].split()
    require(len(tail) >= 20 and len(tail[0]) == 1 and all(re.fullmatch(r"[0-9]+", tail[i]) for i in (1, 2, 3, 19)), "process_scan")
    return (tail[0], int(tail[1]), int(tail[2]), int(tail[3]), tail[19])


def process_conflict(reason, scan, pid, ident, supervisor_pid, supervisor_tick, additional_pids, argv_flags=(False, False, False)):
    global PROCESS_CONFLICT_COUNT
    PROCESS_CONFLICT_COUNT += 1
    if len(PROCESS_CONFLICTS) >= 8:
        return
    related = [supervisor_pid] + additional_pids
    PROCESS_CONFLICTS.append({"reason": reason, "scan": scan,
        "state": ident[0] if ident is not None and ident[0] in "RSDZTtXxKWPI" else None,
        "oldPidMatch": pid == supervisor_pid,
        "oldStartTickMatch": pid == supervisor_pid and ident is not None and ident[4] == supervisor_tick,
        "childPidMatch": pid in additional_pids,
        "parentMatch": ident is not None and ident[1] in related,
        "groupMatch": ident is not None and ident[2] in related,
        "sessionMatch": ident is not None and ident[3] in related,
        "diagnosticSelf": pid == os.getpid(),
        "ghostprovArgument": argv_flags[0], "oldEntrypointArgument": argv_flags[1], "oldWorkspaceArgument": argv_flags[2]})


def scan_processes(old_root, supervisor_pid, supervisor_tick, additional_pids, scan):
    global INSPECTED, SCANS_COMPLETED
    require(time.monotonic() - STARTED < 20, "process_scan")
    with os.scandir("/proc") as entries:
        names = []
        count = 0
        for entry in entries:
            count += 1
            require(count <= MAX_PROCESSES + 1024, "process_scan")
            if entry.name.isascii() and entry.name.isdecimal():
                names.append(int(entry.name))
                require(len(names) <= MAX_PROCESSES, "process_scan")
    for pid in names:
        INSPECTED += 1
        require(time.monotonic() - STARTED < 20, "process_scan")
        ident = proc_identity(pid)
        if ident is None:
            continue
        # Reused PIDs and zombies block too: this experiment asks for absence.
        related = [supervisor_pid] + additional_pids
        related_match = any(value in related for value in (pid, ident[1], ident[2], ident[3]))
        cmdline = read_regular("/proc/%d/cmdline" % pid, 16384, optional=True, proc=True, role="process")
        if cmdline is None:
            if related_match:
                process_conflict("related_process", scan, pid, ident, supervisor_pid, supervisor_tick, additional_pids)
            require(proc_identity(pid) is None, "process_scan")
            continue
        args = cmdline.split(b"\0")
        flags = (any(arg.startswith(b"--ghostprov-") for arg in args),
                 any(arg == (old_root + "/" + AUTHORITY["entrypoint"]).encode() for arg in args),
                 any(arg.startswith((old_root + "/.ghostprov-v2/").encode()) for arg in args))
        if related_match or any(flags):
            process_conflict("related_process" if related_match else "argument_match", scan, pid, ident,
                             supervisor_pid, supervisor_tick, additional_pids, flags)
        after = proc_identity(pid)
        require(after is None or after[4] == ident[4], "process_scan")
        if after is not None and any(value in related for value in after[1:4]) and not related_match:
            process_conflict("relationship_changed", scan, pid, after, supervisor_pid, supervisor_tick, additional_pids)
    for pid in [supervisor_pid] + additional_pids:
        ident = proc_identity(pid)
        if ident is not None:
            process_conflict("known_pid_after_scan", scan, pid, ident, supervisor_pid, supervisor_tick, additional_pids)
    SCANS_COMPLETED += 1


def unchanged():
    for path, role in ABSENT:
        try:
            fd = open_path(path, os.O_RDONLY)
        except OSError as error:
            if error.errno == errno.ENOENT:
                continue
            file_failure(role, access_reason(error), "recheck", "authority_changed")
        os.close(fd)
        file_failure(role, "changed", "recheck", "authority_changed")
    for fd, path, expected, digest, role in HELD:
        if identity(os.fstat(fd)) != expected:
            file_failure(role, "changed", "recheck", "authority_changed")
        try:
            current = open_path(path, os.O_RDONLY)
        except OSError as error:
            file_failure(role, access_reason(error), "recheck", "authority_changed")
        try:
            if identity(os.fstat(current)) != expected:
                file_failure(role, "changed", "recheck", "authority_changed")
            os.lseek(current, 0, os.SEEK_SET)
            hasher = hashlib.sha256()
            remaining = expected[2]
            while remaining:
                piece = os.read(current, min(remaining, 65536))
                if not piece:
                    file_failure(role, "size_mismatch", "recheck", "authority_changed")
                remaining -= len(piece)
                hasher.update(piece)
            if os.read(current, 1):
                file_failure(role, "size_mismatch", "recheck", "authority_changed")
            if hasher.hexdigest() != digest:
                file_failure(role, "hash_mismatch", "recheck", "authority_changed")
        finally:
            os.close(current)


def check():
    global RETIREMENT_HASH
    new_root = "/codex_workspace/" + AUTHORITY["sessionId"]
    old_root = "/codex_workspace/" + AUTHORITY["oldSessionId"]
    require(os.getcwd() == new_root and os.path.realpath(new_root) == new_root and new_root != old_root, "workspace_identity")
    CHECKS["workspace"] = True
    authority_file(old_root, AUTHORITY["static"], "static")
    CHECKS["static"] = True
    for artifact in AUTHORITY["artifacts"]:
        authority_file(old_root, artifact, "artifact")
    CHECKS["artifacts"] = True
    retired = read_regular("/tmp/ghostprov-v2/supervisor/retired.pid", 4096, keep=True, role="retirement")
    match = re.fullmatch(rb"([0-9]{1,16}) ([A-Za-z0-9][A-Za-z0-9._:-]{0,127}) ([0-9]{1,32})\n", retired)
    if match is None:
        file_failure("retirement", "format_invalid", "content", "retirement_record")
    if not (match[2].decode() == AUTHORITY["supervisorNonce"] and 2 <= int(match[1]) < 2**53):
        file_failure("retirement", "identity_mismatch", "content", "retirement_record")
    pid = int(match[1])
    RETIREMENT_HASH = hashlib.sha256(retired).hexdigest()
    CHECKS["retirement"] = True
    owner_file("/tmp/ghostprov-v2/supervisor/sidecar.owner.lock", pid, AUTHORITY["supervisorInstanceId"], role="supervisor_owner")
    child_pid = owner_file("/tmp/ghostprov-v2/sidecar.owner.lock", role="child_owner")
    supervisor_pid_file = read_regular("/tmp/ghostprov-v2/supervisor/sidecar.pid", 4096, optional=True, keep=True, role="supervisor_pid")
    child_pid_file = read_regular("/tmp/ghostprov-v2/sidecar.pid", 4096, optional=True, keep=True, role="child_pid")
    if supervisor_pid_file is not None or child_pid_file is not None:
        process_conflict("pid_file_present", 0, -1, None, pid, match[3].decode(), [])
        raise Blocked("live_process")
    CHECKS["owners"] = True
    extra = [] if child_pid is None else [child_pid]
    scan_processes(old_root, pid, match[3].decode(), extra, 1)
    time.sleep(0.1)
    scan_processes(old_root, pid, match[3].decode(), extra, 2)
    unchanged()
    CHECKS["unchanged"] = True
    require(PROCESS_CONFLICT_COUNT == 0, "live_process")
    CHECKS["processes"] = True


def main():
    code = "retired"
    try:
        check()
    except Blocked as error:
        code = str(error)
    except Exception:
        code = "checker_failure"
    try:
        script = read_regular(os.getcwd() + "/exec", 48 * 1024, role="diagnostic_script")
        result = {"version": 1, "sessionId": AUTHORITY["sessionId"], "probeNonce": AUTHORITY["probeNonce"],
                  "authoritySha256": hashlib.sha256(AUTHORITY_BYTES).hexdigest(), "scriptSha256": hashlib.sha256(script).hexdigest(),
                  "outcome": "retired" if code == "retired" else "blocked", "code": code, "checks": CHECKS,
                  "processesInspected": INSPECTED, "retirementSha256": RETIREMENT_HASH,
                  "processScansCompleted": SCANS_COMPLETED, "processConflicts": PROCESS_CONFLICTS,
                  "processConflictCount": PROCESS_CONFLICT_COUNT, "processConflictsTruncated": PROCESS_CONFLICT_COUNT > len(PROCESS_CONFLICTS),
                  "fileFailure": FILE_FAILURE}
        encoded = json.dumps(result, separators=(",", ":")).encode()
        require(len(encoded) <= 4096, "checker_failure")
        filename = os.getcwd() + "/.ghostprov-v2/retirement-preflight-" + AUTHORITY["probeNonce"] + ".json"
        temporary = filename + ".pending"
        fd = open_path(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        try:
            os.fchmod(fd, 0o600)
            require(os.write(fd, encoded) == len(encoded), "checker_failure")
            os.fsync(fd)
        finally:
            os.close(fd)
        os.link(temporary, filename, follow_symlinks=False)
        os.unlink(temporary)
    finally:
        for fd, _, _, _, _ in HELD:
            os.close(fd)


main()
