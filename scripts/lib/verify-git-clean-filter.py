"""Real Linux Git and process acceptance; no network, credentials, or production paths."""
import concurrent.futures
import fcntl
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

if len(sys.argv) != 3:
    raise SystemExit("Expected pinned hook and marker source paths")
SOURCE = pathlib.Path(sys.argv[1]).read_text().replace("\r\n", "\n")
ROOT = pathlib.Path(tempfile.mkdtemp(prefix="ghostprov-fifth-acceptance-", dir="/tmp"))
SESSION = "11111111-1111-4111-8111-111111111111"
NAME = "ghostprov-fifth-" + SESSION + ".tex"
PAYLOAD = b"\\documentclass{article}\n\\begin{document}GhostProv probe\\end{document}\n"
ORIGINAL = b"[user]\n\tname = Preserved Offline Name\n[include]\n\tpath = relative-config\n"
CASES = []


def digest(value):
    return hashlib.sha256(value).hexdigest()


def run(f, args, data=None, timeout=5):
    result = subprocess.run(args, input=data, cwd=f["repo"], env=f["env"],
                            capture_output=True, timeout=timeout)
    if len(result.stdout) + len(result.stderr) > 32768:
        raise RuntimeError("offline_output_bound")
    return result


def setup(name, present=True, payload=PAYLOAD, launcher=None, transform=None):
    base = ROOT / name
    workspace = base / "codex_workspace" / SESSION
    directory = workspace / ".ghostprov-v2" / "clean-bootstrap"
    directory.mkdir(parents=True)
    home = base / "home"
    home.mkdir()
    repo = base / "repository"
    repo.mkdir()
    config = home / ".gitconfig"
    lock = home / ".gitconfig.lock"
    lock.mkdir()
    (lock / "owner").write_text(SESSION + "\n")
    original_inode = None
    if present:
        (directory / "original.gitconfig").write_bytes(ORIGINAL)
        original_inode = (directory / "original.gitconfig").stat().st_ino
    else:
        (directory / "config-was-absent").mkdir()
    hook = directory / "hook.py"
    hook.write_text(transform(SOURCE) if transform else SOURCE)
    hook.chmod(0o600)
    attrs = directory / "attributes"
    attrs.write_text("/" + NAME + " filter=ghostprov_fifth -text -ident\n")
    replacement = ('[core]\n\tattributesFile = ' + str(attrs) + '\n'
                   '[filter "ghostprov_fifth"]\n\trequired = true\n'
                   '\tclean = "/usr/bin/python3 ' + str(hook) + ' %f"\n').encode()
    env = {"PATH": "/usr/bin:/bin", "LANG": "C", "HOME": str(home), "TMPDIR": str(base),
           "GIT_CONFIG_GLOBAL": str(config), "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
    f = {"base": base, "workspace": workspace, "directory": directory, "config": config,
         "lock": lock, "repo": repo, "hook": hook, "env": env, "present": present,
         "replacement": replacement, "original_inode": original_inode, "payload": payload}
    config.write_bytes(replacement)
    for command in (["git", "init", "--quiet"], ["git", "config", "user.name", "Crixet Sandbox"],
                    ["git", "config", "user.email", "sandbox@example.com"]):
        assert run(f, command).returncode == 0
    (repo / ".git" / "info" / "exclude").write_text("*\n!*/\n!*.tex\n")
    (repo / NAME).write_bytes(payload)
    (repo / "unrelated.tex").write_bytes(b"unrelated\r\n$Id$\n")
    assert run(f, ["git", "commit", "--allow-empty", "-m", "INIT offline"]).returncode == 0
    checker = "import pathlib,json,os,hashlib\np=pathlib.Path(" + repr(str(config)) + ")\n"
    checker += "restored=" + ("p.exists() and hashlib.sha256(p.read_bytes()).hexdigest()==" + repr(digest(ORIGINAL))
                              if present else "not p.exists()") + "\n"
    checker += "lock_gone=not pathlib.Path(" + repr(str(lock)) + ").exists()\n"
    checker += ("with open('launches','a') as f: f.write('launch\\n')\n"
                "pathlib.Path('marker.json').write_text(json.dumps({'restoredBeforeLaunch':restored,"
                "'lockGoneBeforeLaunch':lock_gone,'uid':os.getuid(),'euid':os.geteuid()}))\n")
    (workspace / "counter.py").write_text(checker)
    launch = (launcher or "#!/bin/sh\nexec /usr/bin/python3 counter.py\n").encode()
    (workspace / "exec").write_bytes(launch)
    plan = {"version": 1, "sessionId": SESSION, "workspace": str(workspace), "gitDirectory": str(repo),
            "gitConfig": str(config), "configLock": str(lock), "configSha256": digest(replacement),
            "hookSha256": digest(hook.read_bytes()), "launchSha256": digest(launch),
            "inputSha256": digest(payload), "probeName": NAME, "expiresAtUnixMs": int(time.time() * 1000) + 60000}
    f["plan"] = plan
    save_plan(f)
    return f


def save_plan(f):
    (f["directory"] / "plan.json").write_text(json.dumps(f["plan"]))


def callback(f, payload=None, name=NAME):
    return run(f, ["/usr/bin/python3", str(f["hook"]), name], f["payload"] if payload is None else payload)


def receipt(f):
    path = f["directory"] / "result.json"
    return json.loads(path.read_text()) if path.exists() else None


def count(f):
    path = f["workspace"] / "launches"
    return len(path.read_text().splitlines()) if path.exists() else 0


def wait_marker(f):
    deadline = time.monotonic() + 2
    path = f["workspace"] / "marker.json"
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            return json.loads(path.read_text())
        time.sleep(0.01)
    return None


def restored(f):
    return (f["config"].read_bytes() == ORIGINAL and f["config"].stat().st_ino == f["original_inode"]
            if f["present"] else not os.path.lexists(f["config"]))


def record(name, passed, **details):
    CASES.append({"name": name, "passed": bool(passed), **details})


def snapshot(f):
    files = run(f, ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"])
    assert files.returncode == 0
    indexed = run(f, ["git", "update-index", "--add", "--remove", "--replace", "-z", "--stdin"], files.stdout)
    committed = run(f, ["git", "commit", "--allow-empty", "-m", "SNAPSHOT offline"])
    return indexed.returncode == 0 and committed.returncode == 0


try:
    if os.getuid() == 0 or os.geteuid() == 0:
        raise RuntimeError("unprivileged_probe_required")
    for name, present, payload in (("original_present", True, PAYLOAD), ("original_absent", False, PAYLOAD),
                                    ("empty_input", True, b""), ("binary_input", True, bytes(range(256)) * 4),
                                    ("maximum_input", True, b"a" * 8192)):
        f = setup(name, present=present, payload=payload)
        no_initial_launch = count(f) == 0 and receipt(f) is None
        success = snapshot(f)
        marker = wait_marker(f)
        value = receipt(f)
        content = run(f, ["git", "show", "HEAD:" + NAME])
        other = run(f, ["git", "show", "HEAD:unrelated.tex"])
        record(name, success and no_initial_launch and marker is not None and
               marker["restoredBeforeLaunch"] and marker["lockGoneBeforeLaunch"] and
               marker["uid"] == os.getuid() and marker["euid"] == os.geteuid() and
               value["outcome"] == "submitted" and all(value[k] for k in
               ("configRestored", "lockReleased", "inputVerified", "payloadWritten", "launchSubmitted")) and
               count(f) == 1 and restored(f) and (f["repo"] / NAME).read_bytes() == payload and
               content.returncode == 0 and content.stdout == payload and other.stdout == b"unrelated\r\n$Id$\n")
        record(name + "_second_snapshot", snapshot(f) and count(f) == 1 and restored(f))

    f = setup("parallel")
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: callback(f), range(32)))
    marker = wait_marker(f)
    record("32_callbacks_one_launch", marker is not None and count(f) == 1 and restored(f)
           and all(r.returncode == 0 and r.stdout == PAYLOAD for r in results), callbacks=32, launches=count(f))
    before = (f["directory"] / "result.json").read_bytes()
    again = callback(f)
    record("duplicate_keeps_receipt", again.returncode == 0 and again.stdout == PAYLOAD and count(f) == 1
           and (f["directory"] / "result.json").read_bytes() == before)

    for name, modify in (
        ("expired", lambda f: f["plan"].update(expiresAtUnixMs=1)),
        ("cancelled", lambda f: (f["directory"] / "cancelled").write_text("cancelled\n")),
        ("changed_launcher", lambda f: (f["workspace"] / "exec").write_text("#!/bin/sh\nexit 91\n")),
    ):
        f = setup(name)
        modify(f)
        save_plan(f)
        result = callback(f)
        record(name, result.returncode != 0 and count(f) == 0 and restored(f) and not f["lock"].exists()
               and not (f["directory"] / "launch.claim").exists() and receipt(f)["launchSubmitted"] is False)

    for name, mutate in (
        ("wrong_session", lambda f: f["plan"].update(sessionId=SESSION + "\n")),
        ("wrong_workspace", lambda f: f["plan"].update(workspace=str(f["workspace"]) + "/extra")),
        ("traversal_workspace", lambda f: f["plan"].update(workspace=str(f["workspace"]) + "/../" + SESSION)),
        ("wrong_git_directory", lambda f: f["plan"].update(gitDirectory=str(f["base"]))),
        ("wrong_probe_name", lambda f: f["plan"].update(probeName=NAME + "\n")),
        ("extra_plan_field", lambda f: f["plan"].update(extra=True)),
        ("bool_expiry", lambda f: f["plan"].update(expiresAtUnixMs=True)),
        ("wrong_hook_hash", lambda f: f["plan"].update(hookSha256="a" * 64)),
        ("hash_suffix", lambda f: f["plan"].update(inputSha256=f["plan"]["inputSha256"] + "\n")),
    ):
        f = setup(name)
        mutate(f)
        save_plan(f)
        result = callback(f)
        record(name, result.returncode != 0 and count(f) == 0 and receipt(f) is None and f["config"].read_bytes() == f["replacement"])

    for name, where, operation in (
        ("foreign_config", "config", "change"), ("foreign_owner", "owner", "change"),
        ("source_symlink", "hook", "symlink"), ("source_hardlink", "hook", "hardlink"),
        ("launcher_symlink", "launcher", "symlink"), ("launcher_hardlink", "launcher", "hardlink"),
        ("original_symlink", "backup", "symlink"), ("original_hardlink", "backup", "hardlink"),
        ("lock_extra_entry", "lock", "extra"),
    ):
        f = setup(name)
        paths = {"config": f["config"], "owner": f["lock"] / "owner", "hook": f["hook"],
                 "launcher": f["workspace"] / "exec", "backup": f["directory"] / "original.gitconfig", "lock": f["lock"]}
        target = paths[where]
        if operation == "change":
            target.write_text("foreign-owned-state\n")
        elif operation == "extra":
            (target / "foreign").write_text("foreign-owned-state\n")
        else:
            foreign = f["base"] / "foreign"
            foreign.write_bytes(target.read_bytes())
            target.unlink()
            if operation == "symlink":
                target.symlink_to(foreign)
            else:
                os.link(foreign, target)
        result = callback(f)
        record(name, result.returncode != 0 and count(f) == 0 and not (f["directory"] / "launch.claim").exists())
        if where == "config":
            record("foreign_config_and_backup_preserved", f["config"].read_text() == "foreign-owned-state\n"
                   and (f["directory"] / "original.gitconfig").read_bytes() == ORIGINAL and f["lock"].exists())

    f = setup("duplicate_json")
    plan_path = f["directory"] / "plan.json"
    plan_path.write_text('{"version":1,' + plan_path.read_text()[1:])
    record("duplicate_json", callback(f).returncode != 0 and receipt(f) is None and count(f) == 0)
    for name, data, filename in (("oversize_input", b"a" * 8193, NAME),
                                  ("wrong_input", b"wrong", NAME), ("wrong_argument", PAYLOAD, NAME + "x")):
        f = setup(name)
        result = callback(f, data, filename)
        record(name, result.returncode != 0 and receipt(f) is None and count(f) == 0)

    f = setup("failed_child", launcher="#!/bin/sh\nexit 71\n")
    first = callback(f)
    second = callback(f)
    record("submission_is_not_readiness", first.returncode == 0 and second.returncode == 0
           and receipt(f)["outcome"] == "submitted" and count(f) == 0 and (f["directory"] / "launch.claim").exists())

    def failed_popen(source):
        return source.replace("def main():", "def fail_submit(*args, **kwargs):\n    raise OSError()\n\n\ndef main():").replace(
            "subprocess.Popen([", "fail_submit([")

    f = setup("failed_submit", transform=failed_popen)
    first = callback(f)
    second = callback(f)
    record("ambiguous_submission_not_replayed", first.returncode != 0 and second.returncode == 0
           and receipt(f)["outcome"] == "ambiguous" and count(f) == 0 and restored(f))

    f = setup("lost_receipt", transform=lambda source: source.replace(
        'json_once(directory / "result.json", result)', 'raise OSError("offline receipt loss")'))
    first = callback(f)
    marker = wait_marker(f)
    second = callback(f)
    record("lost_receipt_not_replayed", first.returncode != 0 and second.returncode == 0 and marker is not None
           and count(f) == 1 and receipt(f) is None and (f["directory"] / "launch.claim").exists())

    f = setup("sealed_launcher", transform=lambda source: source.replace(
        "fd = sealed_memory(launcher)", "fd = sealed_memory(launcher)\n        (pathlib.Path(plan['workspace']) / 'exec').write_text('#!/bin/sh\\nexit 91\\n')"))
    result = callback(f)
    record("launch_uses_sealed_bytes", result.returncode == 0 and wait_marker(f) is not None and count(f) == 1)

    f = setup("stdin_timeout")
    before = time.monotonic()
    child = subprocess.Popen(["/usr/bin/python3", str(f["hook"]), NAME], cwd=f["repo"], env=f["env"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    code = child.wait(timeout=3)
    child.stdin.close()
    child.stdout.close()
    child.stderr.close()
    elapsed = time.monotonic() - before
    record("stdin_deadline", code != 0 and elapsed < 2.5 and count(f) == 0, elapsedMs=round(elapsed * 1000))

    marker_source = pathlib.Path(sys.argv[2]).read_text().replace("\r\n", "\n")
    marker_nonce = "22222222-2222-4222-8222-222222222222"
    marker_launch = "#!/bin/sh\nexec /usr/bin/python3 - '" + marker_nonce + "' <<'GP_FIFTH_PY'\n" + marker_source + "\nGP_FIFTH_PY\n"
    f = setup("exact_live_marker", launcher=marker_launch)
    success = snapshot(f)
    marker_path = f["directory"] / "marker.json"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and (not marker_path.exists() or marker_path.stat().st_size == 0):
        time.sleep(0.01)
    marker = json.loads(marker_path.read_text()) if marker_path.exists() else None
    record("exact_live_marker", success and marker is not None and marker["outcome"] == "verified"
           and marker["nonce"] == marker_nonce and marker["uid"] == os.getuid() and marker["euid"] == os.geteuid()
           and all(marker[name] for name in ("configInactive", "configLockReleased", "claimVerified", "gitSnapshotVerified", "worktreeBytesPreserved")))

    report = {"candidate": "git-clean-filter", "live": False, "sourceSha256": digest(SOURCE.encode()), "markerSha256": digest(marker_source.encode()),
              "uid": os.getuid(), "euid": os.geteuid(), "gitVersion": run(f, ["git", "--version"]).stdout.decode().strip(),
              "passed": sum(c["passed"] for c in CASES), "failed": sum(not c["passed"] for c in CASES), "cases": CASES}
    print(json.dumps(report))
    sys.exit(1 if report["failed"] else 0)
finally:
    assert ROOT.parent == pathlib.Path("/tmp") and ROOT.name.startswith("ghostprov-fifth-acceptance-")
    shutil.rmtree(ROOT)
