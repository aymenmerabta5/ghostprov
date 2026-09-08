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
ROOT = pathlib.Path(tempfile.mkdtemp(prefix="ghostprov-sixth-acceptance-", dir="/tmp"))
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


scope = {'SOURCE':SOURCE,'ROOT':ROOT,'SESSION':SESSION,'NAME':NAME,'PAYLOAD':PAYLOAD,'ORIGINAL':ORIGINAL,'CASES':CASES,'digest':digest,'run':run,'setup':setup,'save_plan':save_plan,'callback':callback,'receipt':receipt,'count':count,'wait_marker':wait_marker,'restored':restored,'record':record,'snapshot':snapshot}
CORE_SHA = "cbaf273695287f5a34f5cac9fb9e359be0d2a4e8ffbb671cd145dd40aec5b555"
RELAY = pathlib.Path(sys.argv[2]).read_text().replace("\r\n", "\n").encode()
RELAY_SHA = hashlib.sha256(RELAY).hexdigest()
assert hashlib.sha256(scope["SOURCE"].encode()).hexdigest() == CORE_SHA
NAME = scope["NAME"]
PAYLOAD = scope["PAYLOAD"]
ROOT = scope["ROOT"]
CASES = scope["CASES"]
record = scope["record"]
run = scope["run"]


def setup(name, **kwargs):
    f = scope["setup"](name, **kwargs)
    relay = f["directory"] / "textconv-input.py"
    relay.write_bytes(RELAY)
    relay.chmod(0o600)
    attrs = f["directory"] / "attributes"
    attrs.write_text("/" + NAME + " diff=ghostprov_textconv -text -ident\n")
    replacement = ('[core]\n\tattributesFile = ' + str(attrs) + '\n'
                   '[diff "ghostprov_textconv"]\n\ttextconv = "/usr/bin/python3 ' + str(relay) + ' ' + RELAY_SHA + '"\n'
                   '\tcachetextconv = false\n\tbinary = false\n').encode()
    f["relay"] = relay
    f["replacement"] = replacement
    f["config"].write_bytes(replacement)
    f["plan"]["configSha256"] = hashlib.sha256(replacement).hexdigest()
    scope["save_plan"](f)
    return f


def callback(f, filename=None, digest=RELAY_SHA, extra=()):
    return run(f, ["/usr/bin/python3", str(f["relay"]), digest, str(filename or f["repo"] / NAME), *extra])


def no_launch(f):
    return scope["count"](f) == 0 and not (f["directory"] / "launch.claim").exists()


try:
    if os.getuid() == 0 or os.geteuid() != os.getuid():
        raise RuntimeError("unprivileged_probe_required")
    for name, present, payload in (("present", True, PAYLOAD), ("absent", False, PAYLOAD),
                                    ("empty", True, b""), ("binary", True, bytes(range(256)) * 4),
                                    ("maximum", True, b"a" * 8192)):
        f = setup(name, present=present, payload=payload)
        indexed = scope["snapshot"](f)
        before = no_launch(f) and scope["receipt"](f) is None
        shown = run(f, ["git", "show", "HEAD"])
        marker = scope["wait_marker"](f)
        receipt = scope["receipt"](f)
        blob = run(f, ["git", "cat-file", "blob", "HEAD:" + NAME])
        record("real_git_" + name, indexed and before and shown.returncode == 0 and marker is not None
               and marker["restoredBeforeLaunch"] and marker["lockGoneBeforeLaunch"] and marker["uid"] == os.getuid()
               and receipt["outcome"] == "submitted" and scope["restored"](f) and scope["count"](f) == 1
               and blob.returncode == 0 and blob.stdout == payload and (f["repo"] / NAME).read_bytes() == payload)
        again = run(f, ["git", "show", "HEAD"])
        record("repeat_show_" + name, again.returncode == 0 and scope["count"](f) == 1 and scope["restored"](f))

    f = setup("parallel")
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: callback(f), range(32)))
    marker = scope["wait_marker"](f)
    record("32_callbacks_one_launch", marker is not None and scope["count"](f) == 1 and scope["restored"](f)
           and all(item.returncode == 0 and item.stdout == PAYLOAD for item in results), callbacks=32, launches=scope["count"](f))

    for name, change in (
        ("input_symlink", "symlink"), ("input_hardlink", "hardlink"), ("input_fifo", "fifo"),
        ("input_directory", "directory"), ("input_missing", "missing"),
        ("input_oversize", "oversize"), ("input_wrong_bytes", "changed"),
    ):
        f = setup(name)
        input_file = f["repo"] / NAME
        input_file.unlink()
        if change in ("symlink", "hardlink"):
            foreign = f["base"] / "foreign"
            foreign.write_bytes(PAYLOAD)
            if change == "symlink": input_file.symlink_to(foreign)
            else: os.link(foreign, input_file)
        elif change == "fifo": os.mkfifo(input_file)
        elif change == "directory": input_file.mkdir()
        elif change == "oversize": input_file.write_bytes(b"a" * 8193)
        elif change == "changed": input_file.write_bytes(b"wrong")
        started = time.monotonic()
        result = callback(f)
        elapsed = time.monotonic() - started
        record(name, result.returncode != 0 and result.stdout == b"" and no_launch(f) and elapsed < 1
               and f["config"].read_bytes() == f["replacement"], elapsedMs=round(elapsed * 1000))

    for name, filename, supplied_hash, extra in (
        ("wrong_relay_hash", None, "a" * 64, ()),
        ("hash_suffix", None, RELAY_SHA + "\n", ()),
        ("relative_input", pathlib.Path(NAME), RELAY_SHA, ()),
        ("extra_argument", None, RELAY_SHA, ("extra",)),
    ):
        f = setup(name)
        result = callback(f, filename=filename, digest=supplied_hash, extra=extra)
        record(name, result.returncode != 0 and no_launch(f) and result.stdout == b"")

    for name, target in (("changed_core", "hook"), ("changed_relay", "relay")):
        f = setup(name)
        f[target].write_text(f[target].read_text() + "\n# changed\n")
        record(name, callback(f).returncode != 0 and no_launch(f))

    for name in ("expired", "cancelled", "foreign_config", "foreign_lock"):
        f = setup(name)
        if name == "expired":
            f["plan"]["expiresAtUnixMs"] = 1
            scope["save_plan"](f)
        elif name == "cancelled": (f["directory"] / "cancelled").write_text("cancelled\n")
        elif name == "foreign_config": f["config"].write_text("foreign config\n")
        else: (f["lock"] / "owner").write_text("foreign owner\n")
        result = callback(f)
        receipt = scope["receipt"](f)
        record(name, result.returncode != 0 and no_launch(f) and receipt is not None and receipt["outcome"] == name
               and (scope["restored"](f) if name in ("expired", "cancelled") else f["lock"].exists()))

    f = setup("wrong_path")
    wrong = f["base"] / NAME
    wrong.write_bytes(PAYLOAD)
    record("wrong_input_directory", callback(f, wrong).returncode != 0 and no_launch(f))
    f = setup("symlink_parent")
    actual = f["base"] / "git-blob-Abc123"
    actual.mkdir()
    (actual / NAME).write_bytes(PAYLOAD)
    link = f["base"] / "git-blob-Zyx321"
    link.symlink_to(actual, target_is_directory=True)
    record("symlink_input_parent", callback(f, link / NAME).returncode != 0 and no_launch(f))

    report = {"candidate": "git-textconv", "live": False, "coreSha256": CORE_SHA, "relaySha256": RELAY_SHA,
              "uid": os.getuid(), "euid": os.geteuid(), "gitVersion": run(f, ["git", "--version"]).stdout.decode().strip(),
              "passed": sum(item["passed"] for item in CASES), "failed": sum(not item["passed"] for item in CASES), "cases": CASES}
    print(json.dumps(report))
    raise SystemExit(1 if report["failed"] else 0)
finally:
    assert ROOT.parent == pathlib.Path("/tmp") and ROOT.name.startswith("ghostprov-sixth-acceptance-")
    shutil.rmtree(ROOT)
