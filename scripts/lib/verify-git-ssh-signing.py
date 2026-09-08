"""Offline acceptance harness. Fresh Linux HOME, real Git, no network or secrets."""
import concurrent.futures
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

if len(sys.argv) != 8:
    raise SystemExit("Expected hook, signer, marker, runtime, report and two source hashes")
SOURCE = pathlib.Path(sys.argv[1]).resolve()
SOURCE_BYTES = SOURCE.read_text().replace("\r\n", "\n").encode()
SIGNER = pathlib.Path(sys.argv[2]).read_text().replace("\r\n", "\n").encode()
MARKER = pathlib.Path(sys.argv[3]).resolve()
RUNTIME = sys.argv[4]
DESTINATION = pathlib.Path(sys.argv[5]).resolve()
assert RUNTIME in ("/tmp/ghostprov-deno/deno", "/usr/local/bin/deno")
assert hashlib.sha256(SOURCE_BYTES).hexdigest() == sys.argv[6]
assert hashlib.sha256(SIGNER).hexdigest() == sys.argv[7]
assert DESTINATION.name == "linux-git.json" and DESTINATION.parent.name.startswith("run-")
assert DESTINATION.parent.parent.name == "git-ssh-signing-offline"
runtime_version = subprocess.run([RUNTIME, "--version"], env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent"},
                                 capture_output=True, timeout=3).stdout.decode().splitlines()[0]
assert runtime_version.startswith("deno 2.9.5 ")
SESSION = "11111111-1111-4111-8111-111111111111"
ORIGINAL = b"[user]\n\tname = Offline Preserved Name\n[include]\n\tpath = relative-config\n"
ROOT = pathlib.Path(tempfile.mkdtemp(prefix="ghostprov-git-ssh-acceptance-", dir="/tmp"))
CASES = []


def digest(value):
    return hashlib.sha256(value).hexdigest()


def run(args, fixture, cwd=None, timeout=6):
    result = subprocess.run(args, cwd=cwd or fixture["repo"], env=fixture["env"],
                            capture_output=True, timeout=timeout)
    if len(result.stdout) + len(result.stderr) > 32768:
        raise RuntimeError("offline output bound exceeded")
    return result


def setup(name, present=True, expired=False, launch=None, transform=None):
    base = ROOT / name
    workspace = base / "codex_workspace" / SESSION
    directory = workspace / ".ghostprov-v2" / "ssh-bootstrap"
    directory.mkdir(parents=True)
    home = base / "home"
    home.mkdir()
    repo = base / "repository"
    repo.mkdir()
    temporary = base / "git-tmp"
    temporary.mkdir()
    config = home / ".gitconfig"
    lock = home / ".gitconfig.lock"
    lock.mkdir()
    (lock / "owner").write_text(SESSION + "\n")
    if present:
        (directory / "original.gitconfig").write_bytes(ORIGINAL)
    else:
        (directory / "config-was-absent").mkdir()
    hook = directory / "hook.py"
    hook.write_bytes(SOURCE_BYTES)
    if transform:
        hook.write_text(transform(hook.read_text()))
    hook.chmod(0o600)
    replacement = (f'[commit]\n\tgpgSign = true\n[gpg]\n\tformat = ssh\n[gpg "ssh"]\n'
                   f'\tprogram = {hook}\n\tdefaultKeyCommand = "/usr/bin/python3 {hook} --prepare"\n').encode()
    env = {"PATH": "/usr/bin:/bin", "LANG": "C", "HOME": str(home),
           "GIT_CONFIG_GLOBAL": str(config), "GIT_CONFIG_SYSTEM": "/dev/null",
           "GIT_CONFIG_NOSYSTEM": "1", "TMPDIR": str(temporary)}
    fixture = {"base": base, "workspace": workspace, "directory": directory,
               "home": home, "repo": repo, "tmp": temporary, "config": config,
               "lock": lock, "hook": hook, "env": env, "present": present}
    assert run(["/usr/bin/git", "init", "--quiet"], fixture).returncode == 0
    for key, value in (("user.name", "Crixet Sandbox"), ("user.email", "sandbox@example.com")):
        assert run(["/usr/bin/git", "config", key, value], fixture).returncode == 0
    checker = "import pathlib,hashlib\np=pathlib.Path(" + repr(str(config)) + ")\n"
    checker += "ok=" + ("p.exists() and hashlib.sha256(p.read_bytes()).hexdigest()==" + repr(digest(ORIGINAL))
                         if present else "not p.exists()") + "\n"
    checker += ("pathlib.Path('restored-before-launch').write_text(str(ok))\n"
                "with open('launches','a') as f: f.write('launch\\n')\n")
    (workspace / "counter.py").write_text(checker)
    launcher = (launch or "#!/bin/sh\nexec /usr/bin/python3 counter.py\n").encode()
    (workspace / "exec").write_bytes(launcher)
    plan = {"version": 1, "sessionId": SESSION, "workspace": str(workspace), "gitDirectory": str(repo),
            "gitConfig": str(config), "configLock": str(lock), "configSha256": digest(replacement),
            "hookSha256": digest(hook.read_bytes()), "launchSha256": digest(launcher),
            "gitTempDirectory": str(temporary), "expiresAtUnixMs": 1 if expired else int(time.time() * 1000) + 60000}
    plan.update(denoPath=RUNTIME, signerSha256=hashlib.sha256(SIGNER).hexdigest())
    (directory / "sshsig.mjs").write_bytes(SIGNER)
    fixture["plan"] = plan
    (directory / "plan.json").write_text(json.dumps(plan))
    config.write_bytes(replacement)
    fixture["replacement"] = replacement
    fixture["original_identity"] = ((directory / "original.gitconfig").stat().st_ino if present else None)
    return fixture


def receipt(fixture):
    path = fixture["directory"] / "result.json"
    return json.loads(path.read_text()) if path.exists() else None


def launches(fixture):
    path = fixture["workspace"] / "launches"
    return len(path.read_text().splitlines()) if path.exists() else 0


def settle(fixture):
    deadline = time.monotonic() + 2
    while (receipt(fixture) or {}).get("launchSubmitted") and not launches(fixture) and time.monotonic() < deadline:
        time.sleep(0.01)


def config_restored(fixture):
    return (fixture["config"].read_bytes() == ORIGINAL if fixture["present"]
            else not os.path.lexists(fixture["config"]))


def keys_removed(fixture):
    return not any(os.path.lexists(fixture["directory"] / name) for name in ("ephemeral-key", "ephemeral-key.pub"))


def commit(fixture):
    return run(["/usr/bin/git", "commit", "--allow-empty", "-m", "INIT offline-fourth-method"], fixture)


def forced_arguments(fixture, program=None):
    return ["/usr/bin/git", "-c", "commit.gpgSign=true", "-c", "gpg.format=ssh", "-c",
            "gpg.ssh.program=" + str(program or fixture["hook"]), "-c",
            "gpg.ssh.defaultKeyCommand=/usr/bin/python3 " + str(fixture["hook"]) + " --prepare",
            "commit", "--allow-empty", "-m", "INIT offline-forced"]


def wait_until(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.01)
    return bool(predicate())


def signer_wrapper(fixture, code):
    wrapper = fixture["base"] / "signer-wrapper.py"
    wrapper.write_text("#!/usr/bin/python3\nimport os,pathlib,sys,time,json\n"
                       "hook=" + repr(str(fixture["hook"])) + "\n"
                       "directory=pathlib.Path(hook).parent\n"
                       "args=sys.argv[1:]\n" + code + "\n"
                       "os.execv('/usr/bin/python3',['/usr/bin/python3',hook,*args])\n")
    wrapper.chmod(0o700)
    return wrapper


def verify(fixture, revision="HEAD"):
    prepared = fixture["directory"] / "prepared.json"
    if not prepared.exists():
        return False
    allowed = fixture["base"] / "allowed-signers"
    allowed.write_text("sandbox@example.com " + json.loads(prepared.read_text())["publicKey"] + "\n")
    return run(["/usr/bin/git", "-c", "gpg.format=ssh", "-c", "gpg.ssh.program=/usr/bin/ssh-keygen",
                "-c", "gpg.ssh.allowedSignersFile=" + str(allowed), "verify-commit", revision], fixture).returncode == 0


def record(name, passed, **evidence):
    CASES.append({"id": name, "passed": bool(passed), "evidence": evidence})
    if not passed:
        print(json.dumps(CASES[-1]), flush=True)


def standard_cases():
    for present in (True, False):
        name = "existing_config" if present else "absent_config"
        fixture = setup(name, present)
        started = time.monotonic()
        result = commit(fixture)
        elapsed = round((time.monotonic() - started) * 1000)
        settle(fixture)
        restored = config_restored(fixture)
        signature = result.returncode == 0 and verify(fixture)
        marker = fixture["workspace"] / "restored-before-launch"
        passed = (result.returncode == 0 and signature and restored and not fixture["lock"].exists()
                  and launches(fixture) == 1 and marker.read_text() == "True" and keys_removed(fixture)
                  and not list(fixture["tmp"].iterdir())
                  and (not present or fixture["config"].stat().st_ino == fixture["original_identity"]))
        record(name, passed, commitExit=result.returncode, signatureValid=signature, configRestored=restored,
               launches=launches(fixture), keysRemoved=keys_removed(fixture), elapsedMs=elapsed,
               receipt=receipt(fixture), gitTemporaryFiles=[p.name for p in fixture["tmp"].iterdir()],
               stderr=result.stderr.decode()[:512])


def race_case():
    fixture = setup("race32")
    empty_tree = run(["/usr/bin/git", "hash-object", "-w", "-t", "tree", "--stdin"], fixture).stdout.decode().strip()
    # commit-tree avoids unrelated index.lock contention. Every real Git parent
    # sees the armed options even after the winner restores the global config.
    arguments = ["/usr/bin/git", "-c", "gpg.format=ssh", "-c", "gpg.ssh.program=" + str(fixture["hook"]),
                 "-c", "gpg.ssh.defaultKeyCommand=/usr/bin/python3 " + str(fixture["hook"]) + " --prepare",
                 "commit-tree", "-S", empty_tree, "-m", "INIT concurrent-offline"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(lambda _: run(arguments, fixture), range(32)))
    settle(fixture)
    successful = [r for r in results if r.returncode == 0]
    verified = len(successful) == 1 and verify(fixture, successful[0].stdout.decode().strip())
    record("concurrent32_once", len(successful) == 1 and launches(fixture) == 1 and verified
           and config_restored(fixture) and keys_removed(fixture), invocations=32, successes=len(successful),
           launches=launches(fixture), signatureValid=verified, receipt=receipt(fixture))
    foreign = b"[user]\n\tname = Later Offline Writer\n"
    fixture["config"].write_bytes(foreign)
    again = [run(arguments, fixture) for _ in range(3)]
    record("no_replay_or_foreign_overwrite", all(r.returncode != 0 for r in again)
           and launches(fixture) == 1 and fixture["config"].read_bytes() == foreign,
           attempts=3, launches=launches(fixture))


def rejection_cases():
    for name in ("cancelled", "expired", "foreign_config", "foreign_lock", "hook_tamper", "launch_tamper",
                 "plan_extra", "missing_original", "config_symlink", "config_hardlink", "launch_symlink",
                 "backup_symlink", "bad_parent", "wrong_cwd"):
        fixture = setup(name, expired=name == "expired")
        expected_restored = name in {"cancelled", "expired", "launch_tamper", "launch_symlink"}
        if name == "cancelled":
            (fixture["directory"] / "cancelled").write_text("cancelled")
        elif name == "foreign_config":
            fixture["config"].write_text("[user]\n\tname = Foreign Offline Writer\n")
        elif name == "foreign_lock":
            (fixture["lock"] / "owner").write_text("foreign\n")
        elif name == "hook_tamper":
            with fixture["hook"].open("a") as stream:
                stream.write("\n# altered\n")
        elif name == "launch_tamper":
            (fixture["workspace"] / "exec").write_text("#!/bin/sh\nexit 0\n")
        elif name == "plan_extra":
            fixture["plan"]["unexpected"] = True
            (fixture["directory"] / "plan.json").write_text(json.dumps(fixture["plan"]))
        elif name == "missing_original":
            (fixture["directory"] / "original.gitconfig").unlink()
        elif name == "config_symlink":
            moved = fixture["base"] / "outside-config"
            fixture["config"].rename(moved)
            fixture["config"].symlink_to(moved)
        elif name == "config_hardlink":
            os.link(fixture["config"], fixture["base"] / "outside-config")
        elif name == "launch_symlink":
            moved = fixture["base"] / "outside-launch"
            (fixture["workspace"] / "exec").rename(moved)
            (fixture["workspace"] / "exec").symlink_to(moved)
        elif name == "backup_symlink":
            moved = fixture["base"] / "outside-backup"
            (fixture["directory"] / "original.gitconfig").rename(moved)
            (fixture["directory"] / "original.gitconfig").symlink_to(moved)
        if name == "bad_parent":
            result = run(["/usr/bin/python3", str(fixture["hook"]), "--prepare"], fixture)
        elif name == "wrong_cwd":
            result = run(["/usr/bin/git", "-c", "gpg.format=ssh", "-c",
                          "gpg.ssh.defaultKeyCommand=/usr/bin/python3 " + str(fixture["hook"]) + " --prepare",
                          "var", "GIT_COMMITTER_IDENT"], fixture, cwd=fixture["base"])
            # Direct invocation makes cwd rejection observable without Git
            # refusing an unrelated non-repository command first.
            result = run(["/usr/bin/python3", str(fixture["hook"]), "--prepare"], fixture, cwd=fixture["base"])
        else:
            # Foreign global config is intentionally no longer armed; supply
            # the captured candidate settings to reach the rejection boundary.
            result = run(["/usr/bin/git", "-c", "commit.gpgSign=true", "-c", "gpg.format=ssh", "-c",
                          "gpg.ssh.program=" + str(fixture["hook"]), "-c",
                          "gpg.ssh.defaultKeyCommand=/usr/bin/python3 " + str(fixture["hook"]) + " --prepare",
                          "commit", "--allow-empty", "-m", "INIT offline-rejected"], fixture)
        restored = config_restored(fixture)
        record(name, result.returncode != 0 and launches(fixture) == 0 and keys_removed(fixture)
               and restored == expected_restored, exit=result.returncode, launches=launches(fixture),
               keysRemoved=keys_removed(fixture), configRestored=restored, receipt=receipt(fixture),
               leftoverGitFiles=len(list(fixture["tmp"].iterdir())), stderr=result.stderr.decode()[:512])


def extended_rejections():
    for name in ("plan_boolean_version", "plan_cross_session", "plan_symlink", "plan_oversize", "plan_duplicate",
                 "gate_symlink", "gate_hardlink", "launch_hardlink", "launch_oversize", "foreign_key"):
        fixture = setup(name)
        foreign = None
        if name == "plan_boolean_version":
            fixture["plan"]["version"] = True
        elif name == "plan_cross_session":
            fixture["plan"]["sessionId"] = "22222222-2222-4222-8222-222222222222"
        if name in {"plan_boolean_version", "plan_cross_session"}:
            (fixture["directory"] / "plan.json").write_text(json.dumps(fixture["plan"]))
        elif name == "plan_symlink":
            saved = fixture["base"] / "outside-plan"
            (fixture["directory"] / "plan.json").rename(saved)
            (fixture["directory"] / "plan.json").symlink_to(saved)
        elif name == "plan_oversize":
            with (fixture["directory"] / "plan.json").open("a") as stream:
                stream.write(" " * 8192)
        elif name == "plan_duplicate":
            filename = fixture["directory"] / "plan.json"
            filename.write_text(filename.read_text().replace('"version": 1', '"version": 1,"version": 1'))
        elif name in {"gate_symlink", "gate_hardlink"}:
            foreign = fixture["base"] / "outside-gate"
            foreign.write_bytes(b"foreign\n")
            if name == "gate_symlink":
                (fixture["directory"] / "gate.lock").symlink_to(foreign)
            else:
                os.link(foreign, fixture["directory"] / "gate.lock")
        elif name == "launch_hardlink":
            os.link(fixture["workspace"] / "exec", fixture["base"] / "outside-launch")
        elif name == "launch_oversize":
            (fixture["workspace"] / "exec").write_bytes(b"#" * 65537)
        elif name == "foreign_key":
            foreign = fixture["directory"] / "ephemeral-key"
            foreign.write_bytes(b"foreign\n")
        result = run(forced_arguments(fixture), fixture)
        settle(fixture)
        preserved = foreign is None or foreign.read_bytes() == b"foreign\n"
        record(name, result.returncode != 0 and launches(fixture) == 0 and preserved
               and (name == "foreign_key" or keys_removed(fixture)), exit=result.returncode,
               launches=launches(fixture), foreignPreserved=preserved, keysRemoved=keys_removed(fixture),
               receipt=receipt(fixture))


def interrupted_cases():
    fixture = setup("signer_not_started")
    result = run(forced_arguments(fixture, "/usr/bin/false"), fixture)
    cleanup = wait_until(lambda: keys_removed(fixture) and receipt(fixture) is not None)
    replay = run(forced_arguments(fixture), fixture)
    record("signer_not_started", result.returncode != 0 and cleanup and config_restored(fixture)
           and launches(fixture) == 0 and replay.returncode != 0,
           keysRemoved=keys_removed(fixture), receipt=receipt(fixture), replayExit=replay.returncode,
           leftoverPublicGitFiles=len(list(fixture["tmp"].iterdir())), disposition="retire_fixture")

    fixture = setup("prepare_lease_expired")
    wrapper = signer_wrapper(fixture, "time.sleep(4.25)")
    started = time.monotonic()
    result = run(forced_arguments(fixture, wrapper), fixture)
    record("prepare_lease_expired", result.returncode != 0 and keys_removed(fixture)
           and (receipt(fixture) or {}).get("outcome") == "expired" and launches(fixture) == 0,
           elapsedMs=round((time.monotonic() - started) * 1000), keysRemoved=keys_removed(fixture),
           launches=launches(fixture), receipt=receipt(fixture))

    fixture = setup("git_parent_killed")
    wrapper = signer_wrapper(fixture, "parent=os.getppid()\n(directory/'waiting-signer').write_text('1')\n"
                             "deadline=time.monotonic()+5\n"
                             "while os.getppid()==parent and time.monotonic()<deadline: time.sleep(0.01)\n"
                             "sys.exit(1)")
    process = subprocess.Popen(forced_arguments(fixture, wrapper), cwd=fixture["repo"], env=fixture["env"],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert wait_until(lambda: (fixture["directory"] / "waiting-signer").exists())
    process.kill()  # Only the retained Popen object created by this offline test.
    process.communicate(timeout=6)
    cleanup = wait_until(lambda: keys_removed(fixture) and receipt(fixture) is not None)
    record("git_parent_killed", cleanup and launches(fixture) == 0 and config_restored(fixture),
           keysRemoved=keys_removed(fixture), receipt=receipt(fixture), launches=launches(fixture))


def handoff_tampering_cases():
    snippets = {
        "sign_arguments": "args.insert(2,'unexpected')",
        "sign_cancelled": "(directory/'cancelled').write_text('1')",
        "sign_wrong_cwd": "os.chdir(directory.parent.parent.parent)",
        "sign_hook_tamper": "with open(hook,'a') as f: f.write('\\n# changed after prepare\\n')",
        "sign_plan_tamper": "(directory/'plan.json').write_text('{}')",
        "sign_public_tamper": "pathlib.Path(args[5]).write_text('ssh-ed25519 invalid')",
        "sign_buffer_symlink": "buffer=pathlib.Path(args[7])\nsaved=directory/'foreign-buffer'\n"
                               "saved.write_text('foreign')\nbuffer.unlink()\nbuffer.symlink_to(saved)",
        "sign_public_symlink": "public=pathlib.Path(args[5])\nsaved=directory/'foreign-public'\n"
                               "saved.write_text('foreign')\npublic.unlink()\npublic.symlink_to(saved)",
        "sign_prepared_parent": "p=directory/'prepared.json'\nv=json.loads(p.read_text())\n"
                                "v['parent']['startTicks']='1'\np.write_text(json.dumps(v))",
    }
    for name, snippet in snippets.items():
        fixture = setup(name)
        wrapper = signer_wrapper(fixture, snippet)
        result = run(forced_arguments(fixture, wrapper), fixture)
        cleanup = wait_until(lambda: keys_removed(fixture))
        preserved = all(p.read_text() == "foreign" for p in fixture["directory"].glob("foreign-*"))
        record(name, result.returncode != 0 and launches(fixture) == 0 and cleanup and preserved,
               exit=result.returncode, launches=launches(fixture), keysRemoved=keys_removed(fixture),
               foreignPreserved=preserved, receipt=receipt(fixture))


def injected_failure_cases():
    claim_line = '        exclusive(directory / "launch.claim", (plan["sessionId"] + "\\n").encode())\n'
    fixture = setup("popen_failure", transform=lambda text: text.replace(claim_line,
                    claim_line + '        raise OSError("injected spawn failure")\n'))
    result = commit(fixture)
    again = run(forced_arguments(fixture), fixture)
    record("claimed_spawn_failure_never_replays", result.returncode != 0 and again.returncode != 0
           and launches(fixture) == 0 and keys_removed(fixture)
           and (receipt(fixture) or {}).get("outcome") == "ambiguous"
           and (fixture["directory"] / "launch.claim").exists() and not list(fixture["tmp"].iterdir()),
           receipt=receipt(fixture), launches=launches(fixture), replayExit=again.returncode)

    fixture = setup("receipt_failure", transform=lambda text: text.replace('def json_once(path, value):\n',
                    'def json_once(path, value):\n    if path.name == "result.json":\n        raise OSError("injected receipt failure")\n'))
    result = commit(fixture)
    wait_until(lambda: launches(fixture) > 0)
    again = run(forced_arguments(fixture), fixture)
    record("lost_receipt_never_replays", result.returncode != 0 and again.returncode != 0
           and launches(fixture) == 1 and keys_removed(fixture) and receipt(fixture) is None
           and (fixture["directory"] / "launch.claim").exists(), launches=launches(fixture),
           receiptPresent=receipt(fixture) is not None, claimPresent=True, replayExit=again.returncode)

    fixture = setup("child_failure", launch="#!/bin/sh\nexit 29\n")
    result = commit(fixture)
    again = run(forced_arguments(fixture), fixture)
    record("child_failure_needs_readiness", result.returncode == 0 and verify(fixture)
           and again.returncode != 0 and launches(fixture) == 0
           and (receipt(fixture) or {}).get("launchSubmitted") is True and keys_removed(fixture),
           signedCommit=True, launchReceiptIsNotReadiness=True, launches=launches(fixture),
           replayExit=again.returncode, receipt=receipt(fixture))

    fixture = setup("launch_snapshot", transform=lambda text: text.replace(claim_line,
                    claim_line + '        (pathlib.Path(plan["workspace"]) / "exec").write_text("#!/bin/sh\\nexit 27\\n")\n'))
    result = commit(fixture)
    settle(fixture)
    record("launch_bytes_frozen", result.returncode == 0 and launches(fixture) == 1 and keys_removed(fixture),
           launches=launches(fixture), changedFileExecuted=False, receipt=receipt(fixture))


def exact_marker_case():
    launcher = MARKER.read_text()
    fixture = setup("exact_marker_launcher", launch=launcher)
    result = commit(fixture)
    marker_file = fixture["directory"] / "marker.json"
    ready = wait_until(lambda: marker_file.exists() and marker_file.stat().st_size > 0, timeout=5)
    marker = json.loads(marker_file.read_text()) if ready else None
    expected = {"version": 1, "nonce": SESSION, "outcome": "verified", "gitCommitVerified": True,
                "gitCommitCount": 1, "configInactive": True, "configLockReleased": True, "keysRemoved": True}
    record("exact_live_marker_launcher", result.returncode == 0 and marker == expected
           and keys_removed(fixture) and config_restored(fixture) and verify(fixture),
           commitExit=result.returncode, marker=marker, launcherSha256=digest(launcher.encode()))


def diagnostic_cases():
    replacements = (
        ("missing_signer", 'command(["/usr/bin/ssh-keygen", "-q"', 'command(["/nonexistent/ghostprov-ssh-keygen", "-q"',
         "generate_key", "FileNotFoundError", 2),
        ("chmod_denied", 'os.fchmod(descriptor, 0o700)', 'raise PermissionError(1, "synthetic private detail")',
         "enable_hook", "PermissionError", 1),
        ("memfd_unavailable", 'os.memfd_create(name, os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)',
         'getattr(os, "synthetic_missing_private_attribute")()', "generate_key", "AttributeError", None),
    )
    for name, old, new, phase, exception, number in replacements:
        fixture = setup(name, transform=lambda text: text.replace(old, new))
        result = commit(fixture)
        detail = json.loads((fixture["directory"] / "failure.json").read_text())
        record("diagnostic_" + name, result.returncode != 0 and launches(fixture) == 0
               and keys_removed(fixture) and config_restored(fixture)
               and detail["phase"] == phase and detail["exception"] == exception and detail["errno"] == number
               and set(detail) == {"version", "sessionId", "phase", "exception", "errno", "line"},
               diagnostic=detail, launches=launches(fixture), keysRemoved=keys_removed(fixture))


try:
    standard_cases()
    race_case()
    rejection_cases()
    extended_rejections()
    interrupted_cases()
    handoff_tampering_cases()
    injected_failure_cases()
    exact_marker_case()
    for name, edit in (
        ("changed_signer", lambda f: (f["directory"] / "sshsig.mjs").write_bytes(SIGNER + b"// changed\n")),
        ("wrong_signer_hash", lambda f: f["plan"].update(signerSha256="a" * 64)),
        ("wrong_runtime_path", lambda f: f["plan"].update(denoPath="/arbitrary/deno")),
    ):
        fixture = setup(name)
        edit(fixture)
        (fixture["directory"] / "plan.json").write_text(json.dumps(fixture["plan"]))
        result = commit(fixture)
        record("reject_" + name, result.returncode != 0 and launches(fixture) == 0 and keys_removed(fixture))
    diagnostic_cases()
    report = {"candidate": "git-ssh-signing", "live": False, "sourceSha256": digest(SOURCE_BYTES),
              "signerSha256": digest(SIGNER), "runtimeVersion": runtime_version,
              "gitVersion": subprocess.run(["/usr/bin/git", "--version"], capture_output=True).stdout.decode().strip(),
              "passed": sum(case["passed"] for case in CASES), "failed": sum(not case["passed"] for case in CASES),
              "cases": CASES}
    DESTINATION.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "failed": report["failed"]}), flush=True)
    sys.exit(1 if report["failed"] else 0)
finally:
    assert ROOT.parent == pathlib.Path("/tmp") and ROOT.name.startswith("ghostprov-git-ssh-acceptance-")
    shutil.rmtree(ROOT)
