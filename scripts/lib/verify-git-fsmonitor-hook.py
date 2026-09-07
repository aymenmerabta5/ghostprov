"""Offline acceptance tests for the candidate using real Linux processes."""
import concurrent.futures
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time

root = pathlib.Path(sys.argv[1]).resolve()
source = pathlib.Path(sys.argv[2]).resolve()
if root.parent.name != "git-fsmonitor-offline" or not root.name.startswith("run-"):
    raise RuntimeError("Unexpected test root")
env = {"PATH": "/usr/bin:/bin", "LANG": "C", "HOME": str(root / "home"),
       "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
cases = []
session = "11111111-1111-4111-8111-111111111111"
original = b"[user]\n\tname = Offline Preserved Name\n[include]\n\tpath = relative-config\n"

def record(case_id, criterion, passed, evidence):
    cases.append({"id": case_id, "criterion": criterion, "scope": "real_linux_owned_hook", "passed": passed, "evidence": evidence})

def prepare(name, present=True, expired=False):
    base = root / name
    workspace = base / "codex_workspace" / session
    directory = workspace / ".ghostprov-v2" / "git-bootstrap"
    directory.mkdir(parents=True)
    home = base / "sandbox-home"
    home.mkdir()
    config = home / ".gitconfig"
    lock = home / ".gitconfig.lock"
    candidate_lock = directory / "config-lock"
    candidate_lock.mkdir()
    (candidate_lock / "owner").write_text(session + "\n")
    candidate_lock.rename(lock)
    if present:
        config.write_bytes(original)
        config.rename(directory / "original.gitconfig")
    else:
        (directory / "config-was-absent").mkdir()
    driver = directory / "hook.py"
    shutil.copyfile(source, driver)
    replacement = f'[core]\n\tfsmonitor = "/usr/bin/python3 {driver}"\n'.encode()
    config.write_bytes(replacement)
    plan = {"sessionId": session, "workspace": str(workspace), "gitConfig": str(config), "configLock": str(lock),
            "configSha256": hashlib.sha256(replacement).hexdigest(), "expiresAtSeconds": 1 if expired else int(time.time()) + 90}
    (directory / "plan.json").write_text(json.dumps(plan))
    checker = "import pathlib,hashlib\np=pathlib.Path(" + repr(str(config)) + ")\n"
    checker += "ok=" + ("p.exists() and hashlib.sha256(p.read_bytes()).hexdigest()==" + repr(hashlib.sha256(original).hexdigest()) if present else "not p.exists()") + "\n"
    checker += "pathlib.Path('restored-before-launch').write_text(str(ok))\nwith open('launches','a') as f: f.write('launch\\n')\n"
    (workspace / "counter.py").write_text(checker)
    (workspace / "exec").write_text("#!/bin/sh\nexec /usr/bin/python3 counter.py\n")
    return {"base": base, "workspace": workspace, "directory": directory, "config": config, "lock": lock, "driver": driver}

def invoke(fixture):
    result = subprocess.run(["/usr/bin/python3", str(fixture["driver"]), "2", "1"], cwd=fixture["workspace"], env=env,
                            capture_output=True, timeout=5)
    if len(result.stdout) + len(result.stderr) > 16384:
        raise RuntimeError("Output bound exceeded")
    return result

def launches(fixture):
    filename = fixture["workspace"] / "launches"
    return len(filename.read_text().splitlines()) if filename.exists() else 0

def settle(fixture):
    deadline = time.monotonic() + 3
    result = receipt(fixture)
    while result.get("launchSubmitted") and launches(fixture) == 0 and time.monotonic() < deadline:
        time.sleep(0.02)
    time.sleep(0.05)

def receipt(fixture):
    return json.loads((fixture["directory"] / "result.json").read_text())

for present in (True, False):
    fixture = prepare("present" if present else "absent", present)
    result = invoke(fixture)
    settle(fixture)
    restored = fixture["config"].read_bytes() == original if present else not fixture["config"].exists()
    record("restore_existing" if present else "restore_absence", "Original configuration state is restored before launch",
           restored and not fixture["lock"].exists() and launches(fixture) == 1 and
           (fixture["workspace"] / "restored-before-launch").read_text() == "True" and result.returncode == 0,
           {"originalRestored": restored, "lockReleased": not fixture["lock"].exists(), "launches": launches(fixture), "receipt": receipt(fixture)})

parallel = prepare("parallel")
started = time.monotonic()
with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
    results = list(pool.map(lambda _: invoke(parallel), range(32)))
settle(parallel)
record("parallel_once", "32 concurrent hooks restore once and submit one launch", launches(parallel) == 1 and
       parallel["config"].read_bytes() == original and all(r.returncode == 0 and r.stdout == b"1\0" for r in results),
       {"invocations": 32, "launches": launches(parallel), "elapsedMs": round((time.monotonic() - started) * 1000)})
foreign = b"[user]\n\tname = Offline New Writer\n"
parallel["config"].write_bytes(foreign)
for _ in range(8):
    invoke(parallel)
record("later_calls_preserve_foreign", "Later hook calls preserve new config and do not replay", launches(parallel) == 1 and parallel["config"].read_bytes() == foreign,
       {"totalInvocations": 40, "launches": launches(parallel), "foreignPreserved": parallel["config"].read_bytes() == foreign})

conflict = prepare("foreign-config")
conflict["config"].write_bytes(foreign)
invoke(conflict)
record("foreign_config_fail_closed", "A foreign config edit is preserved and prevents launch", receipt(conflict)["outcome"] == "foreign_config" and
       conflict["config"].read_bytes() == foreign and launches(conflict) == 0 and (conflict["directory"] / "original.gitconfig").read_bytes() == original,
       {"outcome": receipt(conflict)["outcome"], "foreignPreserved": conflict["config"].read_bytes() == foreign, "originalBackupPreserved": True, "launches": launches(conflict)})

for name in ("cancelled", "expired"):
    fixture = prepare(name, expired=name == "expired")
    if name == "cancelled":
        (fixture["directory"] / "cancelled").write_text("cancelled\n")
    invoke(fixture)
    record(name, "A cancelled or expired hook restores configuration without launching", receipt(fixture)["outcome"] == name and
           fixture["config"].read_bytes() == original and not fixture["lock"].exists() and launches(fixture) == 0,
           {"outcome": receipt(fixture)["outcome"], "launches": launches(fixture), "configRestored": fixture["config"].read_bytes() == original,
            "lockReleased": not fixture["lock"].exists()})

lock_conflict = prepare("foreign-lock")
(lock_conflict["lock"] / "owner").write_text("foreign\n")
invoke(lock_conflict)
record("foreign_lock_fail_closed", "Foreign lock ownership is preserved and prevents launch", receipt(lock_conflict)["outcome"] == "foreign_lock" and
       (lock_conflict["lock"] / "owner").read_text() == "foreign\n" and launches(lock_conflict) == 0,
       {"outcome": receipt(lock_conflict)["outcome"], "launches": launches(lock_conflict)})

symlink = prepare("symlink")
target = symlink["base"] / "unrelated-file"
target.write_bytes(foreign)
symlink["config"].unlink()
symlink["config"].symlink_to(target)
invoke(symlink)
record("config_symlink_rejected", "A substituted symlink is not followed or modified", receipt(symlink)["outcome"] == "local_failure" and
       target.read_bytes() == foreign and symlink["config"].is_symlink() and launches(symlink) == 0,
       {"outcome": receipt(symlink)["outcome"], "targetUnchanged": target.read_bytes() == foreign, "launches": launches(symlink)})

failed = prepare("failed-child")
(failed["workspace"] / "exec").write_text("#!/bin/sh\nprintf 'launch\\n' >> launches\nexit 23\n")
invoke(failed)
settle(failed)
invoke(failed)
record("failed_child_no_replay", "An unsuccessful child leaves its no-replay claim", launches(failed) == 1 and (failed["directory"] / "launch.claim").exists(),
       {"launches": launches(failed), "claimRetained": (failed["directory"] / "launch.claim").exists()})

atomic = root / "atomic-lock"
atomic.mkdir()
target_lock = atomic / ".gitconfig.lock"
one, two = atomic / "one", atomic / "two"
one.mkdir(); two.mkdir()
(one / "owner").write_text("one"); (two / "owner").write_text("two")
one.rename(target_lock)
rejected = False
try:
    two.rename(target_lock)
except OSError:
    rejected = True
record("atomic_lock_collision", "A second nonempty lock cannot replace the original lock", rejected and (target_lock / "owner").read_text() == "one",
       {"secondRejected": rejected, "firstOwnerPreserved": (target_lock / "owner").read_text() == "one"})

gitcase = prepare("actual-git")
repo = gitcase["base"] / "repository"
repo.mkdir()
subprocess.run(["git", "init", "--quiet"], cwd=repo, env=env, check=True, capture_output=True, timeout=5)
git_env = {**env, "GIT_CONFIG_GLOBAL": str(gitcase["config"])}
results = [subprocess.run(["git", "status", "--porcelain"], cwd=repo, env=git_env, capture_output=True, timeout=5) for _ in range(3)]
settle(gitcase)
record("git_trigger_and_restore", "Actual Git triggers the hook, then reads restored configuration", all(r.returncode == 0 for r in results) and
       launches(gitcase) == 1 and gitcase["config"].read_bytes() == original,
       {"gitCalls": 3, "launches": launches(gitcase), "allGitCallsSucceeded": all(r.returncode == 0 for r in results)})
print(json.dumps({"cases": cases}))
