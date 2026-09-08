import { directSupervisorExecBody } from "../../src/prism/direct-exec";

/** A real signed Git commit is required; the process writes no model request. */
function markerLauncher(nonce: string): string {
  directSupervisorExecBody({ sessionId: nonce });
  return `#!/bin/sh
set -eu
exec /usr/bin/python3 - <<'GHOSTPROV_MARKER_PY'
import json,os,pathlib,subprocess,time
directory=pathlib.Path.cwd()/'.ghostprov-v2'/'ssh-bootstrap'
plan=json.loads((directory/'plan.json').read_bytes())
prepared=json.loads((directory/'prepared.json').read_bytes())
descriptor=os.open(directory/'marker.claim',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
os.close(descriptor)
record={'version':1,'nonce':'${nonce}','outcome':'verification_failed','gitCommitVerified':False,
        'gitCommitCount':0,'configInactive':False,'configLockReleased':False,'keysRemoved':False}
try:
    parent=prepared['parent']
    deadline=time.monotonic()+3
    while time.monotonic()<deadline:
        try:
            fields=pathlib.Path('/proc/'+str(parent['pid'])+'/stat').read_text().rsplit(')',1)[1].split()
            same=fields[19]==parent['startTicks'] and os.readlink('/proc/'+str(parent['pid'])+'/exe')=='/usr/bin/git'
        except (OSError,IndexError):
            same=False
        if not same: break
        time.sleep(0.01)
    else: raise RuntimeError('Git did not terminate')
    environment={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent',
                 'GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_SYSTEM':'/dev/null','GIT_CONFIG_NOSYSTEM':'1'}
    allowed=os.memfd_create('ghostprov-marker-signers',os.MFD_CLOEXEC)
    try:
        os.fchmod(allowed,0o600)
        os.write(allowed,('sandbox@example.com '+prepared['publicKey']+'\\n').encode())
        checked=subprocess.run(['/usr/bin/git','-C',plan['gitDirectory'],'-c','gpg.format=ssh',
            '-c','gpg.ssh.program=/usr/bin/ssh-keygen','-c','gpg.ssh.allowedSignersFile=/proc/self/fd/'+str(allowed),
            'verify-commit','HEAD'],env=environment,pass_fds=(allowed,),capture_output=True,timeout=2)
        record['gitCommitVerified']=checked.returncode==0 and len(checked.stdout)+len(checked.stderr)<8192
    finally: os.close(allowed)
    count=subprocess.run(['/usr/bin/git','-C',plan['gitDirectory'],'rev-list','--count','HEAD'],
                         env=environment,capture_output=True,timeout=2)
    if count.returncode==0 and len(count.stdout)<16 and count.stdout.strip().isdigit():
        record['gitCommitCount']=int(count.stdout.strip())
    config=pathlib.Path(plan['gitConfig'])
    record['configInactive']=not os.path.lexists(config) or (config.is_file() and not config.is_symlink()
        and config.stat().st_size<=8192 and str(directory).encode() not in config.read_bytes())
    record['configLockReleased']=not os.path.lexists(plan['configLock'])
    record['keysRemoved']=all(not os.path.lexists(directory/name) for name in ('ephemeral-key','ephemeral-key.pub'))
    if all(record[key] for key in ('gitCommitVerified','configInactive','configLockReleased','keysRemoved')) and record['gitCommitCount']==1:
        record['outcome']='verified'
except Exception:
    record['outcome']='local_failure'
descriptor=os.open(directory/'marker.json',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
try:
    os.write(descriptor,json.dumps(record,separators=(',',':'),sort_keys=True).encode())
    os.fsync(descriptor)
finally: os.close(descriptor)
GHOSTPROV_MARKER_PY
`;
}

export function checkMarker(value: unknown, nonce: string): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const raw = value as Record<string, unknown>;
  const names = ["version", "nonce", "outcome", "gitCommitVerified", "gitCommitCount", "configInactive", "configLockReleased", "keysRemoved"];
  return Object.keys(raw).sort().join() === names.sort().join() && raw.version === 1 && raw.nonce === nonce &&
    raw.outcome === "verified" && raw.gitCommitVerified === true && raw.gitCommitCount === 1 &&
    raw.configInactive === true && raw.configLockReleased === true && raw.keysRemoved === true;
}


export function gitSshSigningMarkerLauncher(nonce: string): string {
  const original = markerLauncher(nonce);
  const start = original.indexOf("        checked=subprocess.run(");
  const end = original.indexOf("    finally: os.close(allowed)", start);
  if (start < 0 || end < 0) throw new Error("Unknown marker structure");
  return original.slice(0, start) + `        # Read the committed object independently of the hook receipt.
        checked=subprocess.run(['/usr/bin/git','-C',plan['gitDirectory'],'cat-file','commit','HEAD'],
                               env=environment,capture_output=True,timeout=2)
        if checked.returncode!=0 or len(checked.stdout)>65536: raise RuntimeError('Missing bounded commit')
        unsigned=bytearray()
        signature=bytearray()
        in_signature=False
        in_headers=True
        found=False
        for line in checked.stdout.splitlines(keepends=True):
            if in_headers and line.startswith(b'gpgsig '):
                if found: raise RuntimeError('Duplicate signature')
                found=True
                in_signature=True
                signature.extend(line[7:])
            elif in_signature and line.startswith(b' '): signature.extend(line[1:])
            else:
                in_signature=False
                unsigned.extend(line)
            if line==b'\\n': in_headers=False
        if not found: raise RuntimeError('Unsigned commit')
        import importlib.util
        spec=importlib.util.spec_from_file_location('owned_marker_hook',directory/'hook.py')
        module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        signature_fd=module.sealed_memory('ghostprov-marker-signature',bytes(signature))
        try:
            os.chdir(plan['gitDirectory'])
            verified=module.webcrypto_command(['/usr/bin/ssh-keygen','-Y','verify','-I','sandbox@example.com','-n','git',
                '-f','/proc/self/fd/'+str(allowed),'-s','/proc/self/fd/'+str(signature_fd)],
                input=bytes(unsigned),pass_fds=(allowed,signature_fd))
            record['gitCommitVerified']=verified==b'verified\\n'
        finally: os.close(signature_fd)
` + original.slice(end);
}
