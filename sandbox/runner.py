"""Run one file-backed Responses API job.

By default this preserves the legacy job.json -> response.json contract.  A
caller may pass distinct paths to run multiple agents concurrently:

    python3 runner.py job_s1.json resp_s1.json
    python3 runner.py job_s2.json resp_s2.json

Each response path has its own lock and completion state, so independent paths
run in parallel while duplicate launches for the same path are coalesced.
"""

import fcntl
import hashlib
import json
import os
import pathlib
import ssl
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request

WINDOW = 64 * 1024
PROGRESS_EVERY = 2048
FINAL_FLUSH_GRACE = 0.1
MAX_CATCH_UP_JOBS = 32


def _atomic_write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        tmp.write_text(json.dumps(value))
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _sidecar_path(response_path, suffix):
    return response_path.with_name(f".{response_path.name}.{suffix}")


def _pidfile_guard(response_path):
    """Serialize runners that target the same response file."""
    pidfile = _sidecar_path(response_path, "runner.pid")
    pidfile.parent.mkdir(parents=True, exist_ok=True)
    handle = pidfile.open("a+")
    # This lock is intentionally blocking. A concurrent latexmk run can clear
    # response.json while the first runner is finishing; the waiter must then
    # re-check completion state instead of exiting and leaving no response.
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


def _read_job_snapshot(job_path):
    for _ in range(3):
        try:
            before = job_path.stat()
            text = job_path.read_text()
            after = job_path.stat()
        except (FileNotFoundError, OSError):
            return None
        before_sig = (before.st_mtime_ns, before.st_size)
        after_sig = (after.st_mtime_ns, after.st_size)
        if before_sig == after_sig:
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            return {
                "mtime_ns": after.st_mtime_ns,
                "size": after.st_size,
                "sha256": digest,
            }, text
        time.sleep(0.01)
    return None


def _read_state(state_path):
    try:
        value = json.loads(state_path.read_text())
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def write_progress(response_path, ack, total, start, tail, **extra):
    value = {
        "streaming": True,
        "ack": ack,
        "total": total,
        "start": start,
        "tail": tail,
    }
    value.update(extra)
    _atomic_write_json(response_path, value)


def write_failed(response_path, ack, code, message):
    _atomic_write_json(response_path, {
        "status": "failed",
        "ack": ack,
        "error": {"code": code, "message": str(message)[:2000]},
    })


def _retry_delay(header):
    try:
        return min(max(float(header), 0.0), 45.0)
    except (TypeError, ValueError):
        return 20.0


def run_job(job_text, response_path):
    ack = hashlib.sha256(job_text.encode("utf-8")).hexdigest()[:12]
    try:
        body = json.loads(job_text)
    except Exception as ex:
        write_failed(response_path, ack, "bad_job_json", repr(ex)[:500])
        return
    if not isinstance(body, dict):
        write_failed(response_path, ack, "bad_job_json", "job body is not an object")
        return

    base = os.environ["OPENAI_BASE_URL"].rstrip("/")
    key = os.environ["OPENAI_API_KEY"]
    ca = os.environ.get("PROXY_CA_CERT_PATH")
    context = ssl.create_default_context(cafile=ca) if ca else None
    body.setdefault("store", False)
    body.setdefault("service_tier", "fast")
    stream = bool(body.get("stream"))
    headers = {
        "Authorization": "Bearer " + key,
        "Content-Type": "application/json",
    }
    if stream:
        headers["Accept"] = "text/event-stream"

    request = urllib.request.Request(
        base + "/responses",
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    try:
        response = urllib.request.urlopen(request, context=context)
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", "replace")[:1500]
        print("HTTPError", error.code, error_body[:300], flush=True)
        if error.code != 429:
            write_failed(response_path, ack, "http_" + str(error.code), error_body)
            return
        wait_s = _retry_delay(error.headers.get("retry-after") if error.headers else None)
        print("429 rate-limited; sleeping", wait_s, "then retrying once", flush=True)
        time.sleep(wait_s)
        retry = urllib.request.Request(
            base + "/responses",
            data=json.dumps(body).encode(),
            headers=headers,
            method="POST",
        )
        try:
            response = urllib.request.urlopen(retry, context=context)
        except urllib.error.HTTPError as retry_error:
            retry_body = retry_error.read().decode("utf-8", "replace")[:1500]
            write_failed(
                response_path,
                ack,
                "rate_limited_" + str(retry_error.code),
                retry_body,
            )
            return
        except Exception as ex:
            write_failed(response_path, ack, "retry_error", repr(ex)[:500])
            return
    except urllib.error.URLError as error:
        write_failed(response_path, ack, "connection_error", str(error.reason))
        return

    with response:
        if not stream:
            _atomic_write_json(response_path, json.load(response))
            return

        chunks = []
        total = 0
        last_progress = 0
        while True:
            line = response.readline()
            if not line:
                break
            text = line.decode("utf-8", "replace")
            chunks.append(text)
            total += len(text)
            if total - last_progress >= PROGRESS_EVERY:
                full = "".join(chunks)
                start = max(0, total - WINDOW)
                write_progress(response_path, ack, total, start, full[start:])
                last_progress = total

        full = "".join(chunks)
        final = None
        for event in full.split("\n\n"):
            for line in event.split("\n"):
                if line.startswith("data:") and "response.completed" in line:
                    try:
                        final = json.loads(line[5:].strip()).get("response")
                    except Exception:
                        pass
        if final is None:
            write_failed(
                response_path,
                ack,
                "stream_truncated",
                f"connection ended before response.completed; received {len(full)} bytes",
            )
            return
        start = max(0, total - WINDOW)
        write_progress(
            response_path,
            ack,
            total,
            start,
            full[start:],
            phase="final_flush",
            complete=True,
        )
        time.sleep(FINAL_FLUSH_GRACE)
        # Preserve the legacy top-level Responses object while exposing the
        # authoritative transcript to bridges that need to flush missed deltas.
        terminal = dict(final)
        terminal["gp_sse"] = full
        terminal["gp_ack"] = ack
        terminal["gp_total"] = total
        _atomic_write_json(response_path, terminal)


def _configured_paths(argv):
    if len(argv) > 2:
        raise SystemExit("usage: runner.py [JOB_PATH [RESPONSE_PATH]]")
    job_path = pathlib.Path(
        os.environ.get("RUNNER_JOB_PATH", argv[0] if argv else "job.json")
    )
    response_path = pathlib.Path(
        os.environ.get(
            "RUNNER_RESPONSE_PATH",
            argv[1] if len(argv) == 2 else "response.json",
        )
    )
    return job_path, response_path


def main(argv=None):
    job_path, response_path = _configured_paths(
        sys.argv[1:] if argv is None else argv
    )
    guard = _pidfile_guard(response_path)

    state_path = _sidecar_path(response_path, "runner-state")
    for _ in range(MAX_CATCH_UP_JOBS):
        snapshot = _read_job_snapshot(job_path)
        if snapshot is None:
            write_failed(response_path, "", "no_job", f"{job_path} missing or unreadable")
            return
        signature, job_text = snapshot
        state = _read_state(state_path)
        if state.get("signature") == signature and response_path.exists():
            return

        try:
            run_job(job_text, response_path)
        except Exception as ex:
            print("RUNNER EXCEPTION", flush=True)
            traceback.print_exc()
            write_failed(response_path, signature["sha256"][:12], "runner_exception", repr(ex)[:500])
        _atomic_write_json(state_path, {
            "signature": signature,
            "completed_at": time.time(),
        })

        latest = _read_job_snapshot(job_path)
        if latest is None or latest[0] == signature:
            return

    write_failed(response_path, "", "job_churn", "job changed too often while runner was active")


if __name__ == "__main__":
    main()
